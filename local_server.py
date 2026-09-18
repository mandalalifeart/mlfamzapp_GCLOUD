"""Always-on local server for the small set of Cloud Functions that only
talk to PocketBase (already hosted on this same machine) and don't need
GCP's paid-report polling - moved here 2026-08-28 to cut Cloud Run compute
cost and the round trip through the Cloudflare tunnel for interactive
frontend calls. See CLAUDE.md "AmzBot: local job runner" for the reasoning.

Routes by path exactly like GCP does (path = function name), dispatching to
the same handlers already used by main.py - so behavior is identical to the
GCP-hosted versions; this is just a different place to run the same code.

Run with: .venv/bin/python3 local_server.py
"""
import os
import threading

from flask import Flask, request

import main as functions_module

PORT = int(os.environ.get("LOCAL_SERVER_PORT", "8092"))

# Cloudflare's edge enforces a hard ~100s idle-response timeout on any
# request proxied through a Tunnel (surfaces to the caller as HTTP 524) on
# this plan - there is no cloudflared/tunnel-config setting that raises it
# (that's an edge-side limit, not an origin-connection setting; only
# Cloudflare Enterprise's "Proxy Read Timeout" origin rule can change it).
# Found live 2026-09-06: a manual multi-year Etsy backfill call through
# https://amzapi.mandalalifeart.com genuinely succeeded server-side (the
# same call against localhost:8092 directly returned fine) but the public
# tunnel call itself came back as a 524 partway through. Rather than push
# every slow admin/backfill call to always route around the tunnel by hand,
# every routed view now acks early if a call runs past this deadline - the
# real handler keeps running to completion in its own thread regardless
# (it already writes its results straight to PocketBase as it goes, not
# just in the final JSON response), so the actual work isn't lost, only the
# synchronous response is skipped for calls slow enough to risk a 524.
ACK_DEADLINE_SECONDS = float(os.environ.get("LOCAL_SERVER_ACK_DEADLINE_SECONDS", "85"))


class RequestSnapshot:
    """Captures exactly the 3 attributes any handler in main.py ever
    touches (.method/.args/.get_json()) from the real Flask request before
    handing off to a background thread - Flask's `request` is a context-
    local proxy that stops working once the view function that received it
    returns, so the background thread can't just keep using the original."""

    def __init__(self, req):
        self.method = req.method
        self.args = req.args
        self._json = req.get_json(silent=True)

    def get_json(self, silent=True):
        return self._json


# Originally just the pure-PocketBase (+ SP-API report request/update trio)
# functions for /sales, /next-order, /update, /batch-update, /product.
# Extended 2026-08-31 to cover everything except the 3 OAuth
# start/callback endpoints (AdsOAuthCallback, EtsyOAuthStart,
# EtsyOAuthCallback), which stay on GCP because their URLs are registered
# as fixed redirect URIs in Amazon's/Etsy's own app dashboards - moving
# them needs a manual dashboard update, not just a routing change here.
# MlfReportGet/MlfReportReq moved back to GCP 2026-09-16 (SP-API-only, no
# PocketBase dependency, negligible GCP cost, and shouldn't depend on this
# mini PC's uptime) - see CLAUDE.md "AmzBot: local job runner".
ROUTED_FUNCTIONS = [
    "AssignSkuGroup",
    "GetMarketplaceSalesSummary",
    "GetNextOrderData",
    "GetProductDetail",
    "GetSalesDepartmentReport",
    "UpdateNextOrderField",
    "UpdateSkuSalesMonth",
    "GetUkInventory",
    "GetDeInventory",
    "GetAwdInventory",
    "UpdateUsaInventory",
    "GetAdsConnectionStatus",
    "UpdateAdsCampaignStats",
    "GetAdsAccountSummary",
    "GetAdsCampaignStats",
    "GetAdsPortfolios",
    "UpdateAdsKeywordStats",
    "GetAdsKeywordStats",
    "UpdateAdsSearchTermStats",
    "GetAdsSearchTermStats",
    "GetEtsyConnectionStatus",
    "UpdateEtsyListings",
    "GetEtsyListings",
    "GetEtsyListingDetail",
    "UpdateEtsyListingContent",
    "MonitorEtsyExperiments",
    "StartEtsyExperiment",
    "GetAmazonListingItem",
    "DeleteAmazonListingItem",
    "ProcessAmazonRelistQueue",
    "ProbeAdsReportColumns",
    "MarkEtsyOrderInProgress",
    "DiagnoseEtsyOrders",
    "UpdateEtsyOrders",
    "GetEtsyOrders",
    "RunEtsyMcfFulfillment",
    "RunEtsyMcfFulfillmentWet",
    "CheckMcfAccess",
    "GetMcfFulfillmentPreview",
    "GetMcfOrderById",
    "PatchAmazonListingAttribute",
    "ProbeEuSellerId",
    "AuditAmazonListings",
    "GetScheduledJobs",
    "UpdateScheduledJob",
    "GetJobRunsLog",
    "GetPocketBaseCollections",
    "CreateMcfOrderForReceipt",
    "UpdateEtsyTrackingFromAmazon",
    "RunBidOptimizerDryRun",
    "ApplyBidChange",
    "DisableBidTarget",
    "PauseProductAd",
    "EnableProductAd",
    "GetBidRuleProfiles",
    "SaveBidRuleProfile",
    "DeleteBidRuleProfile",
    "GetBidChangePerformance",
    "GetBidChangeLog",
    "UpdateAdsAdvertisedProductStats",
    "GetAdsAdvertisedProductStats",
    "UpdateCountryPpcDaily",
    "GetCountryPpcDaily",
    "SendDailyAdsDigest",
    "SendWeeklyReturnRateDigest",
    "RunWeeklyUsaInventorySync",
    "UpdateAmazonFinances",
    "GetPaymentsExpensesSummary",
    "UpdateWiseFinances",
    "GetWiseFinancesSummary",
    "UpdateAmazonReturns",
    "GetReturnStats",
    "GetReturnStatsByMonth",
    "GetReturnStatsByAsin",
]

app = Flask(__name__)


def make_view(function_name):
    handler = getattr(functions_module, function_name)

    def view():
        snapshot = RequestSnapshot(request)
        outcome = {}
        finished = threading.Event()

        def run():
            try:
                outcome["result"] = handler(snapshot)
            except Exception as exc:
                outcome["exception"] = exc
            finally:
                finished.set()

        thread = threading.Thread(target=run, name=f"{function_name}-bg", daemon=True)
        thread.start()
        finished.wait(ACK_DEADLINE_SECONDS)

        if finished.is_set():
            if "exception" in outcome:
                raise outcome["exception"]
            return outcome["result"]

        # Still running past the safety deadline - ack now so Cloudflare's
        # edge doesn't 524 the caller; `thread` (daemon) keeps running to
        # completion on its own and finishes writing to PocketBase normally.
        return {
            "acceptedInBackground": True,
            "note": (
                f"{function_name} is still running past {ACK_DEADLINE_SECONDS:.0f}s "
                "(likely to avoid a Cloudflare tunnel 524) - it keeps running in the "
                "background and will finish writing its results normally; check the "
                "relevant PocketBase collection directly rather than waiting on this "
                "response, or call it again against http://localhost:8092 to avoid "
                "the tunnel's timeout entirely."
            ),
        }, 202

    view.__name__ = function_name
    return view


for name in ROUTED_FUNCTIONS:
    app.add_url_rule(f"/{name}", endpoint=name, view_func=make_view(name), methods=["GET", "POST", "OPTIONS"])


@app.route("/healthz")
def healthz():
    return {"ok": True, "routes": ROUTED_FUNCTIONS}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, threaded=True)
