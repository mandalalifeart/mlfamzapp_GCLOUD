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

from flask import Flask, request

import main as functions_module

PORT = int(os.environ.get("LOCAL_SERVER_PORT", "8092"))

# Only the functions this migration covers - pure PocketBase (+ SP-API for
# the report request/update trio) reads/writes triggered by the /sales,
# /next-order, /update, /batch-update, and /product frontend pages.
ROUTED_FUNCTIONS = [
    "AssignSkuGroup",
    "GetMarketplaceSalesSummary",
    "GetNextOrderData",
    "GetProductDetail",
    "GetSalesDepartmentReport",
    "MlfReportGet",
    "MlfReportReq",
    "UpdateNextOrderField",
    "UpdateSkuSalesMonth",
]

app = Flask(__name__)


def make_view(function_name):
    handler = getattr(functions_module, function_name)

    def view():
        return handler(request)

    view.__name__ = function_name
    return view


for name in ROUTED_FUNCTIONS:
    app.add_url_rule(f"/{name}", endpoint=name, view_func=make_view(name), methods=["GET", "POST", "OPTIONS"])


@app.route("/healthz")
def healthz():
    return {"ok": True, "routes": ROUTED_FUNCTIONS}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
