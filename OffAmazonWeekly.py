"""Weekly off-Amazon check (2026-10-20, per the user: "check off amazon
performance every monday and alert in case we are loosing money there").

Sponsored Products also serve OFF Amazon (third-party sites/apps) and every
campaign is opted in by default - found in the T8 placement report: US
20 Sep-19 Oct = $2,301 (34% of US SP spend) at 103% ACOS. The off-Amazon
part can't be switched off, only limited per campaign via
offAmazonSettings.offAmazonBudgetControlStrategy = MINIMIZE_SPEND.

Pulls the SP placement report (spCampaigns grouped by campaign +
campaignPlacement, SUMMARY) for last Mon-Sun for every seller profile with
enabled SP campaigns, and flags each campaign whose off-Amazon spend >= 10
(x11 SEK, x4.3 PLN) with off-Amazon ACOS >= 50% or no sales. Sends one
Telegram to @baba_update_bot every run; saved on /ads-reviews.
Args: dry=1 (no Telegram), today=YYYY-MM-DD.
"""
import json
import os
import re
import time
from collections import defaultdict
from datetime import date, timedelta

import requests

import AdsReporting as A
from AdsAuth import AD_PROFILES, ADS_REGION_ENDPOINTS
from AdsReviews import save_review
from NotificationRouting import notify

ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app")
JOB_NAME = "off-amazon-weekly"
CUR = {"US": "$", "CA": "C$", "MX": "MX$", "UK": "£", "GB": "£", "DE": "€", "FR": "€", "IT": "€", "ES": "€",
       "NL": "€", "BE": "€", "IE": "€", "SE": "kr ", "PL": "zł "}
MONEY_SCALE = {"SE": 11, "PL": 4.3}
MIN_OFF_SPEND = 10
MAX_OFF_ACOS = 50
COLUMNS = ["campaignId", "campaignName", "placementClassification", "impressions",
           "clicks", "cost", "purchases7d", "sales7d"]
OFF = "Off Amazon"
# Creator storefront clicks count as on-Amazon SP from 2026-11-11 and show as
# "Other on-Amazon" in the placement report; the off-Amazon "Minimize spend"
# setting does not limit them (per the user 2026-11-04: watch it weekly).
OTHER = "Other on-Amazon"


def _json(body, status=200):
    return json.dumps(body), status, {"Access-Control-Allow-Origin": ALLOWED_ORIGIN, "Content-Type": "application/json"}


def seller_profiles(pb):
    """Profile ids that own enabled SP campaigns (ads_campaigns snapshot)."""
    rows, page = [], 1
    while True:
        resp = requests.get(f"{A.POCKETBASE_URL}/api/collections/ads_campaigns/records", headers={"Authorization": pb},
                            params={"perPage": 500, "page": page, "fields": "profile_id",
                                    "filter": 'ad_product = "SPONSORED_PRODUCTS" && campaign_status = "ENABLED"'},
                            timeout=30)
        resp.raise_for_status()
        data = resp.json()
        rows += data["items"]
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return {r["profile_id"] for r in rows if r.get("profile_id", "").isdigit()}


def request_report(base, headers, start, end):
    for attempt in range(6):
        r = requests.post(f"{base}/reporting/reports", timeout=30, headers={
            **headers, "Content-Type": "application/vnd.createasyncreportrequest.v3+json"}, json={
            "name": f"off-amazon weekly {start} {end}", "startDate": start, "endDate": end,
            "configuration": {"adProduct": "SPONSORED_PRODUCTS", "groupBy": ["campaign", "campaignPlacement"],
                              "columns": COLUMNS, "reportTypeId": "spCampaigns",
                              "timeUnit": "SUMMARY", "format": "GZIP_JSON"}})
        if r.status_code != 429:
            break
        time.sleep(5 * (attempt + 1))
    if r.status_code == 425:
        m = re.search(r"duplicate of\s*:\s*([\w-]+)", r.text)
        if m:
            return m.group(1)
    if r.status_code not in (200, 202):
        raise RuntimeError(f"HTTP {r.status_code} {r.text[:300]}")
    return r.json()["reportId"]


def off_amazon_settings(base, headers):
    out, nt = {}, None
    while True:
        body = {"stateFilter": {"include": ["ENABLED", "PAUSED"]}, "maxResults": 100}
        if nt:
            body["nextToken"] = nt
        r = requests.post(f"{base}/sp/campaigns/list", json=body, timeout=60, headers={
            **headers, "Content-Type": "application/vnd.spCampaign.v3+json", "Accept": "application/vnd.spCampaign.v3+json"})
        r.raise_for_status()
        j = r.json()
        for c in j.get("campaigns", []):
            strategy = (c.get("offAmazonSettings") or {}).get("offAmazonBudgetControlStrategy") or "default"
            out[str(c["campaignId"])] = "PAUSED" if c.get("state") == "PAUSED" else strategy
        nt = j.get("nextToken")
        if not nt:
            return out


def pull(start, end, errors):
    """{country: {"rows": [...], "settings": {campaign_id: strategy}}}, one
    profile per campaign (the same campaign can sit under two profiles)."""
    pb = A.pb_authenticate()
    wanted = seller_profiles(pb)
    conn = [c for c in A.pb_list_connected(pb) if c.get("region") == "USA"][0]
    token = A.refresh_access_token("USA", conn["refresh_token"])
    client_id = AD_PROFILES["USA"]["client_id"]
    jobs = []
    for prof in conn["profiles"]:
        pid = str(prof["profileId"])
        if pid not in wanted or prof.get("accountType") != "seller":
            continue
        base = ADS_REGION_ENDPOINTS[prof["region"]]
        headers = {"Authorization": f"Bearer {token}", "Amazon-Advertising-API-ClientId": client_id,
                   "Amazon-Advertising-API-Scope": pid}
        try:
            settings = off_amazon_settings(base, headers)
            rid = request_report(base, headers, start, end)
            jobs.append({"cc": prof["countryCode"], "pid": pid, "base_url": base, "headers": headers,
                         "report_id": rid, "settings": settings})
        except Exception as exc:
            errors.append(f"{prof['countryCode']}/{pid}: {exc}")
        time.sleep(2)
    pending = list(jobs)
    for _ in range(360):  # report generation can take 30+ min
        for job in list(pending):
            try:
                st, url = A.check_report_status(job)
            except Exception as exc:
                errors.append(f"{job['cc']}/{job['pid']}: {exc}")
                pending.remove(job)
                continue
            if st == "done":
                job["rows"] = A.download_report_rows(url)
                pending.remove(job)
        if not pending:
            break
        time.sleep(10)
    for job in pending:
        errors.append(f"{job['cc']}/{job['pid']}: report not ready in time")
    out, seen = defaultdict(lambda: {"rows": [], "settings": {}}), set()
    for job in jobs:
        rows = job.get("rows") or []
        ids = {str(r["campaignId"]) for r in rows} - seen
        seen |= ids
        out[job["cc"]]["rows"] += [r for r in rows if str(r["campaignId"]) in ids]
        out[job["cc"]]["settings"].update(job["settings"])
    return out


def _acos(spend, sales):
    return spend / sales * 100 if sales else None


def build(today=None):
    today = today or date.today()
    end = today - timedelta(days=today.weekday() + 1)  # last Sunday
    start = end - timedelta(days=6)
    errors = []
    data = pull(start.isoformat(), end.isoformat(), errors)
    lines = [f"🌐 Off-Amazon check · Sponsored Products · {start:%d %b}-{end:%d %b}",
             f"Alert = campaign with off-Amazon spend >= {MIN_OFF_SPEND} and ACOS >= {MAX_OFF_ACOS}% or no sales.", ""]
    flags, other_flags, other_lines, summary = [], [], [], {}
    for cc in sorted(data, key=lambda c: (c != "US", c)):
        rows, settings = data[cc]["rows"], data[cc]["settings"]
        cur, scale = CUR.get(cc, ""), MONEY_SCALE.get(cc, 1)
        on = [0.0, 0.0]
        off = [0.0, 0.0, 0, 0]
        oth = [0.0, 0.0, 0]
        for r in rows:
            spend, sales = r.get("cost") or 0, r.get("sales7d") or 0
            if r["placementClassification"] == OFF:
                off = [off[0] + spend, off[1] + sales, off[2] + (r.get("purchases7d") or 0), off[3] + (r.get("clicks") or 0)]
                acos = _acos(spend, sales)
                if spend >= MIN_OFF_SPEND * scale and (acos is None or acos >= MAX_OFF_ACOS):
                    flags.append((cc, r, settings.get(str(r["campaignId"]), "default")))
            else:
                on = [on[0] + spend, on[1] + sales]
                if r["placementClassification"] == OTHER:
                    oth = [oth[0] + spend, oth[1] + sales, oth[2] + (r.get("purchases7d") or 0)]
                    acos = _acos(spend, sales)
                    if spend >= MIN_OFF_SPEND * scale and (acos is None or acos >= MAX_OFF_ACOS):
                        other_flags.append((cc, r))
        total = on[0] + off[0]
        if not total:
            continue
        summary[cc] = {"offSpend": round(off[0], 2), "offSales": round(off[1], 2), "offOrders": off[2],
                       "totalSpend": round(total, 2), "onAcos": _acos(*on), "offAcos": _acos(off[0], off[1]),
                       "otherSpend": round(oth[0], 2), "otherSales": round(oth[1], 2), "otherOrders": oth[2],
                       "otherAcos": _acos(oth[0], oth[1])}
        if oth[0] >= 1:
            xa = _acos(oth[0], oth[1])
            other_lines.append(f"{cc}: {cur}{oth[0]:,.2f} = {oth[0] / total * 100:.0f}% of spend, {oth[2]} orders, "
                               f"ACOS {f'{xa:.0f}%' if xa is not None else 'no sales'}")
        if off[0] < 1:
            lines.append(f"{cc}: off-Amazon {cur}{off[0]:.2f} (spend {cur}{total:,.0f})")
            continue
        oa, na = _acos(off[0], off[1]), _acos(*on)
        lines.append(f"{cc}: off-Amazon {cur}{off[0]:,.2f} = {off[0] / total * 100:.0f}% of {cur}{total:,.0f}, "
                     f"{off[2]} orders, ACOS {f'{oa:.0f}%' if oa is not None else 'no sales'} "
                     f"(on Amazon {f'{na:.0f}%' if na is not None else '-'})")
    lines.append("")
    if flags:
        lines.append(f"🔴 Losing money off Amazon ({len(flags)}):")
        for cc, r, strategy in sorted(flags, key=lambda f: -(f[1].get("cost") or 0)):
            acos = _acos(r.get("cost") or 0, r.get("sales7d") or 0)
            # the setting as it is NOW (it may have changed after the week)
            hint = {"MINIMIZE_SPEND": "now on Minimize spend", "PAUSED": "campaign paused now"}.get(
                strategy, "off-Amazon setting is default → set Minimize spend?")
            lines.append(f"• {cc} {r['campaignName']}: {CUR.get(cc, '')}{r['cost']:.2f}, {r.get('purchases7d') or 0} orders, "
                         f"ACOS {f'{acos:.0f}%' if acos is not None else 'no sales'} · {hint}")
    else:
        lines.append("✅ No campaign is losing money off Amazon this week.")
    lines += ["", "🧑‍🎤 Other on-Amazon (incl. creator storefronts from 11 Nov; 'Minimize spend' does not limit it):"]
    lines += other_lines or ["(under 1 in every marketplace)"]
    for cc, r in sorted(other_flags, key=lambda f: -(f[1].get("cost") or 0)):
        acos = _acos(r.get("cost") or 0, r.get("sales7d") or 0)
        lines.append(f"🔴 {cc} {r['campaignName']}: {CUR.get(cc, '')}{r['cost']:.2f}, {r.get('purchases7d') or 0} orders, "
                     f"ACOS {f'{acos:.0f}%' if acos is not None else 'no sales'} · check the Creator Report / deny list")
    if errors:
        lines += ["", "⚠️ " + "; ".join(errors)[:600]]
    flag_rows = [{"country": cc, "campaign": r["campaignName"], "campaignId": str(r["campaignId"]),
                  "spend": r.get("cost"), "sales": r.get("sales7d"), "orders": r.get("purchases7d"),
                  "strategy": s} for cc, r, s in flags]
    flag_rows += [{"country": cc, "campaign": r["campaignName"], "campaignId": str(r["campaignId"]),
                   "spend": r.get("cost"), "sales": r.get("sales7d"), "orders": r.get("purchases7d"),
                   "placement": OTHER} for cc, r in other_flags]
    return "\n".join(lines), {"start": start.isoformat(), "end": end.isoformat(), "summary": summary,
                              "flags": flag_rows, "errors": errors}


def SendOffAmazonWeekly(request):
    try:
        args = getattr(request, "args", {}) or {}
        today = date.fromisoformat(args["today"]) if args.get("today") else None
        text, data = build(today)
        if args.get("dry") != "1":
            notify(JOB_NAME, "amzbot", text, is_error=bool(data["errors"]) and not data["summary"])
            save_review(A.pb_authenticate(), JOB_NAME, (today or date.today()).isoformat(), text, data)
        return _json({"text": text, **data})
    except Exception as exc:
        notify(JOB_NAME, "amzbot", f"🚨 Weekly off-Amazon check failed: {exc}", is_error=True)
        return _json({"error": str(exc), "type": exc.__class__.__name__}, 500)
