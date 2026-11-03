"""Free-text notes the user leaves for themself on a page (per the user
2026-11-03: "leave a comment for myself" on /price-simulation). PocketBase
page_notes: page, note_key (e.g. "general" or "US|<asin>"), text.
GetPageNotes?page= lists; SetPageNote POST ?key=PROPOSAL_KEY {page, noteKey, text}
upserts (empty text deletes)."""
import json
import os

from AsinAnalytics import pb_authenticate, pb_list_all
from UpdateSkuSalesMonth import pb_batch

COLLECTION = "page_notes"


def _cors(methods="GET, POST, OPTIONS"):
    return {"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": methods,
            "Access-Control-Allow-Headers": "Content-Type", "Content-Type": "application/json"}


def _q(s):
    return (s or "").replace("\\", "").replace('"', "")


def GetPageNotes(request):
    if request.method == "OPTIONS":
        return "", 204, _cors()
    try:
        page = _q(request.args.get("page"))
        rows = pb_list_all(pb_authenticate(), COLLECTION, f'page = "{page}"')
        return json.dumps({"notes": {r["note_key"]: {"text": r.get("text") or "", "updated": r.get("updated")}
                                     for r in rows}}), 200, _cors()
    except Exception as exc:
        return json.dumps({"error": str(exc)}), 500, _cors()


def SetPageNote(request):
    if request.method == "OPTIONS":
        return "", 204, _cors("POST, OPTIONS")
    if (getattr(request, "args", {}) or {}).get("key") != os.environ.get("PROPOSAL_KEY", "~"):
        return json.dumps({"error": "Unauthorized"}), 401, _cors("POST, OPTIONS")
    try:
        b = request.get_json(silent=True) or {}
        page, key, text = _q(b.get("page")), _q(b.get("noteKey")), (b.get("text") or "").strip()
        if not page or not key:
            return json.dumps({"error": "page and noteKey required"}), 400, _cors("POST, OPTIONS")
        token = pb_authenticate()
        old = pb_list_all(token, COLLECTION, f'page = "{page}" && note_key = "{key}"')
        if old and not text:
            op = {"method": "DELETE", "url": f"/api/collections/{COLLECTION}/records/{old[0]['id']}"}
        elif old:
            op = {"method": "PATCH", "url": f"/api/collections/{COLLECTION}/records/{old[0]['id']}", "body": {"text": text}}
        elif text:
            op = {"method": "POST", "url": f"/api/collections/{COLLECTION}/records",
                  "body": {"page": page, "note_key": key, "text": text}}
        else:
            return json.dumps({"ok": True}), 200, _cors("POST, OPTIONS")
        pb_batch(token, [op])
        return json.dumps({"ok": True}), 200, _cors("POST, OPTIONS")
    except Exception as exc:
        return json.dumps({"error": str(exc)}), 500, _cors("POST, OPTIONS")
