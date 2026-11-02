"""To-do list (per the user 2026-11-02: "lets maintain todo list"). PocketBase
todo_items: title, detail, area, marketplace, status (open / done), source,
done_at, note. Page /todo. Claude adds items when the user says "add to the
todo list" (add_todo); the page adds, ticks done, reopens and edits notes
(SetTodo, key = PROPOSAL_KEY)."""
import json
import os
from datetime import datetime

from AsinAnalytics import pb_authenticate, pb_list_all
from UpdateSkuSalesMonth import pb_batch

COLLECTION = "todo_items"


def _cors(methods="GET, POST, OPTIONS"):
    return {"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": methods,
            "Access-Control-Allow-Headers": "Content-Type", "Content-Type": "application/json"}


def add_todo(title, detail="", area="", marketplace="", source="chat", token=None):
    token = token or pb_authenticate()
    pb_batch(token, [{"method": "POST", "url": f"/api/collections/{COLLECTION}/records",
                      "body": {"title": title, "detail": detail, "area": area, "marketplace": marketplace,
                               "status": "open", "source": source}}])


def GetTodos(request):
    if request.method == "OPTIONS":
        return "", 204, _cors()
    try:
        rows = pb_list_all(pb_authenticate(), COLLECTION, "")
        rows.sort(key=lambda r: (r.get("status") == "done", r.get("done_at") or "", r.get("created") or ""), reverse=False)
        items = [{k: r.get(k) for k in ("id", "title", "detail", "area", "marketplace", "status", "source", "done_at",
                                         "note", "created")} for r in rows]
        return json.dumps({"items": items}), 200, _cors()
    except Exception as exc:
        return json.dumps({"error": str(exc)}), 500, _cors()


def SetTodo(request):
    """POST ?key=PROPOSAL_KEY {id?, title?, detail?, area?, marketplace?, status?, note?, delete?}."""
    if request.method == "OPTIONS":
        return "", 204, _cors("POST, OPTIONS")
    if (getattr(request, "args", {}) or {}).get("key") != os.environ.get("PROPOSAL_KEY", "~"):
        return json.dumps({"error": "Unauthorized"}), 401, _cors("POST, OPTIONS")
    try:
        b = request.get_json(silent=True) or {}
        token = pb_authenticate()
        if not b.get("id"):
            if not (b.get("title") or "").strip():
                return json.dumps({"error": "title required"}), 400, _cors("POST, OPTIONS")
            add_todo(b["title"].strip(), b.get("detail", ""), b.get("area", ""), b.get("marketplace", ""), "page", token)
            return json.dumps({"ok": True}), 200, _cors("POST, OPTIONS")
        if b.get("delete"):
            pb_batch(token, [{"method": "DELETE", "url": f"/api/collections/{COLLECTION}/records/{b['id']}"}])
            return json.dumps({"ok": True}), 200, _cors("POST, OPTIONS")
        body = {k: b[k] for k in ("title", "detail", "area", "marketplace", "note") if k in b}
        if b.get("status") in ("open", "done"):
            body["status"] = b["status"]
            body["done_at"] = datetime.now().isoformat(timespec="seconds") if b["status"] == "done" else ""
        pb_batch(token, [{"method": "PATCH", "url": f"/api/collections/{COLLECTION}/records/{b['id']}", "body": body}])
        return json.dumps({"ok": True}), 200, _cors("POST, OPTIONS")
    except Exception as exc:
        return json.dumps({"error": str(exc)}), 500, _cors("POST, OPTIONS")
