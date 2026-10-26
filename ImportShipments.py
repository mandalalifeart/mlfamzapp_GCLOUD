"""Import shipments page (/shipments, 2026-10-26, per the user): "each shipment
from malani/acacia should be there with all its customs documents and
invoices, let me know what is missing".

One row per physical shipment in PocketBase `import_shipments`:
- supplier invoice(s) (Malani/Acacia, already on /wise as supplier|<id> docs)
- freight + customs documents: Unicargo (EU/UK sea, one Unicargo file "UNC..."
  per shipment), Xhipment/First Shipment (USA sea via Amazon Global Logistics,
  billed to us through the Amazon settlement), supplier DDP recharges (Acacia
  courier shipments), customs declarations (DE Zoll notice / UK C88 / US 7501)
- Amazon AGL charges from amazon_expense_events (USA duty/freight)

`docs` = [{role, doc_id}] pointing at wise_documents (files stay in
~/accounting/wise/<month>/). Which roles a shipment needs depends on its route
(REQUIRED_ROLES); a role can be marked not needed per shipment. Uploads from the
page are read by Claude (shipment schema) for the amounts."""
import base64
import json
import os
from datetime import datetime

from WiseFinances import WISE_REFRESH_KEY, cors_headers, json_response, pb_authenticate, pb_batch
from WiseLedger import (DOCUMENTS_COLLECTION, MAX_DOCUMENT_BYTES, SHEET_EXTENSIONS, WISE_DOCS_DIR, _MEDIA_TYPES, pb_list_all,
                        spreadsheet_to_text, store_document)

COLLECTION = "import_shipments"
AI_MODEL = os.environ.get("SHIPMENT_AI_MODEL", "claude-opus-5-5")

ROLES = {
    "supplier_invoice": "Supplier invoice",
    "packing_list": "Packing list",
    "bill_of_lading": "Bill of lading / AWB",
    "freight_invoice": "Freight invoice",
    "customs_invoice": "Duty / import VAT invoice",
    "duty_recharge": "Supplier duty recharge (DDP)",
    "customs_declaration": "Customs declaration",
    "other": "Other",
}
ROUTES = {
    "unicargo": "Unicargo (sea)",
    "xhipment": "Xhipment (charged in the Amazon settlement)",
    "amazon_agl": "Amazon Global Logistics (AGL)",
    "supplier_courier": "Supplier courier, DDP",
    "supplier_air_cif": "Supplier air freight (CIF)",
    "unknown": "Route unknown",
}
REQUIRED_ROLES = {
    "unicargo": ["supplier_invoice", "bill_of_lading", "freight_invoice", "customs_invoice", "customs_declaration"],
    # Xhipment sends no invoices to us (per the user 2026-10-26) - the bill is the
    # Amazon settlement charge; only the customs release paper (7501) is tracked.
    "xhipment": ["supplier_invoice", "customs_declaration"],
    "amazon_agl": ["supplier_invoice", "customs_declaration"],
    "supplier_courier": ["supplier_invoice", "duty_recharge"],
    "supplier_air_cif": ["supplier_invoice", "bill_of_lading", "customs_invoice", "customs_declaration"],
    "unknown": ["supplier_invoice", "freight_invoice", "customs_invoice", "customs_declaration"],
}
CUSTOMS_DECLARATION_HINT = {
    "DE": "Zoll import tax notice (Einfuhrabgabenbescheid) - needed to deduct the import VAT",
    "PL": "Customs import declaration (SAD) - needed to deduct the import VAT",
    "GB": "C88 import entry / PVA statement",
    "US": "CBP 7501 entry summary",
    "AU": "Import declaration (N10)",
}

SHIPMENT_DOC_SCHEMA = {"type": "object", "additionalProperties": False, "properties": {
    "doc_type": {"type": "string"}, "issuer": {"type": "string"}, "doc_number": {"type": "string"},
    "doc_date": {"type": "string"}, "total": {"type": "number"}, "currency": {"type": "string"},
    "forwarder_file": {"type": "string"}, "bl_awb": {"type": "string"}, "packages": {"type": "number"},
    "gross_weight_kg": {"type": "number"}, "freight_amount": {"type": "number"}, "duty_amount": {"type": "number"},
    "import_vat_amount": {"type": "number"}, "summary": {"type": "string"}},
    "required": ["doc_type", "issuer", "doc_number", "doc_date", "total", "currency", "forwarder_file", "bl_awb",
                 "packages", "gross_weight_kg", "freight_amount", "duty_amount", "import_vat_amount", "summary"]}


def _key_ok(request, body=None):
    args = getattr(request, "args", {}) or {}
    return not WISE_REFRESH_KEY or (args.get("key") or (body or {}).get("key")) == WISE_REFRESH_KEY


def extract_shipment_doc(filename, data):
    """Claude reads an uploaded shipment document -> SHIPMENT_DOC_SCHEMA dict (or None)."""
    import anthropic

    ext = os.path.splitext(filename or "")[1].lower()
    if ext in SHEET_EXTENSIONS:
        block = {"type": "document", "source": {"type": "text", "media_type": "text/plain",
                                                "data": spreadsheet_to_text(filename, data)}}
    elif _MEDIA_TYPES.get(ext) == "application/pdf":
        block = {"type": "document", "source": {"type": "base64", "media_type": "application/pdf",
                                                "data": base64.standard_b64encode(data).decode()}}
    elif _MEDIA_TYPES.get(ext):
        block = {"type": "image", "source": {"type": "base64", "media_type": _MEDIA_TYPES[ext],
                                             "data": base64.standard_b64encode(data).decode()}}
    else:
        return None
    response = anthropic.Anthropic().beta.messages.create(
        model=AI_MODEL, max_tokens=4000, thinking={"type": "adaptive"},
        output_config={"effort": "low", "format": {"type": "json_schema", "schema": SHIPMENT_DOC_SCHEMA}},
        messages=[{"role": "user", "content": [block, {"type": "text", "text": (
            f"File: {filename}\nA document of an import shipment of textiles from India for Mandala Life ART OÜ "
            "(invoice, bill of lading, customs declaration, duty bill...). Extract the fields; amounts in the "
            "document's own currency (total in `currency`; freight/duty/import VAT in that same currency). "
            "Use 0 or an empty string when absent.")}]}])
    text = next((b.text for b in response.content if b.type == "text"), "")
    return json.loads(text) if text else None


def _doc_view(doc):
    ex = doc.get("extracted") or {}
    return {"id": doc["id"], "filename": doc.get("filename"), "kind": doc.get("kind"),
            "number": ex.get("invoice_number") or ex.get("doc_number") or "",
            "date": ex.get("invoice_date") or ex.get("doc_date") or doc.get("month") or "",
            "total": ex.get("total"), "currency": (ex.get("currency") or "").upper(),
            "issuer": ex.get("vendor") or ex.get("issuer") or "",
            "summary": ex.get("description") or ex.get("summary") or "",
            "paid": bool(doc.get("ledger_key"))}


def GetImportShipments(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    try:
        token = pb_authenticate()
        rows = pb_list_all(token, COLLECTION)
        doc_ids = {d["doc_id"] for r in rows for d in (r.get("docs") or [])}
        docs = {d["id"]: d for d in pb_list_all(token, DOCUMENTS_COLLECTION) if d["id"] in doc_ids}
        out = []
        for r in rows:
            entries = []
            for link in r.get("docs") or []:
                d = docs.get(link["doc_id"])
                if not d:
                    continue
                v = _doc_view(d)
                v.update({"role": link["role"], "freightUsd": link.get("freight_usd"), "dutyUsd": link.get("duty_usd"),
                          "vatUsd": link.get("vat_usd"), "note": link.get("note") or ""})
                entries.append(v)
            not_needed = set(r.get("not_needed") or [])
            have = {e["role"] for e in entries} | {c.get("role") for c in (r.get("amazon_charges") or [])}
            if r.get("bl"):  # the B/L number printed on the forwarder's invoices is enough for the books
                have.add("bill_of_lading")
            missing = [role for role in REQUIRED_ROLES.get(r.get("route") or "unknown", [])
                       if role not in have and role not in not_needed]
            for m in r.get("missing_docs") or []:  # named documents known to be missing (e.g. a 2025 invoice)
                missing.append(m)
            cost = {"goods": r.get("goods_value") or 0, "freight": 0.0, "duty": 0.0, "importVat": 0.0}
            for e in entries:
                cost["freight"] += e.get("freightUsd") or 0
                cost["duty"] += e.get("dutyUsd") or 0
                cost["importVat"] += e.get("vatUsd") or 0
            for c in r.get("amazon_charges") or []:
                if c.get("counted"):
                    cost["duty" if "duty" in c.get("category", "").lower() else "freight"] += abs(c.get("amount") or 0)
            units = r.get("units") or 0
            out.append({
                "id": r["id"], "code": r.get("code"), "supplier": r.get("supplier"), "destination": r.get("destination"),
                "mode": r.get("mode"), "route": r.get("route"), "routeLabel": ROUTES.get(r.get("route"), r.get("route")),
                "shipDate": r.get("ship_date"), "status": r.get("status"), "packages": r.get("packages"),
                "weightKg": r.get("weight_kg"), "cbm": r.get("cbm"), "units": units,
                "supplierInvoices": r.get("supplier_invoices") or [], "forwarderFile": r.get("forwarder_file") or "",
                "bl": r.get("bl") or "", "amazonRefs": r.get("amazon_refs") or [], "docs": entries,
                "amazonCharges": r.get("amazon_charges") or [], "notes": r.get("notes") or [],
                "missing": missing, "notNeeded": sorted(not_needed),
                "declarationHint": CUSTOMS_DECLARATION_HINT.get(r.get("destination"), ""),
                "cost": {k: round(v, 2) for k, v in cost.items()},
                "landedPerUnit": round((cost["goods"] + cost["freight"] + cost["duty"]) / units, 2) if units else None,
            })
        out.sort(key=lambda s: s.get("shipDate") or "", reverse=True)
        unassigned = [json.loads(x["value"]) if isinstance(x.get("value"), str) else x.get("value")
                      for x in pb_list_all(token, "job_state", filter_str='key = "import_shipments_unassigned"')]
        return json_response({"shipments": out, "roles": ROLES, "routes": ROUTES,
                              "unassignedAmazonCharges": unassigned[0] if unassigned else []})
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)


def _get_shipment(token, shipment_id):
    found = pb_list_all(token, COLLECTION, filter_str=f"id = {json.dumps(shipment_id)}")
    if not found:
        raise ValueError("Unknown shipment")
    return found[0]


def UploadShipmentDocument(request):
    """POST {shipmentId, role, filename, contentBase64} (+ key): stores the file
    with the shipment's month on /wise documents and links it to the shipment."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    body = request.get_json(silent=True) or {}
    if not _key_ok(request, body):
        return json_response({"error": "Unauthorized"}, 401)
    role = body.get("role")
    if role not in ROLES:
        return json_response({"error": "Unknown role"}, 400)
    try:
        data = base64.b64decode(body.get("contentBase64") or "")
        if not data or len(data) > MAX_DOCUMENT_BYTES:
            return json_response({"error": "Empty or too large file"}, 400)
        token = pb_authenticate()
        ship = _get_shipment(token, body.get("shipmentId"))
        filename = body.get("filename") or "document"
        try:
            ex = extract_shipment_doc(filename, data)
        except Exception:
            ex = None
        month = ((ex or {}).get("doc_date") or ship.get("ship_date") or datetime.now().strftime("%Y-%m"))[:7]
        rel = store_document(token, "shipment", month, filename, data, prefix=f"{ship['code']}_{role}_",
                             group_key=f"shipment|{ship['code']}", extracted=ex)
        path = os.path.join(WISE_DOCS_DIR, rel)
        doc = pb_list_all(token, DOCUMENTS_COLLECTION, filter_str=f"stored_path = {json.dumps(path)}")[0]
        link = {"role": role, "doc_id": doc["id"]}
        cur = (ex or {}).get("currency", "").upper()
        if cur == "USD":  # amounts in other currencies are entered by hand later
            link.update({"freight_usd": (ex or {}).get("freight_amount") or 0, "duty_usd": (ex or {}).get("duty_amount") or 0,
                         "vat_usd": (ex or {}).get("import_vat_amount") or 0})
        pb_batch(token, [{"method": "PATCH", "url": f"/api/collections/{COLLECTION}/records/{ship['id']}",
                          "body": {"docs": (ship.get("docs") or []) + [link]}}])
        return json_response({"ok": True, "docId": doc["id"], "extracted": ex})
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)


def SetImportShipmentRole(request):
    """POST {shipmentId, role, notNeeded: bool} (+ key) - marks a required
    document as not needed for this shipment (or needed again). Also
    {shipmentId, unlinkDocId} removes a document link (the file stays on /wise)."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    body = request.get_json(silent=True) or {}
    if not _key_ok(request, body):
        return json_response({"error": "Unauthorized"}, 401)
    try:
        token = pb_authenticate()
        ship = _get_shipment(token, body.get("shipmentId"))
        patch = {}
        if body.get("unlinkDocId"):
            patch["docs"] = [d for d in ship.get("docs") or [] if d["doc_id"] != body["unlinkDocId"]]
        if body.get("role"):
            nn = set(ship.get("not_needed") or [])
            (nn.add if body.get("notNeeded") else nn.discard)(body["role"])
            patch["not_needed"] = sorted(nn)
        if patch:
            pb_batch(token, [{"method": "PATCH", "url": f"/api/collections/{COLLECTION}/records/{ship['id']}", "body": patch}])
        return json_response({"ok": True})
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)
