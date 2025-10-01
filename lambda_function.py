# === lambda_function.py ===
import json
import os
import base64
import boto3
import requests
import logging

from utils import DocumentProcessor
from auth_refresh import get_dynamic_headers

# ---------- logging ----------
logger = logging.getLogger(__name__)
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter('%(levelname)s\t%(asctime)s\t%(message)s'))
    logger.addHandler(_h)
logger.setLevel(logging.INFO)

_lambda = boto3.client("lambda")

# ---------- helpers ----------

def parse_input(event):
    """
    Handles API Gateway proxy (string body, optional base64) and direct invoke (dict).
    Returns a dict body.
    """
    if not isinstance(event, dict):
        return {}
    body = event.get("body", event)

    # API Gateway can base64-encode
    if isinstance(body, string_types := str):
        if event.get("isBase64Encoded"):
            try:
                body = base64.b64decode(body).decode("utf-8", errors="ignore")
            except Exception as e:
                logger.error(f"Failed to base64-decode body: {e}")
        try:
            return json.loads(body)
        except Exception as e:
            logger.error(f"Failed to JSON-decode body: {e}")
            return {}
    return body if isinstance(body, dict) else {}

def extract_event_type(body, event):
    """
    Pull an event-type-ish hint from common places.
    Example values you might see: 'DocumentDeleted', 'DocumentCreated', 'DocumentUpdated', etc.
    """
    candidates = [
        body.get("eventType"),
        body.get("event"),
        body.get("type"),
        body.get("name"),
        body.get("action"),
        (event.get("headers", {}) or {}).get("x-filevine-event"),
        (event.get("headers", {}) or {}).get("X-Filevine-Event"),
    ]
    for c in candidates:
        if isinstance(c, str) and c.strip():
            return c.strip().lower()
    return ""

def extract_document_id(body):
    """
    Accepts documentId in many shapes:
      - body["documentId"] = 123
      - body["DocumentId"] = {"native": 123}
      - body["payload"]["documentId"] = {"native": 123}
    Returns int or None.
    """
    raw = (
        body.get("documentId")
        or body.get("DocumentId")
        or (body.get("payload") or {}).get("documentId")
    )
    if raw is None:
        return None
    if isinstance(raw, dict):
        raw = raw.get("native", None)
    try:
        return int(raw) if raw is not None else None
    except Exception:
        return None

def looks_like_delete(ev: str) -> bool:
    tokens = ("delete", "deleted", "remove", "removed", "trash", "purge")
    return any(t in ev for t in tokens)

def looks_like_create_or_update(ev: str) -> bool:
    tokens = ("create", "created", "upload", "uploaded", "update", "updated", "rename", "moved")
    return any(t in ev for t in tokens)

def doc_exists(proc: DocumentProcessor, doc_id: int, headers) -> bool:
    """
    Probe Filevine: 200 -> exists, 404 -> gone (treat as delete),
    anything else -> unknown; log and assume exists to be conservative.
    """
    url = f"{proc.base_url}/core/documents/{doc_id}"
    try:
        res = requests.get(url, headers=headers, timeout=8)
        if res.status_code == 200:
            return True
        if res.status_code == 404:
            return False
        logger.info(f"doc_exists: unexpected {res.status_code} for {doc_id}; body={res.text[:200]}")
        return True
    except Exception as e:
        logger.error(f"doc_exists: request failed for {doc_id}: {e}")
        return True  # avoid accidental deletes on transient errors

def _delegate_upload(proc: DocumentProcessor, body: dict, headers: dict):
    """
    Call the correct upload handler based on what's available on the processor.
    This prevents AttributeError if only handle_single_document_upload exists.
    """
    if hasattr(proc, "handle_document_upload"):
        logger.info("router: using handle_document_upload")
        return proc.handle_document_upload(body, headers)
    else:
        logger.info("router: handle_document_upload not found; delegating to handle_single_document_upload")
        return proc.handle_single_document_upload(body, headers)

# ---------- handler ----------

def lambda_handler(event, context):
    body    = parse_input(event)
    proc    = DocumentProcessor()
    headers = get_dynamic_headers()
    # ALLOWED_PID = 2370300
    # 0) background seed run?
    if body.get("__background_sync"):
        pid = body.get("projectId")
        # if pid != ALLOWED_PID:
        #     logger.info(f"⏭️ skipping background sync pid={pid} (not allowed)")
        #     return proc.success_response({"status": "skipped", "projectId": pid, "reason": "not_allowed"})
        logger.info(f"↩️ background sync for project {pid}")
        return proc.sync_documents(pid, headers)

    # # 1) project filter
    # pid = proc.extract_project_id(body)
    # if pid != 2370300:
    #     logger.info(f"⏭️ skipping project pid={pid}")
    #     return proc.success_response({"status": "skipped", "projectId": pid})
    
    # 1) project id (no hard-coded filter)
    pid = proc.extract_project_id(body)
    if not pid:
        return proc.error_response(400, "missing projectId")

    # Optional: rollout allowlist via env var PROJECT_ALLOWLIST_JSON='[2370300, 2455703]'
    allow_json = os.getenv("PROJECT_ALLOWLIST_JSON", "").strip()
    if allow_json:
        try:
            allowed = set(int(x) for x in json.loads(allow_json))
            if allowed and pid not in allowed:
                logger.info(f"⏭️ skipping project pid={pid} (not in allowlist)")
                return proc.success_response({"status": "skipped", "projectId": pid, "reason": "not_in_allowlist"})
        except Exception as e:
            logger.error(f"Invalid PROJECT_ALLOWLIST_JSON: {e}")

    # 2) compute project prefix existence (seed path)
    def ensure_seed_if_needed():
        project_name = proc.sanitize(proc.get_project_name(pid, headers))
        project_pref = f"{proc.prefix}{project_name}/"
        s3 = boto3.client("s3")
        exists = s3.list_objects_v2(Bucket=proc.bucket, Prefix=project_pref, MaxKeys=1)
        if exists.get("KeyCount", 0) == 0:
            logger.info(f"🌱 queueing initial seed for {project_pref}")
            try:
                _lambda.invoke(
                    FunctionName=context.function_name,
                    InvocationType="Event",
                    Payload=json.dumps({"__background_sync": True, "projectId": pid}).encode()
                )
            except Exception as e:
                logger.error(f"Failed to queue background seed: {e}")
                # fall through; not fatal for single doc handling
            return proc.success_response({
                "status": "initial_seed_queued",
                "message": "Project seed scheduled in background."
            })
        return None  # seed not needed

    # 3) classify the event
    ev = extract_event_type(body, event)
    did = extract_document_id(body)
    logger.info(f"🧭 router: eventType='{ev}' documentId={did} projectId={pid}")

    # 4) direct routes when event type is clear
    if looks_like_delete(ev):
        if did is None:
            return proc.error_response(400, "delete event missing documentId")
        return proc.handle_document_delete(body, headers)

    if looks_like_create_or_update(ev):
        if did is None:
            return proc.error_response(400, "create/update event missing documentId")
        # seed if needed, then upload path
        seeded = ensure_seed_if_needed()
        if seeded:
            return seeded
        return _delegate_upload(proc, body, headers)

    # 5) ambiguous events: fall back to probing the doc
    if did is not None:
        exists = doc_exists(proc, did, headers)
        if exists:
            seeded = ensure_seed_if_needed()
            if seeded:
                return seeded
            return _delegate_upload(proc, body, headers)
        else:
            # 404 -> treat as delete
            return proc.handle_document_delete(body, headers)

    # # 6) no documentId and unclassified -> no-op (or queue a small sync if you prefer)
    # logger.info("ℹ️ Unclassified event without documentId; acknowledging with no action.")
    # return proc.success_response({"status": "ignored", "reason": "unclassified_no_documentId"})


        # 6) no documentId case
    if did is None:
        logger.info(f"ℹ No documentId provided; running project-wide refresh for pid={pid}")
        try:
            return proc.sync_documents(pid, headers)
        except Exception as e:
            logger.error(f"Project-wide sync failed for pid={pid}: {e}")
            return proc.error_response(500, f"project-wide sync failed: {e}")
