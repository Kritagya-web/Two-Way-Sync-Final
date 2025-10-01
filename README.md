# Two-Way Sync Between Filevine, AWS S3, and Z-Drive

This project implements a **two-way synchronization system** that keeps files consistent between:

- **Filevine** (legal case management system)  
- **AWS S3** (cloud mirror)  
- **Z-Drive** (local network drive used by staff)  

The goal is simple:  
- If a file is uploaded, updated, or deleted in **Filevine**, those changes appear on **Z-Drive**.  
- If a file is uploaded, updated, or deleted in **Z-Drive**, those changes appear in **Filevine**.  

---

## How the System Works

### High-Level Flow
1. **Filevine → S3 → Z-Drive**  
   - Filevine sends a webhook (document created/updated/deleted).  
   - AWS Lambda processes it, downloads the file (or deletes it), and updates S3.  
   - Local sync job mirrors S3 into Z-Drive.  

2. **Z-Drive → S3 → Filevine**  
   - A watcher script detects new/changed files on Z-Drive.  
   - The file is uploaded to S3, then registered and finalized in Filevine.  

---

## Authentication (`auth_refresh.py`)

Every call to Filevine needs fresh headers.  
This module builds them dynamically using **MD5 hashing** and a timestamp.

```python
import hashlib, requests, logging, os
from datetime import datetime, timezone
from dotenv import load_dotenv
load_dotenv()

def compute_md5_hash(data: str) -> str:
    return hashlib.md5(data.encode("utf-8")).hexdigest()

def refresh_access_token() -> dict:
    api_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    api_hash = compute_md5_hash(f"{os.getenv('API_KEY')}/{api_timestamp}/{os.getenv('API_SECRET')}")

    payload = {
        "mode": "key",
        "apiKey": os.getenv("API_KEY"),
        "apiSecret": os.getenv("API_SECRET"),
        "apiHash": api_hash,
        "apiTimestamp": api_timestamp,
        "userId": os.getenv("USER_ID"),
        "orgId": os.getenv("ORG_ID"),
    }

    response = requests.post(os.getenv("SESSION_URL"), json=payload)
    response.raise_for_status()
    data = response.json()

    return {
        "access_token": data["accessToken"],
        "session_id": data["refreshToken"],
        "user_id": data["userId"],
    }

def get_dynamic_headers() -> dict:
    auth = refresh_access_token()
    return {
        "Authorization": f"Bearer {auth['access_token']}",
        "x-fv-userid": str(auth["user_id"]),
        "x-fv-orgid": os.getenv("ORG_ID"),
        "x-fv-sessionid": auth["session_id"],
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
```

**Why hashing?**  
- Combines `API_KEY`, `SECRET`, and a UTC `TIMESTAMP` into an MD5 hash.  
- This prevents replay attacks and proves you know the secret without sending it raw.  

---

## Document Processing (`utils.py`)

This is the **engine** that manages folder trees, documents, and S3 uploads.

### Fetch folder structure

```python
folder_map = proc.fetch_complete_folder_structure(project_id, headers)
# { folderId: "Discovery/To Client/Responses", ... }
```

- Starts from root folders.  
- Recursively traverses children using `/core/folders/{id}/children`.  
- Produces a full mapping of folder IDs to human-readable paths.  

### List all project documents

```python
docs = proc.fetch_all_documents(project_id, headers)
# Each doc: {id, filename, size, folder_id, folder_name, modified}
```

### Ensure S3 folder placeholders

```python
proc.ensure_placeholders(project_prefix, folder_paths)
# Creates zero-byte .placeholder files so empty folders are preserved in S3
```

### Download + upload docs to S3

```python
ids = [d["id"] for d in docs_with_paths]
link_by_id = proc.get_download_links_batch(ids, headers)

for d in docs_with_paths:
    url = link_by_id.get(d["id"])
    resp = proc.http.get(url, timeout=30)
    key = f"{project_prefix}{d['folder_path']}/{d['filename']}"
    proc.upload_to_s3(
        key, resp.content, d['filename'],
        metadata={
            "documentId": str(d["id"]),
            "projectId": str(project_id),
            "folderId": str(d.get("folder_id") or ""),
            "folderPath": d["folder_path"],
        },
        tags={"origin": "filevine", "fv_docid": str(d["id"]), "projectId": str(project_id)},
    )
```

---

## AWS Lambda Router (`lambda_function.py`)

The entrypoint that handles webhooks and routes them correctly.

```python
def lambda_handler(event, context):
    body    = parse_input(event)
    proc    = DocumentProcessor()
    headers = get_dynamic_headers()

    # Background full sync
    if body.get("__background_sync"):
        pid = body.get("projectId")
        return proc.sync_documents(pid, headers)

    pid = proc.extract_project_id(body)
    ev  = extract_event_type(body, event).lower()
    did = extract_document_id(body)

    if looks_like_delete(ev):
        return proc.handle_document_delete(body, headers)

    if looks_like_create_or_update(ev):
        return _delegate_upload(proc, body, headers)

    # Fallback: probe doc existence
    if did is not None:
        if doc_exists(proc, did, headers):
            return _delegate_upload(proc, body, headers)
        else:
            return proc.handle_document_delete(body, headers)

    return proc.sync_documents(pid, headers)
```

- **Delete event** → removes file from S3.  
- **Create/Update event** → downloads from Filevine, uploads to S3.  
- **First-time project** → queues a full background sync.  
- **No documentId** → runs a project-wide refresh.  

---

## End-to-End Examples

### Full sync of a project
```python
headers = get_dynamic_headers()
proc = DocumentProcessor()
proc.sync_documents(2370300, headers)
```

### Handle one new document event
```python
body = {
    "eventType": "DocumentCreated",
    "projectId": 2370300,
    "documentId": {"native": 12345678},
}
headers = get_dynamic_headers()
proc = DocumentProcessor()
proc.handle_document_upload(body, headers)
```

### Handle one delete event
```python
body = {
    "eventType": "DocumentDeleted",
    "projectId": 2370300,
    "documentId": {"native": 12345678},
}
headers = get_dynamic_headers()
proc = DocumentProcessor()
proc.handle_document_delete(body, headers)
```

---

## Key Features
- Fresh **auth tokens** generated automatically.  
- Complete **folder structure mapping** from Filevine.  
- **Retry/backoff** on 401, 429, and 5xx errors.  
- **Placeholder files** preserve empty folders.  
- **Two-way**: both Filevine → Z-Drive and Z-Drive → Filevine.  
- **Safe deletes**: removes S3 files when Filevine deletes docs.  
