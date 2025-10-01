# #!/usr/bin/env python
# import argparse
# import time
# import os
# import re
# import mimetypes
# import requests
# from typing import Optional, List, Tuple
# from auth_refresh import get_dynamic_headers
# from config import BASE_URL

# # Helpful MIME additions
# mimetypes.add_type('application/pdf', '.pdf')
# mimetypes.add_type('image/jpeg', '.jpg')
# mimetypes.add_type('image/png', '.png')
# mimetypes.add_type('application/msword', '.doc')
# mimetypes.add_type('application/vnd.openxmlformats-officedocument.wordprocessingml.document', '.docx')
# mimetypes.add_type('text/csv', '.csv')

# def log(msg: str) -> None:
#     # ASCII-only to avoid console code-page issues
#     try:
#         print(msg.encode("ascii", "ignore").decode("ascii"), flush=True)
#     except Exception:
#         print(msg, flush=True)

# def sanitize(name: Optional[str]) -> str:
#     if not name:
#         return "Unnamed"
#     name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "", name)
#     name = re.sub(r'\s+', " ", name).strip().strip(".")
#     return name or "Unnamed"

# # -------- HTTP with retry/backoff (429-safe) ---------------------------------
# _session = requests.Session()

# def _request(method: str, url: str, **kw) -> requests.Response:
#     timeout = kw.pop("timeout", 25)
#     sleep = 0.25
#     last: Optional[requests.Response] = None
#     for _ in range(6):
#         try:
#             headers = get_dynamic_headers()
#             r = _session.request(method, url, headers=headers, timeout=timeout, **kw)
#             if r.status_code == 401:
#                 headers = get_dynamic_headers()
#                 r = _session.request(method, url, headers=headers, timeout=timeout, **kw)
#             if r.status_code == 429:
#                 wait = float(r.headers.get("Retry-After", sleep))
#                 log(f"FV: 429 Too Many Requests - backoff {wait:.2f}s {url}")
#                 time.sleep(wait)
#                 sleep = min(sleep * 2, 2.0)
#                 last = r
#                 continue
#             r.raise_for_status()
#             return r
#         except requests.RequestException:
#             time.sleep(sleep)
#             sleep = min(sleep * 2, 2.0)
#             last = r if 'r' in locals() else None
#             continue
#     if last is not None:
#         last.raise_for_status()
#     raise RuntimeError("Filevine request failed after retries")

# def fv_get(url: str, **kw) -> requests.Response:
#     return _request("GET", url, **kw)

# def fv_post(url: str, json: dict, **kw) -> requests.Response:
#     return _request("POST", url, json=json, **kw)

# # -------- Folder helpers -----------------------------------------------------
# def list_children(project_id: int, folder_id: int, offset=0, limit=500) -> List[dict]:
#     url = f"{BASE_URL}/core/folders/{folder_id}/children?projectId={project_id}&offset={offset}&limit={limit}"
#     j = fv_get(url).json()
#     return (j or {}).get("items", [])

# def list_roots(project_id: int) -> List[dict]:
#     """
#     Returns the 'tiles' you see on the Docs page. Each item has:
#     - name
#     - folderId.native (tile id)
#     - parentId.native (the project root id)
#     """
#     url = f"{BASE_URL}/core/folders?projectId={project_id}&offset=0&limit=500"
#     j = fv_get(url).json()
#     return (j or {}).get("items", [])

# def guess_project_root_id(project_id: int) -> Optional[int]:
#     """
#     Infer the project root folderId by finding the most common parentId among tiles.
#     """
#     items = list_roots(project_id)
#     parents = [ (it.get("parentId") or {}).get("native") for it in items ]
#     parents = [ int(p) for p in parents if p ]
#     if not parents:
#         return None
#     from collections import Counter
#     return Counter(parents).most_common(1)[0][0]

# def resolve_under_root(project_id: int, root_folder_id: int, subpath: str) -> Optional[int]:
#     """
#     Resolve 'subpath' under the given root_folder_id.
#     - If subpath == "", returns the root_folder_id (upload into project root).
#     - If subpath starts with a tile name, we match that child of root (case-insensitive),
#       then continue down using /children calls.
#     """
#     if not subpath:
#         return int(root_folder_id)

#     segments = [s.strip() for s in subpath.replace("\\", "/").split("/") if s.strip()]
#     if not segments:
#         return int(root_folder_id)

#     current = int(root_folder_id)

#     for seg in segments:
#         target = seg.lower()
#         found: Optional[int] = None
#         offset = 0
#         while True:
#             kids = list_children(project_id, current, offset=offset, limit=500)
#             if not kids:
#                 break
#             for ch in kids:
#                 cid = (ch.get("folderId") or {}).get("native")
#                 nm  = (ch.get("name") or "").strip().lower()
#                 if cid and nm == target:
#                     found = int(cid)
#                     break
#             if found is not None or len(kids) < 500:
#                 break
#             offset += 500

#         if found is None:
#             # Not resolvable
#             return None
#         current = found

#     return current

# # -------- Document upload ----------------------------------------------------
# def register_document(file_name: str, file_size: int) -> Tuple[int, str]:
#     url = f"{BASE_URL}/core/Documents"
#     payload = {
#         "fileName": file_name,
#         "length": int(file_size),
#         "contentType": mimetypes.guess_type(file_name)[0] or "application/octet-stream",
#     }
#     data = fv_post(url, payload).json()
#     return int(data["documentId"]["native"]), data["url"]

# def upload_to_signed_url(put_url: str, local_path: str) -> bool:
#     with open(local_path, "rb") as f:
#         headers = {"Content-Type": mimetypes.guess_type(local_path)[0] or "application/octet-stream"}
#         rr = _session.put(put_url, data=f, headers=headers, timeout=180)
#         return rr.status_code in (200, 204)

# def finalize_document(project_id: int, doc_id: int, file_name: str, file_size: int,
#                       folder_id: Optional[int]) -> bool:
#     """
#     IMPORTANT: Filevine routes placement by folderId in the QUERY STRING.
#     """
#     base = f"{BASE_URL}/core/projects/{project_id}/Documents/{doc_id}"
#     url  = f"{base}?folderId={int(folder_id)}" if folder_id else base
#     payload = {
#         "fileName": file_name,
#         "contentType": mimetypes.guess_type(file_name)[0] or "application/octet-stream",
#         "length": int(file_size),
#     }
#     fv_post(url, payload)
#     return True

# # -------- CLI ----------------------------------------------------------------
# def main() -> int:
#     ap = argparse.ArgumentParser(description="Upload one file to Filevine at given folder path.")
#     ap.add_argument("--project-id", type=int, required=True)
#     ap.add_argument("--file", required=True, help="Full local path to file")
#     ap.add_argument("--folder-path", default="",
#                     help="Subpath under the project root, e.g. 'Clients Docs/Subfolder'")
#     ap.add_argument("--root-folder-id", type=int, default=None,
#                     help="Project ROOT folderId (parent of tiles). If omitted we infer it.")
#     ap.add_argument("--require-resolved", action="store_true",
#                     help="Abort if the folder path cannot be resolved.")
#     args = ap.parse_args()

#     project_id = int(args.project_id)
#     local_path = args.file
#     subpath    = (args.folder_path or "").strip().strip("\\/")

#     if not os.path.isfile(local_path):
#         log(f"[ERROR] File not found: {local_path}")
#         return 2

#     # 1) project root folderId
#     root_id = int(args.root_folder_id) if args.root_folder_id else guess_project_root_id(project_id)
#     if not root_id:
#         log("[ERROR] Could not determine project root folderId.")
#         return 5

#     # 2) resolve folder path to a concrete folderId
#     folder_id = resolve_under_root(project_id, root_id, subpath)
#     if folder_id is None:
#         msg = f"[WARN] Could not resolve '{subpath}' under rootId={root_id}."
#         if args.require_resolved:
#             log(msg + " Aborting.")
#             return 6
#         log(msg + " Uploading into project ROOT.")
#         folder_id = int(root_id)

#     # 3) register → upload bytes → finalize (with folderId in query)
#     file_name = os.path.basename(local_path)
#     file_size = os.path.getsize(local_path)

#     doc_id, put_url = register_document(file_name, file_size)
#     log(f"[OK] Registered docId={doc_id}")

#     if not upload_to_signed_url(put_url, local_path):
#         log("[ERROR] Upload to signed URL failed.")
#         return 3
#     log("[OK] Content uploaded")

#     finalize_document(project_id, int(doc_id), file_name, file_size, folder_id)
#     log(f"[OK] Finalized in Filevine (folderId={folder_id})")
#     return 0

# if __name__ == "__main__":
#     raise SystemExit(main())

# #!/usr/bin/env python
# import argparse
# import time
# import os
# import re
# import mimetypes
# import requests
# from functools import lru_cache
# from typing import Optional, List, Tuple, Dict
# from auth_refresh import get_dynamic_headers
# from config import BASE_URL

# # Helpful MIME additions
# mimetypes.add_type('application/pdf', '.pdf')
# mimetypes.add_type('image/jpeg', '.jpg')
# mimetypes.add_type('image/png', '.png')
# mimetypes.add_type('application/msword', '.doc')
# mimetypes.add_type('application/vnd.openxmlformats-officedocument.wordprocessingml.document', '.docx')
# mimetypes.add_type('text/csv', '.csv')

# def log(msg: str) -> None:
#     print(msg, flush=True)

# def sanitize(name: Optional[str]) -> str:
#     if not name:
#         return "Unnamed"
#     name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "", name)
#     name = re.sub(r'\s+', " ", name).strip().strip(".")
#     return name or "Unnamed"

# # -------- HTTP with retry/backoff (429-safe) ---------------------------------
# _session = requests.Session()

# def _request(method: str, url: str, **kw) -> requests.Response:
#     timeout = kw.pop("timeout", 30)
#     sleep = 0.25
#     last = None
#     for _ in range(6):
#         try:
#             headers = get_dynamic_headers()
#             r = _session.request(method, url, headers=headers, timeout=timeout, **kw)
#             if r.status_code == 401:
#                 headers = get_dynamic_headers()
#                 r = _session.request(method, url, headers=headers, timeout=timeout, **kw)
#             if r.status_code == 429:
#                 wait = float(r.headers.get("Retry-After", sleep))
#                 log(f"FV: 429 Too Many Requests → backing off {wait:.2f}s for {url}")
#                 time.sleep(wait)
#                 sleep = min(sleep * 2, 2.0)
#                 last = r
#                 continue
#             r.raise_for_status()
#             return r
#         except requests.RequestException:
#             time.sleep(sleep)
#             sleep = min(sleep * 2, 2.0)
#             last = r if 'r' in locals() else None
#             continue
#     if last is not None:
#         last.raise_for_status()
#     raise RuntimeError("Filevine request failed after retries")

# def fv_get(url: str, **kw) -> requests.Response:
#     return _request("GET", url, **kw)

# def fv_post(url: str, json_body: dict, **kw) -> requests.Response:
#     return _request("POST", url, json=json_body, **kw)

# # -------- Folder helpers -----------------------------------------------------

# @lru_cache(maxsize=2048)
# def _children_page(project_id: int, folder_id: int, offset: int, limit: int) -> List[dict]:
#     url = f"{BASE_URL}/core/folders/{folder_id}/children?projectId={project_id}&offset={offset}&limit={limit}"
#     return fv_get(url).json().get("items", [])

# def list_children(project_id: int, folder_id: int) -> List[dict]:
#     """Fetch *all* children with pagination (cached per page)."""
#     items, off = [], 0
#     while True:
#         page = _children_page(project_id, folder_id, off, 500)
#         if not page:
#             break
#         items.extend(page)
#         if len(page) < 500:
#             break
#         off += 500
#     return items

# def guess_project_root_id(project_id: int) -> Optional[int]:
#     """
#     Heuristic: call /core/folders?projectId=.. and take the most common parentId of items.
#     Typically the parent of 'Pictures', 'PD', etc.
#     """
#     url = f"{BASE_URL}/core/folders?projectId={project_id}&offset=0&limit=200"
#     items = fv_get(url).json().get("items", [])
#     parents = [ (it.get("parentId") or {}).get("native") for it in items ]
#     parents = [ int(p) for p in parents if p ]
#     if not parents:
#         return None
#     from collections import Counter
#     return Counter(parents).most_common(1)[0][0]

# def resolve_under_root(project_id: int, root_folder_id: int, subpath: str) -> Optional[int]:
#     """
#     Resolve 'subpath' (case-insensitive) under root_folder_id.
#     If subpath is empty -> returns root_folder_id.
#     """
#     if not subpath:
#         return int(root_folder_id)

#     segs = [s for s in subpath.replace("\\", "/").split("/") if s]
#     current = int(root_folder_id)

#     for seg in segs:
#         target = seg.lower()
#         found = None
#         for ch in list_children(project_id, current):
#             cid = (ch.get("folderId") or {}).get("native")
#             nm  = (ch.get("name") or "").lower()
#             if cid and nm == target:
#                 found = int(cid)
#                 break
#         if found is None:
#             return None
#         current = found

#     return current

# # -------- Document upload ----------------------------------------------------

# def register_document(file_name: str, file_size: int) -> Tuple[str, Dict]:
#     """
#     Returns (document_id, upload_info).
#     upload_info can be:
#       - {'url': 'https://...'}  → use PUT to that URL
#       - {'url': 'https://...', 'fields': {...}} → use POST multipart with given fields
#     """
#     url = f"{BASE_URL}/core/Documents"
#     payload = {
#         "fileName": file_name,
#         "length": int(file_size),
#         "contentType": mimetypes.guess_type(file_name)[0] or "application/octet-stream",
#     }
#     r = fv_post(url, payload)
#     data = r.json()
#     doc_id = str((data.get("documentId") or {}).get("native") or data.get("documentId"))
#     if not doc_id:
#         raise RuntimeError(f"Missing documentId in response: {data}")
#     # Normalize upload info shape
#     upload_info = {}
#     if "fields" in data and "url" in data:
#         upload_info = {"url": data["url"], "fields": data["fields"]}
#     elif "url" in data:
#         upload_info = {"url": data["url"]}
#     else:
#         raise RuntimeError(f"Missing upload URL in response: {data}")
#     return doc_id, upload_info

# def upload_to_signed_url(upload_info: Dict, local_path: str) -> bool:
#     """
#     Supports both AWS S3 presigned POST (form fields) and PUT (bare URL).
#     """
#     url = upload_info.get("url")
#     fields = upload_info.get("fields")
#     ctype = mimetypes.guess_type(local_path)[0] or "application/octet-stream"

#     with open(local_path, "rb") as f:
#         if fields:
#             # Presigned POST
#             # Compose multipart form where 'file' is the content field
#             files = {"file": (os.path.basename(local_path), f, ctype)}
#             rr = _session.post(url, data=fields, files=files, timeout=300)
#             return 200 <= rr.status_code < 300
#         else:
#             # Presigned PUT
#             rr = _session.put(url, data=f, headers={"Content-Type": ctype}, timeout=300)
#             return rr.status_code in (200, 204)

# def finalize_document(project_id: int, doc_id: int, file_name: str, file_size: int, folder_id: Optional[int]) -> bool:
#     """
#     Finalize and associate the uploaded content to the Filevine project.
#     Uses query param ?folderId=<resolved_id> (proven working in your tests).
#     """
#     base = f"{BASE_URL}/core/projects/{project_id}/Documents/{doc_id}"
#     url = f"{base}?folderId={int(folder_id)}" if folder_id else base

#     payload = {
#         "fileName": file_name,
#         "contentType": mimetypes.guess_type(file_name)[0] or "application/octet-stream",
#         "length": int(file_size),
#         # NOTE: Do not send FolderId in body unless your tenant requires it.
#         # Query param works consistently and avoids schema mismatches.
#     }
#     fv_post(url, payload)
#     return True

# # -------- CLI ----------------------------------------------------------------
# def main() -> int:
#     ap = argparse.ArgumentParser(description="Upload one file to Filevine at given folder path.")
#     ap.add_argument("--project-id", type=int, required=True)
#     ap.add_argument("--file", required=True, help="Full local path to file")
#     ap.add_argument("--folder-path", default="", help="Subpath under the project root (e.g. 'Pictures/Sub')")
#     ap.add_argument("--root-folder-id", type=int, default=None,
#                     help="Project ROOT folderId (parent of Pictures/PD/etc). If absent, we try to guess it.")
#     ap.add_argument("--require-resolved", action="store_true",
#                     help="Fail if subfolder cannot be resolved (no fallback to root).")
#     args = ap.parse_args()

#     project_id = int(args.project_id)
#     local_path = args.file
#     subpath    = (args.folder_path or "").strip().strip("\\/")

#     if not os.path.isfile(local_path):
#         log(f"[ERROR] File not found: {local_path}")
#         return 2

#     # Root folder id
#     root_id = int(args.root_folder_id) if args.root_folder_id else None
#     if root_id is None:
#         root_id = guess_project_root_id(project_id)
#         if root_id is None:
#             log("[ERROR] Could not determine project root folderId.")
#             return 5

#     # Resolve subpath under root
#     folder_id = resolve_under_root(project_id, root_id, subpath)
#     if folder_id is None:
#         msg = f"[WARN] Could not resolve subpath '{subpath}' under rootId={root_id}."
#         if args.require_resolved and subpath:
#             log(msg + " Aborting.")
#             return 6
#         log(msg + " Uploading to the ROOT folder.")
#         folder_id = int(root_id)

#     file_name = os.path.basename(local_path)
#     file_size = os.path.getsize(local_path)

#     doc_id, upload_info = register_document(file_name, file_size)
#     log(f"[OK] Registered docId={doc_id}")

#     if not upload_to_signed_url(upload_info, local_path):
#         log("[ERROR] Upload to signed URL failed.")
#         return 3
#     log("[OK] Content uploaded")

#     finalize_document(project_id, int(doc_id), file_name, file_size, folder_id)
#     log(f"[OK] Finalized in Filevine (folderId={folder_id})")
#     return 0

# if __name__ == "__main__":
#     raise SystemExit(main())
#!/usr/bin/env python
import argparse
import time
import os
import re
import sys
import mimetypes
import requests
from functools import lru_cache
from typing import Optional, List, Tuple, Dict
from auth_refresh import get_dynamic_headers
# from config import BASE_URL
from dotenv import load_dotenv
load_dotenv()
BASE_URL = os.getenv("BASE_URL", "https://calljacob.api.filevineapp.com")

# Helpful MIME additions
mimetypes.add_type('application/pdf', '.pdf')
mimetypes.add_type('image/jpeg', '.jpg')
mimetypes.add_type('image/png', '.png')
mimetypes.add_type('application/msword', '.doc')
mimetypes.add_type('application/vnd.openxmlformats-officedocument.wordprocessingml.document', '.docx')
mimetypes.add_type('text/csv', '.csv')

def log(msg: str, project_id: Optional[int] = None, doc_id: Optional[int] = None) -> None:
    prefix = ""
    if project_id:
        prefix += f"[Project {project_id}] "
    if doc_id:
        prefix += f"[Doc {doc_id}] "
    print(prefix + msg, flush=True)


def sanitize(name: Optional[str]) -> str:
    if not name:
        return "Unnamed"
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "", name)
    name = re.sub(r'\s+', " ", name).strip().strip(".")
    return name or "Unnamed"

# -------- HTTP with retry/backoff (429-safe) ---------------------------------
_session = requests.Session()

def _request(method: str, url: str, **kw) -> requests.Response:
    timeout = kw.pop("timeout", 30)
    sleep = 0.25
    last = None
    for _ in range(6):
        try:
            headers = get_dynamic_headers()
            r = _session.request(method, url, headers=headers, timeout=timeout, **kw)
            if r.status_code == 401:
                headers = get_dynamic_headers()
                r = _session.request(method, url, headers=headers, timeout=timeout, **kw)
            if r.status_code == 429:
                wait = float(r.headers.get("Retry-After", sleep))
                log(f"FV: 429 Too Many Requests → backing off {wait:.2f}s for {url}")
                time.sleep(wait)
                sleep = min(sleep * 2, 2.0)
                last = r
                continue
            r.raise_for_status()
            return r
        except requests.RequestException:
            time.sleep(sleep)
            sleep = min(sleep * 2, 2.0)
            last = r if 'r' in locals() else None
            continue
    if last is not None:
        last.raise_for_status()
    raise RuntimeError("Filevine request failed after retries")

def fv_get(url: str, **kw) -> requests.Response:
    return _request("GET", url, **kw)

def fv_post(url: str, json_body: dict, **kw) -> requests.Response:
    return _request("POST", url, json=json_body, **kw)

# -------- Folder helpers -----------------------------------------------------

@lru_cache(maxsize=2048)
def _children_page(project_id: int, folder_id: int, offset: int, limit: int) -> List[dict]:
    url = f"{BASE_URL}/core/folders/{folder_id}/children?projectId={project_id}&offset={offset}&limit={limit}"
    return fv_get(url).json().get("items", [])

def list_children(project_id: int, folder_id: int) -> List[dict]:
    """Fetch *all* children with pagination (cached per page)."""
    items, off = [], 0
    while True:
        page = _children_page(project_id, folder_id, off, 500)
        if not page:
            break
        items.extend(page)
        if len(page) < 500:
            break
        off += 500
    return items

def guess_project_root_id(project_id: int) -> Optional[int]:
    """
    Heuristic: call /core/folders?projectId=.. and take the most common parentId of items.
    Typically the parent of 'Pictures', 'PD', etc.
    """
    url = f"{BASE_URL}/core/folders?projectId={project_id}&offset=0&limit=200"
    items = fv_get(url).json().get("items", [])
    parents = [ (it.get("parentId") or {}).get("native") for it in items ]
    parents = [ int(p) for p in parents if p ]
    if not parents:
        return None
    from collections import Counter
    return Counter(parents).most_common(1)[0][0]

def resolve_under_root(project_id: int, root_folder_id: int, subpath: str) -> Optional[int]:
    """
    Resolve 'subpath' (case-insensitive) under root_folder_id.
    If subpath is empty -> returns root_folder_id.
    """
    if not subpath:
        return int(root_folder_id)

    segs = [s for s in subpath.replace("\\", "/").split("/") if s]
    current = int(root_folder_id)

    for seg in segs:
        target = seg.lower()
        found = None
        for ch in list_children(project_id, current):
            cid = (ch.get("folderId") or {}).get("native")
            nm  = (ch.get("name") or "").lower()
            if cid and nm == target:
                found = int(cid)
                break
        if found is None:
            return None
        current = found

    return current

# -------- Document upload ----------------------------------------------------

def register_document(file_name: str, file_size: int) -> Tuple[str, Dict]:
    """
    Returns (document_id, upload_info).
    upload_info can be:
      - {'url': 'https://...'}  → use PUT to that URL
      - {'url': 'https://...', 'fields': {...}} → use POST multipart with given fields
    """
    url = f"{BASE_URL}/core/Documents"
    payload = {
        "fileName": file_name,
        "length": int(file_size),
        "contentType": mimetypes.guess_type(file_name)[0] or "application/octet-stream",
    }
    r = fv_post(url, payload)
    data = r.json()
    doc_id = str((data.get("documentId") or {}).get("native") or data.get("documentId"))
    if not doc_id:
        raise RuntimeError(f"Missing documentId in response: {data}")
    # Normalize upload info shape
    upload_info = {}
    if "fields" in data and "url" in data:
        upload_info = {"url": data["url"], "fields": data["fields"]}
    elif "url" in data:
        upload_info = {"url": data["url"]}
    else:
        raise RuntimeError(f"Missing upload URL in response: {data}")
    return doc_id, upload_info

def upload_to_signed_url(upload_info: Dict, local_path: str) -> bool:
    """
    Supports both AWS S3 presigned POST (form fields) and PUT (bare URL).
    """
    url = upload_info.get("url")
    fields = upload_info.get("fields")
    ctype = mimetypes.guess_type(local_path)[0] or "application/octet-stream"

    with open(local_path, "rb") as f:
        if fields:
            # Presigned POST
            files = {"file": (os.path.basename(local_path), f, ctype)}
            rr = _session.post(url, data=fields, files=files, timeout=300)
            return 200 <= rr.status_code < 300
        else:
            # Presigned PUT
            rr = _session.put(url, data=f, headers={"Content-Type": ctype}, timeout=300)
            return rr.status_code in (200, 204)

def finalize_document(project_id: int, doc_id: int, file_name: str, file_size: int, folder_id: Optional[int]) -> bool:
    """
    Finalize and associate the uploaded content to the Filevine project.
    Uses query param ?folderId=<resolved_id>.
    """
    base = f"{BASE_URL}/core/projects/{project_id}/Documents/{doc_id}"
    url = f"{base}?folderId={int(folder_id)}" if folder_id else base

    payload = {
        "fileName": file_name,
        "contentType": mimetypes.guess_type(file_name)[0] or "application/octet-stream",
        "length": int(file_size),
    }
    fv_post(url, payload)
    return True

def upload_file(project_id: int, local_path: str, folder_id: int) -> str:
    """Reusable upload function: returns documentId on success."""
    file_name = os.path.basename(local_path)
    file_size = os.path.getsize(local_path)

    doc_id, upload_info = register_document(file_name, file_size)
    log(f"[OK] Registered docId={doc_id}", project_id=project_id, doc_id=doc_id)

    # retry up to 3 times for signed URL upload
    for attempt in range(3):
        if upload_to_signed_url(upload_info, local_path):
            break
        log(f"[WARN] Upload attempt {attempt+1} failed, retrying...", project_id=project_id, doc_id=doc_id)
        time.sleep(2)
    else:
        raise RuntimeError(f"Upload failed after retries for docId={doc_id}")

    log("[OK] Content uploaded", project_id=project_id, doc_id=doc_id)
    finalize_document(project_id, int(doc_id), file_name, file_size, folder_id)
    log(f"[OK] Finalized in Filevine (folderId={folder_id})", project_id=project_id, doc_id=doc_id)
    return doc_id
def lookup_project(name):
    headers = get_dynamic_headers()
    offset = 0
    limit = 100
    while True:
        url = f"{BASE_URL}/core/projects?offset={offset}&limit={limit}"
        resp = requests.get(url, headers=headers)
        if resp.status_code != 200:
            print(0)
            sys.exit(1)
        data = resp.json()
        items = data.get("items", [])
        if not items: 
            break
        for proj in items:
            if proj["projectName"].strip().lower() == name.strip().lower():
                print(proj["projectId"])
                sys.exit(0)
        offset += limit
    print(0)
    sys.exit(1)
# -------- CLI ----------------------------------------------------------------
# def main() -> int:
#     ap = argparse.ArgumentParser(description="Upload one file to Filevine at given folder path.")
#     ap.add_argument("--project-id", type=int, required=True)
#     ap.add_argument("--file", required=True, help="Full local path to file")
#     ap.add_argument("--folder-path", default="", help="Subpath under the project root (e.g. 'Pictures/Sub')")
#     ap.add_argument("--root-folder-id", type=int, default=None,
#                     help="Project ROOT folderId (parent of Pictures/PD/etc). If absent, we try to guess it.")
#     ap.add_argument("--require-resolved", action="store_true",
#                     help="Fail if subfolder cannot be resolved (no fallback to root).")
#     args = ap.parse_args()

#     # project_id = int(args.project_id)
#     project_id = int(args.project_id or os.getenv("PROJECT_ID", "0"))
#     if not project_id:
#         log("[ERROR] projectId is required (via --project-id or PROJECT_ID env var)")
#         return 1

#     local_path = args.file
#     subpath    = (args.folder_path or "").strip().strip("\\/")

#     if not os.path.isfile(local_path):
#         log(f"[ERROR] File not found: {local_path}")
#         return 2

#     # Root folder id
#     root_id = int(args.root_folder_id) if args.root_folder_id else None
#     if root_id is None:
#         root_id = guess_project_root_id(project_id)
#         if root_id is None:
#             log("[ERROR] Could not determine project root folderId.")
#             return 5

#     # Resolve subpath under root
#     folder_id = resolve_under_root(project_id, root_id, subpath)
#     if folder_id is None:
#         msg = f"[WARN] Could not resolve subpath '{subpath}' under rootId={root_id}."
#         if args.require_resolved and subpath:
#             log(msg + " Aborting.")
#             return 6
#         log(msg + " Uploading to the ROOT folder.")
#         folder_id = int(root_id)
#     doc_id = upload_file(project_id, local_path, folder_id)
#     log(f"[OK] Uploaded and finalized docId={doc_id}", project_id=project_id, doc_id=doc_id)
#     return 0
def resolve_smart_path(project_id: int, root_id: int, subpath: str) -> Optional[int]:
    """
    Try to resolve subpath under the project root. If that fails, try common variants:
    - prepend 'Documents/' if a 'Documents' tile exists
    - drop a leading 'Documents/' if present
    - try any root tile containing 'doc' in its name
    """
    # direct
    fid = resolve_under_root(project_id, root_id, subpath)
    if fid is not None:
        return fid

    # discover root tiles
    root_children = list_children(project_id, root_id)
    names = [(c.get("name") or "").strip() for c in root_children]
    names_lc = [n.lower() for n in names]

    # prepend Documents/
    if 'documents' in names_lc and not subpath.lower().startswith('documents/'):
        fid = resolve_under_root(project_id, root_id, f"Documents/{subpath}")
        if fid is not None:
            return fid

    # drop leading Documents/
    if subpath.lower().startswith('documents/'):
        fid = resolve_under_root(project_id, root_id, subpath.split('/', 1)[1])
        if fid is not None:
            return fid

    # try any tile containing 'doc'
    for n in names:
        if 'doc' in n.lower():
            fid = resolve_under_root(project_id, root_id, f"{n}/{subpath}")
            if fid is not None:
                return fid

    return None

def main() -> int:
    ap = argparse.ArgumentParser(description="Upload one file to Filevine at given folder path.")
    ap.add_argument("--project-id", type=int, required=True)
    ap.add_argument("--file", required=True, help="Full local path to file")
    ap.add_argument("--folder-path", default="", help="Subpath under the project root (e.g. 'Pictures/Sub')")
    ap.add_argument("--root-folder-id", type=int, default=None,
                    help="Project ROOT folderId (parent of Pictures/PD/etc). If absent, we try to guess it.")
    ap.add_argument("--require-resolved", action="store_true",
                    help="Fail if subfolder cannot be resolved (no fallback to root).")
    args = ap.parse_args()

    project_id = int(args.project_id or os.getenv("PROJECT_ID", "0"))
    if not project_id:
        log("[ERROR] projectId is required")
        return 1

    local_path = args.file
    subpath = (args.folder_path or "").strip().strip("\\/")

    if not os.path.isfile(local_path):
        log(f"[ERROR] File not found: {local_path}")
        return 2

    # Resolve root folder id
    root_id = int(args.root_folder_id) if args.root_folder_id else None
    if root_id is None:
        root_id = guess_project_root_id(project_id)
        if root_id is None:
            log("[ERROR] Could not determine project root folderId.")
            return 5

    # Resolve subpath to folderId
    # (old) folder_id = resolve_under_root(project_id, root_id, subpath)
    folder_id = resolve_smart_path(project_id, root_id, subpath)

    if folder_id is None:
        msg = f"[WARN] Could not resolve subpath '{subpath}' under rootId={root_id}."
        if args.require_resolved and subpath:
            log(msg + " Aborting.")
            return 6
        log(msg + " Uploading to ROOT instead.")
        folder_id = int(root_id)

    doc_id = upload_file(project_id, local_path, folder_id)
    log(f"[OK] Uploaded docId={doc_id} → folderId={folder_id}", project_id=project_id, doc_id=doc_id)
    return 0

if __name__ == "__main__":
    # Lightweight handler for the helper command used by PowerShell
    if any(arg.startswith("--lookup-project") for arg in sys.argv[1:]):
        p = argparse.ArgumentParser()
        p.add_argument("--lookup-project", required=True)
        a = p.parse_args()
        lookup_project(a.lookup_project)
        sys.exit(0)

    # Normal CLI: run the uploader
    sys.exit(main())
