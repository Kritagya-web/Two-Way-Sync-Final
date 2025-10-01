# === utils.py ===
import os
import re
import time
import random
import json
import logging
import mimetypes
from typing import Dict, List, Optional, Tuple, Set, Deque
from collections import deque
from urllib.parse import urlencode

import boto3
import requests
from botocore.exceptions import ClientError

# ---------------------------
# Logging
# ---------------------------
logger = logging.getLogger(__name__)
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter('%(levelname)s\t%(asctime)s\t%(message)s'))
    logger.addHandler(_h)
logger.setLevel(logging.INFO)

# ---------------------------
# Constants
# ---------------------------
BASE_URL  = os.getenv("FILEVINE_BASE_URL", "https://calljacob.api.filevineapp.com")
S3_BUCKET = os.getenv("S3_BUCKET", "two-way-sync")
S3_PREFIX = os.getenv("S3_PREFIX", "lojedemofolder/")  # project folder is appended to this

# Optional toggles
S3_PUBLIC_READ = os.getenv("S3_PUBLIC_READ", "false").lower() in ("1", "true", "yes")
FV_PAGE_LIMIT  = int(os.getenv("FV_PAGE_LIMIT", "500"))  # for folder/doc listings


# Helpful MIME additions
mimetypes.add_type('application/pdf', '.pdf')
mimetypes.add_type('image/jpeg', '.jpg')
mimetypes.add_type('image/png', '.png')
mimetypes.add_type('application/msword', '.doc')
mimetypes.add_type('application/vnd.openxmlformats-officedocument.wordprocessingml.document', '.docx')
mimetypes.add_type('text/csv', '.csv')

# ---------------------------
# Small helpers
# ---------------------------


def _guess_content_type(filename: str) -> str:
    ctype, _ = mimetypes.guess_type(filename)
    if ctype:
        return ctype
    fallback = {
        '.doc':  'application/msword',
        '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        '.xls':  'application/vnd.ms-excel',
        '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        '.ppt':  'application/vnd.ms-powerpoint',
        '.pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
        '.json': 'application/json',
        '.csv':  'text/csv',
        '.txt':  'text/plain',
        '.png':  'image/png',
        '.jpg':  'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.pdf':  'application/pdf',
    }
    return fallback.get(os.path.splitext(filename)[1].lower(), 'application/octet-stream')


def _to_s3_key(*parts: str) -> str:
    """Join using forward slashes, trimming any extra separators."""
    joined = "/".join(p.strip("/").replace("\\", "/") for p in parts if p is not None)
    return joined.strip("/")


def _path_levels(path: str) -> List[str]:
    """
    Expand 'A/B/C' -> ['A', 'A/B', 'A/B/C'].
    Used to create placeholders at each level.
    """
    path = path.strip("/").replace("\\", "/")
    out, acc = [], []
    for p in [p for p in path.split("/") if p]:
        acc.append(p)
        out.append("/".join(acc))
    return out


def _extract_parent_id_from_folder_payload(data: dict) -> Optional[int]:
    """
    Filevine returns the parent in a few different shapes. Normalize them.
    Returns an int folderId or None.
    """
    # Structured fields first
    for key in ("parentId", "parentFolderId", "parentFolder"):
        val = data.get(key)
        if isinstance(val, dict) and "native" in val and val["native"] is not None:
            try:
                return int(val["native"])
            except Exception:
                pass

    # Fallback: links.parent like "/folders/54224569"
    try:
        link_parent = ((data.get("links") or {}).get("parent")) or ""
        m = re.search(r"/folders/(\d+)", link_parent)
        if m:
            return int(m.group(1))
    except Exception:
        pass

    return None


class DocumentProcessor:
    """
    Full sync that mirrors Filevine’s folder structure in S3:
    - Builds folder paths by BFS over /core/folders/{id}/children (to include empty subfolders).
    - Also resolves paths for any folders referenced by documents.
    - Creates S3 placeholders for every path, then uploads docs to the exact path.
    - Single-doc webhook upload & delete supported.
    """
    def _sleep_backoff(self, attempt: int, base: float = 0.5, cap: float = 8.0):
        """
        Exponential backoff with full jitter.
        attempt: 0,1,2,...
        """
        delay = min(cap, base * (2 ** attempt)) + random.uniform(0, 0.25)
        logger.warning(f"Backing off {delay:.2f}s (attempt {attempt+1})")
        time.sleep(delay)

    def __init__(self):
        self.s3       = boto3.client("s3")
        self.bucket   = S3_BUCKET
        self.prefix   = S3_PREFIX
        self.base_url = BASE_URL

        # cache: folderId -> "full/path"
        self.folder_cache: Dict[int, str] = {}

        # HTTP session for reuse
        self.http = requests.Session()

    # ---------------------------
    # Request layer with 401 refresh
    # ---------------------------
    def _refresh_headers_inplace(self, headers: dict) -> bool:
        """
        Try to refresh OAuth headers (token) once.
        Returns True if refreshed; False otherwise.
        """
        try:
            from auth_refresh import get_dynamic_headers
        except Exception:
            logger.error("Token refresh module (auth_refresh) not available.")
            return False

        try:
            new_headers = get_dynamic_headers()
            if isinstance(new_headers, dict) and new_headers:
                headers.clear()
                headers.update(new_headers)
                logger.info("🔄 Refreshed Filevine headers after 401.")
                return True
        except Exception as e:
            logger.error(f"Failed to refresh headers: {e}")
        return False

    # def _request(self, method: str, url: str, headers: dict, **kwargs) -> requests.Response:
    #     """
    #     Make a request; on 401 once, refresh headers and retry.
    #     """
    #     try:
    #         r = self.http.request(method, url, headers=headers, **kwargs)
    #         r.raise_for_status()
    #         return r
    #     except requests.HTTPError as e:
    #         if e.response is not None and e.response.status_code == 401:
    #             logger.error(f"401 Unauthorized for {url}. Attempting token refresh…")
    #             if self._refresh_headers_inplace(headers):
    #                 r2 = self.http.request(method, url, headers=headers, **kwargs)
    #                 r2.raise_for_status()
    #                 return r2
    #         raise
    def _request(self, method: str, url: str, headers: dict, **kwargs) -> requests.Response:
        """
        Make a request with retries:
        - On 401: refresh headers once, then retry immediately
        - On 429/5xx or network errors: exponential backoff + jitter
        """
        MAX_RETRIES = 5
        attempt = 0
        refreshed = False

        while True:
            try:
                r = self.http.request(method, url, headers=headers, **kwargs)
                r.raise_for_status()
                return r
            except requests.HTTPError as e:
                code = e.response.status_code if e.response is not None else 0

                # One-time token refresh on 401
                if code == 401 and not refreshed:
                    logger.error(f"401 Unauthorized for {url}. Attempting token refresh…")
                    refreshed = self._refresh_headers_inplace(headers)
                    if refreshed:
                        continue  # try again immediately with new headers

                # Backoff on 429 / 5xx
                if code == 429 or 500 <= code < 600:
                    if attempt >= MAX_RETRIES:
                        raise
                    self._sleep_backoff(attempt)
                    attempt += 1
                    continue

                # Other HTTP errors: bubble up
                raise
            except (requests.ConnectionError, requests.Timeout) as e:
                if attempt >= MAX_RETRIES:
                    raise
                self._sleep_backoff(attempt)
                attempt += 1
                continue

    def _get(self, url: str, headers: dict, timeout: int = 15) -> requests.Response:
        return self._request("GET", url, headers, timeout=timeout)

    def _post(self, url: str, headers: dict, json_body: dict, timeout: int = 15) -> requests.Response:
        return self._request("POST", url, headers, json=json_body, timeout=timeout)

    # ---------------------------
    # Project & folder structure
    # ---------------------------
    def extract_project_id(self, body: dict) -> int:
        try:
            raw = (
                body.get("projectId")
                or body.get("ProjectId")
                or body.get("payload", {}).get("projectId")
                or body.get("recordId")
            )
            pid = int(raw) if raw is not None else 0
            return pid
        except Exception as e:
            logger.error(f"Failed to extract projectId: {e}")
            return 0

    def sanitize(self, name: Optional[str]) -> str:
        if not name:
            return "Unnamed"
        # Remove illegal + control chars; normalize spaces/dots
        name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "", name)
        name = re.sub(r'\s+', " ", name).strip().strip(".")
        return name or "Unnamed"

    def get_project_name(self, project_id: int, headers: dict) -> str:
        try:
            url = f"{self.base_url}/core/projects/{project_id}"
            r = self._get(url, headers=headers, timeout=10)
            name = self.sanitize(r.json().get("projectOrClientName", f"Project_{project_id}"))
            logger.info(f"Resolved project {project_id} name: {name}")
            return name
        except Exception as e:
            logger.error(f"Failed to fetch project name: {e}")
            return f"Project_{project_id}"

    def _fetch_root_folders(self, project_id: int, headers: dict) -> List[int]:
        roots: List[int] = []
        offset, limit = 0, 500
        while True:
            url = f"{self.base_url}/core/folders?projectId={project_id}&offset={offset}&limit={limit}"
            try:
                r = self._get(url, headers=headers)
                payload = r.json()
                for item in payload.get("items", []):
                    fid = (item.get("folderId") or {}).get("native")
                    if fid:
                        roots.append(int(fid))
                if not payload.get("hasMore", False):
                    break
                offset += limit
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"Failed to fetch root folders (offset={offset}): {e}")
                break
        return roots

    def _get_folder_info(self, folder_id: int, headers: dict) -> Tuple[str, Optional[int]]:
        """
        Return (name, parent_id) for the folder.
        """
        url = f"{self.base_url}/core/folders/{folder_id}"
        r = self._get(url, headers=headers, timeout=10)
        data = r.json()
        name = self.sanitize(data.get("name", "Unnamed"))
        parent_id = _extract_parent_id_from_folder_payload(data)
        return name, parent_id

    # def resolve_folder_path(self, folder_id: Optional[int], headers: dict, fallback: str = "Documents") -> str:
    #     """
    #     Resolve full folder path (A/B/C) for a given folderId by walking parents.
    #     Caches results. Falls back to `fallback` if fetch fails.
    #     """
    #     if not folder_id:
    #         return self.sanitize(fallback)
    #     if folder_id in self.folder_cache:
    #         return self.folder_cache[folder_id]

    #     try:
    #         name, parent_id = self._get_folder_info(folder_id, headers)
    #     except Exception as e:
    #         logger.error(f"Cannot fetch folder {folder_id}: {e}; falling back to '{fallback}'")
    #         full = self.sanitize(fallback)
    #         self.folder_cache[folder_id] = full
    #         return full

    #     if parent_id:
    #         parent_path = self.resolve_folder_path(parent_id, headers, fallback="")
    #         full = _to_s3_key(parent_path, name) if parent_path else name
    #     else:
    #         full = name

    #     self.folder_cache[folder_id] = full
    #     logger.info(f"Resolved folderId {folder_id} -> '{full}'")
    #     return full

    def resolve_folder_path(self, folder_id: Optional[int], headers: dict,
                            fallback: str = "Documents", *,
                            strict: bool = False) -> str:
        """
        Resolve full folder path (A/B/C).
        - strict=True: never return a guessed fallback; bubble errors so caller can retry.
        - strict=False: return sanitized fallback on failure (but DO NOT cache it).
        """
        if not folder_id:
            return self.sanitize(fallback)

        if folder_id in self.folder_cache:
            return self.folder_cache[folder_id]

        try:
            name, parent_id = self._get_folder_info(folder_id, headers)
        except Exception as e:
            logger.error(f"Cannot fetch folder {folder_id}: {e}")
            if strict:
                # Signal caller to retry instead of misplacing
                raise
            # Return a non-cached fallback
            return self.sanitize(fallback)

        if parent_id:
            parent_path = self.resolve_folder_path(parent_id, headers, fallback="", strict=strict)
            full = _to_s3_key(parent_path, name) if parent_path else name
        else:
            full = name

        # Only cache resolved, non-fallback paths
        self.folder_cache[int(folder_id)] = full
        logger.info(f"Resolved folderId {folder_id} -> '{full}'")
        return full


    def enumerate_all_folders(self, project_id: int, headers: dict) -> Set[str]:
        """
        BFS over the folder tree using /core/folders/{id}/children to discover ALL subfolders,
        including empty ones. Names are resolved via /core/folders/{id}.
        Returns a set of full folder paths (e.g., {'Discovery/To Client/Responses', ...}).
        """
        roots = self._fetch_root_folders(project_id, headers)
        if not roots:
            return set()

        # queue of (folder_id)
        q: Deque[int] = deque(roots)

        # ensure each root has a resolved path (this will walk parents if needed)
        for fid in roots:
            self.resolve_folder_path(fid, headers)

        visited: Set[int] = set()
        paths: Set[str]   = set(self.folder_cache.values())

        while q:
            fid = q.popleft()
            if fid in visited:
                continue
            visited.add(fid)

            # children listing
            offset, limit = 0, 500
            while True:
                url = f"{self.base_url}/core/folders/{fid}/children?projectId={project_id}&offset={offset}&limit={limit}"
                try:
                    r = self._get(url, headers=headers, timeout=15)
                    payload = r.json()
                except Exception as e:
                    logger.error(f"Failed to fetch children of folder {fid}: {e}")
                    break

                items = payload.get("items", [])
                if not items:
                    break

                for child in items:
                    cid = (child.get("folderId") or {}).get("native")
                    if not cid:
                        continue
                    cid = int(cid)

                    child_path = self.resolve_folder_path(cid, headers, fallback="Documents")
                    if child_path:
                        paths.add(child_path)

                    if cid not in visited:
                        q.append(cid)

                if not payload.get("hasMore", False):
                    break
                offset += limit
                time.sleep(0.1)

        logger.info(f"Folder tree size: {len(paths)}")
        return paths

    # def fetch_complete_folder_structure(self, project_id: int, headers: dict) -> Dict[int, str]:
    #     """
    #     Build {folderId -> full_path} for every folder using parent→child traversal.
    #     No per-folder documents call.
    #     """
    #     folder_map: Dict[int, str] = {}
    #     logger.info(f"🔄 Fetching complete structure for project {project_id}")

    #     # get roots
    #     offset, limit = 0, 500
    #     root_ids: List[int] = []
    #     while True:
    #         url = f"{self.base_url}/core/folders?projectId={project_id}&offset={offset}&limit={limit}"
    #         try:
    #             r = self._get(url, headers=headers, timeout=15)
    #             data = r.json()
    #         except Exception as e:
    #             logger.error(f"⚠️ Failed to fetch root folders: {e}")
    #             break

    #         for f in data.get("items", []):
    #             fid = (f.get("folderId") or {}).get("native")
    #             if fid:
    #                 root_ids.append(int(fid))

    #         if not data.get("hasMore", False):
    #             break
    #         offset += limit

    #     # resolve each root's full path (walk parents) and BFS into children
    #     q = deque()
    #     for fid in root_ids:
    #         try:
    #             full_path = self.resolve_folder_path(fid, headers, fallback="Documents")
    #             folder_map[fid] = full_path
    #             q.append(fid)
    #             logger.info(f"📁 ROOT {fid} -> '{full_path}'")
    #         except Exception as e:
    #             logger.error(f"⚠️ Cannot resolve root folder {fid}: {e}")

    #     # BFS children
    #     while q:
    #         parent_id = q.popleft()
    #         parent_path = folder_map.get(parent_id, "")
    #         offset = 0
    #         while True:
    #             url = f"{self.base_url}/core/folders/{parent_id}/children?projectId={project_id}&offset={offset}&limit=500"
    #             try:
    #                 c_res = self._get(url, headers=headers, timeout=15)
    #                 payload = c_res.json()
    #             except Exception as e:
    #                 logger.error(f"⚠️ Cannot fetch children of folder {parent_id}: {e}")
    #                 break

    #             for child in payload.get("items", []):
    #                 cid = (child.get("folderId") or {}).get("native")
    #                 if not cid:
    #                     continue
    #                 cid = int(cid)

    #                 # prefer child name from /children; fall back to /core/folders/{id}
    #                 cname = child.get("name")
    #                 if not cname:
    #                     try:
    #                         info = self._get(f"{self.base_url}/core/folders/{cid}", headers=headers, timeout=15)
    #                         cname = info.json().get("name", "Unnamed")
    #                     except Exception as e:
    #                         logger.error(f"⚠️ Cannot resolve child {cid} name: {e}")
    #                         continue

    #                 cname = self.sanitize(cname)
    #                 # Ensure we have the parent's full path. If missing (pagination/out-of-order),
    #                 # resolve it from the API rather than guessing (‘+1’ is NOT reliable).
    #                 if not parent_path:
    #                     try:
    #                         # Parent path might not be in folder_map yet; climb parents on-demand.
    #                         # This uses /core/folders/{id} to walk up via links/parentId.
    #                         parent_path = self.resolve_folder_path(parent_id, headers, fallback="")
    #                         # backfill for future siblings:
    #                         if parent_id and parent_path:
    #                             folder_map[parent_id] = parent_path
    #                     except Exception as e:
    #                         logger.error(f"⚠️ Could not resolve parent path for {parent_id}: {e}")
    #                         parent_path = ""
    #                 full_path = f"{parent_path}/{cname}" if parent_path else cname
    #                 folder_map[cid] = full_path
    #                 q.append(cid)
    #                 logger.info(f"  ↳ CHILD {cid} -> '{full_path}' (parent {parent_id}: '{parent_path}')")
                
    #             if not payload.get("hasMore", False):
    #                 break
    #             offset += 500

    #     logger.info(f"📊 Structure fetch complete: {len(folder_map)} folders")
    #     return folder_map
    
    def fetch_complete_folder_structure(self, project_id: int, headers: dict) -> Dict[int, str]:
        """
        Preferred: BFS from roots.
        Fallback: if roots cannot be listed (e.g., 429), derive folder paths
                by resolving unique folderIds seen in /core/documents.
        """
        folder_map: Dict[int, str] = {}
        logger.info(f"🔄 Fetching complete structure for project {project_id}")

        # --- Try to fetch roots with retries handled in _request ---
        offset, limit = 0, 500
        root_ids: List[int] = []
        while True:
            url = f"{self.base_url}/core/folders?projectId={project_id}&offset={offset}&limit={limit}"
            try:
                r = self._get(url, headers=headers, timeout=15)
                data = r.json()
            except Exception as e:
                logger.error(f"⚠️ Failed to fetch root folders (offset={offset}): {e}")
                break

            for f in data.get("items", []):
                fid = (f.get("folderId") or {}).get("native")
                if fid:
                    root_ids.append(int(fid))

            if not data.get("hasMore", False):
                break
            offset += limit
            time.sleep(0.1)  # gentle throttle

        # If we got roots, do the normal BFS
        if root_ids:
            q = deque()
            for fid in root_ids:
                try:
                    full_path = self.resolve_folder_path(fid, headers, fallback="Documents")
                    folder_map[fid] = full_path
                    q.append(fid)
                    logger.info(f"📁 ROOT {fid} -> '{full_path}'")
                except Exception as e:
                    logger.error(f"⚠️ Cannot resolve root folder {fid}: {e}")

            while q:
                parent_id = q.popleft()
                parent_path = folder_map.get(parent_id, "")
                offset = 0
                while True:
                    url = f"{self.base_url}/core/folders/{parent_id}/children?projectId={project_id}&offset={offset}&limit=500"
                    try:
                        c_res = self._get(url, headers=headers, timeout=15)
                        payload = c_res.json()
                    except Exception as e:
                        logger.error(f"⚠️ Cannot fetch children of folder {parent_id}: {e}")
                        break

                    for child in payload.get("items", []):
                        cid = (child.get("folderId") or {}).get("native")
                        if not cid:
                            continue
                        cid = int(cid)

                        cname = child.get("name")
                        if not cname:
                            try:
                                info = self._get(f"{self.base_url}/core/folders/{cid}", headers=headers, timeout=15)
                                cname = info.json().get("name", "Unnamed")
                            except Exception as e:
                                logger.error(f"⚠️ Cannot resolve child {cid} name: {e}")
                                continue

                        cname = self.sanitize(cname)
                        if not parent_path:
                            try:
                                parent_path = self.resolve_folder_path(parent_id, headers, fallback="")
                                if parent_id and parent_path:
                                    folder_map[parent_id] = parent_path
                            except Exception as e:
                                logger.error(f"⚠️ Could not resolve parent path for {parent_id}: {e}")
                                parent_path = ""
                        full_path = f"{parent_path}/{cname}" if parent_path else cname
                        folder_map[cid] = full_path
                        q.append(cid)

                    if not payload.get("hasMore", False):
                        break
                    offset += 500
                    time.sleep(0.1)  # gentle throttle

            logger.info(f"📊 Structure fetch complete: {len(folder_map)} folders")
            return folder_map

        # --- Fallback: derive structure from documents ---
        logger.warning("Root folder listing unavailable; deriving structure from documents list.")
        try:
            docs = self.fetch_all_documents(project_id, headers)
        except Exception as e:
            logger.error(f"Fallback failed: cannot list documents: {e}")
            return {}

        unique_fids: Set[int] = set()
        for d in docs:
            fid = d.get("folder_id")
            if fid:
                unique_fids.add(int(fid))

        cache: Dict[int, str] = {}
        for fid in unique_fids:
            path = self.resolve_folder_path(fid, headers, cache.get(fid) or "Documents")
            if path:
                folder_map[fid] = path
                cache[fid] = path

        logger.info(f"📊 Fallback structure built from documents: {len(folder_map)} folders")
        return folder_map



    # ---------------------------
    # Documents
    # ---------------------------
    def fetch_all_documents(self, project_id: int, headers: dict) -> List[dict]:
        """
        Use /core/documents?projectId=... to gather all docs.
        Each item -> {id, filename, size, folder_id, folder_name, uploadDate}
        """
        docs, offset, limit = [], 0, 200
        logger.info(f"📥 Fetching all project documents via /core/documents?projectId={project_id}")

        while True:
            url = f"{self.base_url}/core/documents?projectId={project_id}&offset={offset}&limit={limit}"
            try:
                r = self._get(url, headers=headers, timeout=20)
                data = r.json()
            except Exception as e:
                logger.error(f"⚠️ Failed to list documents (offset={offset}): {e}")
                break
            batch = data.get("items", [])
            for d in batch:
                doc_id = (d.get("documentId") or {}).get("native")
                if not doc_id:
                    continue
                docs.append({
                    "id": int(doc_id),
                    "filename": self.sanitize(d.get("filename", "unnamed")),
                    "size": d.get("size", 0),
                    "folder_id": (d.get("folderId") or {}).get("native"),
                    "folder_name": d.get("folderName"),  # last segment, useful as fallback
                    "modified": d.get("modifiedDate") or d.get("uploadDate")
                })

            logger.info(f"Fetched {len(batch)} documents at offset {offset}")
            if not data.get("hasMore", False):
                break
            offset += limit
            time.sleep(0.1)
        logger.info(f"📦 Total documents collected: {len(docs)}")
        return docs

    def resolve_path_via_parents(self, folder_id: int, headers: dict, cache: dict) -> Optional[str]:
        """
        Build 'A/B/.../Z' by climbing parents using /core/folders/{id}.
        Uses in-memory cache to minimize API calls.
        """
        if not folder_id:
            return None
        fid = int(folder_id)
        if fid in cache:
            return cache[fid]

        chain = []
        cursor = fid
        while cursor:
            if cursor in cache:
                chain.append(cache[cursor])
                break
            try:
                r = self._get(f"{self.base_url}/core/folders/{cursor}", headers=headers, timeout=15)
                f = r.json()
            except Exception as e:
                logger.error(f"⚠️ Cannot fetch folder {cursor}: {e}")
                return None
            name = self.sanitize(f.get("name", "Unnamed"))
            chain.append(name)
            cursor = _extract_parent_id_from_folder_payload(f)

        full_path = "/".join(reversed(chain))
        cache[fid] = full_path
        logger.info(f"Resolved folderId {fid} via parents -> '{full_path}'")
        return full_path

    def ensure_all_folders_and_map_docs(self, project_prefix: str, folder_map: Dict[int, str],
                                        documents: List[dict], headers: dict):
        """
        Returns (folder_paths_set, docs_with_paths)
        - folder_paths_set: every folder path we must materialize in S3
        - docs_with_paths: docs annotated with 'folder_path'
        """
        folder_paths = set(folder_map.values())
        cache = dict(folder_map)  # seed cache with known mappings {id: path}
        docs_out = []

        for d in documents:
            fid = d.get("folder_id")
            path = None
            if fid:
                # 1) BFS map result
                path = folder_map.get(int(fid))
                # 2) if missing, climb parents on-demand
                if not path:
                    path = self.resolve_path_via_parents(int(fid), headers, cache)
            # 3) if still missing (rare), use last segment or 'Documents'
            if not path:
                path = self.sanitize(d.get("folder_name") or "Documents")

            d2 = dict(d)
            d2["folder_path"] = path
            docs_out.append(d2)
            folder_paths.add(path)

        # Always include a bucket for loose docs
        folder_paths.add("Documents")

        # # Create placeholders for every folder (leaf)
        # for path in sorted(folder_paths):
        #     key = f"{project_prefix}{path}/.placeholder"
        #     try:
        #         self.s3.put_object(Bucket=self.bucket, Key=key, Body=b'')
        #         logger.info(f"Created folder placeholder: s3://{self.bucket}/{key}")
        #     except Exception as e:
        #         logger.error(f"Failed to create placeholder for {path}: {e}")

        return folder_paths, docs_out

    # def get_download_links_batch(self, ids: List[int], headers: dict) -> Dict[int, str]:
    #     """
    #     Get download links. Try batch; fallback to single for misses.
    #     """
    #     out: Dict[int, str] = {}
    #     if not ids:
    #         return out

    #     url = f"{self.base_url}/core/documents/batch/download"
    #     body = {"DocumentIds": ids, "DownloadUrlTimeToLive": 600}

    #     try:
    #         r = self._post(url, headers=headers, json_body=body, timeout=15)
    #         payload = r.json()
    #         if isinstance(payload, list):
    #             for i, item in enumerate(payload):
    #                 link = (item or {}).get("downloadLink")
    #                 if link:
    #                     out[ids[i]] = link
    #     except Exception as e:
    #         logger.error(f"Batch download failed for {len(ids)} docs: {e}")

    #     # Fallback for any missing docIds
    #     missing = [d for d in ids if d not in out]
    #     for doc_id in missing:
    #         try:
    #             r = self._post(url, headers=headers,
    #                            json_body={"DocumentIds": [doc_id], "DownloadUrlTimeToLive": 600},
    #                            timeout=15)
    #             arr = r.json() if r.ok else []
    #             if isinstance(arr, list) and arr and arr[0].get("downloadLink"):
    #                 out[doc_id] = arr[0]["downloadLink"]
    #             else:
    #                 logger.error(f"No download link for doc {doc_id}")
    #         except Exception as e:
    #             logger.error(f"Single download link fetch failed for doc {doc_id}: {e}")
    #     return out

    def get_download_links_batch(self, ids: List[int], headers: dict) -> Dict[int, str]:
        """
        Robust download-link fetch:
        - Splits requests into small chunks to reduce 429s
        - Retries 429/5xx with exponential backoff + jitter
        - Falls back to single-doc batch calls (still the batch endpoint) with backoff
        """
        out: Dict[int, str] = {}
        if not ids:
            return out

        endpoint = f"{self.base_url}/core/documents/batch/download"
        CHUNK_SIZE = 10          # keep small to avoid rate limits
        MAX_RETRIES = 5          # exponential backoff attempts
        TTL_SECONDS = 600

        def post_batch(doc_ids: List[int]) -> Optional[List[dict]]:
            attempt = 0
            while attempt <= MAX_RETRIES:
                try:
                    r = self._post(
                        endpoint,
                        headers=headers,
                        json_body={"DocumentIds": doc_ids, "DownloadUrlTimeToLive": TTL_SECONDS},
                        timeout=20
                    )
                    payload = r.json()
                    if isinstance(payload, list):
                        return payload
                    logger.error(f"Unexpected batch payload shape for ids={doc_ids[:3]}...: {payload}")
                    return None
                except requests.HTTPError as e:
                    code = e.response.status_code if e.response is not None else 0
                    if code == 429 or 500 <= code < 600:
                        logger.error(f"Batch {doc_ids[:3]}... attempt {attempt} failed with {code}; backing off")
                        self._sleep_backoff(attempt)
                        attempt += 1
                        continue
                    logger.error(f"Batch request failed with {code}: {e}")
                    return None
                except Exception as e:
                    logger.error(f"Batch request exception: {e}")
                    self._sleep_backoff(attempt)
                    attempt += 1
            return None

        # 1) Chunked batches
        for i in range(0, len(ids), CHUNK_SIZE):
            chunk = ids[i:i + CHUNK_SIZE]
            payload = post_batch(chunk)
            if payload and isinstance(payload, list):
                for idx, item in enumerate(payload):
                    link = (item or {}).get("downloadLink")
                    if link:
                        out[chunk[idx]] = link
            else:
                logger.error(f"Batch download failed for chunk starting at index {i}; will fallback per-doc")

        # 2) Fallback per-doc with backoff
        missing = [d for d in ids if d not in out]
        for doc_id in missing:
            attempt = 0
            while attempt <= MAX_RETRIES:
                try:
                    r = self._post(
                        endpoint,
                        headers=headers,
                        json_body={"DocumentIds": [doc_id], "DownloadUrlTimeToLive": TTL_SECONDS},
                        timeout=15
                    )
                    arr = r.json() if r.ok else []
                    if isinstance(arr, list) and arr and arr[0].get("downloadLink"):
                        out[doc_id] = arr[0]["downloadLink"]
                        break
                    logger.error(f"No download link for doc {doc_id} (attempt {attempt})")
                    self._sleep_backoff(attempt)
                    attempt += 1
                except requests.HTTPError as e:
                    code = e.response.status_code if e.response is not None else 0
                    if code == 429 or 500 <= code < 600:
                        logger.error(f"Single-doc {doc_id} attempt {attempt} got {code}; backing off")
                        self._sleep_backoff(attempt)
                        attempt += 1
                        continue
                    logger.error(f"Single-doc {doc_id} failed with {code}: {e}")
                    break
                except Exception as e:
                    logger.error(f"Single-doc {doc_id} exception: {e}")
                    self._sleep_backoff(attempt)
                    attempt += 1

        return out


    # ---------------------------
    # S3 ops
    # ---------------------------
    def ensure_placeholders(self, project_prefix: str, folder_paths: Set[str]) -> None:
        """
        Create zero-byte placeholders for every folder path and each of its parent levels.
        """
        all_levels: Set[str] = set()
        for p in folder_paths:
            for lvl in _path_levels(p):
                all_levels.add(lvl)

        for rel in sorted(all_levels):
            key = _to_s3_key(project_prefix, rel, ".placeholder")
            try:
                self.s3.head_object(Bucket=self.bucket, Key=key)
                logger.info(f"S3 folder exists: s3://{self.bucket}/{key}")
            except ClientError as e:
                if e.response.get("Error", {}).get("Code") == "404":
                    self.s3.put_object(Bucket=self.bucket, Key=key, Body=b"")
                    logger.info(f"Created folder placeholder: s3://{self.bucket}/{key}")
                else:
                    logger.error(f"head_object error for {key}: {e}")
    
    def upload_to_s3(self, key: str, content: bytes, filename: str,
                     metadata: Optional[dict] = None, tags: Optional[dict] = None) -> bool:
        try:
            content_type = _guess_content_type(filename)
            disposition  = 'inline' if content_type.startswith('image/') else 'attachment'
            meta   = {k: str(v) for k, v in (metadata or {}).items()}
            tagstr = urlencode({k: str(v) for k, v in (tags or {}).items()}) if tags else None

            kwargs = {
                "Bucket": self.bucket,
                "Key": key,
                "Body": content,
                "ContentType": content_type,
                "ContentDisposition": f'{disposition}; filename="{filename}"',
                "Metadata": meta
            }
            if tagstr:
                kwargs["Tagging"] = tagstr

            logger.info(f"Uploading → s3://{self.bucket}/{key} (ContentType={content_type})")
            self.s3.put_object(**kwargs)
            logger.info(f"✅ Uploaded: s3://{self.bucket}/{key}")
            return True
        except Exception as e:
            logger.error(f"❌ Upload failed for s3://{self.bucket}/{key}: {e}")
            return False

    # ---------------------------
    # Full sync (folders first, then docs)
    # ---------------------------
    def sync_documents(self, project_id: int, headers: dict):
        project_name   = self.get_project_name(project_id, headers)
        project_prefix = f"{S3_PREFIX}{self.sanitize(project_name)}/"
        logger.info(f"Starting full sync for project {project_id} -> prefix {project_prefix}")

        # Build folder tree, get docs
        folder_map = self.fetch_complete_folder_structure(project_id, headers)
        documents  = self.fetch_all_documents(project_id, headers)

        # Materialize folders and attach exact paths to docs
        # _, docs_with_paths = self.ensure_all_folders_and_map_docs(project_prefix, folder_map, documents, headers)
        # Materialize folders and attach exact paths to docs
        folder_paths, docs_with_paths = self.ensure_all_folders_and_map_docs(
            project_prefix, folder_map, documents, headers
        )

        # Create placeholders for every path and ALL parent levels
        self.ensure_placeholders(project_prefix, folder_paths)

        if not docs_with_paths:
            logger.info("No documents to upload.")
            result = {
                "status": "success",
                "projectId": project_id,
                "projectName": project_name,
                "documentCount": 0,
                "uploadedCount": 0,
                "failedCount": 0
            }
            logger.info(f"Full sync complete: {result}")
            return result

        # Upload each doc to its exact path
        uploaded = failed = 0
        ids = [d["id"] for d in docs_with_paths]
        link_by_id = self.get_download_links_batch(ids, headers)

        for d in docs_with_paths:
            doc_id = d["id"]
            filename = d["filename"]
            folder_path = d["folder_path"]
            s3_key = f"{project_prefix}{folder_path}/{filename}"

            url = link_by_id.get(doc_id)
            if not url:
                logger.error(f"No download link for doc {doc_id} ({filename}); doc={json.dumps(d)}")
                failed += 1
                continue

            # Gentle client-side throttle
            time.sleep(0.1)

            # Retry the file GET on transient errors
            get_attempt = 0
            MAX_GET_RETRIES = 4
            try:
                while True:
                    try:
                        resp = self.http.get(url, timeout=30)
                        resp.raise_for_status()
                        break
                    except requests.HTTPError as e:
                        code = e.response.status_code if e.response is not None else 0
                        if code in (429,) or 500 <= code < 600:
                            self._sleep_backoff(get_attempt)
                            get_attempt += 1
                            if get_attempt > MAX_GET_RETRIES:
                                raise
                            continue
                        raise
                    except Exception as e:
                        # network timeouts or other transient errors
                        self._sleep_backoff(get_attempt)
                        get_attempt += 1
                        if get_attempt > MAX_GET_RETRIES:
                            raise
                        continue
            except Exception as e:
                logger.error(f"Download failed for doc {doc_id} ({filename}): {e}")
                failed += 1
                continue


            ok = self.upload_to_s3(
                s3_key,
                resp.content,
                filename,
                metadata={
                    "documentId": str(doc_id),
                    "projectId": str(project_id),
                    "folderId": str(d.get("folder_id") or ""),
                    "folderPath": folder_path
                },
                tags={"origin": "filevine", "fv_docid": str(doc_id), "projectId": str(project_id)}
            )
            # if ok and S3_PUBLIC_READ:
            #     try:
            #         self.s3.put_object_acl(Bucket=self.bucket, Key=s3_key, ACL="public-read")
            #     except ClientError:
            #         pass
            #     uploaded += 1
            # else:
            #     failed += 1
            if ok:
                if S3_PUBLIC_READ:
                    try:
                        self.s3.put_object_acl(Bucket=self.bucket, Key=s3_key, ACL="public-read")
                    except ClientError:
                        pass
                uploaded += 1
            else:
                failed += 1



        result = {
            "status": "success",
            "projectId": project_id,
            "projectName": project_name,
            "documentCount": len(docs_with_paths),
            "uploadedCount": uploaded,
            "failedCount": failed
        }
        logger.info(f"Full sync complete: {result}")
        return result

    # ---------------------------
    # Webhook: single upload & delete
    # ---------------------------
    def handle_single_document_upload(self, body: dict, headers: dict):
        try:
            raw = body.get("documentId") or body.get("DocumentId")
            if raw is None:
                return self.error_response(400, "Missing document ID")
            document_id = raw.get("native") if isinstance(raw, dict) else int(raw)

            project_id    = self.extract_project_id(body)
            project_name  = self.get_project_name(project_id, headers)
            project_prefix = _to_s3_key(self.prefix, project_name) + "/"

            # Fetch doc metadata
            meta_url = f"{self.base_url}/core/documents/{document_id}"
            r = self._get(meta_url, headers=headers, timeout=10)
            doc = r.json()

            filename    = self.sanitize(doc.get("filename") or f"document_{document_id}")
            folder_id   = (doc.get("folderId") or {}).get("native")
            folder_nm   = self.sanitize(doc.get("folderName") or "Documents")
            # Try to strictly resolve the full path with backoff
            attempt = 0
            while True:
                try:
                    folder_path = self.resolve_folder_path(folder_id, headers, fallback=folder_nm, strict=True)
                    break
                except Exception:
                    if attempt >= 5:
                        # Make it retryable – do not upload to a guessed folder
                        return self.error_response(503, "Rate-limited resolving folder path; please retry")
                    self._sleep_backoff(attempt)
                    attempt += 1

            # Ensure all levels exist and upload
            self.ensure_placeholders(project_prefix, {folder_path})
            # folder_path = self.resolve_folder_path(folder_id, headers, fallback=folder_nm)

            # Ensure folder exists (all levels)
            # self.ensure_placeholders(project_prefix, {folder_path})

            # Download link
            links = self.get_download_links_batch([document_id], headers)
            url = links.get(document_id)
            if not url:
                return self.error_response(502, f"No download link for document {document_id}")

            binr = self.http.get(url, timeout=30)
            binr.raise_for_status()

            s3_key = _to_s3_key(project_prefix, folder_path, filename)
            logger.info(f"Single upload → '{filename}' → s3://{self.bucket}/{s3_key}")

            ok = self.upload_to_s3(
                s3_key,
                binr.content,
                filename,
                metadata={
                    "documentId": document_id,
                    "projectId": project_id,
                    "folderId": folder_id or "",
                    "folderPath": folder_path
                },
                tags={"origin": "filevine", "fv_docid": document_id, "projectId": project_id}
            )
            if ok and S3_PUBLIC_READ:
                try:
                    self.s3.put_object_acl(Bucket=self.bucket, Key=s3_key, ACL="public-read")
                except ClientError:
                    pass
                return self.success_response({"s3Key": s3_key})

            return self.error_response(500, "Failed to upload to S3")
        except Exception as e:
            logger.error(f"Single-document upload failed: {e}")
            return self.error_response(500, "Internal server error")

    def handle_document_upload(self, body: dict, headers: dict):
        """
        Alias for single-document upload events coming from the router.
        Delegates to handle_single_document_upload to keep backward compatibility.
        """
        logger.info("handle_document_upload → delegating to handle_single_document_upload")
        return self.handle_single_document_upload(body, headers)

    def find_keys_by_docid(self, project_prefix: str, doc_id: int) -> List[str]:
        matches: List[str] = []
        token, target = None, str(doc_id)
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": project_prefix, "MaxKeys": 1000}
            if token:
                kwargs["ContinuationToken"] = token
            page = self.s3.list_objects_v2(**kwargs)

            for obj in page.get("Contents", []):
                k = obj["Key"]
                if k.endswith("/.placeholder"):
                    continue
                try:
                    t = self.s3.get_object_tagging(Bucket=self.bucket, Key=k)
                    tagset = {d["Key"]: d["Value"] for d in t.get("TagSet", [])}
                    if tagset.get("fv_docid") == target:
                        matches.append(k)
                        continue
                except ClientError as e:
                    logger.error(f"get_object_tagging failed for {k}: {e}")
                try:
                    h = self.s3.head_object(Bucket=self.bucket, Key=k)
                    meta = {(mk or "").lower(): mv for mk, mv in (h.get("Metadata") or {}).items()}
                    if meta.get("documentid") == target:
                        matches.append(k)
                except ClientError as e:
                    logger.error(f"head_object failed for {k}: {e}")

            if page.get("IsTruncated"):
                token = page.get("NextContinuationToken")
            else:
                break
        return matches

    def handle_document_delete(self, body: dict, headers: dict):
        try:
            raw = body.get("documentId") or body.get("DocumentId")
            if raw is None:
                return self.error_response(400, "Missing document ID")
            document_id = raw.get("native") if isinstance(raw, dict) else int(raw)

            project_id    = self.extract_project_id(body)
            project_name  = self.get_project_name(project_id, headers)
            project_prefix = _to_s3_key(self.prefix, project_name) + "/"

            keys = self.find_keys_by_docid(project_prefix, document_id)
            if not keys:
                logger.info(f"No S3 objects found for deleted doc {document_id} (project {project_id})")
                return self.success_response({"status": "not_found", "projectId": project_id, "documentId": document_id})

            deleted = []
            for k in keys:
                try:
                    self.s3.delete_object(Bucket=self.bucket, Key=k)
                    logger.info(f"Deleted S3 object: s3://{self.bucket}/{k}")
                    deleted.append(k)
                except ClientError as e:
                    logger.error(f"Failed to delete {k}: {e}")

            return self.success_response({"status": "deleted", "projectId": project_id, "documentId": document_id, "deletedKeys": deleted})
        except Exception as e:
            logger.error(f"Document delete handler failed: {e}")
            return self.error_response(500, "Internal server error")

    # ---------------------------
    # Response helpers
    # ---------------------------
    def success_response(self, data: dict):
        return {"statusCode": 200, "body": json.dumps(data), "headers": {"Content-Type": "application/json"}}

    def error_response(self, code: int, message: str):
        return {"statusCode": code, "body": json.dumps({"error": message}), "headers": {"Content-Type": "application/json"}}