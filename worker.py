#!/usr/bin/env python3
"""Cia Athletica member-photo extractor worker.

Crawls the unauthenticated retornafoto.php CGI, saves real member photos,
and uploads zip batches + state to Google Drive (via domain-wide delegation).
Resumable: state lives in the Drive folder as state.json.
"""
import base64
import io
import json
import os
import random
import time
import zipfile

import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# ---- config from env ----
SA_B64 = os.environ["GCP_SA_B64"]                      # base64 service-account JSON
DRIVE_USER = os.environ.get("DRIVE_USER", "joao@homodeus.com.br")
FOLDER_NAME = os.environ.get("DRIVE_FOLDER", "Cia Athletica - fotos extraidas")
RATE = float(os.environ.get("RATE", "4.0"))            # requests/sec average
BATCH_PHOTOS = int(os.environ.get("BATCH_PHOTOS", "500"))
MAX_ID = int(os.environ.get("MAX_ID", "401724"))
UNIT = int(os.environ.get("UNIT", "6"))
SUBDOMAIN = os.environ.get("SUBDOMAIN", "analia")
PLACEHOLDER_SIZE = int(os.environ.get("PLACEHOLDER_SIZE", "120490"))
UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
]
BASE = f"https://{SUBDOMAIN}.ciaathletica.com.br/cgi/retornafoto.php"


def log(*a):
    print(*a, flush=True)


def drive_service():
    info = json.loads(base64.b64decode(SA_B64))
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/drive"], subject=DRIVE_USER)
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def get_or_create_folder(svc):
    q = f"name='{FOLDER_NAME}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    res = svc.files().list(q=q, spaces="drive", fields="files(id,name)", pageSize=5).execute()
    for f in res.get("files", []):
        return f["id"]
    f = svc.files().create(body={"name": FOLDER_NAME,
                                 "mimeType": "application/vnd.google-apps.folder"},
                           fields="id").execute()
    log("[+] created Drive folder", FOLDER_NAME, f["id"])
    return f["id"]


def drive_get_json(svc, folder_id, name):
    q = f"name='{name}' and '{folder_id}' in parents and trashed=false"
    res = svc.files().list(q=q, spaces="drive", fields="files(id,name)", pageSize=5).execute()
    files = res.get("files", [])
    if not files:
        return None
    data = svc.files().get_media(fileId=files[0]["id"]).execute()
    return json.loads(data)


def drive_put(svc, folder_id, name, data: bytes, mime="application/octet-stream"):
    q = f"name='{name}' and '{folder_id}' in parents and trashed=false"
    res = svc.files().list(q=q, spaces="drive", fields="files(id,name)", pageSize=5).execute()
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime, resumable=False)
    if res.get("files"):
        fid = res["files"][0]["id"]
        svc.files().update(fileId=fid, media_body=media).execute()
        return fid
    f = svc.files().create(body={"name": name, "parents": [folder_id]},
                           media_body=media, fields="id").execute()
    return f["id"]


def fetch(pid, sess):
    r = sess.get(f"{BASE}?codpess={pid}&codunid={UNIT}", timeout=40)
    if r.status_code == 200 and len(r.content) != PLACEHOLDER_SIZE and len(r.content) > 5000:
        return r.content
    return None


def main():
    log("[*] worker starting; unit", UNIT, "subdomain", SUBDOMAIN, "max_id", MAX_ID)
    svc = drive_service()
    folder = get_or_create_folder(svc)
    log("[+] Drive folder:", folder)

    state = drive_get_json(svc, folder, "state.json") or {
        "next_id": 1, "photos": 0, "scanned": 0, "batches": 0, "manifest": []
    }
    log("[*] resume state:", {k: state[k] for k in ("next_id", "photos", "scanned", "batches")})

    sess = requests.Session()
    batch = {}          # pid -> bytes
    manifest_batch = []  # pid, size records for the batch
    backoff = 1.0

    pid = state["next_id"]
    while pid <= MAX_ID:
        try:
            content = fetch(pid, sess)
            backoff = 1.0
        except Exception as e:
            log(f"[!] err pid={pid}: {e}; sleeping {backoff:.0f}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 600)
            continue

        state["scanned"] += 1
        if content is not None:
            batch[pid] = content
            manifest_batch.append({"codpess": pid, "bytes": len(content), "unit": UNIT})
            state["photos"] += 1

        pid += 1
        state["next_id"] = pid

        # rate limit with jitter
        time.sleep(max(0.05, random.gauss(1.0 / RATE, 0.15 / RATE)))

        if len(batch) >= BATCH_PHOTOS or pid > MAX_ID:
            if batch:
                buf = io.BytesIO()
                with zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED) as z:
                    for k, v in batch.items():
                        z.writestr(f"codpess_{k}.png", v)
                    z.writestr("manifest.json", json.dumps(manifest_batch))
                bstart = min(batch); bend = max(batch)
                name = f"batch_{bstart:06d}_{bend:06d}.zip"
                drive_put(svc, folder, name, buf.getvalue(), "application/zip")
                state["batches"] += 1
                log(f"[+] uploaded {name} ({len(batch)} photos, {len(buf.getvalue())/1e6:.1f} MB)")
                batch, manifest_batch = {}, []
                buf.close()

        if state["scanned"] % 2000 == 0:
            drive_put(svc, folder, "state.json",
                     json.dumps(state).encode(), "application/json")
            log(f"[*] checkpoint: scanned={state['scanned']} photos={state['photos']} "
                f"next_id={state['next_id']} batches={state['batches']}")

    # final flush + state
    if batch:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED) as z:
            for k, v in batch.items():
                z.writestr(f"codpess_{k}.png", v)
            z.writestr("manifest.json", json.dumps(manifest_batch))
        bstart = min(batch); bend = max(batch)
        drive_put(svc, folder, f"batch_{bstart:06d}_{bend:06d}.zip", buf.getvalue(),
                  "application/zip")
        state["batches"] += 1
    state["done"] = True
    drive_put(svc, folder, "state.json", json.dumps(state).encode(), "application/json")
    log("[✓] DONE", json.dumps({k: state[k] for k in ("photos", "scanned", "batches")}))


if __name__ == "__main__":
    main()
