import json
import os
from urllib.parse import urlencode

import requests


API_KEY = "AIzaSyDHRhS4FIO_1s_2Tn2C77noJRgbs-y_mks"
PROJECT_ID = "starcrafttmgbeta"

DATABASE_IDS = [
    "starcrafttmgbeta",
    "(default)",
]

OUTPUT_DIR = "firestore_allowed_dump"

KNOWN_COLLECTIONS = [
    "army_units",
    "tactical_cards",
    "faction_cards",
    "shared_rosters",
    "system_metadata",
    "rules_sections",
    "users",
]

PAGE_SIZE = 100

# Не вписывай пароль в код.
# Если нужен вход обычным пользователем:
# PowerShell:
#   $env:FIREBASE_EMAIL="you@example.com"
#   $env:FIREBASE_PASSWORD="your_password"
FIREBASE_EMAIL = os.getenv("FIREBASE_EMAIL", "").strip()
FIREBASE_PASSWORD = os.getenv("FIREBASE_PASSWORD", "").strip()


def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def fs_value_to_python(value):
    if "nullValue" in value:
        return None
    if "stringValue" in value:
        return value["stringValue"]
    if "integerValue" in value:
        return int(value["integerValue"])
    if "doubleValue" in value:
        return float(value["doubleValue"])
    if "booleanValue" in value:
        return value["booleanValue"]
    if "timestampValue" in value:
        return value["timestampValue"]
    if "mapValue" in value:
        fields = value["mapValue"].get("fields", {})
        return {k: fs_value_to_python(v) for k, v in fields.items()}
    if "arrayValue" in value:
        values = value["arrayValue"].get("values", [])
        return [fs_value_to_python(v) for v in values]
    if "referenceValue" in value:
        return value["referenceValue"]
    if "geoPointValue" in value:
        return value["geoPointValue"]
    if "bytesValue" in value:
        return value["bytesValue"]
    return value


def decode_doc(doc):
    fields = doc.get("fields", {})
    parsed = {k: fs_value_to_python(v) for k, v in fields.items()}
    parsed["id"] = doc["name"].split("/")[-1]
    parsed["_path"] = doc.get("name")
    parsed["_createTime"] = doc.get("createTime")
    parsed["_updateTime"] = doc.get("updateTime")
    return parsed


def sign_in_email_password(email, password):
    url = f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={API_KEY}"
    payload = {
        "email": email,
        "password": password,
        "returnSecureToken": True,
    }

    r = requests.post(url, json=payload, timeout=30)
    if r.status_code != 200:
        return None, f"Email login failed: HTTP {r.status_code} - {r.text[:500]}"
    data = r.json()
    return data.get("idToken"), None


def fetch_collection(database_id, collection_name, id_token=None):
    docs = []
    next_page_token = None

    while True:
        params = {
            "pageSize": PAGE_SIZE,
            "key": API_KEY,
        }
        if next_page_token:
            params["pageToken"] = next_page_token

        url = (
            f"https://firestore.googleapis.com/v1/projects/{PROJECT_ID}"
            f"/databases/{database_id}/documents/{collection_name}?{urlencode(params)}"
        )

        headers = {"Accept": "application/json"}
        if id_token:
            headers["Authorization"] = f"Bearer {id_token}"

        r = requests.get(url, headers=headers, timeout=30)

        if r.status_code != 200:
            return {
                "ok": False,
                "status_code": r.status_code,
                "error": r.text,
                "documents": [],
            }

        data = r.json()
        for d in data.get("documents", []):
            docs.append(decode_doc(d))

        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break

    return {
        "ok": True,
        "status_code": 200,
        "error": None,
        "documents": docs,
    }


def save_json(filename, data):
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main():
    ensure_output_dir()

    id_token = None
    auth_mode = "unauthenticated"

    if FIREBASE_EMAIL and FIREBASE_PASSWORD:
        print("Trying email/password login...")
        token, err = sign_in_email_password(FIREBASE_EMAIL, FIREBASE_PASSWORD)
        if token:
            id_token = token
            auth_mode = "email_password"
            print("Email login OK")
        else:
            print(err)
            print("Falling back to unauthenticated read...\n")
    else:
        print("No FIREBASE_EMAIL / FIREBASE_PASSWORD set.")
        print("Trying unauthenticated read only...\n")

    summary = {
        "project_id": PROJECT_ID,
        "auth_mode": auth_mode,
        "databases": {},
    }

    for database_id in DATABASE_IDS:
        print(f"=== DATABASE: {database_id} ===")
        db_summary = {}

        for collection_name in KNOWN_COLLECTIONS:
            print(f"Checking collection: {collection_name}")

            result = fetch_collection(database_id, collection_name, id_token=id_token)

            if result["ok"]:
                docs = result["documents"]
                print(f"  OK - {len(docs)} docs")

                filename = f"{database_id.replace('(', '').replace(')', '').replace('/', '_')}__{collection_name}.json"
                save_json(filename, docs)

                db_summary[collection_name] = {
                    "accessible": True,
                    "documents_count": len(docs),
                    "file": filename,
                }
            else:
                print(f"  DENIED/ERROR - HTTP {result['status_code']}")
                db_summary[collection_name] = {
                    "accessible": False,
                    "status_code": result["status_code"],
                    "error": result["error"][:1000],
                }

        summary["databases"][database_id] = db_summary
        print()

    save_json("summary.json", summary)
    print(f"Done. Saved to: {os.path.abspath(OUTPUT_DIR)}")


if __name__ == "__main__":
    main()