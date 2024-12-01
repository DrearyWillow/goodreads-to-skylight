
import csv
import requests
import json
import re
from datetime import datetime, timezone

def safe_get(obj, *keys):
    for key in keys:
        if isinstance(obj, dict):
            obj = obj.get(key)
        elif isinstance(obj, list) and key < len(obj):
            obj = obj[key]
        else:
            return None
    return obj

def safe_request(mode, url, params=None, json=None, headers=None, data=None):
    try:
        if mode.upper() == 'GET':
            res = requests.get(url, params=params, headers=headers)
        elif mode.upper() == 'POST':
            res = requests.post(url, headers=headers, json=json, data=data)
        else:
            print("Error: invalid mode.")
            return None
        res.raise_for_status()
    except Exception as e:
        print(f"Error: {e}")
        return None
    return res.json()

def list_records(did, service_endpoint, nsid):
    api = f'{service_endpoint}/xrpc/com.atproto.repo.listRecords'
    params = {
        'repo': did,
        'collection': nsid,
        'limit': 100,
    }
    res = safe_request('get', api, params=params)
    output = res.get('records')
    while cursor := res.get('cursor'):
        res = safe_request('get', api, params={**params, 'cursor': cursor})
        output.extend(res.get('records'))
    return output

def linkify(text, link=None):
    return f"\033]8;;{link if link else text}\033\\{text}\033]8;;\033\\"

def get_session(actor, password, service_endpoint):
    url = f'{service_endpoint}/xrpc/com.atproto.server.createSession'
    payload = {
        'identifier': actor,
        'password': password,
    }
    return safe_request('post', url, json=payload)

def resolve_handle(handle):
    if handle.startswith("did:"):
        return handle
    if handle.startswith("@"):
        handle = handle[1:]
    if not handle:
        return None
    return safe_request('get',
        f'https://public.api.bsky.app/xrpc/com.atproto.identity.resolveHandle?handle={handle}'
        ).get('did')

def get_did_doc(did):
    if not did.startswith('did:'):
        did = resolve_handle(did)
    if did.startswith('did:web:'):
        url = f'https://{did.split(":")[-1]}/.well-known/did.json'
    else:
        url = f'https://plc.directory/{did}'
    return safe_request('get', url)

def get_service_endpoint(did):
    for service in (get_did_doc(did).get('service') or []):
        if service.get('type') == 'AtprotoPersonalDataServer':
            return service.get('serviceEndpoint')
    print('Could not retrieve service endpoint. Defaulting to bsky.social')
    return 'https://bsky.social'

def query_open_lib(cat, val=None, params=None):
    url = f'https://openlibrary.org/{cat}{f'/{val}' if val else ''}.json'
    print(f"Query: {url}")
    print(f"Params: {params}")
    return safe_request('get', url, params=params)

def retrieve_key(row):
    for isbn_key in ['ISBN13', 'ISBN']:
        if not (isbn := row[isbn_key].lstrip('="').rstrip('"')):
            continue
        res = query_open_lib('isbn', val=isbn)
        if not res or not (key := res.get('key')):
            continue
        return key.split("/")[-1] if isinstance(key, str) else None

    # title = row['Title'].split(':')
    title = re.sub(r'[\[\(].*?[\]\)]', '', row['Title']).split(':') # remove brackets
    params = {
        'title': title[0].strip(), # it doesn't like subtitles
        'subtitle': title[1].strip() if len(title) > 1 else '',
        'author': row['Author'],
        'id_goodreads': row['Book Id'],
        'publisher': row['Publisher'],
        'publish_year': row['Year Published'],
        'first_publish_year': row['Original Publication Year'],
        'q': 'language:eng', # the lang=en parameter doesn't work
        'fields': 'key,editions,editions.key' # the minimum number of fields necessary to get the edition key
    }
    params = {key: value for key, value in params.items() if value} # filter out null values
    res = query_open_lib('search', params=params)
    print(json.dumps(res))
    if not res or (res.get('num_found') == 0):
        # try again with more loose criteria
        params = {param: params[param] for param in ['title', 'author', 'q', 'fields'] if param in params}
        res = query_open_lib('search', params=params)
        print(json.dumps(res))
        if not res or (res.get('num_found') == 0):
            return None
    key = safe_get(res, 'docs', 0, 'editions', 'docs', 0, 'key')
    return key.split("/")[-1] if isinstance(key, str) else None

def create_record(did, endpoint, session, row, open_lib_key):
    timestamp = datetime.now(timezone.utc).isoformat()[:-9] + "Z"
    if (rating := int(row['My Rating'])*2) < 1:
        rating = 1
    finish_date = row['Date Read'] or row['Date Added']
    finish_date = timestamp if not finish_date else finish_date.replace("/", "-") + "T00:00:00.000Z"

    record = {
        "$type": "my.skylights.rel",
        "item": {
            "ref": "open-library",
            "value": open_lib_key
        },
        "note": {
            "value": row['My Review'],
            "createdAt": timestamp,
            "updatedAt": timestamp
        },
        "rating": {
            "value": rating,
            "createdAt": timestamp
        },
        "finishedAt": [finish_date for _ in range(int(row['Read Count']))]
    }
    print(f"Record: {record}")

    payload = json.dumps({
        "repo": did,
        "collection": "my.skylights.rel",
        "record": record,
    })

    token = session.get('accessJwt')
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    api = f"{endpoint}/xrpc/com.atproto.repo.createRecord"
    res = safe_request('post', api, headers=headers, data=payload)
    if not res:
        return None
    uri = res.get('uri').replace("at://", "")
    link = linkify(f"https://pdsls.dev/at/{uri}")
    print(f"Record created: {link}")

def valid_read_count(count):
    if count in ['0', '']:
        return False
    try:
        int(count)
        return True
    except ValueError:
        return False

if __name__ == "__main__":
    csv_file = input("GoodReads export CSV: ")
    handle = input("Handle: ")
    password = input("Password: ")
    did = resolve_handle(handle)
    if not did:
        print('No DID found.')
        exit()
    endpoint = get_service_endpoint(did)
    if not endpoint:
        print('No service endpoint found.')
        exit()
    session = get_session(handle, password, endpoint)
    if not session:
        print('Session creation unsuccessful.')
        exit()
    records = list_records(did, endpoint, 'my.skylights.rel')
    used_keys = [safe_get(record, 'value', 'item', 'value') for record in records]
    print(f"Used keys: {used_keys}")

    results = []

    with open(csv_file, mode='r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        data = [row for row in csv_reader]

    for row in data:
        if not valid_read_count(row['Read Count']): # goodreads can mess up the export
            print(f"Excluded: {row['Title']} - {row['Author']}. 'Read Count' column is 0 or invalid")
            row['Import Result'] = "Excluded: Read count invalid"
            results.append(row)
            continue

        open_lib_key = retrieve_key(row)
        print(f"open_lib_key: {open_lib_key}")

        if not open_lib_key:
            print(f'Failed: {row['Title']} - {row['Author']} has no key')
            row['Import Result'] = "Failure: No open library key found"
            results.append(row)
            continue
        if open_lib_key in used_keys:
            print(f'Failed: {row['Title']} - {row['Author']} already has a record')
            row['Import Result'] = "Skipped: Already had a record"
            results.append(row)
            continue

        print(f"Success: {open_lib_key} for {row['Title']} - {row['Author']}")
        row['Import Result'] = "Success"
        results.append(row)

        create_record(did, endpoint, session, row, open_lib_key)

    output_file = 'goodreads-import-report.csv' # hard coded because i don't feel like doing validation
    # output_file = input("Enter a filename to write report to: ")
    # if not output_file.endswith(".csv"):
    #     output_file = f"{output_file}.csv"
    with open(output_file, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)
    print(f"CSV with results column written to '{output_file}'")
