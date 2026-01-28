import os
import re
import json
import time
import hashlib
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

INFO_URL = "https://sessionnet.owl-it.de/witzenhausen/bi/info.asp"
BASE = "https://sessionnet.owl-it.de/witzenhausen/bi/"

STATE_FILE = "state.json"

TG_TOKEN = os.environ.get("TG_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")
POST_EXISTING = os.environ.get("POST_EXISTING", "0") == "1"

if not TG_TOKEN or not TG_CHAT_ID:
    raise SystemExit("Fehlen: Umgebungsvariablen TG_TOKEN und TG_CHAT_ID.")

session = requests.Session()
session.headers.update({"User-Agent": "wiz_infoticker_bot/1.0"})

DAY_TOKENS = {"Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"}

def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {"posted": []}
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def save_state(state: dict) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def tg_send(text: str) -> None:
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": text, "disable_web_page_preview": True}
    r = requests.post(url, json=payload, timeout=30)
    r.raise_for_status()

def normalize_ws(s: str) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", s.replace("\xa0", " ")).strip()

def strip_bullets(s: str) -> str:
    return normalize_ws(s).lstrip("•*·- ").strip()

def first_time_token(s: str) -> str:
    m = re.search(r"(\d{1,2}:\d{2})", s or "")
    return m.group(1) if m else "99:99"

def parse_date(d: str):
    try:
        return datetime.strptime(d, "%d.%m.%Y").date()
    except Exception:
        return None

def stable_text_uid(date_str: str, title: str, time_str: str, location: str) -> str:
    raw = f"{date_str}|{normalize_ws(title).lower()}|{normalize_ws(time_str).lower()}|{normalize_ws(location).lower()}"
    h = hashlib.sha1(raw.encode("utf-8")).hexdigest()
    return f"txt:{h}"

def pick_value_from_detail(html: str, label: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    lines = [normalize_ws(x) for x in soup.get_text("\n", strip=True).split("\n")]
    lines = [x for x in lines if x]
    for i, v in enumerate(lines):
        if v == label and i + 1 < len(lines):
            return lines[i + 1]
    return ""

def fetch_session_details(detail_url: str) -> dict:
    r = session.get(detail_url, timeout=30)
    r.raise_for_status()
    html = r.text
    return {
        "gremium": pick_value_from_detail(html, "Gremium"),
        "datum": pick_value_from_detail(html, "Datum"),
        "zeit": pick_value_from_detail(html, "Zeit"),
        "raum": pick_value_from_detail(html, "Raum"),
        "url": detail_url,
    }

def extract_ksinr(href: str):
    m = re.search(r"__ksinr=(\d+)", href or "")
    return m.group(1) if m else None

def parse_text_events_from_info(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    lines = [normalize_ws(x) for x in soup.get_text("\n", strip=True).split("\n")]
    lines = [x for x in lines if x]

    idx = None
    for i, v in enumerate(lines):
        if v == "Aktuelle Sitzungen":
            idx = i
            break
    if idx is None:
        return []

    events = []
    i = idx + 1

    while i < len(lines):
        line = lines[i]

        if line.startswith("Software:"):
            break

        if line in DAY_TOKENS:
            i += 1
            continue

        m = re.match(r"^(\d{2}\.\d{2}\.\d{4})\b(.*)$", line)
        if not m:
            i += 1
            continue

        date_str = m.group(1)
        rest = normalize_ws(m.group(2))

        j = i + 1
        while j < len(lines) and not lines[j]:
            j += 1

        if rest:
            title = rest
        else:
            if j >= len(lines):
                break
            title = normalize_ws(lines[j])
            j += 1

        while j < len(lines) and (not lines[j] or lines[j] in DAY_TOKENS):
            j += 1
        if j >= len(lines):
            break
        time_line = strip_bullets(lines[j])
        j += 1

        while j < len(lines) and (not lines[j] or lines[j] in DAY_TOKENS):
            j += 1
        location_line = strip_bullets(lines[j]) if j < len(lines) else ""

        uid = stable_text_uid(date_str, title, time_line, location_line)
        events.append({
            "uid": uid,
            "gremium": title,
            "datum": date_str,
            "zeit": time_line,
            "raum": location_line,
            "url": INFO_URL,
            "source": "text",
        })

        i = j + 1

    return events

def parse_linked_events_from_info(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    seen = set()
    detail_urls = []

    for a in soup.find_all("a", href=True):
        ksinr = extract_ksinr(a["href"])
        if not ksinr:
            continue
        if ksinr in seen:
            continue
        seen.add(ksinr)
        detail_urls.append((ksinr, urljoin(BASE, a["href"])))

    events = []
    for ksinr, detail_url in detail_urls:
        details = fetch_session_details(detail_url)
        details["uid"] = f"ksinr:{ksinr}"
        details["source"] = "detail"
        events.append(details)
        time.sleep(0.5)

    return events

def format_message(ev: dict) -> str:
    return (
        f"Gremium: {normalize_ws(ev.get('gremium',''))}\n"
        f"Datum: {normalize_ws(ev.get('datum',''))}\n"
        f"Zeit: {normalize_ws(ev.get('zeit',''))}\n"
        f"Ort: {normalize_ws(ev.get('raum',''))}\n"
        f"Link: {ev.get('url','')}"
    )

def event_sort_key(ev: dict):
    d = parse_date(normalize_ws(ev.get("datum", ""))) or datetime.max.date()
    t = first_time_token(normalize_ws(ev.get("zeit", "")))
    return (d.isoformat(), t, normalize_ws(ev.get("gremium", "")).lower())

def run():
    r = session.get(INFO_URL, timeout=30)
    r.raise_for_status()
    html = r.text

    linked_events = parse_linked_events_from_info(html)
    text_events = parse_text_events_from_info(html)

    linked_signatures = set()
    for ev in linked_events:
        linked_signatures.add((
            normalize_ws(ev.get("datum", "")),
            normalize_ws(ev.get("gremium", "")).lower()
        ))

    merged = {ev["uid"]: ev for ev in linked_events}
    for ev in text_events:
        sig = (normalize_ws(ev.get("datum", "")), normalize_ws(ev.get("gremium", "")).lower())
        if sig in linked_signatures:
            continue
        merged[ev["uid"]] = ev

    events = sorted(merged.values(), key=event_sort_key)

    state = load_state()
    posted = set(state.get("posted", []))

    current_uids = {ev["uid"] for ev in events}
    new_events = [ev for ev in events if ev["uid"] not in posted]

    first_run = (not os.path.exists(STATE_FILE)) or (len(posted) == 0)
    if first_run and not POST_EXISTING:
        state["posted"] = sorted(current_uids)
        save_state(state)
        return

    for ev in new_events:
        tg_send(format_message(ev))
        posted.add(ev["uid"])

    posted.update(current_uids)
    state["posted"] = sorted(posted)
    save_state(state)

if __name__ == "__main__":
    run()
