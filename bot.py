import os
import re
import json
import time
import hashlib
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET

# SessionNet, wichtig: BI groß
INFO_URL = "https://sessionnet.owl-it.de/witzenhausen/BI/info.asp"
BASE = "https://sessionnet.owl-it.de/witzenhausen/BI/"

# Witzenhausen Veranstaltungen
WITZ_BASE = "https://www.witzenhausen.eu"
WITZ_LIST_URL = "https://www.witzenhausen.eu/veranstaltungen/"
WITZ_SITEMAP_CANDIDATES = [
    "https://www.witzenhausen.eu/wp-sitemap.xml",
    "https://www.witzenhausen.eu/sitemap_index.xml",
    "https://www.witzenhausen.eu/sitemap.xml",
]

STATE_FILE = "state.json"

TG_TOKEN = os.environ.get("TG_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")
POST_EXISTING = os.environ.get("POST_EXISTING", "0") == "1"

if not TG_TOKEN or not TG_CHAT_ID:
    raise SystemExit("Fehlen: Umgebungsvariablen TG_TOKEN und TG_CHAT_ID.")

session = requests.Session()
session.headers.update({"User-Agent": "wiz_infoticker_bot/1.0"})

DAY_TOKENS = {"Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"}

GERMAN_MONTHS = {
    "jan": "01", "januar": "01",
    "feb": "02", "februar": "02",
    "mär": "03", "maerz": "03", "märz": "03",
    "apr": "04", "april": "04",
    "mai": "05",
    "jun": "06", "juni": "06",
    "jul": "07", "juli": "07",
    "aug": "08", "august": "08",
    "sep": "09", "sept": "09", "september": "09",
    "okt": "10", "oktober": "10",
    "nov": "11", "november": "11",
    "dez": "12", "dezember": "12",
}

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

def sha_uid(prefix: str, raw: str) -> str:
    h = hashlib.sha1(raw.encode("utf-8")).hexdigest()
    return f"{prefix}:{h}"

def stable_text_uid(date_str: str, title: str, time_str: str, location: str) -> str:
    raw = f"{date_str}|{normalize_ws(title).lower()}|{normalize_ws(time_str).lower()}|{normalize_ws(location).lower()}"
    h = hashlib.sha1(raw.encode("utf-8")).hexdigest()
    return f"txt:{h}"

def format_message(ev: dict) -> str:
    title = normalize_ws(ev.get("title", ""))
    datum = normalize_ws(ev.get("datum", ""))
    zeit = normalize_ws(ev.get("zeit", ""))
    ort = normalize_ws(ev.get("ort", ""))
    url = (ev.get("url", "") or "").strip()

    lines = []
    if title:
        lines.append(title)
    if datum:
        lines.append(datum)
    if zeit:
        lines.append(zeit)
    if ort:
        lines.append(ort)
    if url:
        lines.append(f"Link: {url}")

    return "\n".join(lines).strip()

def event_sort_key(ev: dict):
    d = parse_date(normalize_ws(ev.get("datum", ""))) or datetime.max.date()
    t = first_time_token(normalize_ws(ev.get("zeit", "")))
    return (d.isoformat(), t, normalize_ws(ev.get("title", "")).lower())

# -------------------------
# SessionNet
# -------------------------

def pick_value_from_detail(html: str, label: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
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
        "title": pick_value_from_detail(html, "Gremium"),
        "datum": pick_value_from_detail(html, "Datum"),
        "zeit": pick_value_from_detail(html, "Zeit"),
        "ort": pick_value_from_detail(html, "Raum"),
        "url": detail_url,
    }

def extract_ksinr(href: str):
    m = re.search(r"__ksinr=(\d+)", href or "")
    return m.group(1) if m else None

def parse_text_events_from_info(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
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
            "title": title,
            "datum": date_str,
            "zeit": time_line,
            "ort": location_line,
            "url": INFO_URL,
            "source": "sessionnet_text",
        })

        i = j + 1

    return events

def parse_linked_events_from_info(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
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
        details["source"] = "sessionnet_detail"
        events.append(details)
        time.sleep(0.3)

    return events

def fetch_sessionnet_events() -> list[dict]:
    r = session.get(INFO_URL, timeout=30)
    r.raise_for_status()
    html = r.text

    linked_events = parse_linked_events_from_info(html)
    text_events = parse_text_events_from_info(html)

    linked_signatures = set()
    for ev in linked_events:
        linked_signatures.add((
            normalize_ws(ev.get("datum", "")),
            normalize_ws(ev.get("title", "")).lower()
        ))

    merged = {ev["uid"]: ev for ev in linked_events}
    for ev in text_events:
        sig = (normalize_ws(ev.get("datum", "")), normalize_ws(ev.get("title", "")).lower())
        if sig in linked_signatures:
            continue
        merged[ev["uid"]] = ev

    return list(merged.values())

# -------------------------
# Witzenhausen Veranstaltungen
# -------------------------

def parse_de_month_date_to_ddmmyyyy(s: str) -> str:
    s = normalize_ws(s).replace("..", ".")
    m = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})$", s)
    if m:
        return s

    m = re.match(r"^(\d{1,2})\.\s*([A-Za-zÄÖÜäöü]+)\.?\s*(\d{4})$", s)
    if not m:
        return s

    day = int(m.group(1))
    mon_raw = m.group(2).lower()
    mon_raw = mon_raw.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue")
    year = int(m.group(3))
    mon_num = GERMAN_MONTHS.get(mon_raw) or GERMAN_MONTHS.get(mon_raw.strip("."))
    if not mon_num:
        return s
    return f"{day:02d}.{mon_num}.{year:04d}"

def xml_loc_text(el) -> str:
    if el is None:
        return ""
    return (el.text or "").strip()

def fetch_xml(url: str) -> str:
    r = session.get(url, timeout=30)
    r.raise_for_status()
    return r.text

def extract_sitemaps_from_index(xml_text: str) -> list[str]:
    sitemaps = []
    root = ET.fromstring(xml_text)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    for sm in root.findall(".//sm:sitemap", ns):
        loc = sm.find("sm:loc", ns)
        loc_url = xml_loc_text(loc)
        if loc_url:
            sitemaps.append(loc_url)
    if not sitemaps:
        for sm in root.findall(".//sitemap"):
            loc = sm.find("loc")
            loc_url = xml_loc_text(loc)
            if loc_url:
                sitemaps.append(loc_url)
    return sitemaps

def extract_urls_from_sitemap(xml_text: str) -> list[str]:
    urls = []
    root = ET.fromstring(xml_text)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    for u in root.findall(".//sm:url", ns):
        loc = u.find("sm:loc", ns)
        loc_url = xml_loc_text(loc)
        if loc_url:
            urls.append(loc_url)
    if not urls:
        for u in root.findall(".//url"):
            loc = u.find("loc")
            loc_url = xml_loc_text(loc)
            if loc_url:
                urls.append(loc_url)
    return urls

def discover_witz_event_urls(max_urls: int = 800) -> list[str]:
    found = set()

    for cand in WITZ_SITEMAP_CANDIDATES:
        try:
            xml = fetch_xml(cand)
        except Exception:
            continue

        urls = []
        try:
            if "<sitemapindex" in xml:
                smaps = extract_sitemaps_from_index(xml)
                for sm_url in smaps:
                    if "veranstaltungen" in sm_url.lower():
                        try:
                            sm_xml = fetch_xml(sm_url)
                            urls.extend(extract_urls_from_sitemap(sm_xml))
                        except Exception:
                            pass
            elif "<urlset" in xml:
                urls = extract_urls_from_sitemap(xml)
        except Exception:
            urls = []

        for u in urls:
            u = (u or "").strip()
            if not u.startswith(WITZ_BASE):
                continue
            if re.match(r"^https://www\.witzenhausen\.eu/veranstaltungen/[^/]+/?$", u):
                found.add(u.rstrip("/") + "/")
            if len(found) >= max_urls:
                break

        if found:
            break

    if not found:
        r = session.get(WITZ_LIST_URL, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith("/"):
                href = urljoin(WITZ_BASE, href)
            if re.match(r"^https://www\.witzenhausen\.eu/veranstaltungen/[^/]+/?$", href):
                found.add(href.rstrip("/") + "/")
            if len(found) >= max_urls:
                break

    return sorted(found)

def parse_witz_event_detail(url: str) -> dict:
    r = session.get(url, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    title = ""
    h1 = soup.find("h1")
    if h1:
        title = normalize_ws(h1.get_text(" ", strip=True))

    text_all = soup.get_text("\n", strip=True)
    text_main = text_all.split("Das könnte Sie auch interessieren")[0]
    lines = [normalize_ws(x) for x in text_main.split("\n")]
    lines = [x for x in lines if x]

    time_idx = None
    time_val = ""
    for i, ln in enumerate(lines):
        m = re.search(r"\b(\d{1,2}:\d{2})\s*Uhr\b", ln)
        if m:
            time_idx = i
            time_val = m.group(1) + " Uhr"
            break

    date_val = ""
    if time_idx is not None:
        for j in range(time_idx, max(-1, time_idx - 6), -1):
            ln = lines[j]
            m1 = re.search(r"\b(\d{2}\.\d{2}\.\d{4})\b", ln)
            if m1:
                date_val = m1.group(1)
                break
            m2 = re.search(r"\b(\d{1,2}\.\s*[A-Za-zÄÖÜäöü]+\.?\s*\d{4})\b", ln)
            if m2:
                date_val = parse_de_month_date_to_ddmmyyyy(m2.group(1))
                break

    ort_val = ""
    if time_idx is not None:
        for k in range(time_idx + 1, min(len(lines), time_idx + 10)):
            ln = lines[k]
            low = ln.lower()
            if "http" in low:
                continue
            if re.search(r"\b\d{5}\b", ln) or re.search(r"\b\d+\b", ln) or "witzenhausen" in low:
                ort_val = ln
                break

    uid = sha_uid("wz", url.rstrip("/") + "/")

    return {
        "uid": uid,
        "title": title,
        "datum": date_val,
        "zeit": time_val,
        "ort": ort_val,
        "url": url.rstrip("/") + "/",
        "source": "witz",
    }

def fetch_witz_events() -> list[dict]:
    urls = discover_witz_event_urls()
    events = []
    for u in urls[:800]:
        try:
            ev = parse_witz_event_detail(u)
            events.append(ev)
            time.sleep(0.2)
        except Exception:
            continue
    unique = {}
    for ev in events:
        unique[ev["uid"]] = ev
    return list(unique.values())

# -------------------------
# Run
# -------------------------

def run():
    is_manual = os.environ.get("GITHUB_EVENT_NAME", "") == "workflow_dispatch"
    if not is_manual:
       
    state = load_state()
    posted = set(state.get("posted", []))

    sessionnet_events = []
    witz_events = []

    # SessionNet holen, Fehler nicht fatal machen
    try:
        sessionnet_events = fetch_sessionnet_events()
    except Exception as e:
        print("SessionNet Fehler:", repr(e))
        sessionnet_events = []

    # Witzenhausen holen, Fehler nicht fatal machen
    try:
        witz_events = fetch_witz_events()
    except Exception as e:
        print("Witzenhausen Fehler:", repr(e))
        witz_events = []

    sessionnet_events_sorted = sorted(sessionnet_events, key=event_sort_key)
    witz_events_sorted = sorted(witz_events, key=event_sort_key)

    current_uids = {ev["uid"] for ev in sessionnet_events_sorted}.union({ev["uid"] for ev in witz_events_sorted})
    first_run = (not os.path.exists(STATE_FILE)) or (len(posted) == 0)

    if first_run and not POST_EXISTING:
        state["posted"] = sorted(current_uids)
        save_state(state)
        return

    new_witz = [ev for ev in witz_events_sorted if ev["uid"] not in posted]
    new_sessionnet = [ev for ev in sessionnet_events_sorted if ev["uid"] not in posted]

    print("Witzenhausen total:", len(witz_events_sorted), "new:", len(new_witz))
    print("SessionNet total:", len(sessionnet_events_sorted), "new:", len(new_sessionnet))

    # Nicht zusammenlegen: erst Witzenhausen, dann SessionNet, jeweils sortiert
    for ev in new_witz:
        msg = format_message(ev)
        if msg:
            tg_send(msg)
        posted.add(ev["uid"])

    for ev in new_sessionnet:
        msg = format_message(ev)
        if msg:
            tg_send(msg)
        posted.add(ev["uid"])

    posted.update(current_uids)
    state["posted"] = sorted(posted)
    save_state(state)

if __name__ == "__main__":
    run()
