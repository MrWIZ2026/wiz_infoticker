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


# SessionNet
INFO_URL = "https://sessionnet.owl-it.de/witzenhausen/bi/info.asp"
BASE = "https://sessionnet.owl-it.de/witzenhausen/bi/"

# Stadt Witzenhausen Veranstaltungen
WITZ_BASE = "https://www.witzenhausen.eu"
WITZ_LIST_URL = "https://www.witzenhausen.eu/veranstaltungen/"
WITZ_SITEMAP_CANDIDATES = [
    "https://www.witzenhausen.eu/wp-sitemap.xml",       # WordPress Core Sitemap
    "https://www.witzenhausen.eu/sitemap_index.xml",    # Yoast
    "https://www.witzenhausen.eu/sitemap.xml",          # sonstige
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
    "dez": "12", "dec": "12", "dezember": "12",
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


# -------------------------
# SessionNet helpers
# -------------------------

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
        details["source"] = "sessionnet_detail"
        events.append(details)
        time.sleep(0.4)

    return events


# -------------------------
# Witzenhausen Veranstaltungen helpers
# -------------------------

def xml_loc_text(el) -> str:
    if el is None:
        return ""
    return (el.text or "").strip()


def fetch_xml(url: str) -> str:
    r = session.get(url, timeout=30)
    r.raise_for_status()
    return r.text


def extract_witz_sitemaps_from_index(xml_text: str) -> list[str]:
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

    # 1) Versuch: WordPress Sitemap
    for cand in WITZ_SITEMAP_CANDIDATES:
        try:
            xml = fetch_xml(cand)
        except Exception:
            continue

        urls = []
        try:
            if "<sitemapindex" in xml:
                smaps = extract_witz_sitemaps_from_index(xml)
                # Wir picken nur Sitemaps, die nach Veranstaltungen aussehen
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
            if not u:
                continue
            if not u.startswith(WITZ_BASE):
                continue
            # echte Event Seiten unter /veranstaltungen/<slug>/
            if re.match(r"^https://www\.witzenhausen\.eu/veranstaltungen/[^/]+/?$", u):
                found.add(u.rstrip("/") + "/")
            if len(found) >= max_urls:
                break

        if found:
            break

    # 2) Fallback: Übersichtsseite, liefert meist nur aktuelle Ansicht
    if not found:
        try:
            r = session.get(WITZ_LIST_URL, timeout=30)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                if not href:
                    continue
                if href.startswith("/"):
                    href = urljoin(WITZ_BASE, href)
                if re.match(r"^https://www\.witzenhausen\.eu/veranstaltungen/[^/]+/?$", href):
                    found.add(href.rstrip("/") + "/")
                if len(found) >= max_urls:
                    break
        except Exception:
            pass

    return sorted(found)


def parse_de_month_date_to_ddmmyyyy(s: str) -> str:
    """
    Input Beispiele:
      23. Jan. 2026
      23. Jan.. 2026
      23. Januar 2026
    Output:
      23.01.2026
    Wenn nicht parsebar, Original zurück.
    """
    s = normalize_ws(s)
    s = s.replace("..", ".")
    s = s.replace(" ,", ",")
    s = re.sub(r"\s+", " ", s).strip()

    # schon numerisch
    m = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})$", s)
    if m:
        return s

    # 23. Jan. 2026
    m = re.match(r"^(\d{1,2})\.\s*([A-Za-zÄÖÜäöü]+)\.?\s*(\d{4})$", s)
    if not m:
        return s

    day = int(m.group(1))
    mon_raw = m.group(2).lower()
    mon_raw = mon_raw.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue")
    year = int(m.group(3))

    mon_num = GERMAN_MONTHS.get(mon_raw)
    if not mon_num:
        # manchmal "jan." als "jan"
        mon_num = GERMAN_MONTHS.get(mon_raw.strip("."))

    if not mon_num:
        return s

    return f"{day:02d}.{mon_num}.{year:04d}"


def parse_witz_event_detail(url: str) -> dict:
    r = session.get(url, timeout=30)
    r.raise_for_status()
    html = r.text

    soup = BeautifulSoup(html, "lxml")

    # Titel
    title = ""
    h1 = soup.find("h1")
    if h1:
        title = normalize_ws(h1.get_text(" ", strip=True))
    if not title:
        h2 = soup.find("h2")
        if h2:
            title = normalize_ws(h2.get_text(" ", strip=True))

    # Nur den relevanten Teil, alles unterhalb von "Das könnte Sie auch interessieren" abschneiden
    text_all = soup.get_text("\n", strip=True)
    text_main = text_all.split("Das könnte Sie auch interessieren")[0]
    lines = [normalize_ws(x) for x in text_main.split("\n")]
    lines = [x for x in lines if x]

    # Zeit finden
    time_idx = None
    time_val = ""
    for i, ln in enumerate(lines):
        m = re.search(r"\b(\d{1,2}:\d{2})\s*Uhr\b", ln)
        if m:
            time_idx = i
            time_val = m.group(1) + " Uhr"
            break

    # Datum finden, bevorzugt direkt vor der Zeit
    date_val = ""
    if time_idx is not None:
        for j in range(time_idx, max(-1, time_idx - 6), -1):
            ln = lines[j]
            # dd.mm.yyyy
            m1 = re.search(r"\b(\d{2}\.\d{2}\.\d{4})\b", ln)
            if m1:
                date_val = m1.group(1)
                break
            # dd. Mon. yyyy
            m2 = re.search(r"\b(\d{1,2}\.\s*[A-Za-zÄÖÜäöü]+\.?\s*\d{4})\b", ln)
            if m2:
                date_val = parse_de_month_date_to_ddmmyyyy(m2.group(1))
                break

    # Ort finden, bevorzugt direkt nach der Zeit
    ort_val = ""
    if time_idx is not None:
        for k in range(time_idx + 1, min(len(lines), time_idx + 8)):
            ln = lines[k]
            low = ln.lower()
            if low in {"veranstalter", "keine angabe"}:
                continue
            if low.startswith("www.") or "http" in low:
                continue
            # typischer Ort enthält Straße oder PLZ oder Hausnummer
            if re.search(r"\b\d{5}\b", ln) or re.search(r"\b\d+\b", ln):
                ort_val = ln
                break

    if not title:
        # Fallback: erste Überschriftartige Zeile
        for ln in lines[:20]:
            if ln and len(ln) > 3 and "Skip to content" not in ln:
                title = ln
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


def parse_witz_events(state_posted: set[str]) -> tuple[set[str], list[dict]]:
    urls = discover_witz_event_urls()

    current_uids = {sha_uid("wz", u.rstrip("/") + "/") for u in urls}
    new_urls = [u for u in urls if sha_uid("wz", u.rstrip("/") + "/") not in state_posted]

    new_events = []
    # Sicherheitslimit, falls mal sehr viele neu wären
    for u in new_urls[:60]:
        try:
            ev = parse_witz_event_detail(u)
            new_events.append(ev)
            time.sleep(0.3)
        except Exception:
            continue

    return current_uids, new_events


# -------------------------
# Common formatting and run
# -------------------------

def format_message(ev: dict) -> str:
    title = normalize_ws(ev.get("title", ""))
    datum = normalize_ws(ev.get("datum", ""))
    zeit = normalize_ws(ev.get("zeit", ""))
    ort = normalize_ws(ev.get("ort", ""))
    url = (ev.get("url", "") or "").strip()

    out = []
    if title:
        out.append(title)
    if datum:
        out.append(datum)
    if zeit:
        out.append(zeit)
    if ort:
        out.append(ort)
    if url:
        out.append(f"Link: {url}")

    return "\n".join(out).strip()


def event_sort_key(ev: dict):
    d = parse_date(normalize_ws(ev.get("datum", ""))) or datetime.max.date()
    t = first_time_token(normalize_ws(ev.get("zeit", "")))
    title = normalize_ws(ev.get("title", "")).lower()
    return (d.isoformat(), t, title)


def run():
    # SessionNet laden
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

    sessionnet_events = sorted(merged.values(), key=event_sort_key)

    # State
    state = load_state()
    posted = set(state.get("posted", []))

    # Witzenhausen Events: erst nur UIDs bestimmen, Details nur für neue laden
    witz_current_uids, witz_new_events = parse_witz_events(posted)

    current_uids = {ev["uid"] for ev in sessionnet_events}.union(witz_current_uids)

    # First run Handling
    first_run = (not os.path.exists(STATE_FILE)) or (len(posted) == 0)
    if first_run and not POST_EXISTING:
        state["posted"] = sorted(current_uids)
        save_state(state)
        return

    # Neue SessionNet Events bestimmen
    new_sessionnet_events = [ev for ev in sessionnet_events if ev["uid"] not in posted]

    # Alles was neu ist posten, sortiert nach Datum, Uhrzeit, Titel
    to_post = new_sessionnet_events + witz_new_events
    to_post_sorted = sorted(to_post, key=event_sort_key)

    for ev in to_post_sorted:
        tg_send(format_message(ev))
        posted.add(ev["uid"])

    posted.update(current_uids)
    state["posted"] = sorted(posted)
    save_state(state)


if __name__ == "__main__":
    run()
