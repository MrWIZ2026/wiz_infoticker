import os
import re
import json
import time
import hashlib
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

INFO_URL = "https://sessionnet.owl-it.de/witzenhausen/bi/info.asp"
BASE = "https://sessionnet.owl-it.de/witzenhausen/bi/"

WITZ_LIST_URL = "https://www.witzenhausen.eu/veranstaltungen/"
WITZ_BASE = "https://www.witzenhausen.eu"

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

def format_message(ev: dict) -> str:
    title = normalize_ws(ev.get("gremium", ""))
    datum = normalize_ws(ev.get("datum", ""))
    zeit = normalize_ws(ev.get("zeit", ""))
    ort = normalize_ws(ev.get("raum", ""))
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
    return (d.isoformat(), t, normalize_ws(ev.get("gremium", "")).lower())

def sha_uid(prefix: str, *parts: str) -> str:
    raw = "|".join([normalize_ws(p).lower() for p in parts if p is not None])
    h = hashlib.sha1(raw.encode("utf-8")).hexdigest()
    return f"{prefix}:{h}"

def iso_to_ddmmyyyy_and_time(iso_value: str, iso_end: str | None = None) -> tuple[str, str]:
    s = normalize_ws(iso_value)
    if not s:
        return "", ""

    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2}))?", s)
    if not m:
        return "", ""

    yyyy, mm, dd = m.group(1), m.group(2), m.group(3)
    hh, mi = m.group(4), m.group(5)

    date_str = f"{dd}.{mm}.{yyyy}"
    if not hh or not mi:
        return date_str, ""

    start_time = f"{hh}:{mi}"
    if iso_end:
        e = normalize_ws(iso_end)
        me = re.match(r"^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2}))?", e)
        if me and me.group(1) == yyyy and me.group(2) == mm and me.group(3) == dd and me.group(4) and me.group(5):
            end_time = f"{me.group(4)}:{me.group(5)}"
            if end_time != start_time:
                return date_str, f"{start_time} bis {end_time} Uhr"

    return date_str, f"{start_time} Uhr"

def jsonld_extract_events(soup: BeautifulSoup) -> list[dict]:
    items = []
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = tag.string or tag.get_text() or ""
        raw = raw.strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue

        def walk(x):
            if isinstance(x, dict):
                items.append(x)
                for v in x.values():
                    walk(v)
            elif isinstance(x, list):
                for v in x:
                    walk(v)

        walk(data)

    out = []
    for obj in items:
        t = obj.get("@type")
        if isinstance(t, list):
            if "Event" in t:
                out.append(obj)
        elif t == "Event":
            out.append(obj)

    return out

def format_location_from_jsonld(loc) -> str:
    if not loc:
        return ""

    if isinstance(loc, list) and loc:
        loc = loc[0]

    if isinstance(loc, str):
        return normalize_ws(loc)

    if not isinstance(loc, dict):
        return ""

    name = normalize_ws(loc.get("name", ""))

    addr = loc.get("address")
    if isinstance(addr, str):
        addr_str = normalize_ws(addr)
        if name and addr_str:
            return f"{name}, {addr_str}"
        return name or addr_str

    parts = []
    if name:
        parts.append(name)

    if isinstance(addr, dict):
        street = normalize_ws(addr.get("streetAddress", ""))
        postal = normalize_ws(addr.get("postalCode", ""))
        city = normalize_ws(addr.get("addressLocality", "")) or normalize_ws(addr.get("addressRegion", ""))
        extra = ", ".join([p for p in [street] if p])
        tail = " ".join([p for p in [postal, city] if p]).strip()

        if extra and tail:
            parts.append(f"{extra}, {tail}")
        elif extra:
            parts.append(extra)
        elif tail:
            parts.append(tail)

    return ", ".join([p for p in parts if p])

def collect_witz_event_urls(list_html: str, limit: int = 40) -> list[str]:
    soup = BeautifulSoup(list_html, "lxml")
    urls = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href:
            continue
        abs_url = urljoin(WITZ_BASE, href)

        p = urlparse(abs_url)
        if p.netloc != urlparse(WITZ_BASE).netloc:
            continue

        if not p.path.startswith("/veranstaltungen/"):
            continue

        if p.path.rstrip("/") == "/veranstaltungen":
            continue

        if not re.match(r"^/veranstaltungen/[^/]+/?$", p.path):
            continue

        key = p.path.rstrip("/").lower()
        if key in seen:
            continue
        seen.add(key)

        urls.append(abs_url)
        if len(urls) >= limit:
            break

    return urls

def fetch_witz_event_detail(url: str) -> dict | None:
    r = session.get(url, timeout=30)
    r.raise_for_status()
    html = r.text
    soup = BeautifulSoup(html, "lxml")

    title = ""
    h = soup.find(["h1", "h2"])
    if h:
        title = normalize_ws(h.get_text(" ", strip=True))

    jsonld_events = jsonld_extract_events(soup)
    if jsonld_events:
        ev = jsonld_events[0]
        title = normalize_ws(ev.get("name", "")) or title
        start = normalize_ws(ev.get("startDate", ""))
        end = normalize_ws(ev.get("endDate", "")) or None
        datum, zeit = iso_to_ddmmyyyy_and_time(start, end)
        ort = format_location_from_jsonld(ev.get("location"))
        uid = sha_uid("wiz", url, title, start, ort)

        return {
            "uid": uid,
            "gremium": title,
            "datum": datum,
            "zeit": zeit,
            "raum": ort,
            "url": url,
            "source": "witzenhausen_jsonld",
        }

    text = soup.get_text("\n", strip=True)
    lines = [normalize_ws(x) for x in text.split("\n")]
    lines = [x for x in lines if x]

    date_str = ""
    time_str = ""
    loc_str = ""

    for ln in lines:
        m = re.search(r"\b(\d{2}\.\d{2}\.\d{4})\b", ln)
        if m:
            date_str = m.group(1)
            break

    for ln in lines:
        m = re.search(r"\b(\d{1,2}:\d{2})\s*Uhr\b", ln)
        if m:
            time_str = f"{m.group(1)} Uhr"
            break

    for ln in lines:
        if "Witzenhausen" in ln and len(ln) > 10 and ":" not in ln and "http" not in ln.lower():
            loc_str = ln
            break

    if not title:
        title = normalize_ws(soup.title.get_text(" ", strip=True)) if soup.title else ""

    uid = sha_uid("wiz", url, title, date_str, time_str, loc_str)

    return {
        "uid": uid,
        "gremium": title,
        "datum": date_str,
        "zeit": time_str,
        "raum": loc_str,
        "url": url,
        "source": "witzenhausen_fallback",
    }

def fetch_witz_events() -> list[dict]:
    r = session.get(WITZ_LIST_URL, timeout=30)
    r.raise_for_status()
    list_html = r.text

    urls = collect_witz_event_urls(list_html, limit=40)

    events = []
    for u in urls:
        try:
            ev = fetch_witz_event_detail(u)
            if ev and (ev.get("gremium") or ev.get("datum") or ev.get("zeit") or ev.get("raum")):
                events.append(ev)
        except Exception:
            continue
        time.sleep(0.4)

    unique = {}
    for ev in events:
        unique[ev["uid"]] = ev
    return list(unique.values())

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

    merged_sessionnet = {ev["uid"]: ev for ev in linked_events}
    for ev in text_events:
        sig = (normalize_ws(ev.get("datum", "")), normalize_ws(ev.get("gremium", "")).lower())
        if sig in linked_signatures:
            continue
        merged_sessionnet[ev["uid"]] = ev

    witz_events = fetch_witz_events()

    all_events = list(merged_sessionnet.values()) + witz_events
    events = sorted(all_events, key=event_sort_key)

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
        msg = format_message(ev)
        if msg:
            tg_send(msg)
        posted.add(ev["uid"])

    posted.update(current_uids)
    state["posted"] = sorted(posted)
    save_state(state)

if __name__ == "__main__":
    run()
