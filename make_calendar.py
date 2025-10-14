"""
Woodstock Film Festival 2025 — All Events -> ICS exporter
- Target URL: https://woodstockfilmfestival.org/2025-all-events
- Output: wff_2025_full.ics

Requirements (on Replit or local):
    pip install requests beautifulsoup4 lxml playwright
    python -m playwright install --with-deps chromium

If Playwright isn't available, the script will still try static parsing.
"""

from __future__ import annotations
import re
import sys
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

import requests
from bs4 import BeautifulSoup

TARGET_URL = "https://woodstockfilmfestival.org/2025-all-events"
YEAR = 2025
OUTPUT_PATH = "wff_2025_full.ics"

# --- Configurable defaults ---
DEFAULT_DURATION_HOURS = 2  # if end time not given on site
TZ_ID = "America/New_York"  # EST/EDT with proper VTIMEZONE
MIN_REASONABLE_EVENTS = 10  # threshold to decide JS rendering fallback


# ---------------- ICS helpers ----------------
VTIMEZONE_BLOCK = """BEGIN:VTIMEZONE
TZID:America/New_York
LAST-MODIFIED:20201011T015843Z
TZURL:https://www.tzurl.org/zoneinfo-outlook/America/New_York
X-LIC-LOCATION:America/New_York
BEGIN:DAYLIGHT
TZNAME:EDT
TZOFFSETFROM:-0500
TZOFFSETTO:-0400
DTSTART:19700308T020000
RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU
END:DAYLIGHT
BEGIN:STANDARD
TZNAME:EST
TZOFFSETFROM:-0400
TZOFFSETTO:-0500
DTSTART:19701101T020000
RRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU
END:STANDARD
END:VTIMEZONE"""

def ics_escape(s: str) -> str:
    return (
        s.replace("\\", "\\\\")
         .replace("\n", "\\n")
         .replace(",", "\\,")
         .replace(";", "\\;")
    )

def make_uid(title: str, dt: datetime, venue: Optional[str], url: Optional[str]) -> str:
    base = f"{title}|{dt.isoformat()}|{venue or ''}|{url or ''}"
    h = hashlib.sha256(base.encode("utf-8")).hexdigest()[:20]
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
    return f"{slug}-{dt.strftime('%Y%m%dT%H%M%S')}-{h}@wff2025"


# ---------------- Fetchers ----------------
def fetch_static(url: str) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; WFF-ICS/1.0; +https://woodstockfilmfestival.org/)"
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.text

def fetch_rendered_with_playwright(url: str) -> str:
    """
    Render page using Playwright Chromium headless.
    Requires:
        pip install playwright
        python -m playwright install --with-deps chromium
    """
    # Import inside function so script still runs if Playwright isn't installed
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context()
        page = ctx.new_page()
        page.goto(url, wait_until="networkidle", timeout=60000)
        # If events load via infinite scroll or button, you might need to scroll:
        # page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        # page.wait_for_timeout(1000)
        content = page.content()
        browser.close()
        return content


# ---------------- Parsers ----------------
MONTHS = {
    "Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,
    "Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12
}

def parse_dt_et(date_text: str) -> Optional[datetime]:
    """
    Examples seen:
      'Wednesday, Oct 15, 5:00 PM ET'
      'Oct 15, 5:00 PM ET'
    We ignore weekday and assume Eastern time by writing DTSTART;TZID in ICS.
    """
    # Strip weekday if present
    # Keep only 'Mon Abbr, d, h:mm AM/PM ET'
    # Try patterns progressively
    s = date_text.strip()
    # Remove trailing timezone label like "ET", "EDT", "EST"
    s = re.sub(r"\bE[DS]?T\b\.?", "", s).strip(", ").strip()

    # Try "Oct 15, 5:00 PM"
    m = re.search(r"([A-Za-z]{3})\s+(\d{1,2}),\s+(\d{1,2}:\d{2}\s+[AP]M)", s)
    if m:
        mon, day, tim = m.groups()
        try:
            return datetime.strptime(
                f"{YEAR}-{MONTHS[mon]:02d}-{int(day):02d} {tim}",
                "%Y-%m-%d %I:%M %p",
            )
        except Exception:
            return None

    # Try with weekday at the front: "Wednesday, Oct 15, 5:00 PM"
    m2 = re.search(
        r"^[A-Za-z]+,\s+([A-Za-z]{3})\s+(\d{1,2}),\s+(\d{1,2}:\d{2}\s+[AP]M)", s
    )
    if m2:
        mon, day, tim = m2.groups()
        try:
            return datetime.strptime(
                f"{YEAR}-{MONTHS[mon]:02d}-{int(day):02d} {tim}",
                "%Y-%m-%d %I:%M %p",
            )
        except Exception:
            return None

    return None


def extract_events(html: str) -> List[Dict]:
    """
    Extract:
      - title (text)
      - start datetime (ET)
      - venue (location)
      - description (best-effort; use available synopsis/description/subtitle/copy elements)
      - url (details link if present)
    """
    soup = BeautifulSoup(html, "lxml")

    # The site uses event tiles; be flexible:
    cards = soup.select(".event-banner, .event-card, div[class*='event'][class*='banner']")
    events = []

    for card in cards:
        # Title
        t = (
            card.select_one("h3.event-title .truncate-title")
            or card.select_one("h3.event-title")
            or card.select_one(".event-title")
        )
        title = (t.get_text(strip=True) if t else "").strip()
        if not title:
            continue

        # Date/Time
        d = card.select_one(".event-date") or card.find(class_=re.compile(r"event-.*date"))
        date_text = d.get_text(" ", strip=True) if d else ""
        dt = parse_dt_et(date_text)
        if not dt:
            # If no time parsed, skip this card (likely a header or malformed)
            continue

        # Venue / Location
        v = (
            card.select_one(".venue-name")
            or card.select_one(".event-venue")
            or card.select_one(".event-location")
        )
        venue = v.get_text(" ", strip=True) if v else ""

        # Description (best-effort)
        desc_node = (
            card.select_one(".event-description")
            or card.select_one(".event-synopsis")
            or card.select_one(".event-copy")
            or card.select_one(".event-subtitle")
        )
        description = desc_node.get_text("\n", strip=True) if desc_node else ""

        # Event detail URL (if present)
        link = card.select_one("a[href*='/event'], a[href*='/film'], a.event-link, a[href*='/2025']")
        url = link["href"].strip() if link and link.has_attr("href") else TARGET_URL
        if url.startswith("/"):
            # make absolute if needed
            url = "https://woodstockfilmfestival.org" + url

        events.append(
            {
                "title": title,
                "start": dt,
                "venue": venue,
                "description": description,
                "url": url,
            }
        )

    # Deduplicate (title + start)
    dedup: Dict[Tuple[str, datetime], Dict] = {}
    for e in events:
        key = (e["title"], e["start"])
        dedup[key] = e
    events = sorted(dedup.values(), key=lambda x: (x["start"], x["title"].lower()))
    return events


# ---------------- ICS writer ----------------
def build_ics(events: List[Dict]) -> str:
    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//WFF 2025 Full Export//EN",
        "CALSCALE:GREGORIAN",
        VTIMEZONE_BLOCK,
    ]

    for e in events:
        start = e["start"]
        end = start + timedelta(hours=DEFAULT_DURATION_HOURS)
        uid = make_uid(e["title"], start, e.get("venue"), e.get("url"))
        summary = ics_escape(e["title"])
        location = ics_escape(e.get("venue") or "")
        description = ics_escape(e.get("description") or "")
        url = e.get("url") or TARGET_URL

        lines.append("BEGIN:VEVENT")
        lines.append(f"UID:{uid}")
        lines.append(f"DTSTART;TZID={TZ_ID}:{start.strftime('%Y%m%dT%H%M%S')}")
        lines.append(f"DTEND;TZID={TZ_ID}:{end.strftime('%Y%m%dT%H%M%S')}")
        lines.append(f"SUMMARY:{summary}")
        if location:
            lines.append(f"LOCATION:{location}")
        if description:
            lines.append(f"DESCRIPTION:{description}")
        lines.append(f"URL:{url}")
        lines.append("END:VEVENT")

    lines.append("END:VCALENDAR")
    return "\n".join(lines)


# ---------------- Main ----------------
def main():
    # 1) Try static fetch
    try:
        html = fetch_static(TARGET_URL)
        events = extract_events(html)
    except Exception as e:
        print(f"[static] fetch/parse error: {e}", file=sys.stderr)
        events = []

    # 2) If too few events, try Playwright render
    if len(events) < MIN_REASONABLE_EVENTS:
        try:
            print("[info] Few events found via static fetch; attempting Playwright render...", file=sys.stderr)
            html2 = fetch_rendered_with_playwright(TARGET_URL)
            events2 = extract_events(html2)
            if len(events2) > len(events):
                events = events2
        except Exception as e:
            print(f"[playwright] render/parse error: {e}", file=sys.stderr)

    if not events:
        print("No events parsed — check that the page structure hasn't changed.", file=sys.stderr)
        sys.exit(2)

    ics_text = build_ics(events)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(ics_text)

    print(f"Wrote {len(events)} events to {OUTPUT_PATH}")

if __name__ == "__main__":
    main()