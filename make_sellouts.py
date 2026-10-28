#!/usr/bin/env python3
"""
Write SELLOUTS.md: every show the current feed marks sold out, with an
estimated ticket count (one ticket per seat at the venue).

Runs after make_calendar.py in the scrape workflow, which commits the result,
so the report refreshes with every scrape.

Usage:
    python make_sellouts.py         # reads woodstockfilmfestival.ics
"""

import sys
from datetime import datetime
from pathlib import Path

import pytz
from dateutil.parser import isoparse
from icalendar import Calendar

import venues

FEED = "woodstockfilmfestival.ics"
OUTPUT = "SELLOUTS.md"
TZ = pytz.timezone("America/New_York")
SOLDOUT_PROP = "X-WFF-SOLDOUT"
STRIP_EMOJI = ("🔴", "🎟️")


def sold_out_shows(path: Path) -> list[dict]:
    """Every event the feed currently marks sold out, earliest showtime first"""
    cal = Calendar.from_ical(path.read_text(encoding="utf-8"))
    shows = []
    for ev in cal.walk("VEVENT"):
        soldout = ev.get(SOLDOUT_PROP)
        if not soldout:
            continue
        title = str(ev.get("summary", ""))
        for mark in STRIP_EMOJI:
            title = title.replace(mark, "")
        venue = str(ev.get("location", "")).split(",")[0].strip()
        start = ev.decoded("dtstart")
        if start.tzinfo is None:
            start = TZ.localize(start)
        shows.append({
            "title": title.strip(),
            "start": start.astimezone(TZ),
            "soldout_at": isoparse(str(soldout)).astimezone(TZ),
            "venue": venue,
            "seats": venues.seats(venue),
            "url": str(ev.get("url", "")),
        })
    return sorted(shows, key=lambda s: s["start"])


def _lead(show: dict) -> str:
    """How far ahead of showtime it sold out, in the coarsest useful unit"""
    hours = (show["start"] - show["soldout_at"]).total_seconds() / 3600
    if hours < 0:
        return "at showtime"
    if hours < 48:
        return f"{hours:.0f}h before"
    return f"{hours / 24:.0f} days before"


def _cell(text: str) -> str:
    """Keep a value from breaking the table row"""
    return text.replace("|", "\\|").replace("\n", " ")


def _seats(show: dict) -> str:
    if not show["seats"]:
        return "?"
    count, confidence = show["seats"]
    return f"{count:,}" + ("\\*" if confidence != "sourced" else "")


def _table(shows: list[dict]) -> str:
    lines = ["| When | Film | Venue | Seats | Sold out |",
             "|---|---|---|--:|---|"]
    for s in shows:
        title = _cell(s["title"])
        if s["url"]:
            title = f"[{title}]({s['url']})"
        lines.append(
            f"| {s['start'].strftime('%a %-d, %-I:%M %p')} | {title} "
            f"| {_cell(s['venue'])} | {_seats(s)} | {_lead(s)} |")
    return "\n".join(lines)


def build_report(shows: list[dict], now: datetime) -> str:
    total = sum(s["seats"][0] for s in shows if s["seats"])
    rough = sum(1 for s in shows if s["seats"] and s["seats"][1] != "sourced")
    unknown = sum(1 for s in shows if not s["seats"])

    out = [
        "# Woodstock Film Festival: sold-out shows",
        "",
        f"_Updated {now.strftime('%a %b %-d, %-I:%M %p %Z')}, with every calendar scrape. "
        "Unofficial._",
        "",
        f"**{len(shows)} shows sold out, about {total:,} tickets** "
        "(estimated: one ticket per seat at the venue).",
        "",
    ]

    upcoming = [s for s in shows if s["start"] >= now]
    past = [s for s in shows if s["start"] < now]
    if upcoming:
        out += [f"## Coming up ({len(upcoming)})", "", _table(upcoming), ""]
    if past:
        out += [f"## Already screened ({len(past)})", "", _table(past), ""]
    if not shows:
        out += ["Nothing has sold out yet.", ""]

    by_venue: dict[str, list[int]] = {}
    for s in shows:
        row = by_venue.setdefault(s["venue"], [0, 0])
        row[0] += 1
        row[1] += s["seats"][0] if s["seats"] else 0
    if by_venue:
        out += ["## By venue", "", "| Venue | Shows | Est. tickets |", "|---|--:|--:|"]
        for venue, (n, seats) in sorted(by_venue.items(), key=lambda kv: -kv[1][1]):
            out.append(f"| {_cell(venue)} | {n} | {f'{seats:,}' if seats else '?'} |")
        out.append("")

    notes = []
    if rough:
        notes.append(f"\\* {rough} show(s) use a rough or guessed seat count "
                     "(no reliable published capacity).")
    if unknown:
        notes.append(f"{unknown} show(s) are at a venue with no seat count and are "
                     "left out of the total.")
    notes.append("A show counts as sold out when the festival site stops offering "
                 "tickets; some come back on sale. Real sales are likely lower than "
                 "capacity (passes, comps, held seats). Seat counts and their sources "
                 "are in `VENUE_SEATS` in [venues.py](venues.py).")
    out += ["---", ""] + [f"{n}  " for n in notes]
    return "\n".join(out).rstrip() + "\n"


def main():
    feed = Path(FEED)
    if not feed.exists():
        sys.exit(f"✗ {FEED} is missing - run make_calendar.py first")
    shows = sold_out_shows(feed)
    Path(OUTPUT).write_text(build_report(shows, datetime.now(TZ)), encoding="utf-8")
    print(f"✓ {OUTPUT}: {len(shows)} sold-out shows")


if __name__ == "__main__":
    main()
