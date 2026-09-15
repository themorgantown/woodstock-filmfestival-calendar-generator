#!/usr/bin/env python3
"""
Add a one-off "resubscribe" notice to a frozen archive calendar.

Archived year files stop updating, but the people subscribed to them stay
subscribed and quietly get a dead calendar. This drops a single all-day event
into the archive pointing them at the year-free feed, dated on the first day of
the *upcoming* festival so it actually lands in the future where they'll see it.

The notice is spliced in as text rather than by re-serializing the calendar, so
the existing events are left byte-for-byte untouched.

Idempotent - re-running on a file that already has the notice does nothing.

Usage:
    python add_subscribe_notice.py wff_2025_complete.ics 2026-10-08
"""

import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from icalendar import Event, vText

README_URL = "https://github.com/themorgantown/woodstock-filmfestival-calendar-generator/blob/main/README.md"
SUMMARY = "Subscribe to the new Woodstock Film Festival Calendar"
UID = "subscribe-notice@woodstockfilmfestival.org"


def build_notice(first_day: date) -> bytes:
    """Build the notice VEVENT, correctly folded, with CRLF line endings"""
    event = Event()
    event.add('summary', SUMMARY)
    event.add('dtstart', first_day)
    event.add('dtend', first_day + timedelta(days=1))
    event.add('description',
              "This calendar has been replaced and is no longer updated.\n"
              "Subscribe to the new feed to get every Woodstock Film Festival "
              "from now on - you only have to do this once.\n\n"
              f"🔗 {README_URL}")
    event.add('url', vText(README_URL))
    event.add('transp', 'TRANSPARENT')  # informational, don't show as busy
    event.add('uid', UID)
    event.add('dtstamp', datetime.utcnow())
    return event.to_ical()


def add_notice(path: Path, first_day: date) -> bool:
    """Splice the notice in before END:VCALENDAR. Returns False if already present"""
    data = path.read_bytes()

    if UID.encode() in data:
        return False

    marker = b"END:VCALENDAR"
    if marker not in data:
        raise ValueError(f"{path}: no END:VCALENDAR found - not a calendar file?")

    # Insert before the final END:VCALENDAR, preserving everything already there
    head, _, tail = data.rpartition(marker)
    path.write_bytes(head + build_notice(first_day) + marker + tail)
    return True


def main():
    if len(sys.argv) != 3:
        sys.exit(f"usage: {sys.argv[0]} <calendar.ics> <YYYY-MM-DD>")

    path = Path(sys.argv[1])
    first_day = date.fromisoformat(sys.argv[2])

    if add_notice(path, first_day):
        print(f"✓ Notice added to {path}, dated {first_day}")
    else:
        print(f"• {path} already has the notice - nothing to do")


if __name__ == "__main__":
    main()
