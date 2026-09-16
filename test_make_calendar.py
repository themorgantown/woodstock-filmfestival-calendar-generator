"""Self-check for the parsing and sellout logic in make_calendar.py.

Run: python test_make_calendar.py
No framework on purpose - it only has to fail when the logic breaks.
"""

import os
import tempfile
from datetime import datetime

os.environ.setdefault("WFF_YEAR", "2026")

import make_calendar as mc


def test_parse_datetime():
    """Both date formats the site has used must parse to the same instant."""
    s = mc.SimplifiedEventScraper()
    expected = mc.TZ.localize(datetime(2026, 10, 18, 15, 15))
    for text in ["Sat, Oct 18, 3:15 PM ET",
                 "Sunday, October 18 at 3:15 PM ET",
                 "Saturday, October 18 at 3:15 PM"]:
        assert s._parse_datetime(text) == expected, f"failed on {text!r}"
    assert s._parse_datetime("") is None
    assert s._parse_datetime("no date here at all") is None


def test_classify_ticket_text():
    c = mc.SimplifiedEventScraper._classify_ticket_text
    assert c("ORDER TICKETS") == 'on_sale'
    assert c("RSVP") == 'on_sale'
    assert c("SOLD OUT") == 'sold_out'
    assert c("Sold Out") == 'sold_out'
    assert c(None) == 'unknown'
    assert c("") == 'unknown'
    assert c("Coming soon") == 'unknown'


def _event(status, title="Test Film"):
    return {
        'title': title,
        'start': mc.TZ.localize(datetime(2026, 10, 18, 15, 15)),
        'venue': 'Tinker Street Cinema',
        'description': 'A film.',
        'has_tickets': status == 'on_sale',
        'ticket_status': status,
        'event_id': 'abc123',
        'url': 'https://example.invalid',
    }


def test_sellout_lifecycle():
    """on sale -> sold out stamps a time, and that time then sticks."""
    uid = 'abc123@woodstockfilmfestival.org'
    with tempfile.TemporaryDirectory() as tmp:
        mc.SELLOUT_LOG_PATH = os.path.join(tmp, 'sellouts.csv')

        # 1. On sale: no sellout time, ticket emoji on the title.
        s = mc.SimplifiedEventScraper()
        s.existing_events_metadata = {}
        on_sale = s._apply_ticket_state([_event('on_sale')])[0]
        assert on_sale['soldout_at'] is None
        assert mc.TICKET_EMOJI in on_sale['title']
        assert not os.path.exists(mc.SELLOUT_LOG_PATH), "no transition, no log row"

        # 2. Tickets vanish: sellout time recorded and logged.
        s = mc.SimplifiedEventScraper()
        s.existing_events_metadata = {uid: {'summary': on_sale['title'],
                                            'had_tickets': True,
                                            'soldout_at': None}}
        sold = s._apply_ticket_state([_event('sold_out')])[0]
        first_seen = sold['soldout_at']
        assert first_seen is not None
        assert mc.SOLDOUT_EMOJI in sold['title']
        assert mc.TICKET_EMOJI not in sold['title']
        log = open(mc.SELLOUT_LOG_PATH).read()
        assert 'sold_out' in log and 'Test Film' in log

        # 2b. Selling out must republish the event, not reuse the old DTSTAMP.
        stale_stamp = mc.TZ.localize(datetime(2026, 9, 1, 8, 0))
        s.existing_events_metadata[uid].update({
            'dtstamp': stale_stamp,
            'summary': on_sale['title'],
            'location': 'Tinker Street Cinema',
            'description': 'A film.',
            'dtstart': sold['start'],
        })
        ics = s.generate_ics_calendar([sold]).replace('\r\n ', '')
        assert stale_stamp.strftime('%Y%m%d') not in ics, "DTSTAMP not bumped on sellout"

        # 3. Still sold out next run: original time kept, no duplicate log row.
        s = mc.SimplifiedEventScraper()
        s.existing_events_metadata = {uid: {'summary': sold['title'],
                                            'had_tickets': False,
                                            'soldout_at': first_seen}}
        again = s._apply_ticket_state([_event('sold_out')])[0]
        assert again['soldout_at'] == first_seen
        assert open(mc.SELLOUT_LOG_PATH).read() == log, "logged the same sellout twice"

        # 4. Widget failed to render: keep the last known state, invent nothing.
        s = mc.SimplifiedEventScraper()
        s.existing_events_metadata = {uid: {'summary': sold['title'],
                                            'had_tickets': False,
                                            'soldout_at': first_seen}}
        unknown = s._apply_ticket_state([_event('unknown')])[0]
        assert unknown['soldout_at'] == first_seen
        assert open(mc.SELLOUT_LOG_PATH).read() == log

        # 5. Back on sale: sellout time cleared and the reversal logged.
        s = mc.SimplifiedEventScraper()
        s.existing_events_metadata = {uid: {'summary': sold['title'],
                                            'had_tickets': False,
                                            'soldout_at': first_seen}}
        back = s._apply_ticket_state([_event('on_sale')])[0]
        assert back['soldout_at'] is None
        assert 'back_on_sale' in open(mc.SELLOUT_LOG_PATH).read()

        # 6. The sellout time lands in the ICS and survives a reload.
        ics = s.generate_ics_calendar([sold])
        assert mc.SOLDOUT_PROP in ics
        assert first_seen.isoformat() in ics.replace('\r\n ', '')
        assert 'Sold out as of' in ics.replace('\r\n ', '')


def test_soldout_roundtrip_through_ics():
    """A written sellout time must be readable by the next scrape."""
    with tempfile.TemporaryDirectory() as tmp:
        mc.SELLOUT_LOG_PATH = os.path.join(tmp, 'sellouts.csv')
        s = mc.SimplifiedEventScraper()
        s.existing_events_metadata = {}
        sold = s._apply_ticket_state([_event('sold_out')])[0]
        ics_path = os.path.join(tmp, 'out.ics')
        with open(ics_path, 'w', encoding='utf-8') as fh:
            fh.write(s.generate_ics_calendar([sold]))

        original = mc.OUTPUT_PATH
        try:
            mc.OUTPUT_PATH = ics_path
            reloaded = mc.SimplifiedEventScraper()._load_existing_ics_metadata()
        finally:
            mc.OUTPUT_PATH = original

        meta = reloaded['abc123@woodstockfilmfestival.org']
        assert meta['soldout_at'] == sold['soldout_at']
        assert meta['had_tickets'] is False


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"ok  {name}")
    print("all checks passed")
