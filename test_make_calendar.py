"""Self-check for the parsing and sellout logic in make_calendar.py.

Run: python test_make_calendar.py
No framework on purpose - it only has to fail when the logic breaks.
"""

import os
import tempfile
from datetime import datetime

os.environ.setdefault("WFF_YEAR", "2026")

import make_calendar as mc
import venues


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


# Every venue block the site printed for the 2026 festival, verbatim.
# If the site adds a venue, this list is what tells us the table needs a row.
SITE_VENUE_BLOCKS = [
    'Assembly\n236 Wall Street 3rd Floor\nKingston, NY 12401',
    'Bearsville Theater\n291 Tinker Street\nWoodstock, NY 12498',
    'Broken Wing Barn at White Feather Farm\n1389 State Route 212, Saugerties, NY 12477',
    'Community Theater\n373 Main St, Catskill, NY 12414',
    'Rosendale Theatre\n408 Main St\nRosendale, NY 12472',
    'Tinker Street Cinema\n132 Tinker Street\nWoodstock, NY 12498',
    'Unicorn Bar\n224 Foxhall Avenue, Kingston NY',
    'Upstate Films (Saugerties): Orpheum Theatre 1\n156 Main St\nSaugerties, NY 12477',
    'Upstate Midtown\n591 Broadway\nKingston, NY 12477',
    'Utopia Studios\n293 Tinker St, Woodstock, NY 12498',
    'Woodstock Community Center\n56 Rock City Rd\nWoodstock, NY 12498',
    'Woodstock Playhouse\n103 Mill Hill Rd\nWoodstock, NY 12498',
]


def test_every_site_venue_has_a_verified_address():
    """No event should fall back to the site's own sloppy address text."""
    unresolved = [b.split(chr(10))[0] for b in SITE_VENUE_BLOCKS
                  if not venues.resolve(b)[1]]
    assert not unresolved, f"venues missing from VENUE_ADDRESSES: {unresolved}"


def test_resolved_locations_are_mappable():
    """Each LOCATION must carry a street number, a state and a 5-digit ZIP."""
    import re
    for block in SITE_VENUE_BLOCKS:
        location, verified = venues.resolve(block)
        assert verified, block
        assert re.search(r'\d+\s+\w', location), f"no street number: {location}"
        assert re.search(r'\bNY\s+\d{5}$', location), f"no state/ZIP: {location}"
        assert location.count(',') >= 3, f"unexpected shape: {location}"


def test_venue_name_matching_is_forgiving():
    """Screen numbers, case and punctuation must not break the lookup."""
    assert venues.normalize('Orpheum Theatre 1') == 'orpheum theatre'
    assert venues.normalize('  TINKER STREET CINEMA  ') == 'tinker street cinema'
    a = venues.lookup('Upstate Films (Saugerties): Orpheum Theatre 1')
    b = venues.lookup('Upstate Films (Saugerties): Orpheum Theatre 2')
    assert a is not None and a == b, "screen numbers must resolve to one venue"


def test_unknown_venue_falls_back_to_site_text():
    """A venue we haven't verified keeps its address instead of losing it."""
    location, verified = venues.resolve('Some New Barn\n1 Made Up Rd\nWoodstock, NY 12498')
    assert verified is False
    assert location == 'Some New Barn, 1 Made Up Rd, Woodstock, NY 12498'


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"ok  {name}")
    print("all checks passed")
