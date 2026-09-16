"""Venue name -> real street address for the calendar's LOCATION field.

The festival site prints an address under each event's venue, but it is
inconsistent (missing ZIPs, a Saugerties ZIP on a Kingston street) and the
names carry screen numbers and prefixes. This module maps whatever the site
says onto one verified address per venue, so calendar apps can map it.

Addresses below were verified against the venues' own sites and listings.
When the site names a venue that isn't in the table, resolve() falls back to
the address the site printed - a new venue degrades to the old behaviour
instead of losing its location.
"""

import re
from typing import Dict, List, Optional, Tuple

# Canonical key -> (display name, street, city, state, zip)
VENUE_ADDRESSES: Dict[str, Tuple[str, str, str, str, str]] = {
    'tinker street cinema': (
        'Tinker Street Cinema', '132 Tinker Street', 'Woodstock', 'NY', '12498'),
    # The playhouse publishes "4 Playhouse Lane at 103 Mill Hill Road"; the
    # Mill Hill Rd form is the one map apps resolve.
    'woodstock playhouse': (
        'Woodstock Playhouse', '103 Mill Hill Rd', 'Woodstock', 'NY', '12498'),
    'bearsville theater': (
        'Bearsville Theater', '291 Tinker Street', 'Woodstock', 'NY', '12498'),
    # The festival calls it Utopia Studios; the venue's own listings say
    # Utopia Soundstage. Same building, next door to Bearsville Theater.
    'utopia studios': (
        'Utopia Studios', '293 Tinker Street', 'Woodstock', 'NY', '12498'),
    # Festival site says 12477 (a Saugerties ZIP) for a Kingston street
    'upstate midtown': (
        'Upstate Midtown', '591 Broadway', 'Kingston', 'NY', '12401'),
    'community theater': (
        'The Community Theatre', '373 Main St', 'Catskill', 'NY', '12414'),
    'woodstock community center': (
        'Woodstock Community Center', '56 Rock City Rd', 'Woodstock', 'NY', '12498'),
    # Festival site gives no ZIP at all for this one
    'unicorn bar': (
        'Unicorn Bar', '224 Foxhall Ave', 'Kingston', 'NY', '12401'),
    # Festival site adds "3rd Floor", which map apps choke on - keep it out of
    # the address and let the venue sort out the floor.
    'assembly': (
        'Assembly', '236 Wall Street', 'Kingston', 'NY', '12401'),
    # Upstate Films' contact page lists 198 Main St - that's their office.
    # 156 is the auditorium entrance (village of Saugerties + festival + Maps).
    'orpheum theatre': (
        'Orpheum Theatre', '156 Main Street', 'Saugerties', 'NY', '12477'),
    'rosendale theatre': (
        'Rosendale Theatre', '408 Main St', 'Rosendale', 'NY', '12472'),
    'broken wing barn at white feather farm': (
        'Broken Wing Barn at White Feather Farm', '1389 Route 212',
        'Saugerties', 'NY', '12477'),
}

# Substring matched against the normalized venue name, for the names the site
# decorates: "Upstate Films (Saugerties): Orpheum Theatre 1" is the Orpheum.
# First match wins, so order these most-specific first.
ALIASES: List[Tuple[str, str]] = [
    # "Upstate Films (Saugerties): Orpheum Theatre 1"
    ('orpheum', 'orpheum theatre'),
    ('utopia', 'utopia studios'),
    ('bearsville', 'bearsville theater'),
]


def normalize(name: str) -> str:
    """Lowercase, punctuation-free key, with trailing screen numbers dropped"""
    key = re.sub(r'[^a-z0-9]+', ' ', name.lower()).strip()
    # "orpheum theatre 1" and "orpheum theatre 2" are the same building
    key = re.sub(r'\s+\d+$', '', key)
    return key


def lookup(venue_name: str) -> Optional[Tuple[str, str, str, str, str]]:
    """Return the verified address tuple for a venue name, or None"""
    key = normalize(venue_name)
    if key in VENUE_ADDRESSES:
        return VENUE_ADDRESSES[key]
    for fragment, canonical in ALIASES:
        if fragment in key:
            return VENUE_ADDRESSES[canonical]
    return None


def format_address(entry: Tuple[str, str, str, str, str]) -> str:
    """One-line LOCATION string - the form Apple/Google Maps parse"""
    display, street, city, state, zipcode = entry
    return f"{display}, {street}, {city}, {state} {zipcode}"


def resolve(venue_block: str) -> Tuple[str, bool]:
    """Turn the site's venue text into a LOCATION string.

    venue_block is the raw text under "Venue:" - the name on the first line
    and the address on the lines below. Returns (location, verified).
    """
    lines = [ln.strip() for ln in venue_block.split('\n') if ln.strip()]
    if not lines:
        return '', False

    name = lines[0]
    entry = lookup(name)
    if entry:
        return format_address(entry), True

    # Unknown venue: keep whatever address the site printed, flattened.
    return ', '.join(lines), False


def venue_name(venue_block: str) -> str:
    """Just the venue's name, for deduplication keys"""
    lines = [ln.strip() for ln in venue_block.split('\n') if ln.strip()]
    return lines[0] if lines else ''
