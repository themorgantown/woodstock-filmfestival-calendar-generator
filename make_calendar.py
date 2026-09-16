"""
Woodstock Film Festival Event Scraper (year set by WFF_YEAR, default 2026)
Reads the single all-events page's own event JSON.

- Single URL: https://woodstockfilmfestival.org/{YEAR}-all-events
- The page fetches the whole Eventive bucket once into `cachedEvents` and
  renders every box and overlay from it, so one page load has all the data
- Deduplicates events by title+venue+datetime
- Generates ICS calendar file
- No scheduling (handled by GitHub Actions)

Dependencies:
    pip install playwright icalendar python-dateutil beautifulsoup4
    python -m playwright install chromium

Usage:
    python make_calendar_v2.py
"""

import csv
import logging
import os
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from icalendar import Calendar, Event, vText, Timezone
import pytz

import venues

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('wff_calendar_v2.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
YEAR = int(os.environ.get("WFF_YEAR", 2026))
ALL_EVENTS_URL = f"https://woodstockfilmfestival.org/{YEAR}-all-events"
OUTPUT_PATH = f"wff_{YEAR}_complete.ics"
# Year-free copy of the current year's calendar - the URL advertised publicly
STABLE_OUTPUT_PATH = "woodstockfilmfestival.ics"
SELLOUT_LOG_PATH = "sellouts.csv"
DEFAULT_DURATION_HOURS = 2
TZ_ID = "America/New_York"
PAGE_LOAD_ATTEMPTS = 3  # The all-events page renders client-side and can stall
EVENT_DATA_TIMEOUT = 30000  # Milliseconds to wait for the page's event JSON
# A scrape holding less than this share of the previous run's events is treated
# as a broken upstream, not as events being cancelled.
MIN_EVENT_RATIO = 0.8
# ICS property holding when an event was first seen sold out
SOLDOUT_PROP = "X-WFF-SOLDOUT"
TICKET_EMOJI = "🎟️"
SOLDOUT_EMOJI = "🔴"

# Timezone support - use pytz for ICS compatibility
TZ = pytz.timezone(TZ_ID)


class SimplifiedEventScraper:
    """Scraper that clicks through event overlays on a single page"""
    
    def __init__(self):
        self.events: List[Dict] = []
        self.seen_event_ids: Set[str] = set()
        self.existing_events_metadata: Dict[str, Dict] = {}
        
    def scrape_all_events(self) -> List[Dict]:
        """Read every event from the JSON the page itself fetched.

        The page asks Eventive once for the whole bucket and keeps the answer
        in `cachedEvents`; every overlay is just that array re-rendered by
        client-side JS. So load the page once and read the array instead of
        clicking through 89 overlays for data that is already in memory.
        """
        logger.info(f"Starting scraper for {ALL_EVENTS_URL}")

        raw_events: List[Dict] = []
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            # Nothing we read is a pixel.
            page.route("**/*.{png,jpg,jpeg,gif,svg,webp,ico}", lambda route: route.abort())

            # Second source for the same data: the Eventive reply the page is
            # reacting to. It survives the page renaming its variables, and
            # costs nothing - the request happens either way. Only the URL is
            # looked at here; reading a body inside a response handler hangs
            # the sync API, so the JSON is parsed later, and only if needed.
            api_responses = []
            page.on("response", lambda response: api_responses.append(response)
                    if 'event_buckets' in response.url and '/events' in response.url
                    else None)

            try:
                # The Squarespace page renders client-side and sometimes
                # stalls, so retry rather than failing the whole hourly run on
                # one slow load.
                for attempt in range(1, PAGE_LOAD_ATTEMPTS + 1):
                    try:
                        logger.info(f"Loading all-events page (attempt {attempt})...")
                        page.goto(ALL_EVENTS_URL, wait_until="domcontentloaded",
                                  timeout=45000)
                        # cachedEvents is a script-scope `let`, not a property
                        # of window, so it has to be named bare.
                        page.wait_for_function(
                            "typeof cachedEvents !== 'undefined' && cachedEvents.length > 0",
                            timeout=EVENT_DATA_TIMEOUT)
                        raw_events = page.evaluate("cachedEvents")
                        logger.info(f"Read {len(raw_events)} events from cachedEvents")
                        break
                    except PlaywrightTimeout as e:
                        # The page's own array is the preferred source because
                        # it is exactly what the site shows. If it's gone, the
                        # API reply still has everything.
                        fallback = self._events_from_responses(api_responses)
                        if fallback:
                            raw_events = fallback
                            logger.warning(
                                f"cachedEvents unusable ({e}); fell back to the "
                                f"Eventive response: {len(raw_events)} events")
                            break
                        if attempt == PAGE_LOAD_ATTEMPTS:
                            raise
                        logger.warning(f"Page load attempt {attempt} failed: {e}")
                        time.sleep(5)

            except Exception as e:
                logger.error(f"Fatal error during scraping: {e}")
            finally:
                browser.close()

        logger.info(f"Page held {len(raw_events)} events")

        for raw in raw_events:
            event_data = self._event_from_api(raw)
            if not event_data:
                continue

            event_id = self._create_event_id(event_data)
            if event_id in self.seen_event_ids:
                logger.info(f"⊘ Duplicate skipped: {event_data['title']}")
                continue

            self.events.append(event_data)
            self.seen_event_ids.add(event_id)

        logger.info(f"Scraping complete. Found {len(self.events)} unique events")
        return self.events

    @staticmethod
    def _events_from_responses(responses) -> List[Dict]:
        """Pull the event list out of the captured Eventive replies.

        Newest first, and every failure is shrugged off: this only runs when
        the page's own array is already gone, so a bad body here just means
        there is nothing to fall back to.
        """
        best: List[Dict] = []
        for response in responses:
            # The loader also asks for ?upcoming_only=true, which stops
            # returning an event the moment it starts - that reply must never
            # become the calendar.
            if 'upcoming_only' in response.url:
                continue
            try:
                body = response.json()
            except Exception as e:
                logger.warning(f"Could not read {response.url}: {e}")
                continue
            events = body.get('events') if isinstance(body, dict) else None
            if events and len(events) > len(best):
                # The page hides virtual events; match it.
                best = [e for e in events if not e.get('is_virtual')]
        return best

    def _event_from_api(self, raw: Dict) -> Optional[Dict]:
        """Map one Eventive event object to the dict the rest of the code uses"""
        event_id = raw.get('id')
        title = (raw.get('name') or '').strip()
        if not event_id or not title:
            logger.warning(f"Skipping event with no id or name: {raw.get('id')!r}")
            return None

        try:
            start = dateparser.isoparse(raw['start_time']).astimezone(TZ)
        except (KeyError, ValueError) as e:
            logger.warning(f"No usable start time for {title}: {e}")
            return None

        # The page prints the venue name with its address underneath and
        # venues.resolve() reads that block; the JSON keeps the two apart.
        venue_obj = raw.get('venue') or {}
        venue_block = '\n'.join(part for part in (venue_obj.get('name', ''),
                                                  venue_obj.get('address', ''))
                                if part)
        if not venue_block:
            logger.warning(f"No venue found for {title}")
            venue_block = "TBD"

        venue = venues.venue_name(venue_block)
        location, address_verified = venues.resolve(venue_block)
        if not address_verified and venue != "TBD":
            logger.warning(f"No verified address for venue {venue!r} - "
                           f"using the site's own text: {location!r}")

        # tickets_available is the field the Eventive button itself renders
        # from, so no widget has to mount for us to see a sellout. None means
        # the API stopped sending it - keep the previous scrape's state.
        available = raw.get('tickets_available')
        if available is None:
            ticket_status = 'unknown'
        else:
            ticket_status = 'on_sale' if available else 'sold_out'

        return {
            'title': title,
            'start': start,
            'venue': venue,
            'location': location,
            'description': self._description_text(raw.get('description')),
            'has_tickets': ticket_status == 'on_sale',
            'ticket_status': ticket_status,
            'event_id': event_id,
            'url': f"{ALL_EVENTS_URL}?eventId={event_id}",
        }

    @staticmethod
    def _description_text(html: Optional[str]) -> str:
        """Flatten the description HTML into the text the ICS carries"""
        if not html:
            return ''
        soup = BeautifulSoup(html, 'html.parser')
        paragraphs = [p.get_text(strip=True) for p in soup.find_all('p')]
        paragraphs = [p for p in paragraphs if p]
        return '\n\n'.join(paragraphs) if paragraphs else soup.get_text(strip=True)
    def _create_event_id(self, event_data: Dict) -> str:
        """Create a unique ID for deduplication"""
        title = event_data.get('title', '').lower().strip()
        start = event_data.get('start')
        venue = event_data.get('venue', '').lower().strip()
        
        # Remove ticket emoji for ID generation
        title = title.replace('🎟️', '').strip()
        
        if start:
            timestamp = start.strftime('%Y%m%d%H%M')
        else:
            timestamp = 'nodate'
        
        return f"{title}_{venue}_{timestamp}"
    
    def _load_existing_ics_metadata(self) -> Dict[str, Dict]:
        """Load existing ICS file and extract DTSTAMP and other metadata for each event"""
        output_file = Path(OUTPUT_PATH)
        if not output_file.exists():
            logger.info("No existing ICS file found - all events will be new")
            return {}
        
        try:
            existing_content = output_file.read_text(encoding='utf-8')
            existing_cal = Calendar.from_ical(existing_content)
            
            metadata = {}
            for component in existing_cal.walk('VEVENT'):
                # Extract UID
                uid = str(component.get('uid', ''))
                if not uid:
                    continue
                
                # Extract event signature for comparison
                summary = str(component.get('summary', ''))
                dtstart = component.get('dtstart')
                location = str(component.get('location', ''))
                description = str(component.get('description', ''))
                
                # Create a comparable signature
                if dtstart:
                    if hasattr(dtstart, 'dt'):
                        start_dt = dtstart.dt
                    else:
                        start_dt = dtstart
                else:
                    start_dt = None
                
                # Get DTSTAMP
                dtstamp = component.get('dtstamp')
                if dtstamp:
                    if hasattr(dtstamp, 'dt'):
                        dtstamp_dt = dtstamp.dt
                    else:
                        dtstamp_dt = dtstamp
                else:
                    dtstamp_dt = None
                
                # Previous ticket state: the emoji in the summary is what the
                # last scrape concluded, and SOLDOUT_PROP is when tickets first
                # disappeared. Both carry forward when a scrape can't tell.
                soldout_at = None
                raw_soldout = component.get(SOLDOUT_PROP)
                if raw_soldout:
                    try:
                        soldout_at = dateparser.isoparse(str(raw_soldout))
                    except ValueError:
                        logger.warning(f"Bad {SOLDOUT_PROP} on {uid}: {raw_soldout}")

                # Store metadata indexed by UID
                metadata[uid] = {
                    'uid': uid,
                    'summary': summary,
                    'dtstart': start_dt,
                    'location': location,
                    'description': description,
                    'dtstamp': dtstamp_dt,
                    'had_tickets': TICKET_EMOJI in summary,
                    'soldout_at': soldout_at,
                }
            
            logger.info(f"Loaded metadata for {len(metadata)} existing events")
            return metadata
            
        except Exception as e:
            logger.warning(f"Could not load existing ICS file: {e}")
            return {}
    
    def _apply_ticket_state(self, events: List[Dict]) -> List[Dict]:
        """Resolve each event's ticket state against the previous scrape.

        Sets 'soldout_at' (when tickets first vanished), decorates the title,
        and appends a row to the sellout log on every state change.
        """
        now = datetime.now(TZ)
        transitions = []

        for event_data in events:
            uid = f"{event_data.get('event_id', self._create_event_id(event_data))}@woodstockfilmfestival.org"
            previous = self.existing_events_metadata.get(uid, {})
            prior_soldout = previous.get('soldout_at')
            status = event_data.get('ticket_status', 'unknown')

            if status == 'on_sale':
                soldout_at = None
                if prior_soldout:
                    transitions.append((now, event_data, 'back_on_sale'))
            elif status == 'sold_out':
                soldout_at = prior_soldout or now
                if not prior_soldout:
                    transitions.append((now, event_data, 'sold_out'))
            else:
                # Widget didn't render - keep the last known state rather than
                # reporting a sellout that may just be a slow page.
                soldout_at = prior_soldout
                event_data['has_tickets'] = previous.get('had_tickets', False)

            event_data['soldout_at'] = soldout_at

            title = event_data['title']
            if soldout_at and SOLDOUT_EMOJI not in title:
                title = f"{title} {SOLDOUT_EMOJI}"
            elif event_data.get('has_tickets') and TICKET_EMOJI not in title:
                title = f"{title} {TICKET_EMOJI}"
            event_data['title'] = title

        if transitions:
            self._log_ticket_transitions(transitions)

        sold_out_now = sum(1 for e in events if e.get('soldout_at'))
        unknown = sum(1 for e in events if e.get('ticket_status') == 'unknown')
        logger.info(f"Ticket state: {sold_out_now} sold out, {unknown} unknown, "
                    f"{len(transitions)} change(s) this run")
        return events

    def _log_ticket_transitions(self, transitions: List[tuple]) -> None:
        """Append sellout / back-on-sale rows to the CSV log"""
        log_path = Path(SELLOUT_LOG_PATH)
        write_header = not log_path.exists()
        try:
            with log_path.open('a', newline='', encoding='utf-8') as fh:
                writer = csv.writer(fh)
                if write_header:
                    writer.writerow(['detected_at', 'status', 'event_id', 'title',
                                     'event_start', 'venue', 'hours_before_start'])
                for detected_at, event_data, status in transitions:
                    start = event_data.get('start')
                    hours_before = ''
                    if start:
                        hours_before = f"{(start - detected_at).total_seconds() / 3600:.2f}"
                    title = event_data['title'].replace(TICKET_EMOJI, '').replace(SOLDOUT_EMOJI, '').strip()
                    writer.writerow([
                        detected_at.isoformat(),
                        status,
                        event_data.get('event_id', ''),
                        title,
                        start.isoformat() if start else '',
                        event_data.get('venue', ''),
                        hours_before,
                    ])
                    logger.info(f"{status.upper()}: {title} "
                                f"({hours_before}h before start)")
        except OSError as e:
            logger.error(f"Could not write {SELLOUT_LOG_PATH}: {e}")

    def _normalize_description(self, desc: str) -> str:
        """Normalize description for comparison - remove URL and ticket lines we add"""
        if not desc:
            return ''
        
        lines = desc.split('\n')
        filtered_lines = []
        for line in lines:
            # Skip lines we add programmatically
            if line.strip().startswith('🎟️'):
                continue
            if line.strip().startswith('🔗'):
                continue
            if line.strip().startswith(SOLDOUT_EMOJI):
                continue
            filtered_lines.append(line)
        
        return '\n'.join(filtered_lines).strip()
    
    def _event_has_changed(self, event_data: Dict, existing_metadata: Dict) -> bool:
        """Compare new event data with existing metadata to detect changes"""
        # Compare key fields
        new_summary = event_data.get('title', '').strip()
        new_location = (event_data.get('location') or event_data.get('venue', '')).strip()
        new_description = event_data.get('description', '')
        new_start = event_data.get('start')
        
        old_summary = existing_metadata.get('summary', '').strip()
        old_location = existing_metadata.get('location', '').strip()
        old_description = existing_metadata.get('description', '')
        old_start = existing_metadata.get('dtstart')
        
        # Normalize descriptions (remove our added URL and ticket lines)
        new_desc_normalized = self._normalize_description(new_description)
        old_desc_normalized = self._normalize_description(old_description)
        
        # Check if anything changed
        if new_summary != old_summary:
            return True
        if new_location != old_location:
            return True
        if new_desc_normalized != old_desc_normalized:
            return True
        
        # Compare timestamps (handle timezone-aware and naive datetimes)
        if new_start and old_start:
            # Convert both to UTC for comparison if they're timezone-aware
            try:
                if hasattr(new_start, 'tzinfo') and new_start.tzinfo:
                    new_utc = new_start.astimezone(pytz.UTC)
                else:
                    new_utc = TZ.localize(new_start).astimezone(pytz.UTC)
                    
                if hasattr(old_start, 'tzinfo') and old_start.tzinfo:
                    old_utc = old_start.astimezone(pytz.UTC)
                else:
                    old_utc = TZ.localize(old_start).astimezone(pytz.UTC)
                
                # Compare with 1-minute tolerance (in case of slight differences)
                time_diff = abs((new_utc - old_utc).total_seconds())
                if time_diff > 60:
                    return True
            except Exception as e:
                # If comparison fails, log and consider it changed
                logger.debug(f"Timestamp comparison failed: {e}")
                return True
        elif new_start != old_start:
            return True
        
        return False
    
    def generate_ics_calendar(self, events: List[Dict]) -> str:
        """Generate ICS calendar file from events, preserving DTSTAMP for unchanged events"""
        cal = Calendar()
        cal.add('prodid', f'-//Woodstock Film Festival {YEAR} Unofficial Calendar//EN')
        cal.add('version', '2.0')
        cal.add('x-wr-calname', f'Woodstock Film Festival {YEAR}')
        cal.add('x-wr-timezone', TZ_ID)
        
        # Add VTIMEZONE component for proper timezone support
        # Note: icalendar library should handle this automatically with pytz timezones
        
        events_unchanged = 0
        events_modified = 0
        events_new = 0
        
        for event_data in events:
            event = Event()
            
            # Add event properties
            event.add('summary', event_data['title'])
            event.add('dtstart', event_data['start'])
            
            # Calculate end time (default 2 hours duration)
            end_time = event_data['start'] + timedelta(hours=DEFAULT_DURATION_HOURS)
            event.add('dtend', end_time)
            
            # Add location - full street address so calendar apps can map it
            location = event_data.get('location') or event_data.get('venue')
            if location:
                event.add('location', vText(location))
            
            # Add description
            description_parts = []
            if event_data.get('description'):
                description_parts.append(event_data['description'])
            soldout_at = event_data.get('soldout_at')
            if soldout_at:
                description_parts.append(
                    f"\n{SOLDOUT_EMOJI} Sold out as of "
                    f"{soldout_at.strftime('%a, %b %d %Y at %-I:%M %p %Z')}")
            elif event_data.get('has_tickets'):
                description_parts.append('\n🎟️ Tickets Available')
            if event_data.get('url'):
                description_parts.append(f"\n🔗 {event_data['url']}")
            
            if description_parts:
                event.add('description', '\n'.join(description_parts))
            
            # Add URL
            if event_data.get('url'):
                event.add('url', vText(event_data['url']))
            
            # Add UID
            uid = f"{event_data.get('event_id', self._create_event_id(event_data))}@woodstockfilmfestival.org"
            event.add('uid', uid)

            # Machine-readable sellout time, so it survives the next scrape
            if soldout_at:
                event.add(SOLDOUT_PROP, vText(soldout_at.isoformat()))
            
            # Add timestamp - preserve existing if event hasn't changed
            if uid in self.existing_events_metadata:
                existing_meta = self.existing_events_metadata[uid]
                if not self._event_has_changed(event_data, existing_meta):
                    # Event unchanged - use existing DTSTAMP
                    if existing_meta.get('dtstamp'):
                        event.add('dtstamp', existing_meta['dtstamp'])
                        events_unchanged += 1
                    else:
                        event.add('dtstamp', datetime.now(TZ))
                        events_modified += 1
                else:
                    # Event changed - use current timestamp
                    event.add('dtstamp', datetime.now(TZ))
                    events_modified += 1
            else:
                # New event - use current timestamp
                event.add('dtstamp', datetime.now(TZ))
                events_new += 1
            
            cal.add_component(event)
        
        logger.info(f"Event changes: {events_new} new, {events_modified} modified, {events_unchanged} unchanged")
        
        return cal.to_ical().decode('utf-8')
    
    def run(self):
        """Main execution method"""
        try:
            # Load existing ICS metadata first
            logger.info("Loading existing ICS file for comparison...")
            self.existing_events_metadata = self._load_existing_ics_metadata()
            
            # Scrape events
            events = self.scrape_all_events()
            
            if not events:
                logger.error("No events found! Check the page structure.")
                # Exit non-zero so CI fails loudly instead of silently no-op'ing
                # and so nothing downstream deploys a stale calendar.
                raise SystemExit(1)

            # A half-empty upstream reply would otherwise quietly delete events
            # out of every subscriber's calendar. Stalling on the last good
            # calendar is the cheaper mistake.
            # ponytail: flat ratio, not per-event reconciliation - revisit if
            # the site ever starts dropping events once they have happened.
            previous_count = len(self.existing_events_metadata)
            if previous_count and len(events) < previous_count * MIN_EVENT_RATIO:
                logger.error(
                    f"Only {len(events)} events, down from {previous_count} - "
                    f"refusing to publish. Re-run once the site is back.")
                raise SystemExit(1)
            
            # Resolve sellouts against the previous scrape before writing
            events = self._apply_ticket_state(events)

            # Generate ICS
            logger.info(f"Generating ICS calendar with {len(events)} events...")
            ics_content = self.generate_ics_calendar(events)
            
            # Write to file
            output_file = Path(OUTPUT_PATH)
            output_file.write_text(ics_content, encoding='utf-8')
            Path(STABLE_OUTPUT_PATH).write_text(ics_content, encoding='utf-8')

            logger.info(f"✓ Calendar saved to {OUTPUT_PATH} and {STABLE_OUTPUT_PATH}")
            logger.info(f"✓ Total events: {len(events)}")
            
            # Print summary
            print("\n" + "="*60)
            print(f"Scraping Complete!")
            print("="*60)
            print(f"Events scraped: {len(events)}")
            print(f"Output files: {OUTPUT_PATH}, {STABLE_OUTPUT_PATH}")
            print("="*60)
            
            # Show sample events
            print("\nSample events:")
            for event in events[:5]:
                print(f"  • {event['title']}")
                print(f"    {event['start'].strftime('%a, %b %d at %I:%M %p')}")
                print(f"    {event['venue']}")
                print()
            
        except Exception as e:
            logger.error(f"Fatal error: {e}", exc_info=True)
            raise


def main():
    """Entry point"""
    scraper = SimplifiedEventScraper()
    scraper.run()


if __name__ == "__main__":
    main()
