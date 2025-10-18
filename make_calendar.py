"""
Woodstock Film Festival 2025 Simplified Event Scraper
Scrapes the single all-events page by clicking each event overlay.

This version replaces the complex multi-venue scraper with a simpler approach:
- Single URL: https://woodstockfilmfestival.org/2025-all-events
- Clicks each event-box to trigger overlay (client-side JS, no server requests)
- Extracts data from overlay DOM following todo.md specification
- Deduplicates events by title+venue+datetime
- Generates ICS calendar file
- No scheduling (handled by GitHub Actions)
- Fast execution with minimal delays (0.1s between clicks)

Dependencies:
    pip install playwright icalendar python-dateutil beautifulsoup4
    python -m playwright install chromium

Usage:
    python make_calendar_v2.py
"""

import logging
import re
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set
from pathlib import Path

from playwright.sync_api import sync_playwright, Page, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup
from icalendar import Calendar, Event, vText

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
ALL_EVENTS_URL = "https://woodstockfilmfestival.org/2025-all-events"
OUTPUT_PATH = "wff_2025_complete.ics"
YEAR = 2025
DEFAULT_DURATION_HOURS = 2
TZ_ID = "America/New_York"
EVENT_BOX_DELAY = 0.1  # Minimal delay - no server requests, just DOM clicks
OVERLAY_WAIT_TIMEOUT = 5000  # Milliseconds to wait for overlay

# Timezone support
try:
    from zoneinfo import ZoneInfo
    TZ = ZoneInfo(TZ_ID)
except ImportError:
    from dateutil import tz as _dateutil_tz
    TZ = _dateutil_tz.gettz(TZ_ID)


class SimplifiedEventScraper:
    """Scraper that clicks through event overlays on a single page"""
    
    def __init__(self):
        self.events: List[Dict] = []
        self.seen_event_ids: Set[str] = set()
        
    def scrape_all_events(self) -> List[Dict]:
        """Main scraping method using Playwright"""
        logger.info(f"Starting scraper for {ALL_EVENTS_URL}")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            try:
                # Navigate to the all-events page
                logger.info("Loading all-events page...")
                page.goto(ALL_EVENTS_URL, wait_until="networkidle", timeout=30000)
                
                # Wait for event boxes to load
                page.wait_for_selector('.event-box', timeout=10000)
                
                # Count total event boxes
                initial_event_boxes = page.query_selector_all('.event-box')
                total_events = len(initial_event_boxes)
                logger.info(f"Found {total_events} event boxes")
                
                # Process each event by index (re-query each time to avoid stale elements)
                for idx in range(total_events):
                    try:
                        logger.info(f"Processing event {idx+1}/{total_events}")
                        
                        # Re-query event boxes to get fresh references
                        event_boxes = page.query_selector_all('.event-box')
                        if idx >= len(event_boxes):
                            logger.warning(f"Event box {idx+1} no longer exists, skipping")
                            continue
                        
                        event_box = event_boxes[idx]
                        event_data = self._scrape_single_event(page, event_box, idx+1)
                        
                        if event_data:
                            # Check for duplicates
                            event_id = self._create_event_id(event_data)
                            if event_id not in self.seen_event_ids:
                                self.events.append(event_data)
                                self.seen_event_ids.add(event_id)
                                logger.info(f"✓ Scraped: {event_data['title']}")
                            else:
                                logger.info(f"⊘ Duplicate skipped: {event_data['title']}")
                        
                        # Minimal delay - no server hammering since it's all client-side JS
                        if EVENT_BOX_DELAY > 0:
                            time.sleep(EVENT_BOX_DELAY)
                        
                    except Exception as e:
                        logger.error(f"❌ Failed to process event {idx+1}: {e}")
                        continue
                
            except Exception as e:
                logger.error(f"Fatal error during scraping: {e}")
            finally:
                browser.close()
        
        logger.info(f"Scraping complete. Found {len(self.events)} unique events")
        return self.events
    
    def _scrape_single_event(self, page: Page, event_box, index: int) -> Optional[Dict]:
        """Click an event box and scrape data from the overlay"""
        try:
            # Get the onclick attribute to extract event ID
            onclick = event_box.get_attribute('onclick')
            if not onclick:
                logger.warning(f"Event {index} has no onclick attribute")
                return None
            
            # Extract event ID from onclick="showSingleEvent('ID')"
            match = re.search(r"showSingleEvent\('([^']+)'\)", onclick)
            if not match:
                logger.warning(f"Could not extract event ID from onclick: {onclick}")
                return None
            
            event_id = match.group(1)
            
            # Click the event box
            event_box.click()
            
            # Wait for the overlay to appear
            try:
                page.wait_for_selector('.event-details', timeout=OVERLAY_WAIT_TIMEOUT)
            except PlaywrightTimeout:
                logger.warning(f"Overlay did not appear for event {event_id}")
                return None
            
            # Small delay to ensure content is fully rendered
            time.sleep(0.5)
            
            # Get the overlay HTML
            overlay_html = page.content()
            
            # Parse the overlay
            event_data = self._parse_overlay(overlay_html, event_id)
            
            # Click back button to return to list view (as per requirements)
            try:
                back_button = page.query_selector('button[onclick="returnToPreviousView()"]')
                if back_button:
                    back_button.click()
                    # Wait for overlay to close
                    page.wait_for_selector('.event-details', state='hidden', timeout=3000)
                else:
                    logger.warning(f"Back button not found for event {event_id}")
                    # Fallback: press Escape
                    page.keyboard.press('Escape')
                    time.sleep(0.2)
            except Exception as e:
                logger.warning(f"Could not close overlay for {event_id}: {e}")
                # Try Escape as last resort
                try:
                    page.keyboard.press('Escape')
                    time.sleep(0.2)
                except:
                    pass
            
            return event_data
            
        except Exception as e:
            logger.error(f"Error scraping event {index}: {e}")
            return None
    
    def _parse_overlay(self, html: str, event_id: str) -> Optional[Dict]:
        """Parse event data from overlay HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        
        # Find the event-details container
        event_details = soup.find('div', class_='event-details')
        if not event_details:
            logger.warning(f"No event-details found for {event_id}")
            return None
        
        # Extract Title
        title_elem = event_details.find('h2', class_='event-title')
        if not title_elem:
            logger.warning(f"No title found for {event_id}")
            return None
        title = title_elem.get_text(strip=True)
        
        # Extract Start Date/Time
        start_dt = None
        start_paragraphs = event_details.find_all('p')
        for p in start_paragraphs:
            strong = p.find('strong')
            if strong and 'Start:' in strong.get_text():
                date_text = p.get_text(strip=True).replace('Start:', '').strip()
                start_dt = self._parse_datetime(date_text)
                break
        
        if not start_dt:
            logger.warning(f"No start date found for {event_id}: {title}")
            return None
        
        # Extract Venue
        venue = None
        for p in start_paragraphs:
            strong = p.find('strong')
            if strong and 'Venue:' in strong.get_text():
                # Get the text after the <strong> tag
                venue_text = p.get_text('\n', strip=True).replace('Venue:', '').strip()
                # Take only the first line (venue name)
                venue = venue_text.split('\n')[0].strip()
                break
        
        if not venue:
            logger.warning(f"No venue found for {event_id}: {title}")
            venue = "TBD"
        
        # Check for tickets availability
        has_tickets = False
        order_tickets_button = event_details.find(string=re.compile(r'Order tickets', re.IGNORECASE))
        if order_tickets_button:
            has_tickets = True
            # Add ticket emoji to title if not already present
            if '🎟️' not in title:
                title = f"{title} 🎟️"
        
        # Extract description
        description_elem = event_details.find('p', class_='event-description')
        description = ""
        if description_elem:
            # Get all siblings after the empty event-description element
            description_parts = []
            for sibling in description_elem.find_next_siblings():
                if sibling.name == 'p':
                    text = sibling.get_text(strip=True)
                    if text:
                        description_parts.append(text)
            description = '\n\n'.join(description_parts)
        
        # Build event data
        event_data = {
            'title': title,
            'start': start_dt,
            'venue': venue,
            'description': description,
            'has_tickets': has_tickets,
            'event_id': event_id,
            'url': f"{ALL_EVENTS_URL}?eventId={event_id}"
        }
        
        return event_data
    
    def _parse_datetime(self, date_text: str) -> Optional[datetime]:
        """Parse datetime from text like 'Sat, Oct 18, 3:15 PM ET'"""
        if not date_text:
            return None
        
        # Clean up the text
        date_text = date_text.strip()
        
        # Remove timezone indicator
        date_text = re.sub(r'\s+(ET|EST|EDT)\s*$', '', date_text)
        
        # Try various datetime formats
        formats = [
            "%a, %b %d, %I:%M %p",  # Sat, Oct 18, 3:15 PM
            "%A, %B %d, %I:%M %p",  # Saturday, October 18, 3:15 PM
            "%b %d, %I:%M %p",      # Oct 18, 3:15 PM
            "%B %d, %I:%M %p",      # October 18, 3:15 PM
            "%m/%d/%Y %I:%M %p",    # 10/18/2025 3:15 PM
            "%Y-%m-%d %I:%M %p",    # 2025-10-18 3:15 PM
        ]
        
        for fmt in formats:
            try:
                # Parse without year first
                dt = datetime.strptime(date_text, fmt)
                # Add the year
                dt = dt.replace(year=YEAR)
                # Add timezone
                if hasattr(dt, 'replace'):
                    dt = dt.replace(tzinfo=TZ)
                return dt
            except ValueError:
                continue
        
        logger.warning(f"Could not parse datetime: {date_text}")
        return None
    
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
    
    def generate_ics_calendar(self, events: List[Dict]) -> str:
        """Generate ICS calendar file from events"""
        cal = Calendar()
        cal.add('prodid', '-//Woodstock Film Festival 2025 Unofficial Calendar//EN')
        cal.add('version', '2.0')
        cal.add('x-wr-calname', 'Woodstock Film Festival 2025')
        cal.add('x-wr-timezone', TZ_ID)
        
        for event_data in events:
            event = Event()
            
            # Add event properties
            event.add('summary', event_data['title'])
            event.add('dtstart', event_data['start'])
            
            # Calculate end time (default 2 hours duration)
            end_time = event_data['start'] + timedelta(hours=DEFAULT_DURATION_HOURS)
            event.add('dtend', end_time)
            
            # Add location
            if event_data.get('venue'):
                event.add('location', vText(event_data['venue']))
            
            # Add description
            description_parts = []
            if event_data.get('description'):
                description_parts.append(event_data['description'])
            if event_data.get('has_tickets'):
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
            
            # Add timestamp
            event.add('dtstamp', datetime.now(tz=TZ))
            
            cal.add_component(event)
        
        return cal.to_ical().decode('utf-8')
    
    def run(self):
        """Main execution method"""
        try:
            # Scrape events
            events = self.scrape_all_events()
            
            if not events:
                logger.error("No events found! Check the page structure.")
                return
            
            # Generate ICS
            logger.info(f"Generating ICS calendar with {len(events)} events...")
            ics_content = self.generate_ics_calendar(events)
            
            # Write to file
            output_file = Path(OUTPUT_PATH)
            output_file.write_text(ics_content, encoding='utf-8')
            
            logger.info(f"✓ Calendar saved to {OUTPUT_PATH}")
            logger.info(f"✓ Total events: {len(events)}")
            
            # Print summary
            print("\n" + "="*60)
            print(f"Scraping Complete!")
            print("="*60)
            print(f"Events scraped: {len(events)}")
            print(f"Output file: {OUTPUT_PATH}")
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
