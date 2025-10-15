"""
Woodstock Film Festival 2025 Comprehensive Event Scraper
Crawls all venue-specific pages and individual event detail pages to create a complete ICS calendar.

Features:
- Discovers venue URLs from sitemap.xml
- Scrapes all venue-specific event pages
- Extracts individual event detail pages for complete information
- Handles JavaScript-rendered content with Playwright
- Generates comprehensive ICS calendar with proper venue mapping
- Runnable as scheduled task with error handling and logging

Dependencies:
    .venv/bin/pip install playwright requests beautifulsoup4 lxml icalendar python-dateutil schedule
    .venv/bin/python -m playwright install --with-deps chromium
"""

import re
import sys
import xml.etree.ElementTree as ET
import hashlib
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Set
from urllib.parse import urljoin, urlparse
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from icalendar import Calendar, Event, vText
import schedule

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('wff_calendar.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
BASE_URL = "https://woodstockfilmfestival.org"
SITEMAP_FILE = "sitemap.xml"
OUTPUT_PATH = "wff_2025_complete.ics"
YEAR = 2025
DEFAULT_DURATION_HOURS = 2
TZ_ID = "America/New_York"
MIN_REASONABLE_EVENTS = 10
MAX_DETAIL_PAGE_ENHANCEMENTS = 50  # Limit detail page fetches to be respectful
VENUE_PAGE_DELAY = 2  # Seconds between venue page requests
DETAIL_PAGE_DELAY = 0.5  # Seconds between detail page requests

# Venue mapping from URL suffix to proper venue name
VENUE_MAPPING = {
    "bearsville": "Bearsville Theater",
    "woodstock-playhouse": "Woodstock Playhouse",
    "tinker-street-cinema": "Tinker Street Cinema",
    "orpheum": "Orpheum Theatre",
    "upstate-midtown": "Upstate Midtown",
    "rosendale": "Rosendale Theatre",
    "assembly": "Assembly",
    "wcc": "Woodstock Community Center [SHORTS]",
    "colony": "Colony",
    "hvlgbtq": "Hudson Valley LGBTQ+ Community Center",
    "special-events": "2025 Special Events",
}

class WoodstockEventScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; WFF-Calendar-Bot/2.0; +https://woodstockfilmfestival.org/)'
        })
        self.venue_urls: List[str] = []
        self.events: List[Dict] = []
        self.processed_event_ids: Set[str] = set()
        
    def discover_venue_urls(self) -> List[str]:
        """Generate venue URLs from VENUE_MAPPING - venues are static"""
        venue_urls = []
        
        # Generate standard venue URLs with /2025-all-events- prefix
        for suffix in VENUE_MAPPING.keys():
            # Skip the special ones that don't follow the pattern
            if suffix in ['panels', 'shorts', 'special-events']:
                continue
            venue_urls.append(f"{BASE_URL}/2025-all-events-{suffix}")
        
        # Add custom URLs that don't follow the standard pattern
        custom_urls = [
            f"{BASE_URL}/2025-panels",
            f"{BASE_URL}/2025-shorts",
            f"{BASE_URL}/2025-special-events",
            f"{BASE_URL}/2025-all-white-feather-farm"
        ]
        venue_urls.extend(custom_urls)
        
        logger.info(f"Generated {len(venue_urls)} venue URLs ({len(venue_urls) - len(custom_urls)} standard + {len(custom_urls)} custom)")
        return venue_urls
    
    def fetch_page_content(self, url: str, use_playwright: bool = False) -> Optional[str]:
        """Fetch page content, optionally using Playwright for JS rendering"""
        try:
            if use_playwright:
                return self._fetch_with_playwright(url)
            else:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                return response.text
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
    
    def _fetch_with_playwright(self, url: str) -> str:
        """Fetch page using Playwright for JS rendering"""
        from playwright.sync_api import sync_playwright
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()
            
            try:
                # Navigate to page and wait for it to load
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
                
                # Wait specifically for event boxes to load
                try:
                    page.wait_for_selector(".event-box.list-view", timeout=15000)
                    logger.info(f"Successfully found .event-box.list-view elements on {url}")
                except:
                    logger.warning(f"No .event-box.list-view found on {url}, waiting for general content")
                    # If specific selectors don't appear, just wait a bit for general content
                    page.wait_for_timeout(5000)
                
                content = page.content()
                return content
            except Exception as e:
                logger.warning(f"Playwright error for {url}: {e}")
                # Try one more time with just basic loading
                try:
                    page.goto(url, timeout=20000)
                    page.wait_for_timeout(5000)  # Just wait 5 seconds
                    return page.content()
                except:
                    raise e
            finally:
                browser.close()

    def extract_events_from_page(self, html: str, source_url: str) -> List[Dict]:
        """Extract events from a venue page"""
        if not html:
            return []

        soup = BeautifulSoup(html, 'lxml')
        events = []
        
        # Look for event containers - updated for venue page format
        event_selectors = [
            ".event-box.list-view",
            ".event-box", 
            ".event-banner", 
            ".event-card",
            "div[class*='event'][class*='banner']",
            "div[class*='event'][class*='card']",
            ".sqs-block-summary-v2 .summary-item"
        ]
        
        cards = []
        for selector in event_selectors:
            found_cards = soup.select(selector)
            if found_cards:
                cards.extend(found_cards)
                logger.info(f"Found {len(found_cards)} cards with selector '{selector}'")
                break
        
        # Debug: If no events found, let's see what's actually on the page
        if not cards:
            # Look for any divs with "event" in the class name
            debug_selectors = [
                "[class*='event']",
                "div[class*='event']", 
                ".event",
                "[data-event]"
            ]
            
            for debug_selector in debug_selectors:
                debug_elements = soup.select(debug_selector)
                if debug_elements:
                    logger.info(f"DEBUG: Found {len(debug_elements)} elements with selector '{debug_selector}'")
                    # Log the first few elements' classes for debugging
                    for i, elem in enumerate(debug_elements[:3]):
                        classes = elem.get('class')
                        if classes:
                            logger.info(f"DEBUG: Element {i+1} classes: {list(classes) if hasattr(classes, '__iter__') else [str(classes)]}")
                        else:
                            logger.info(f"DEBUG: Element {i+1} has no class attribute")

        logger.info(f"Found {len(cards)} event cards on {source_url}")
        
        duplicates_skipped = 0
        for card in cards:
            event_data = self._extract_event_from_card(card, source_url)
            if event_data and event_data.get('title') and event_data.get('start'):
                # Create unique ID for deduplication
                event_id = self._create_event_id(event_data)
                if event_id not in self.processed_event_ids:
                    events.append(event_data)
                    self.processed_event_ids.add(event_id)
                else:
                    duplicates_skipped += 1
                    logger.debug(f"Skipping duplicate event: {event_data.get('title')} at {event_data.get('start')}")
        
        if duplicates_skipped > 0:
            logger.info(f"Skipped {duplicates_skipped} duplicate events from {source_url}")
        
        return events
    
    def _extract_event_from_card(self, card: BeautifulSoup, source_url: str) -> Optional[Dict]:
        """Extract event data from individual event card"""
        # Title - updated for venue page format
        title_selectors = [
            "h3.event-title", 
            ".event-title",
            "h3.event-title .truncate-title",
            ".summary-title a",
            ".summary-title",
            "h3 a", "h2 a", "h1 a"
        ]
        title = self._extract_text_by_selectors(card, title_selectors)
        if not title:
            return None
            
        # Date/Time - updated for venue page format  
        date_selectors = [
            "span.event-date",
            ".event-date", 
            ".summary-metadata-item--date",
            "[class*='date']",
            ".event-time"
        ]
        date_text = self._extract_text_by_selectors(card, date_selectors)
        start_dt = self._parse_datetime(date_text) if date_text else None
        
        if not start_dt:
            return None
        
        # Venue/Location - updated for venue page format
        venue = self._extract_venue_from_card(card)
        if not venue:
            venue = self._infer_venue_from_url(source_url)
        
        # Description - extract from venue page format
        description = self._extract_description_from_card(card)
        
        # Event detail URL - extract from onclick attribute
        detail_url = self._extract_event_detail_url(card, source_url)
        
        return {
            'title': title.strip(),
            'start': start_dt,
            'venue': venue.strip() if venue else '',
            'description': description.strip() if description else '',
            'url': detail_url or source_url,
            'source_url': source_url
        }
    
    def _extract_venue_from_card(self, card: BeautifulSoup) -> Optional[str]:
        """Extract venue from event card using venue page format"""
        # Look for <p><strong>Venue:</strong> Venue Name</p> pattern
        venue_paragraphs = card.find_all('p')
        for p in venue_paragraphs:
            text = p.get_text()
            if 'Venue:' in text:
                # Extract text after "Venue:"
                venue_text = text.split('Venue:', 1)[1].strip()
                if venue_text:
                    return venue_text
        return None
    
    def _extract_description_from_card(self, card: BeautifulSoup) -> Optional[str]:
        """Extract event description from venue page format"""
        description_parts = []
        
        # Find the empty <p class="event-description"></p> element
        desc_elem = card.select_one('p.event-description')
        if desc_elem:
            # Get all following <p> siblings until we hit the venue paragraph
            current_elem = desc_elem.next_sibling
            
            while current_elem:
                # Skip text nodes and look for <p> elements
                if hasattr(current_elem, 'name') and getattr(current_elem, 'name', None) == 'p':
                    text = current_elem.get_text(strip=True)
                    
                    # Stop when we reach the venue information
                    if text and text.startswith('Venue:'):
                        break
                    
                    # Stop when we hit an empty paragraph (common separator)
                    if not text:
                        break
                    
                    # Add non-empty paragraphs that aren't venue info
                    # Filter out image-only paragraphs and venue info
                    if (text and 
                        'Venue:' not in text and 
                        not text.startswith('Moderator:') and  # Keep moderator info
                        len(text) > 20):  # Skip very short paragraphs that are likely formatting
                        description_parts.append(text)
                        
                current_elem = current_elem.next_sibling
            
            return '\n\n'.join(description_parts) if description_parts else None
        
        # Fallback: look within the event-details div for description paragraphs
        event_details = card.select_one('.event-details')
        if event_details:
            paragraphs = event_details.find_all('p')
            for p in paragraphs:
                text = p.get_text(strip=True)
                # Skip empty paragraphs, venue info, and the event-description class itself
                classes = p.get('class')
                is_description_elem = False
                if classes:
                    is_description_elem = 'event-description' in (classes if isinstance(classes, list) else [str(classes)])
                
                if (text and 
                    not text.startswith('Venue:') and 
                    'Venue:' not in text and
                    not is_description_elem):
                    description_parts.append(text)
        
        return '\n\n'.join(description_parts) if description_parts else None
    
    def _extract_text_by_selectors(self, element: BeautifulSoup, selectors: List[str]) -> Optional[str]:
        """Try multiple selectors to extract text"""
        for selector in selectors:
            found = element.select_one(selector)
            if found:
                text = found.get_text(" ", strip=True)
                if text:
                    return text
        return None
    
    def _infer_venue_from_url(self, url: str) -> str:
        """Infer venue name from URL pattern"""
        # Check for custom URLs first (exact match)
        if url.endswith('/2025-panels'):
            return "2025 Panels"
        elif url.endswith('/2025-shorts'):
            return "2025 Shorts"
        elif url.endswith('/2025-special-events'):
            return "2025 Special Events"
        
        # Standard pattern matching
        for suffix, venue_name in VENUE_MAPPING.items():
            if f"-{suffix}" in url or url.endswith(suffix):
                return venue_name
        return ""
    
    def _extract_event_detail_url(self, card: BeautifulSoup, source_url: str) -> Optional[str]:
        """Extract event detail URL from card - updated for venue page format"""
        # First try to extract event ID from onclick attributes like showSingleEvent('68c821f8753473748e277a05')
        onclick_elements = card.find_all(attrs={'onclick': True})
        for elem in onclick_elements:
            onclick_attr = elem.get('onclick')
            if onclick_attr:
                onclick = str(onclick_attr)
                if 'showSingleEvent' in onclick:
                    # Extract event ID from showSingleEvent('event-id')
                    match = re.search(r"showSingleEvent\('([^']+)'\)", onclick)
                    if match:
                        event_id = match.group(1)
                        return f"{BASE_URL}/2025-all-events?eventId={event_id}"
        
        # Fallback to existing link extraction
        link_selectors = [
            "a[href*='eventId=']",
            "a[href*='/event']", 
            "a[href*='/film']",
            ".event-link",
            "a[href*='/2025']",
            "a"
        ]
        
        for selector in link_selectors:
            link = card.select_one(selector)
            if link and link.has_attr('href'):
                href_attr = link.get('href')
                if href_attr:
                    href = str(href_attr).strip()
                    if href.startswith('/'):
                        href = urljoin(BASE_URL, href)
                    # Prefer URLs with eventId parameter
                    if 'eventId=' in href:
                        return href
        return None
    
    def _parse_datetime(self, date_text: str) -> Optional[datetime]:
        """Parse datetime from various formats"""
        if not date_text:
            return None
            
        # Clean up the text
        s = re.sub(r'\bE[DS]?T\b\.?', '', date_text).strip(', ').strip()
        
        # Patterns to try - updated for venue page format
        patterns = [
            # "Wednesday, Oct 15 at 5:00 PM"
            r'^[A-Za-z]+,\s+([A-Za-z]{3})\s+(\d{1,2})\s+at\s+(\d{1,2}:\d{2}\s+[AP]M)',
            # "Oct 15, 5:00 PM"
            r'([A-Za-z]{3})\s+(\d{1,2}),\s+(\d{1,2}:\d{2}\s+[AP]M)',
            # "Wednesday, Oct 15, 5:00 PM"  
            r'^[A-Za-z]+,\s+([A-Za-z]{3})\s+(\d{1,2}),\s+(\d{1,2}:\d{2}\s+[AP]M)',
            # "Wed, Oct 15, 11:00 AM"
            r'^[A-Za-z]{3},\s+([A-Za-z]{3})\s+(\d{1,2}),\s+(\d{1,2}:\d{2}\s+[AP]M)',
        ]
        
        months = {
            'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
            'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
        }
        
        for pattern in patterns:
            match = re.search(pattern, s)
            if match:
                try:
                    if len(match.groups()) == 3:
                        month_str, day_str, time_str = match.groups()
                    else:
                        continue
                        
                    month_num = months.get(month_str)
                    if not month_num:
                        continue
                        
                    return datetime.strptime(
                        f"{YEAR}-{month_num:02d}-{int(day_str):02d} {time_str}",
                        "%Y-%m-%d %I:%M %p"
                    )
                except (ValueError, KeyError):
                    continue
        
        return None
    
    def _create_event_id(self, event_data: Dict) -> str:
        """Create unique ID for event deduplication"""
        key_parts = [
            event_data.get('title', ''),
            event_data.get('start', datetime.now()).isoformat(),
            event_data.get('venue', ''),
        ]
        return hashlib.md5('|'.join(str(p) for p in key_parts).encode()).hexdigest()
    
    def _get_custom_events(self) -> List[Dict]:
        """Return list of custom hardcoded events that should be added to the calendar"""
        custom_events = []
        
        # Colony - A BREAK IN THE RAIN
        try:
            custom_events.append({
                'title': 'A BREAK IN THE RAIN',
                'start': datetime(2025, 10, 15, 19, 0),  # 10/15 7:00 PM
                'venue': 'Colony',
                'description': "Jake Watson has been on the road for ten years since his wife passed away. When his grown son dies, he comes home to a life he walked out on. A stranger in his own house, he takes a job driving a limo to help his daughter-in-law pay the bills. One rainy afternoon he picks up Catriona Walsh, a Nashville singer with her own secret. One ride becomes a two week road trip. Catriona sets his poems to music, and Jake begins to heal, as they both find their way home.",
                'url': 'https://woodstockfilmfestival.org/2025-film-guide?filmId=689f72571f570dd52f0c566e',
                'source_url': 'https://woodstockfilmfestival.org/2025-all-events-colony'
            })
            logger.info("Added custom event: A BREAK IN THE RAIN at Colony")
        except Exception as e:
            logger.error(f"Error creating custom event: {e}")
        
        return custom_events
    
    def enhance_event_with_detail_page(self, event: Dict) -> Dict:
        """Fetch event detail page for enhanced information"""
        detail_url = event.get('url')
        if not detail_url or 'eventId=' not in detail_url:
            return event
            
        logger.info(f"Fetching detail page: {detail_url}")
        
        try:
            detail_html = self.fetch_page_content(detail_url)
            if not detail_html:
                return event
                
            soup = BeautifulSoup(detail_html, 'lxml')
            
            # Enhanced description from detail page
            # Only enhance if the current description is empty or generic
            current_desc = event.get('description', '')
            
            desc_selectors = [
                ".event-description p",
                ".event-details p",  
                "[class*='description'] p"
            ]
            
            enhanced_desc = ""
            for selector in desc_selectors:
                elements = soup.select(selector)
                if elements:
                    # Filter out venue information and empty paragraphs
                    paragraphs = []
                    for elem in elements:
                        text = elem.get_text(strip=True)
                        if text and not text.startswith('Venue:') and 'Venue:' not in text:
                            # Also filter out the venue list that appears on detail pages
                            if not any(v in text for v in ['Bearsville Theater', 'Colony', 'WOODSTOCK', 'KINGSTON', 'ROSENDALE', 'SAUGERTIES']):
                                paragraphs.append(text)
                    
                    enhanced_desc = '\n\n'.join(p for p in paragraphs if p and len(p) > 10)
                    if enhanced_desc:
                        break
            
            # Only update description if we got something better
            if enhanced_desc and (not current_desc or len(enhanced_desc) > len(current_desc)):
                event['description'] = enhanced_desc
                
            # Try to get more precise venue info
            venue_elem = soup.select_one('.event-details strong:contains("Venue:") + br')
            if venue_elem and venue_elem.next_sibling:
                venue_text = str(venue_elem.next_sibling).strip()
                if venue_text and len(venue_text) < 200:  # Reasonable venue name length
                    event['venue'] = venue_text
            
            time.sleep(DETAIL_PAGE_DELAY)  # Rate limiting
            
        except Exception as e:
            logger.error(f"Error enhancing event with detail page {detail_url}: {e}")
        
        return event
    
    def generate_ics_calendar(self, events: List[Dict]) -> str:
        """Generate ICS calendar from events"""
        cal = Calendar()
        cal.add('prodid', '-//Woodstock Film Festival 2025 Complete Calendar//EN')
        cal.add('version', '2.0')
        cal.add('calscale', 'GREGORIAN')
        
        for event_data in events:
            event = Event()
            
            # Required fields
            event.add('uid', self._generate_uid(event_data))
            event.add('dtstart', event_data['start'])
            event.add('dtend', event_data['start'] + timedelta(hours=DEFAULT_DURATION_HOURS))
            event.add('summary', vText(event_data['title']))
            
            # Optional fields
            if event_data.get('venue'):
                event.add('location', vText(event_data['venue']))
            if event_data.get('description'):
                event.add('description', vText(event_data['description']))
            if event_data.get('url'):
                event.add('url', vText(event_data['url']))
                
            event.add('dtstamp', datetime.now())
            cal.add_component(event)
        
        return cal.to_ical().decode('utf-8')
    
    def _generate_uid(self, event_data: Dict) -> str:
        """Generate unique UID for ICS event"""
        base = f"{event_data['title']}|{event_data['start'].isoformat()}|{event_data.get('venue', '')}"
        hash_part = hashlib.sha256(base.encode()).hexdigest()[:16]
        slug = re.sub(r'[^a-z0-9]+', '-', event_data['title'].lower()).strip('-')[:30]
        return f"{slug}-{event_data['start'].strftime('%Y%m%dT%H%M%S')}-{hash_part}@wff2025"
    
    def scrape_all_events(self) -> List[Dict]:
        """Main scraping workflow"""
        logger.info("Starting comprehensive event scraping...")
        
        # Discover venue URLs
        self.venue_urls = self.discover_venue_urls()
        logger.info(f"Will scrape {len(self.venue_urls)} venue pages")
        
        all_events = []
        
        # Scrape each venue page
        for i, venue_url in enumerate(self.venue_urls, 1):
            logger.info(f"Scraping venue page {i}/{len(self.venue_urls)}: {venue_url}")
            
            # Use Playwright directly since these pages require JavaScript
            try:
                html = self.fetch_page_content(venue_url, use_playwright=True)
                events = self.extract_events_from_page(html or "", venue_url)
                logger.info(f"Extracted {len(events)} events from {venue_url}")
                all_events.extend(events)
            except Exception as e:
                logger.error(f"Failed to scrape {venue_url}: {e}")
                # Continue with next venue instead of stopping
                continue
            
            # Rate limiting between requests
            time.sleep(VENUE_PAGE_DELAY)
        
        logger.info(f"Found {len(all_events)} unique events after deduplication")
        
        # Add custom hardcoded events
        custom_events = self._get_custom_events()
        if custom_events:
            logger.info(f"Adding {len(custom_events)} custom hardcoded events")
            all_events.extend(custom_events)
        
        # Enhance selected events with detail pages (limit to avoid overload)
        events_with_detail_urls = [e for e in all_events if e.get('url') and 'eventId=' in e.get('url', '')]
        logger.info(f"Enhancing up to {MAX_DETAIL_PAGE_ENHANCEMENTS} events with detail pages (found {len(events_with_detail_urls)} eligible)...")
        
        enhanced_events = []
        enhancements_done = 0
        for event in all_events:
            if (event.get('url') and 'eventId=' in event.get('url', '') and 
                enhancements_done < MAX_DETAIL_PAGE_ENHANCEMENTS):
                enhanced_event = self.enhance_event_with_detail_page(event)
                enhanced_events.append(enhanced_event)
                enhancements_done += 1
            else:
                enhanced_events.append(event)
        
        # Sort by date
        enhanced_events.sort(key=lambda x: x['start'])
        
        # Log summary
        total_cards_found = sum(1 for _ in self.processed_event_ids)
        logger.info(f"Completed scraping with {len(enhanced_events)} unique events")
        logger.info(f"Deduplication: Processed {total_cards_found} event IDs across all venues")
        
        return enhanced_events
    
    def run_scraping_job(self):
        """Run complete scraping job and generate calendar"""
        try:
            events = self.scrape_all_events()
            
            if not events:
                logger.error("No events found!")
                return False
            
            # Generate ICS calendar
            ics_content = self.generate_ics_calendar(events)
            
            # Write to file
            with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
                f.write(ics_content)
            
            logger.info(f"Successfully wrote {len(events)} events to {OUTPUT_PATH}")
            return True
            
        except Exception as e:
            logger.error(f"Scraping job failed: {e}", exc_info=True)
            return False

def setup_scheduler():
    """Setup scheduled execution"""
    scraper = WoodstockEventScraper()
    
    # Schedule to run every hour
    schedule.every().hour.do(scraper.run_scraping_job)
    
    # Also run immediately
    logger.info("Running initial scraping job...")
    scraper.run_scraping_job()
    
    # Keep running
    logger.info("Starting scheduler (runs every hour)...")
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--schedule":
        setup_scheduler()
    else:
        # Single run
        scraper = WoodstockEventScraper()
        success = scraper.run_scraping_job()
        sys.exit(0 if success else 1)