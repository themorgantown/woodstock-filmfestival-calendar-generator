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
    "kleinert-james": "Kleinert/James Art Center [PANELS]",
    "hvlgbtq": "Hudson Valley LGBTQ+ Community Center",
    "broken-wing-barn": "Broken Wing Barn at White Feather Farm",
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
        """Extract venue URLs from sitemap.xml"""
        try:
            if not Path(SITEMAP_FILE).exists():
                logger.warning(f"Sitemap file {SITEMAP_FILE} not found, using fallback URLs")
                return self._get_fallback_venue_urls()
            
            # Read and clean the XML content
            with open(SITEMAP_FILE, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Remove the comment line at the top if present
            lines = content.split('\n')
            xml_start = 0
            for i, line in enumerate(lines):
                if line.strip().startswith('<urlset'):
                    xml_start = i
                    break
            
            clean_content = '\n'.join(lines[xml_start:])
            root = ET.fromstring(clean_content)
            
            # Extract URLs matching the venue pattern
            venue_urls = []
            ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            
            for url_elem in root.findall('.//ns:url', ns):
                loc_elem = url_elem.find('ns:loc', ns)
                if loc_elem is not None and loc_elem.text:
                    url = loc_elem.text.strip()
                    if '/2025-all-events' in url and url != f"{BASE_URL}/2025-all-events":
                        venue_urls.append(url)
            
            # Don't include the main events page per user request
                
            logger.info(f"Discovered {len(venue_urls)} venue URLs from sitemap")
            return venue_urls
            
        except Exception as e:
            logger.error(f"Error parsing sitemap: {e}")
            return self._get_fallback_venue_urls()
    
    def _get_fallback_venue_urls(self) -> List[str]:
        """Fallback venue URLs if sitemap parsing fails - exclude main page per user request"""
        return [
            f"{BASE_URL}/2025-all-events-bearsville",
            f"{BASE_URL}/2025-all-events-woodstock-playhouse", 
            f"{BASE_URL}/2025-all-events-tinker-street-cinema",
            f"{BASE_URL}/2025-all-events-orpheum",
            f"{BASE_URL}/2025-all-events-upstate-midtown",
            f"{BASE_URL}/2025-all-events-rosendale",
            f"{BASE_URL}/2025-all-events-assembly",
            f"{BASE_URL}/2025-all-events-wcc",
        ]
    
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
                page.goto(url, wait_until="networkidle", timeout=60000)
                # Wait for events to load
                page.wait_for_selector(".event-banner, .event-card, [class*='event']", timeout=10000)
                content = page.content()
                return content
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
                break
        
        logger.info(f"Found {len(cards)} event cards on {source_url}")
        
        for card in cards:
            event_data = self._extract_event_from_card(card, source_url)
            if event_data and event_data.get('title') and event_data.get('start'):
                # Create unique ID for deduplication
                event_id = self._create_event_id(event_data)
                if event_id not in self.processed_event_ids:
                    events.append(event_data)
                    self.processed_event_ids.add(event_id)
        
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
        
        # Description
        desc_selectors = [
            ".event-description", 
            ".event-synopsis", 
            ".event-copy",
            ".summary-content",
            ".event-subtitle"
        ]
        description = self._extract_text_by_selectors(card, desc_selectors)
        
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
            desc_selectors = [
                ".event-description p",
                ".event-details p",  
                "[class*='description'] p"
            ]
            
            enhanced_desc = ""
            for selector in desc_selectors:
                elements = soup.select(selector)
                if elements:
                    paragraphs = [elem.get_text(strip=True) for elem in elements]
                    enhanced_desc = '\n\n'.join(p for p in paragraphs if p)
                    break
            
            if enhanced_desc:
                event['description'] = enhanced_desc
                
            # Try to get more precise venue info
            venue_elem = soup.select_one('.event-details strong:contains("Venue:") + br')
            if venue_elem and venue_elem.next_sibling:
                venue_text = str(venue_elem.next_sibling).strip()
                if venue_text and len(venue_text) < 200:  # Reasonable venue name length
                    event['venue'] = venue_text
            
            time.sleep(0.5)  # Rate limiting
            
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
            
            # Try static first, then Playwright if needed
            html = self.fetch_page_content(venue_url, use_playwright=False)
            events = self.extract_events_from_page(html or "", venue_url)
            
            # If too few events found, try with JavaScript rendering
            if len(events) < 3:  # Arbitrary threshold
                logger.info(f"Found only {len(events)} events, trying with Playwright...")
                html = self.fetch_page_content(venue_url, use_playwright=True)
                playwright_events = self.extract_events_from_page(html or "", venue_url)
                if len(playwright_events) > len(events):
                    events = playwright_events
            
            logger.info(f"Extracted {len(events)} events from {venue_url}")
            all_events.extend(events)
            
            # Rate limiting
            time.sleep(1)
        
        logger.info(f"Found {len(all_events)} total events before enhancement")
        
        # Enhance selected events with detail pages (limit to avoid overload)
        events_with_detail_urls = [e for e in all_events if e.get('url') and 'eventId=' in e.get('url', '')]
        logger.info(f"Enhancing {len(events_with_detail_urls)} events with detail pages...")
        
        enhanced_events = []
        for i, event in enumerate(all_events):
            if event.get('url') and 'eventId=' in event.get('url', '') and i < 50:  # Limit enhancement
                enhanced_event = self.enhance_event_with_detail_page(event)
                enhanced_events.append(enhanced_event)
            else:
                enhanced_events.append(event)
        
        # Sort by date
        enhanced_events.sort(key=lambda x: x['start'])
        
        logger.info(f"Completed scraping with {len(enhanced_events)} final events")
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