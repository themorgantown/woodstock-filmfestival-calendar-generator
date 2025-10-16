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
import os
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

# Environment helpers
def _load_env_file(path: str = ".env") -> None:
    """Best-effort .env loader so OPENROUTER_API_KEY can be defined outside the shell."""
    env_path = Path(path)
    if not env_path.exists():
        return

    try:
        with env_path.open('r', encoding='utf-8') as env_file:
            for raw_line in env_file:
                line = raw_line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' not in line:
                    continue
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                # Do not override environment variables that are already set
                if key and key not in os.environ:
                    os.environ[key] = value
    except Exception as exc:
        logger.warning("Failed to load environment file %s: %s", env_path, exc)


_load_env_file()

# Configuration
BASE_URL = "https://woodstockfilmfestival.org"
SITEMAP_FILE = "sitemap.xml"
OUTPUT_PATH = "wff_2025_complete.ics"
YEAR = 2025
DEFAULT_DURATION_HOURS = 2
TZ_ID = "America/New_York"
MIN_REASONABLE_EVENTS = 10
MAX_DETAIL_PAGE_ENHANCEMENTS = 150
VENUE_PAGE_DELAY = 2  # Seconds between venue page requests
DETAIL_PAGE_DELAY = 0.5  # Seconds between detail page requests

# LLM Configuration
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
LLM_MODEL = os.getenv("OPENROUTER_MODEL", "meta-llama/llama-3.2-3b-instruct:free")

# List of venue URLs to scrape
VENUE_URLS = [
    "https://woodstockfilmfestival.org/2025-all-events-assembly",
    "https://woodstockfilmfestival.org/2025-all-events-bearsville",
    "https://woodstockfilmfestival.org/2025-all-events-orpheum",
    "https://woodstockfilmfestival.org/2025-all-events-rosendale",
    "https://woodstockfilmfestival.org/2025-all-events-tinker-street-cinema",
    "https://woodstockfilmfestival.org/2025-all-events-upstate-midtown",
    "https://woodstockfilmfestival.org/2025-all-events-wcc",
    "https://woodstockfilmfestival.org/2025-all-events-woodstock-playhouse",
    "https://woodstockfilmfestival.org/2025-all-white-feather-farm",
    "https://woodstockfilmfestival.org/2025-panels",
    "https://woodstockfilmfestival.org/2025-special-events",
]

# URLs that consistently return 0 events (monitored but not actively scraped)
ZERO_EVENT_URLS = [
    "https://woodstockfilmfestival.org/2025-all-events-hvlgbtq",
    "https://woodstockfilmfestival.org/2025-colony",
    "https://woodstockfilmfestival.org/2025-film-guide?filmId=689f72571f570dd52f0c566e",
]

# URLs that require LLM processing when DOM parsing fails
LLM_REQUIRED_URLS = [
    "https://woodstockfilmfestival.org/2025-colony",
    "https://woodstockfilmfestival.org/2025-shorts",
    "https://woodstockfilmfestival.org/2025-film-guide?filmId=689f72571f570dd52f0c566e",
]

class WoodstockEventScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; WFF-Calendar-Bot/2.0; +https://woodstockfilmfestival.org/)'
        })
        self.venue_urls: List[str] = []
        self.events: List[Dict] = []
        self.event_registry: Dict[str, Dict] = {}
        self.processed_event_ids: Set[str] = set()
        self.llm_activity: List[Dict[str, object]] = []
        
    def discover_venue_urls(self) -> List[str]:
        """Return the list of venue URLs to scrape"""
        logger.info(f"Using {len(VENUE_URLS)} predefined venue URLs")
        # Ensure LLM-required URLs are always scraped
        combined_urls = sorted(set(VENUE_URLS).union(LLM_REQUIRED_URLS))
        if len(combined_urls) != len(VENUE_URLS):
            logger.debug(
                "Ensuring coverage for LLM-required URLs: added %d supplemental entries",
                len(combined_urls) - len(VENUE_URLS)
            )
        return combined_urls
    
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
        duplicates_merged = 0
        for card in cards:
            event_data = self._extract_event_from_card(card, source_url)
            if event_data and event_data.get('title') and event_data.get('start'):
                is_new, stored_event, updated = self._register_event(event_data)
                if is_new:
                    events.append(stored_event)
                else:
                    duplicates_skipped += 1
                    if updated:
                        logger.debug(
                            "Merged duplicate event with new details: %s at %s",
                            stored_event.get('title'),
                            stored_event.get('start')
                        )
                        duplicates_merged += 1
                    else:
                        logger.debug(
                            "Skipping duplicate event: %s at %s",
                            stored_event.get('title'),
                            stored_event.get('start')
                        )
        
        if duplicates_skipped > 0:
            logger.info(
                "Deduplicated %d events from %s (%d merged with richer data)",
                duplicates_skipped,
                source_url,
                duplicates_merged
            )
        
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
        
        # Check if event has ticketing (Order tickets button)
        card_text = card.get_text(" ", strip=True)
        title = title.strip()
        if 'order tickets' in card_text.lower() and '🎟️' not in title:
            title = f"🎟️ {title}"
        
        return {
            'title': title,
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
        elif 'white-feather-farm' in url:
            return "Broken Wing Barn at White Feather Farm"
        elif 'colony' in url:
            return "Colony"
        elif 'assembly' in url:
            return "Assembly"
        elif 'bearsville' in url:
            return "Bearsville Theater"
        elif 'woodstock-playhouse' in url:
            return "Woodstock Playhouse"
        elif 'tinker-street-cinema' in url:
            return "Tinker Street Cinema"
        elif 'orpheum' in url:
            return "Orpheum Theatre"
        elif 'upstate-midtown' in url:
            return "Upstate Midtown"
        elif 'rosendale' in url:
            return "Rosendale Theatre"
        elif 'wcc' in url:
            return "Woodstock Community Center [SHORTS]"
        elif 'hvlgbtq' in url:
            return "Hudson Valley LGBTQ+ Community Center"
        
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

    def _prepare_html_for_llm(self, html: str) -> str:
        """Prepare HTML content for LLM processing by limiting size."""
        # Limit HTML content to prevent token overflow
        return html[:15000]  # Allow more content for better context

    def _parse_events_with_llm(self, html: str, source_url: str) -> List[Dict]:
        """Ask a lightweight LLM to extract events when DOM parsing fails."""
        if not OPENROUTER_API_KEY:
            logger.warning("OPENROUTER_API_KEY not set; skipping LLM parsing")
            return []
        
        payload = self._prepare_html_for_llm(html)
        if not payload.strip():
            return []

        logger.info(
            "LLM request prepared for %s (payload=%d chars)",
            source_url,
            len(payload)
        )

        prompt = (
            "You extract Woodstock Film Festival 2025 schedule data from HTML. "
            "Return one or more events using this template exactly:\n\n"
            "Event:\n"
            "Title: <title>\n"
            "Date: <weekday, month day>\n"
            "Time: <time with AM/PM>\n"
            "Venue: <location name>\n"
            "Description: <verbatim event description copied from the input without omitting sentences.>\n\n"
            "Do not use markdown formatting or bullets. Leave one blank line between events. "
            "If a field is unknown, leave it blank after the colon. An event has a date and start time and title and description. Do your best to find them."
        )

        try:
            start_time = time.time()
            response = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "https://github.com/themorgantown/woodstock-filmfestival-calendar-generator",
                },
                json={
                    "model": LLM_MODEL,
                    "messages": [
                        {"role": "system", "content": "You are a structured data extractor."},
                        {"role": "user", "content": f"Source URL: {source_url}\n\nContent:\n{payload}"},
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": 0.1,
                    "max_tokens": 10000,  # Increased tokens for better extraction
                },
                timeout=30
            )
            response.raise_for_status()
            content = response.json()["choices"][0]["message"]["content"]
            elapsed = time.time() - start_time
            logger.info(
                "LLM response received for %s (status=%d, duration=%.2fs, bytes=%d)",
                source_url,
                response.status_code,
                elapsed,
                len(content or "")
            )
        except Exception as exc:
            logger.error(f"LLM parsing failed for {source_url}: {exc}")
            return []

        events: List[Dict] = []
        chunks = [chunk.strip() for chunk in content.split("Event:") if chunk.strip()]
        duplicates_skipped = 0
        duplicates_updated = 0
        for chunk in chunks:
            fields: Dict[str, str] = {}
            for line in chunk.splitlines():
                if ':' not in line:
                    continue
                key, value = line.split(':', 1)
                fields[key.strip().lower()] = value.strip()
            title = fields.get('title', '')
            date_part = fields.get('date', '')
            time_part = fields.get('time', '')
            venue = fields.get('venue', '') or self._infer_venue_from_url(source_url)
            desc = fields.get('description', '')
            if not title:
                continue
            combined_datetime = ''
            if date_part and time_part:
                combined_datetime = f"{date_part} at {time_part}"
            elif date_part:
                combined_datetime = date_part
            start_dt = self._parse_datetime(combined_datetime)
            if not start_dt:
                logger.debug(f"LLM event discarded due to unparsed datetime: {title} ({combined_datetime})")
                continue
            event_obj = {
                'title': title,
                'start': start_dt,
                'venue': venue,
                'description': desc,
                'url': source_url,
                'source_url': source_url
            }
            is_new, stored_event, updated = self._register_event(event_obj)
            if is_new:
                events.append(stored_event)
            else:
                duplicates_skipped += 1
                if updated:
                    duplicates_updated += 1

        if duplicates_skipped:
            logger.info(
                "LLM deduplicated %d events for %s (%d merged with richer data)",
                duplicates_skipped,
                source_url,
                duplicates_updated
            )
        logger.info(f"LLM extracted {len(events)} unique events from {source_url}")
        self.llm_activity.append({
            'url': source_url,
            'payload_chars': len(payload),
            'events_returned': len(events),
        })
        return events
    
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
    
    def _split_title_prefixes(self, title: Optional[str]) -> Tuple[List[str], str]:
        """Return leading status emoji markers and the core title text."""
        if not title:
            return [], ''
        
        remaining = title.strip()
        statuses: List[str] = []
        known_prefixes = ('🫷', '🎟️')
        
        while remaining:
            matched_prefix = False
            for prefix in known_prefixes:
                if remaining.startswith(prefix):
                    statuses.append(prefix)
                    remaining = remaining[len(prefix):].lstrip()
                    matched_prefix = True
                    break
            if not matched_prefix:
                break
        
        return statuses, remaining

    def _normalize_title_for_id(self, title: Optional[str]) -> str:
        """Normalize title for deduplication ID comparison."""
        _, core = self._split_title_prefixes(title)
        normalized = re.sub(r'\s+', ' ', core).strip()
        return normalized.lower()

    def _merge_event_records(self, existing: Dict, new_data: Dict) -> bool:
        """
        Merge duplicate event records, preferring richer data and ensuring status
        markers like ticket or standby are preserved.
        Returns True if any field was updated.
        """
        changed = False

        existing_statuses, existing_base = self._split_title_prefixes(existing.get('title'))
        new_statuses, new_base = self._split_title_prefixes(new_data.get('title'))

        status_order = ('🫷', '🎟️')
        merged_statuses = [
            status for status in status_order
            if status in existing_statuses or status in new_statuses
        ]

        base_candidates = [text for text in (existing_base, new_base) if text]
        merged_base = ''
        if base_candidates:
            merged_base = max(base_candidates, key=len)

        if merged_base or merged_statuses:
            assembled_title = merged_base
            if merged_statuses:
                assembled_title = f"{' '.join(merged_statuses)} {merged_base}".strip()
            if assembled_title and assembled_title != existing.get('title'):
                existing['title'] = assembled_title
                changed = True

        # Description - keep the longer one
        existing_desc = existing.get('description') or ''
        new_desc = new_data.get('description') or ''
        if new_desc and len(new_desc) > len(existing_desc):
            existing['description'] = new_desc
            changed = True

        # Venue - prefer a specific venue over placeholders
        existing_venue = existing.get('venue') or ''
        new_venue = new_data.get('venue') or ''
        if new_venue and (not existing_venue or existing_venue in {'', 'TBD', 'Unknown'}):
            if new_venue != existing_venue:
                existing['venue'] = new_venue
                changed = True

        # URL - prefer one with an explicit eventId parameter
        existing_url = existing.get('url') or ''
        new_url = new_data.get('url') or ''
        if new_url:
            prefer_new_url = (
                not existing_url or
                ('eventId=' in new_url and 'eventId=' not in existing_url)
            )
            if prefer_new_url and new_url != existing_url:
                existing['url'] = new_url
                changed = True

        # Source URL - keep original if set, otherwise fill in
        if not existing.get('source_url') and new_data.get('source_url'):
            existing['source_url'] = new_data['source_url']
            changed = True

        return changed
    
    def _create_event_id(self, event_data: Dict) -> str:
        """Create unique ID for event deduplication"""
        key_parts = [
            self._normalize_title_for_id(event_data.get('title', '')),
            event_data.get('start', datetime.now()).isoformat(),
            (event_data.get('venue') or '').strip().lower(),
        ]
        return hashlib.md5('|'.join(str(p) for p in key_parts).encode()).hexdigest()

    def _register_event(self, event_data: Dict) -> Tuple[bool, Dict, bool]:
        """
        Register an event with deduplication.
        Returns (is_new, stored_event, changed) where changed indicates an update
        to an existing record.
        """
        event_id = self._create_event_id(event_data)
        existing = self.event_registry.get(event_id)
        if existing is None:
            self.event_registry[event_id] = event_data
            self.processed_event_ids.add(event_id)
            return True, event_data, False

        updated = self._merge_event_records(existing, event_data)
        return False, existing, updated
    
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
            
        # Hudson Valley LGBTQ+ Community Center event
        try:
            custom_events.append({
                'title': 'LGBTQ+ Community Center Event',
                'start': datetime(2025, 10, 18, 16, 30),  # Sat, Oct 18, 4:30 PM ET
                'venue': 'Hudson Valley LGBTQ+ Community Center',
                'description': "Event at Hudson Valley LGBTQ+ Community Center",
                'url': 'https://woodstockfilmfestival.org/2025-all-events?eventId=68ac9de3e4f9783fb47dc2f3',
                'source_url': 'https://woodstockfilmfestival.org/2025-all-events?eventId=68ac9de3e4f9783fb47dc2f3'
            })
            logger.info("Added custom event: LGBTQ+ Community Center Event")
        except Exception as e:
            logger.error(f"Error creating custom event: {e}")
            
        # Film School Shorts
        try:
            custom_events.append({
                'title': 'Film School Shorts',
                'start': datetime(2025, 10, 16, 19, 30),  # Thu, Oct 16, 7:30 PM ET
                'venue': 'TBD',  # Venue not specified in the description
                'description': "Finely-crafted and distinctive narrative and documentary shorts from up and coming visionary film school students.\n\nFilms Showing:\n\nDawn's World\nA lonely gallery docent consults the imaginary creatures in her head when she learns that a fellow docent is getting fired. Should she talk to him? Or remain solitary forever?\n\nHow I Learned to Die\n16-year-old Iris finds out she has a 60% chance of dying in four days from a high-risk surgery… so now she's gotta live it up. Chasing a wild bucket-list, she makes unexpected discoveries along the way.\n\nAre You Having Fun?\nUnemployed Ava has planned a fun bachelorette party weekend in Miami for her picture-perfect best friend, Kendall. But when Kendall's party and Ava's job prospects come in conflict with each other, Ava tests just how far she is willing to go for her friend to have a good time.\n\nHotspot\nA spoiled city teen is dragged upstate for spring break with his dad and his dad's new girlfriend as they renovate their Airbnb. But when he meets a troubled local with a history of arson, an unexpected common ground is formed.\n\nSt. Joe's Hoes\nFour young women, a dancer, a painter, a photographer, and a writer, come to New York City from different parts of the world with suitcases full of hope and a single address just blocks from Times Square: a former convent converted into affordable housing for nearly 80 women. In tight communal spaces, from a rat-prone kitchen to a stained-glass chapel and clogged bathrooms, the women forge unlikely bonds while navigating personal struggles, creative ambition, and the shared audacity of \"making it\" in New York. Set in a time just before current shifts in immigration and international student policies, the film captures their resilience and precarity.\n\nThe Wrath of Othell-Yo!\nOn the set of the erotic blaxploitation film The Wrath of Othell-Yo, Tommy, the black production assistant, replaces the lead actor of the film who fails to get erect and finds himself thrust into a racialized Othello pastiche that becomes more than he bargained for.",
                'url': 'https://woodstockfilmfestival.org/2025-shorts',
                'source_url': 'https://woodstockfilmfestival.org/2025-shorts'
            })
            logger.info("Added custom event: Film School Shorts")
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
                
            # Check if event is sold out (STANDBY ONLY)
            page_text = soup.get_text()
            if 'STANDBY ONLY' in page_text:
                current_title = event.get('title', '')
                if not current_title.startswith('🫷'):
                    event['title'] = f"🫷 {current_title}"
                
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
        existing_metadata = self._load_existing_event_metadata()
        cal = Calendar()
        cal.add('prodid', '-//Woodstock Film Festival 2025 Complete Calendar//EN')
        cal.add('version', '2.0')
        cal.add('calscale', 'GREGORIAN')
        
        for event_data in events:
            event = Event()
            
            # Required fields
            uid = self._generate_uid(event_data)
            event.add('uid', uid)
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

            signature = self._event_signature_from_data(event_data)
            previous = existing_metadata.get(uid)
            if previous and previous.get('signature') == signature and previous.get('dtstamp'):
                event.add('dtstamp', previous['dtstamp'])
            else:
                event.add('dtstamp', datetime.now())
            cal.add_component(event)
        
        return cal.to_ical().decode('utf-8')
    
    def _generate_uid(self, event_data: Dict) -> str:
        """Generate unique UID for ICS event"""
        base = f"{event_data['title']}|{event_data['start'].isoformat()}|{event_data.get('venue', '')}"
        hash_part = hashlib.sha256(base.encode()).hexdigest()[:16]
        slug = re.sub(r'[^a-z0-9]+', '-', event_data['title'].lower()).strip('-')[:30]
        return f"{slug}-{event_data['start'].strftime('%Y%m%dT%H%M%S')}-{hash_part}@wff2025"

    def _load_existing_event_metadata(self) -> Dict[str, Dict[str, object]]:
        """Load existing event dtstamp and content signatures from current ICS output."""
        metadata: Dict[str, Dict[str, object]] = {}
        if not os.path.exists(OUTPUT_PATH):
            return metadata

        try:
            with open(OUTPUT_PATH, 'rb') as ics_file:
                existing_cal = Calendar.from_ical(ics_file.read())

            for component in existing_cal.walk('VEVENT'):
                uid = str(component.get('uid')) if component.get('uid') else None
                if not uid:
                    continue

                signature = self._event_signature_from_component(component)
                dtstamp_field = component.get('dtstamp')
                dtstamp_value = dtstamp_field.dt if dtstamp_field else None
                metadata[uid] = {
                    'dtstamp': dtstamp_value,
                    'signature': signature,
                }
        except Exception as exc:
            logger.warning(f"Failed to parse existing ICS for dtstamp reuse: {exc}")

        return metadata

    def _event_signature_from_data(self, event_data: Dict) -> str:
        """Create a deterministic signature representing current event fields."""
        dtstart = event_data.get('start')
        dtend = dtstart + timedelta(hours=DEFAULT_DURATION_HOURS) if dtstart else None
        fields = [
            event_data.get('title', ''),
            dtstart.isoformat() if isinstance(dtstart, datetime) else '',
            dtend.isoformat() if isinstance(dtend, datetime) else '',
            event_data.get('venue', ''),
            event_data.get('description', ''),
            event_data.get('url', ''),
        ]
        return '|'.join(fields)

    def _event_signature_from_component(self, component: Event) -> str:
        """Create a deterministic signature from an existing ICS component."""
        def normalize(name: str) -> str:
            value = component.get(name)
            if value is None:
                return ''
            raw = value.dt if hasattr(value, 'dt') else value
            return raw.isoformat() if isinstance(raw, datetime) else str(raw)

        fields = [
            normalize('summary'),
            normalize('dtstart'),
            normalize('dtend'),
            normalize('location'),
            normalize('description'),
            normalize('url'),
        ]
        return '|'.join(fields)
    
    def scrape_all_events(self) -> List[Dict]:
        """Main scraping workflow"""
        logger.info("Starting comprehensive event scraping...")
        self.llm_activity.clear()
        self.event_registry.clear()
        self.processed_event_ids.clear()
        
        # Discover venue URLs
        self.venue_urls = self.discover_venue_urls()
        logger.info(f"Will scrape {len(self.venue_urls)} venue pages")

        all_events = []
        visited_urls: Set[str] = set()
        
        # Scrape each venue page
        for i, venue_url in enumerate(self.venue_urls, 1):
            visited_urls.add(venue_url)
            is_llm_required = venue_url in LLM_REQUIRED_URLS
            context_label = "LLM-required" if is_llm_required else "standard"
            logger.info(
                "Scraping %s page %d/%d: %s",
                context_label,
                i,
                len(self.venue_urls),
                venue_url
            )
            
            # Use Playwright directly since these pages require JavaScript
            try:
                html = self.fetch_page_content(venue_url, use_playwright=True)
                events = self.extract_events_from_page(html or "", venue_url)
                logger.info(
                    "Extracted %d DOM events from %s",
                    len(events),
                    venue_url
                )
                if not events and venue_url in LLM_REQUIRED_URLS and html:
                    logger.info(
                        "LLM-required URL %s yielded 0 DOM events; invoking LLM fallback",
                        venue_url
                    )
                    events = self._parse_events_with_llm(html, venue_url)
                elif not events and venue_url in LLM_REQUIRED_URLS:
                    logger.warning(
                        "LLM-required URL %s returned empty HTML response; skipping LLM fallback",
                        venue_url
                    )
                elif not events:
                    logger.debug(
                        "No DOM events for %s; skipping LLM fallback (not in required list)",
                        venue_url
                    )
                logger.info(f"Extracted {len(events)} events from {venue_url}")
                all_events.extend(events)
            except Exception as e:
                logger.error(f"Failed to scrape {venue_url}: {e}")
                # Continue with next venue instead of stopping
                continue
            
            # Rate limiting between requests
            time.sleep(VENUE_PAGE_DELAY)
        
        logger.info(f"Found {len(all_events)} unique events after deduplication")

        missing_llm_urls = set(LLM_REQUIRED_URLS) - visited_urls
        if missing_llm_urls:
            logger.error("Missing LLM-required URLs from scrape: %s", ", ".join(sorted(missing_llm_urls)))
        else:
            logger.info("All LLM-required URLs were scraped this run (%d total)", len(LLM_REQUIRED_URLS))

        if self.llm_activity:
            activity_summary = ", ".join(
                f"{entry['url']} ({entry['events_returned']} events)" for entry in self.llm_activity
            )
            logger.info("LLM processed %d URLs: %s", len(self.llm_activity), activity_summary)
        
        # Add custom hardcoded events
        custom_events = self._get_custom_events()
        if custom_events:
            logger.info(f"Adding {len(custom_events)} custom hardcoded events")
            custom_duplicates = 0
            custom_updates = 0
            for custom_event in custom_events:
                is_new, stored_event, updated = self._register_event(custom_event)
                if is_new:
                    all_events.append(stored_event)
                else:
                    custom_duplicates += 1
                    if updated:
                        custom_updates += 1
            if custom_duplicates:
                logger.info(
                    "Custom events deduplicated %d entries (%d merged with additional data)",
                    custom_duplicates,
                    custom_updates
                )
        
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
        total_cards_found = len(self.event_registry)
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
