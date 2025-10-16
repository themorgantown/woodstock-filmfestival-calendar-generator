# Woodstock Film Festival 2025 Calendar (Unofficial)

Automatically generated (and not guaranteed to be correct) calendar feed for the Woodstock Film Festival 2025, updated every 6 hours. Shows 🎟️ when event has tickets available. 


## How to use

You can subscribe to this url using the [following ics file](https://github.com/themorgantown/woodstock-filmfestival-calendar-generator/raw/main/wff_2025_complete.ics):

`https://github.com/themorgantown/woodstock-filmfestival-calendar-generator/raw/main/wff_2025_complete.ics`

![Add to Calendar](calendar_add.png)

Or:

1. Download the `.ics` file using the link above
2. Import it into your preferred calendar application:
   - **Apple Calendar**: File → Import
   - **Google Calendar**: Settings → Import & Export → Import
   - **Outlook**: File → Open & Export → Import/Export

**[Download Latest Calendar (ICS file)](https://github.com/themorgantown/woodstock-filmfestival-calendar-generator/raw/main/wff_2025_complete.ics)**

Right-click and "Save Link As..." or click to open directly in your calendar application.

## What is this?

This repository automatically reads the [Woodstock Film Festival website](https://woodstockfilmfestival.org) to generate a comprehensive calendar file (.ics) containing all festival events. The calendar includes:

- Event titles and descriptions
- Screening times and dates
- Venue locations
- Direct links to event details
 

## Respectful Scraping

This scraper is designed to be respectful of the festival's website:
- **Rate Limited**: 2-second delays between venue pages, 0.5-second delays between detail pages
- **Limited Scope**: Only enhances first 50 events with detail pages (out of ~119 total)
- **Transparent**: Identifies itself with a clear User-Agent
- **Total bandwidth**: ~1 minute of requests every 2 hours

This is equivalent to a single person browsing the website once per day.

## Updates

The calendar is automatically updated every 2 hours via GitHub Actions (00:00, 02:00, 04:00, 06:00, 08:00, 10:00, 12:00, 14:00, 16:00, 18:00, 20:00, 22:00 UTC).


## Known Limitations

- Detail page descriptions are only fetched for the first 50 events (to limit server load)
- ICS URLs may appear line-wrapped in some calendar apps (this is correct per ICS standard)
- Event descriptions are extracted from venue pages and may occasionally include extra formatting

 
*This is an unofficial calendar. Please verify event details on the [official Woodstock Film Festival website](https://woodstockfilmfestival.org).*
