# Woodstock Film Festival 2025 Calendar (Unofficial)

Automatically generated (and not guaranteed to be correct) calendar feed for the Woodstock Film Festival 2025, updated every 6 hours.


Results:
📊 Final Event Count: 120+ events (from venue-specific pages + custom events)

Venue Breakdown:

✅ Bearsville Theater: 19 events
✅ Woodstock Playhouse: 18 events
✅ Tinker Street Cinema: 19 events
✅ Orpheum Theatre: 28 events
✅ Upstate Midtown: 9 events
✅ Rosendale Theatre: 11 events
✅ Assembly: 2 events
✅ WCC (Shorts): 13 events
✅ Colony: 1 custom event
✅ 2025 Panels: Events extracted
✅ 2025 Shorts: Events extracted
✅ 2025 Special Events: Events extracted
⚪ Kleinert/James: 0 events
⚪ HVLGBTQ: 0 events
⚪ Broken Wing Barn: 0 events


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
- **Efficient**: Uses cached results, runs only 4 times per day
- **Transparent**: Identifies itself with a clear User-Agent
- **Total bandwidth**: ~1 minute of requests every 6 hours

This is equivalent to a single person browsing the website once per day.

## Updates

The calendar is automatically updated every 6 hours via GitHub Actions (00:00, 06:00, 12:00, 18:00 UTC). 

**Last successful scrape**: Check the [latest commit](https://github.com/themorgantown/woodstock-filmfestival-calendar-generator/commits/main) or [workflow runs](https://github.com/themorgantown/woodstock-filmfestival-calendar-generator/actions)

## Known Limitations

- Detail page descriptions are only fetched for the first 50 events (to limit server load)
- ICS URLs may appear line-wrapped in some calendar apps (this is correct per ICS standard)
- Event descriptions are extracted from venue pages and may occasionally include extra formatting


*This is an unofficial calendar. Please verify event details on the [official Woodstock Film Festival website](https://woodstockfilmfestival.org).*
