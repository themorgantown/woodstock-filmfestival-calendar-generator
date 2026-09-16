# Woodstock Film Festival Calendar (Unofficial)

Automatically generated (and not guaranteed to be correct) calendar feed for the current Woodstock Film Festival, updated every hour. Shows 🎟️ when an event has tickets available and 🔴 once it has sold out. 

[![](https://img.shields.io/badge/iCal-Download-blue?style=for-the-badge&logo=apple&logoColor=white)](https://woodstock-filmfestival-calendar-generator.themorgantown.workers.dev/woodstockfilmfestival.ics)

## How to use

You can subscribe to this url using the [following ics file](https://woodstock-filmfestival-calendar-generator.themorgantown.workers.dev/woodstockfilmfestival.ics):

`https://woodstock-filmfestival-calendar-generator.themorgantown.workers.dev/woodstockfilmfestival.ics`

This URL always points at the current festival year, so you only have to subscribe once.
The shorter `…workers.dev/calendar.ics` redirects to the same feed.

**To subscribe rather than download**, use the `webcal://` form of the same URL:

`webcal://woodstock-filmfestival-calendar-generator.themorgantown.workers.dev/woodstockfilmfestival.ics`

Clicking an `https://` .ics link makes the browser download a one-time snapshot that never
updates. `webcal://` hands the URL to your calendar app, which subscribes and keeps it
current. The [website](https://woodstock-filmfestival-calendar-generator.themorgantown.workers.dev)
has one-click buttons for Apple, Google, and Outlook.

![Add to Calendar](calendar_add.png)

Or:

1. Download the `.ics` file using the link above
2. Import it into your preferred calendar application:
   - **Apple Calendar**: File → Import
   - **Google Calendar**: Settings → Import & Export → Import
   - **Outlook**: File → Open & Export → Import/Export

**[Download Latest Calendar (ICS file)](https://woodstock-filmfestival-calendar-generator.themorgantown.workers.dev/woodstockfilmfestival.ics)**

Right-click and "Save Link As..." or click to open directly in your calendar application.

## What is this?

This repository automatically reads the [Woodstock Film Festival website](https://woodstockfilmfestival.org) to generate a comprehensive calendar file (.ics) containing all festival events. The calendar includes:

- Event titles and descriptions
- Screening times and dates
- Venue locations
- Direct links to event details
- Indication of ticket availability (🎟️)
- Sellout marker (🔴) with the time tickets ran out, both in the event description
  and as an `X-WFF-SOLDOUT` property; sellouts are also appended to `sellouts.csv`
 
## Respectful Scraping

This scraper is designed to be respectful of the festival's website:

* It loads a single page (https://woodstockfilmfestival.org/2026-all-events) and clicks around a bit to get data. 

## Updates

The calendar is automatically updated every 2 hours via GitHub Actions (00:00, 02:00, 04:00, 06:00, 08:00, 10:00, 12:00, 14:00, 16:00, 18:00, 20:00, 22:00 UTC).

## Archive

Past festival years are kept as-is and are no longer updated: `wff_2025_complete.ics`, `wff_2026_complete.ics`.

## Hosting

The feeds are served from Cloudflare Workers (static assets) at
`https://woodstock-filmfestival-calendar-generator.themorgantown.workers.dev`, which gives
them a proper `text/calendar` content-type, request logs, and redirects.

`build_site.py` assembles `public/` (feeds + `_headers` + `_redirects` + landing page) and
refuses to build an empty or unparseable calendar. The GitHub Action runs it and deploys
with `wrangler` after each successful scrape.

Required repository secrets: `CLOUDFLARE_API_TOKEN` (with the *Edit Cloudflare Workers*
template) and `CLOUDFLARE_ACCOUNT_ID`.
 
*This is an unofficial calendar. Please verify event details on the [official Woodstock Film Festival website](https://woodstockfilmfestival.org).*
