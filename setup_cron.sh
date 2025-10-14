#!/usr/bin/env bash
# Setup script for scheduling the Woodstock Film Festival calendar scraper

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CRON_ENTRY="0 * * * * cd $SCRIPT_DIR && ./run_calendar_scraper.sh"

echo "Setting up hourly cron job for WFF calendar scraper..."
echo "Script directory: $SCRIPT_DIR"

# Check if cron entry already exists
if crontab -l 2>/dev/null | grep -q "run_calendar_scraper.sh"; then
    echo "Cron job already exists. Current crontab:"
    crontab -l | grep "run_calendar_scraper.sh"
else
    # Add cron entry
    (crontab -l 2>/dev/null; echo "$CRON_ENTRY") | crontab -
    echo "Added hourly cron job: $CRON_ENTRY"
fi

echo ""
echo "Current crontab entries for this script:"
crontab -l 2>/dev/null | grep "run_calendar_scraper.sh" || echo "No entries found"

echo ""
echo "To verify the setup:"
echo "1. Check logs: tail -f $SCRIPT_DIR/wff_calendar.log"
echo "2. Test run: $SCRIPT_DIR/run_calendar_scraper.sh"
echo "3. View crontab: crontab -l"
echo ""
echo "To remove the cron job:"
echo "crontab -e  # then delete the line containing 'run_calendar_scraper.sh'"