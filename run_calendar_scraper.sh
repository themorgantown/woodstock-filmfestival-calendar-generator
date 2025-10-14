#!/usr/bin/env bash
# Woodstock Film Festival Calendar Runner Script
# Runs the enhanced calendar scraper with proper error handling and logging

set -e

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Logging
LOG_FILE="wff_calendar.log"
DATE=$(date '+%Y-%m-%d %H:%M:%S')

echo "[$DATE] Starting WFF calendar scraping..." | tee -a "$LOG_FILE"

# Activate virtual environment and run scraper
if [ -d ".venv" ]; then
    source .venv/bin/activate
    echo "[$DATE] Virtual environment activated" | tee -a "$LOG_FILE"
else
    echo "[$DATE] ERROR: Virtual environment not found at .venv/" | tee -a "$LOG_FILE"
    exit 1
fi

# Run the scraper
if .venv/bin/python enhanced_make_calendar.py >> "$LOG_FILE" 2>&1; then
    echo "[$DATE] Calendar scraping completed successfully" | tee -a "$LOG_FILE"
    
    # Check if ICS file was created and has content
    if [ -f "wff_2025_complete.ics" ] && [ -s "wff_2025_complete.ics" ]; then
        EVENTS_COUNT=$(grep -c "BEGIN:VEVENT" wff_2025_complete.ics || echo "0")
        echo "[$DATE] Generated calendar with $EVENTS_COUNT events" | tee -a "$LOG_FILE"
    else
        echo "[$DATE] WARNING: No calendar file generated or file is empty" | tee -a "$LOG_FILE"
        exit 1
    fi
else
    echo "[$DATE] ERROR: Calendar scraping failed" | tee -a "$LOG_FILE"
    exit 1
fi

echo "[$DATE] Script completed" | tee -a "$LOG_FILE"