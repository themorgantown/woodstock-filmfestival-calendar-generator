import re
from icalendar import Calendar
import os

def normalize_text(text):
  """Remove ICS-specific formatting and normalize text."""
  if not text:
    return ""
  
  # Convert to string if bytes
  if isinstance(text, bytes):
    text = text.decode('utf-8')
  
  # Remove excessive whitespace and newlines
  text = re.sub(r'\s+', ' ', text)
  
  # Remove common ICS escaping
  text = text.replace('\\n', ' ')
  text = text.replace('\\,', ',')
  text = text.replace('\\;', ';')
  
  return text.strip()

def extract_titles_and_descriptions(ics_file, output_file):
  """Extract event titles and descriptions from ICS file."""
  with open(ics_file, 'rb') as f:
    cal = Calendar.from_ical(f.read())
  
  with open(output_file, 'w', encoding='utf-8') as out:
    for component in cal.walk('VEVENT'):
      # Get title (summary)
      title = component.get('SUMMARY', '')
      title = normalize_text(title)
      
      # Get description
      description = component.get('DESCRIPTION', '')
      description = normalize_text(description)
      
      # Write to file
      if title:
        out.write(f"# {title}\n\n")
        if description:
          out.write(f"{description}\n\n")

if __name__ == "__main__":
  
  script_dir = os.path.dirname(os.path.abspath(__file__))
  ics_file = os.path.join(script_dir, 'wff_2025_complete.ics')
  output_file = os.path.join(script_dir, 'title_descr.md')
  
  extract_titles_and_descriptions(ics_file, output_file)
  print(f"Extracted titles and descriptions to {output_file}")