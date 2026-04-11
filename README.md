# ApplyBoard Program Scraper

A powerful, anti-block browser automation scraper for **applyboard.com/search**.
Supports interactive filter selection, full pagination, detail-page scraping, and export to XLSX or CSV.

---

## Features

| Feature | Details |
|---|---|
| 🔍 Filters | Country, Subject, University, Degree Level |
| 📄 Pagination | Auto-detects and follows "Next" across all pages |
| 🔎 Detail Pages | Visits each program page for deeper data |
| 🛡️ Anti-block | Random user-agents, human-like delays, stealth JS |
| 📊 XLSX Export | Styled spreadsheet with hyperlinks, freeze panes, auto-filter, summary sheet |
| 📄 CSV Export | UTF-8 CSV for any tool |
| 🔁 De-duplication | Removes duplicate programs automatically |

---

## Setup

### 1. Install Python 3.10+

### 2. Install dependencies

```bash
pip install playwright beautifulsoup4 openpyxl pandas fake-useragent
python -m playwright install chromium
```

### 3. Run the scraper

```bash
python applyboard_scraper.py
```

You'll be prompted interactively:

```
═══════════════════════════════════════════════════════════
  ApplyBoard Program Scraper
═══════════════════════════════════════════════════════════

🌍  Country (e.g. Canada, UK, Australia): Canada
📚  Study Subject (e.g. Computer Science, Business): Computer Science
🏫  University name (e.g. University of Toronto): [blank]
🎓  Degree Level (e.g. Bachelor, Master, Diploma): Master
🔍  Scrape detail pages for each program? (y/n) [y]: y
📄  Max pages to scrape (0 = all pages) [0]: 5
💾  Output format: xlsx / csv / both [xlsx]: both
📁  Output filename (without extension) [applyboard_programs]: cs_masters_canada
```

---

## Output Files

### XLSX Spreadsheet (`your_name.xlsx`)

Two sheets:

**Programs** — one row per program with these columns:

| Column | Description |
|---|---|
| Program Name | Name of the program |
| University | Institution name |
| Country | Country of study |
| City / Campus | Campus location |
| Degree Level | Bachelor / Master / Diploma etc. |
| Subject / Field | Study area |
| Duration | Program length |
| Tuition / Fees | Fee information |
| Language | Language of instruction |
| Intake / Semester | Available intakes |
| Program URL | 🔗 Hyperlink to program page |
| Description | Scraped from detail page |
| Admission Requirements | Entry requirements |
| English Requirements | IELTS / TOEFL scores etc. |
| Application Fee | Fee to apply |
| Min GPA | GPA requirement |
| Work Permit / Co-op | Work/co-op opportunities |
| Scholarships | Scholarship info |
| Accreditation | Accreditation status |
| Campus Location | Detailed campus info |
| Start Dates | Available intake dates |
| Application Deadline | Deadline from detail page |
| Raw Card Text | Full text from search card |

**Summary** — total count, scrape date, breakdown by country.

---

## Configuration

Edit constants at the top of `applyboard_scraper.py`:

```python
STEALTH_DELAYS = (1.5, 3.5)   # seconds between requests
PAGE_LOAD_WAIT = 8_000         # ms to wait after page load
SCROLL_PAUSE   = 1_200         # ms between scroll steps
```

---

## Anti-Block Techniques Used

- Randomized User-Agent rotation (Chrome/Firefox on Windows/Mac/Linux)
- `navigator.webdriver` hidden via JS injection
- Random human-like delays between each action
- Scroll simulation to trigger lazy-loaded content
- Realistic browser headers (Accept-Language, Accept)
- Timezone and locale set to match target region

---

## Notes

- ApplyBoard may require login for full data access; the scraper works with publicly visible cards
- Scraping speeds are intentionally slow to avoid rate limiting
- If the site updates its HTML structure, update the CSS selectors in `parse_cards()` and `scrape_detail()`
- Use responsibly and in accordance with ApplyBoard's Terms of Service
