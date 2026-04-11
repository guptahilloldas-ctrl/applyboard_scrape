"""
ApplyBoard Program Scraper
Run on your LOCAL machine.

Usage:
    python applyboard_scraper.py              # interactive (prompts for login + filters)
    python applyboard_scraper.py --debug      # saves raw HTML files for inspection
    python applyboard_scraper.py --no-details # skip detail pages (faster)
    python applyboard_scraper.py --headless   # headless browser
"""

import argparse
import asyncio
import getpass
import random
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode

import pandas as pd
from bs4 import BeautifulSoup, Tag
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter
from playwright.async_api import async_playwright, Page

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────

LOGIN_URL   = "https://accounts.applyboard.com/oauth2/default/v1/authorize?client_id=0oasbh5xhhoozpCwp5d6&redirect_uri=https%3A%2F%2Fwww.applyboard.com%2Fusers%2Fauth%2Foktaoauth%2Fcallback&response_type=code&scope=openid+profile+email+offline_access&state=c3ebbcc4369a060f97cc275d5a2146cb17ea35c4a620e310"
BASE_URL    = "https://www.applyboard.com/search"
DETAIL_BASE = "https://www.applyboard.com"

# These match the exact flag params in ApplyBoard's search URL
DEFAULT_FLAGS = {
    "filter[conditional_offer]":        "f",
    "filter[free_application_only]":    "f",
    "filter[exclude_visa_cap_programs]":"f",
    "filter[pgwp_available]":           "f",
    "filter[ignore_availability]":      "f",
    "filter[unpublished]":              "f",
    "filter[include_pathways]":         "t",
    "sort":                             "-success_score",
}

PAGE_SIZE      = 48
PAGE_LOAD_WAIT = 10_000   # ms
STEALTH_MIN    = 1.5
STEALTH_MAX    = 3.2
SCROLL_STEPS   = 8
SCROLL_PAUSE   = 800

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.122 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

# ──────────────────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def rand_sleep(lo=STEALTH_MIN, hi=STEALTH_MAX):
    time.sleep(random.uniform(lo, hi))

def clean(text) -> str:
    return re.sub(r'\s+', ' ', (text or '').strip())

def save_debug_html(html: str, label: str = "page"):
    path = Path(f"debug_{label}_{int(time.time())}.html")
    path.write_text(html, encoding="utf-8")
    print(f"   💾  Debug HTML → {path}")

def build_search_url(page_number: int = 1, school_id: str = None,
                     country: str = None, subject: str = None,
                     degree_level: str = None) -> str:
    """
    Build the exact URL format ApplyBoard uses, e.g.:
    /search?filter[school_ids]=1715&page[number]=1&page[size]=48&sort=-success_score...
    """
    params = dict(DEFAULT_FLAGS)
    params["page[number]"] = str(page_number)
    params["page[size]"]   = str(PAGE_SIZE)

    if school_id:
        params["filter[school_ids]"] = school_id
    if country:
        params["filter[country]"] = country
    if subject:
        params["filter[subject]"] = subject
    if degree_level:
        params["filter[degree_type]"] = degree_level

    return f"{BASE_URL}?{urlencode(params)}"

# ──────────────────────────────────────────────────────────────────────────────
# BROWSER
# ──────────────────────────────────────────────────────────────────────────────

async def create_browser(playwright, headless: bool = False):
    browser = await playwright.chromium.launch(
        headless=headless,
        slow_mo=60,
        args=[
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--disable-infobars",
            "--disable-dev-shm-usage",
        ],
    )
    ctx = await browser.new_context(
        user_agent=random.choice(USER_AGENTS),
        viewport={"width": 1440, "height": 900},
        locale="en-US",
        timezone_id="America/Toronto",
        extra_http_headers={
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        },
    )
    await ctx.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        Object.defineProperty(navigator, 'plugins',   { get: () => [1, 2, 3] });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
        window.chrome = { runtime: {}, loadTimes: () => {}, csi: () => {} };
    """)
    return browser, ctx

# ──────────────────────────────────────────────────────────────────────────────
# LOGIN
# ──────────────────────────────────────────────────────────────────────────────

async def login(page: Page, email: str, password: str, debug: bool = False) -> bool:
    """
    Log in to ApplyBoard. Returns True on success.
    Credentials are passed in at runtime — never stored in code.
    """
    print(f"\n🔐  Logging in as {email} ...")
    await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=30_000)
    await page.wait_for_timeout(3000)

    if debug:
        save_debug_html(await page.content(), "login_page")

    # Fill email
    email_selectors = [
        'input[type="email"]',
        'input[name="email"]',
        'input[placeholder*="email" i]',
        'input[autocomplete="email"]',
        'input[id*="email" i]',
    ]
    filled_email = False
    for sel in email_selectors:
        try:
            el = page.locator(sel).first
            if await el.count() > 0 and await el.is_visible():
                await el.fill(email)
                filled_email = True
                break
        except Exception:
            continue

    if not filled_email:
        print("   ⚠  Could not find email field.")
        return False

    await page.wait_for_timeout(400)

    # Fill password
    password_selectors = [
        'input[type="password"]',
        'input[name="password"]',
        'input[placeholder*="password" i]',
        'input[autocomplete="current-password"]',
    ]
    filled_password = False
    for sel in password_selectors:
        try:
            el = page.locator(sel).first
            if await el.count() > 0 and await el.is_visible():
                await el.fill(password)
                filled_password = True
                break
        except Exception:
            continue

    if not filled_password:
        print("   ⚠  Could not find password field.")
        return False

    await page.wait_for_timeout(400)

    # Submit
    submit_selectors = [
        'button[type="submit"]',
        'input[type="submit"]',
        'button:has-text("Sign in")',
        'button:has-text("Log in")',
        'button:has-text("Login")',
        'button:has-text("Sign In")',
    ]
    submitted = False
    for sel in submit_selectors:
        try:
            btn = page.locator(sel).first
            if await btn.count() > 0 and await btn.is_visible():
                await btn.click()
                submitted = True
                break
        except Exception:
            continue

    if not submitted:
        # Fallback: press Enter
        await page.keyboard.press("Enter")

    await page.wait_for_timeout(PAGE_LOAD_WAIT)

    if debug:
        save_debug_html(await page.content(), "after_login")

    # Verify login succeeded — URL should no longer be the sign-in page
    current_url = page.url
    if "sign-in" in current_url or "login" in current_url:
        print("   ✗  Login may have failed — still on login page. Check credentials.")
        return False

    print("   ✓  Login successful")
    return True

# ──────────────────────────────────────────────────────────────────────────────
# SCHOOL ID LOOKUP
# ──────────────────────────────────────────────────────────────────────────────

async def resolve_school_id(page: Page, university_name: str) -> str | None:
    """
    ApplyBoard URLs use numeric school IDs (e.g. filter[school_ids]=1715).
    We find the ID by using the search page's institution filter autocomplete
    and extracting the ID from the resulting URL after selection.
    """
    if not university_name:
        return None

    print(f"\n🔍  Looking up school ID for: {university_name}")

    # Go to base search to use the filter UI
    await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=30_000)
    await page.wait_for_timeout(5000)

    # Find the institution search input
    input_selectors = [
        'input[placeholder*="institution" i]',
        'input[placeholder*="university" i]',
        'input[placeholder*="school" i]',
        'input[aria-label*="institution" i]',
        'input[aria-label*="school" i]',
        '[role="combobox"][aria-label*="institution" i]',
        '[role="combobox"][aria-label*="school" i]',
    ]

    found_input = None
    for sel in input_selectors:
        try:
            el = page.locator(sel).first
            if await el.count() > 0 and await el.is_visible():
                found_input = el
                break
        except Exception:
            continue

    if not found_input:
        print("   ⚠  Could not find institution search input.")
        return None

    # Type the university name to trigger autocomplete
    await found_input.click()
    await page.wait_for_timeout(500)
    await found_input.fill("")
    await found_input.type(university_name, delay=80)
    await page.wait_for_timeout(1500)  # wait for suggestions

    # Look for the suggestion and click it
    option_selectors = [
        '[role="option"]',
        '[role="listbox"] li',
        '[class*="option" i]',
        '[class*="suggestion" i]',
        '[class*="autocomplete" i] li',
    ]

    for sel in option_selectors:
        try:
            opts = page.locator(sel).filter(has_text=university_name)
            if await opts.count() == 0:
                # Try partial match
                opts = page.locator(sel)
                if await opts.count() == 0:
                    continue
            await opts.first.click(timeout=3000)
            await page.wait_for_timeout(2500)  # let URL update
            break
        except Exception:
            continue

    # Extract school_id from the updated URL
    current_url = page.url
    match = re.search(r'filter\[school_ids\]=(\d+)', current_url)
    if match:
        sid = match.group(1)
        print(f"   ✓  School ID: {sid}")
        return sid

    print(f"   ⚠  Could not extract school ID from URL: {current_url}")
    return None

# ──────────────────────────────────────────────────────────────────────────────
# SCROLL
# ──────────────────────────────────────────────────────────────────────────────

async def human_scroll(page: Page):
    height = await page.evaluate("document.body.scrollHeight")
    step = max(height // SCROLL_STEPS, 300)
    for i in range(1, SCROLL_STEPS + 1):
        await page.evaluate(f"window.scrollTo(0, {step * i})")
        await page.wait_for_timeout(SCROLL_PAUSE + random.randint(-150, 200))
    await page.evaluate("window.scrollTo(0, 300)")
    await page.wait_for_timeout(400)

# ──────────────────────────────────────────────────────────────────────────────
# CARD DETECTION + PARSING
# ──────────────────────────────────────────────────────────────────────────────

def detect_cards(soup: BeautifulSoup) -> list[Tag]:
    specific = [
        '[data-testid*="program"]', '[data-testid*="card"]',
        '[class*="ProgramCard"]',   '[class*="program-card"]',
        '[class*="programCard"]',   '[class*="SearchResult"]',
        '[class*="search-result"]', '[class*="ResultCard"]',
        '[class*="result-card"]',   'article',
        '[class*="Program"][class*="Item"]',
    ]
    for sel in specific:
        cards = soup.select(sel)
        if len(cards) >= 2:
            print(f"   🎯  Selector '{sel}' → {len(cards)} cards")
            return cards

    # Structural heuristic fallback
    candidates = []
    for tag in soup.find_all(['div','li','section','article'], recursive=True):
        if (tag.find(['h1','h2','h3','h4','h5']) and
                tag.find('a', href=True) and
                60 < len(tag.get_text(strip=True)) < 2500):
            candidates.append(tag)

    # De-nest: remove elements that are ancestors of other candidates
    cand_ids = {id(c) for c in candidates}
    unique = [c for c in candidates
              if not any(id(p) in cand_ids for p in c.parents)]

    if unique:
        print(f"   🔍  Heuristic → {len(unique)} cards")
        return unique[:200]

    print("   ⚠  No cards detected. Run with --debug to inspect HTML.")
    return []

def parse_card(card: Tag) -> dict:
    text = card.get_text(" ", strip=True)

    link_tag = (card.select_one('a[href*="/programs/"]') or
                card.select_one('a[href*="/apply"]') or
                card.select_one('a[href^="/"]') or
                card.select_one('a[href^="http"]'))
    href = link_tag.get("href","") if link_tag else ""
    if href.startswith("/"): href = DETAIL_BASE + href

    def find_val(patterns, max_len=180):
        for p in patterns:
            for el in card.select(f'[data-testid*="{p}"], [class*="{p}"]'):
                v = clean(el.get_text())
                if v and len(v) < max_len: return v
        for p in patterns:
            m = re.search(rf'{p}[:\s]+([^\n|•<>]+)', text, re.I)
            if m: return clean(m.group(1))[:max_len]
        return ""

    heading = card.find(['h1','h2','h3','h4','h5'])
    return {
        "program_name":  clean(heading.get_text()) if heading else find_val(["title","name","program"]),
        "university":    find_val(["school","institution","university","college"]),
        "country":       find_val(["country","location","nation"]),
        "city":          find_val(["city","campus"]),
        "degree_level":  find_val(["degree","level","credential","qualification"]),
        "subject":       find_val(["subject","discipline","field","area"]),
        "duration":      find_val(["duration","length","year","month"]),
        "tuition":       find_val(["tuition","fee","cost","price"]),
        "language":      find_val(["language","english","instruction"]),
        "intake":        find_val(["intake","start","semester","term"]),
        "program_url":   href,
        "raw_text":      clean(text)[:600],
    }

def parse_all_cards(html: str, debug: bool = False) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    if debug:
        classes = sorted({c[:40] for t in soup.find_all(True) for c in (t.get("class") or [])})
        testids = [t.get("data-testid") for t in soup.find_all(attrs={"data-testid": True})]
        print(f"\n   🔬  Classes (first 60): {classes[:60]}")
        print(f"   🔬  data-testids: {testids[:40]}\n")
    cards = detect_cards(soup)
    return [r for c in cards if (r := parse_card(c)) and (r.get("program_name") or r.get("university"))]

# ──────────────────────────────────────────────────────────────────────────────
# DETAIL PAGE
# ──────────────────────────────────────────────────────────────────────────────

async def scrape_detail(page: Page, url: str, debug: bool = False) -> dict:
    if not url: return {}
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        await page.wait_for_timeout(4000)
        html = await page.content()
        if debug: save_debug_html(html, "detail")
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)

        def g(*keys):
            for k in keys:
                for el in soup.select(f'[data-testid*="{k}"], [class*="{k}"]'):
                    v = clean(el.get_text())
                    if v and len(v) < 500: return v
            return ""

        def p(pattern):
            m = re.search(pattern, text, re.I)
            return clean(m.group(1))[:300] if m else ""

        meta = soup.select_one('meta[name="description"], meta[property="og:description"]')
        desc = clean(meta["content"]) if meta and meta.get("content") else g("description","overview","about")

        return {
            "detail_description":     desc,
            "detail_requirements":    g("requirement","admission","eligibility")    or p(r'(?:admission|entry)\s*requirements?[:\s]+([^.]{20,300})'),
            "detail_english_req":     g("english","ielts","toefl","language-req")  or p(r'(?:ielts|toefl|english)[:\s]+([^.]{10,200})'),
            "detail_application_fee": g("application-fee","applicationFee")        or p(r'application\s*fee[:\s]+([^\n]{5,100})'),
            "detail_gpa":             g("gpa","grade","average")                   or p(r'(?:gpa|grade)[:\s]+([^\n]{3,80})'),
            "detail_work_permit":     g("work-permit","coop","co-op","internship") or p(r'(?:work permit|co-?op)[:\s]+([^.]{10,200})'),
            "detail_scholarship":     g("scholarship","bursary","funding")         or p(r'scholarship[:\s]+([^.]{10,200})'),
            "detail_accreditation":   g("accreditation","accredited")              or p(r'accreditati\w+[:\s]+([^.]{10,200})'),
            "detail_campus":          g("campus-location","campus")                or p(r'campus[:\s]+([^\n]{5,150})'),
            "detail_start_dates":     g("intake","start-date","start-dates")       or p(r'(?:start date|intake)[:\s]+([^\n]{5,150})'),
            "detail_deadline":        g("deadline","application-deadline")         or p(r'deadline[:\s]+([^\n]{5,100})'),
            "detail_tuition_detail":  g("tuition-detail","annual-tuition")         or p(r'(?:annual\s+)?tuition[:\s]+([^\n]{5,150})'),
        }
    except Exception as e:
        print(f"      ⚠  Detail error: {e}")
        return {}

# ──────────────────────────────────────────────────────────────────────────────
# EXPORT
# ──────────────────────────────────────────────────────────────────────────────

COLUMNS = [
    "program_name","university","country","city","degree_level","subject",
    "duration","tuition","language","intake","program_url",
    "detail_description","detail_requirements","detail_english_req",
    "detail_application_fee","detail_gpa","detail_work_permit","detail_scholarship",
    "detail_accreditation","detail_campus","detail_start_dates","detail_deadline",
    "detail_tuition_detail","raw_text",
]
HEADERS = {
    "program_name":"Program Name","university":"University","country":"Country",
    "city":"City / Campus","degree_level":"Degree Level","subject":"Subject / Field",
    "duration":"Duration","tuition":"Tuition (Card)","language":"Language",
    "intake":"Intake","program_url":"Program URL","detail_description":"Description",
    "detail_requirements":"Admission Requirements","detail_english_req":"English Requirements",
    "detail_application_fee":"Application Fee","detail_gpa":"Min GPA",
    "detail_work_permit":"Work Permit / Co-op","detail_scholarship":"Scholarships",
    "detail_accreditation":"Accreditation","detail_campus":"Campus Location",
    "detail_start_dates":"Start Dates","detail_deadline":"Application Deadline",
    "detail_tuition_detail":"Tuition (Detail)","raw_text":"Raw Card Text",
}
COL_WIDTHS = {
    "program_name":35,"university":30,"country":14,"city":16,"degree_level":16,
    "subject":22,"duration":12,"tuition":18,"language":12,"intake":14,"program_url":24,
    "detail_description":45,"detail_requirements":38,"detail_english_req":28,
    "detail_application_fee":18,"detail_gpa":12,"detail_work_permit":22,
    "detail_scholarship":28,"detail_accreditation":22,"detail_campus":22,
    "detail_start_dates":20,"detail_deadline":20,"detail_tuition_detail":22,"raw_text":50,
}

def export_xlsx(records, path, applied_filters):
    wb = Workbook()
    ws = wb.active
    ws.title = "Programs"

    hdr_font   = Font(name="Arial", bold=True, color="FFFFFF", size=11)
    hdr_fill   = PatternFill("solid", fgColor="1F4E79")
    hdr_align  = Alignment(horizontal="center", vertical="center", wrap_text=True)
    alt_fill   = PatternFill("solid", fgColor="D6E4F0")
    link_font  = Font(name="Arial", color="1155CC", underline="single", size=10)
    body_font  = Font(name="Arial", size=10)
    wrap_align = Alignment(vertical="top", wrap_text=True)

    for ci, col in enumerate(COLUMNS, 1):
        cell = ws.cell(row=1, column=ci, value=HEADERS[col])
        cell.font, cell.fill, cell.alignment = hdr_font, hdr_fill, hdr_align
    ws.row_dimensions[1].height = 32

    for ri, rec in enumerate(records, 2):
        fill = alt_fill if ri % 2 == 0 else None
        for ci, col in enumerate(COLUMNS, 1):
            val = rec.get(col, "") or ""
            cell = ws.cell(row=ri, column=ci, value=val)
            if fill: cell.fill = fill
            if col == "program_url" and val:
                cell.hyperlink = val
                cell.font = link_font
            else:
                cell.font = body_font
            cell.alignment = wrap_align

    for ci, col in enumerate(COLUMNS, 1):
        ws.column_dimensions[get_column_letter(ci)].width = COL_WIDTHS.get(col, 18)
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions

    ws2 = wb.create_sheet("Summary")
    ws2.column_dimensions["A"].width = 32
    ws2.column_dimensions["B"].width = 20
    bold = Font(name="Arial", bold=True, size=11)
    ws2["A1"] = "ApplyBoard Scrape Summary"
    ws2["A1"].font = Font(name="Arial", bold=True, size=14)
    ws2.append([])
    ws2.append(["Scrape Date", datetime.now().strftime("%Y-%m-%d %H:%M")])
    ws2.append(["Total Programs", len(records)])
    ws2.append([])
    if applied_filters:
        ws2.append(["── Filters Applied ──"])
        ws2[f"A{ws2.max_row}"].font = bold
        for k, v in applied_filters.items():
            ws2.append([k.replace("_"," ").title(), v])
        ws2.append([])
    df = pd.DataFrame(records)
    for col_name, label in [("country","By Country"),("degree_level","By Degree Level"),("subject","By Subject")]:
        if col_name in df.columns and not df[col_name].dropna().empty:
            ws2.append([f"── {label} ──", "Count"])
            r = ws2.max_row
            ws2[f"A{r}"].font = bold
            ws2[f"B{r}"].font = bold
            for val, cnt in df[col_name].value_counts().head(20).items():
                if val: ws2.append([val, cnt])
            ws2.append([])

    wb.save(path)
    print(f"\n✅  XLSX → {path}")

def export_csv(records, path):
    pd.DataFrame(records, columns=COLUMNS).to_csv(path, index=False, encoding="utf-8-sig")
    print(f"✅  CSV  → {path}")

# ──────────────────────────────────────────────────────────────────────────────
# PROMPT
# ──────────────────────────────────────────────────────────────────────────────

def prompt_user():
    print("\n" + "═"*60)
    print("  ApplyBoard Program Scraper  🎓")
    print("═"*60)

    # Login credentials — entered securely at runtime, never stored
    print("\n🔐  Login (required for full access)")
    email    = input("   Email: ").strip()
    password = getpass.getpass("   Password: ")  # hides input while typing

    print("\n📌  Filters (press Enter to skip any)\n")
    filters = {}
    val = input("  University name (e.g. University of Toronto): ").strip()
    if val: filters["university"] = val

    val = input("  Country (e.g. Canada, UK, Australia): ").strip()
    if val: filters["country"] = val

    val = input("  Subject (e.g. Computer Science, Business): ").strip()
    if val: filters["subject"] = val

    val = input("  Degree Level (e.g. Bachelor, Master, Diploma): ").strip()
    if val: filters["degree_level"] = val

    print()
    do_details = input("🔍  Scrape detail pages? (y/n) [y]: ").strip().lower() != "n"
    max_p      = input("📄  Max pages (0 = all) [0]: ").strip()
    max_pages  = int(max_p) if max_p.isdigit() else 0
    out_fmt    = input("💾  Output: xlsx / csv / both [xlsx]: ").strip().lower() or "xlsx"
    out_name   = input("📁  Filename (no extension) [applyboard_programs]: ").strip() or "applyboard_programs"

    return email, password, filters, do_details, max_pages, out_fmt, out_name

# ──────────────────────────────────────────────────────────────────────────────
# MAIN RUN
# ──────────────────────────────────────────────────────────────────────────────

async def run(email, password, filters, do_details, max_pages,
              out_fmt, out_name, debug=False, headless=False):

    all_records = []

    async with async_playwright() as pw:
        browser, ctx = await create_browser(pw, headless=headless)
        page = await ctx.new_page()

        # Step 1: Login
        logged_in = await login(page, email, password, debug=debug)
        if not logged_in:
            print("\n❌  Login failed. Please check your credentials and try again.")
            await browser.close()
            return []

        # Step 2: Resolve school ID from university name if provided
        school_id = None
        if filters.get("university"):
            school_id = await resolve_school_id(page, filters["university"])
            if not school_id:
                print(f"   ⚠  Proceeding without school_id filter for '{filters['university']}'")

        # Step 3: Paginate using direct URL construction (no UI filter clicks)
        detail_tab = await ctx.new_page() if do_details else None
        page_num   = 1

        while True:
            url = build_search_url(
                page_number  = page_num,
                school_id    = school_id,
                country      = filters.get("country"),
                subject      = filters.get("subject"),
                degree_level = filters.get("degree_level"),
            )

            print(f"\n📄  Page {page_num} → {url}")
            await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            await page.wait_for_timeout(PAGE_LOAD_WAIT)
            await human_scroll(page)
            await page.wait_for_timeout(1000)

            html = await page.content()
            if debug:
                save_debug_html(html, f"page_{page_num}")

            records = parse_all_cards(html, debug=(debug and page_num == 1))
            print(f"   Found {len(records)} programs")

            if not records:
                print("   ✅  No more results.")
                break

            if do_details and detail_tab:
                for i, rec in enumerate(records):
                    url_d = rec.get("program_url","")
                    if url_d and url_d.startswith("http"):
                        print(f"   [{i+1}/{len(records)}] {url_d[:80]}")
                        rec.update(await scrape_detail(detail_tab, url_d, debug=(debug and i == 0)))
                        rand_sleep(0.8, 2.0)

            all_records.extend(records)

            if max_pages and page_num >= max_pages:
                print("   ✋  Page limit reached.")
                break

            # If we got fewer results than page size, we're on the last page
            if len(records) < PAGE_SIZE:
                print("   ✅  Last page (partial results).")
                break

            page_num += 1
            rand_sleep()

        if detail_tab: await detail_tab.close()
        await browser.close()

    # Deduplicate
    seen, unique = set(), []
    for r in all_records:
        key = (r.get("program_name","").lower(), r.get("university","").lower(), r.get("program_url",""))
        if key not in seen:
            seen.add(key)
            unique.append(r)

    print(f"\n📊  Total unique programs: {len(unique)}")

    if not unique:
        print("\n⚠  No data scraped.")
        print("   Run with --debug to save HTML and inspect the page structure.")
        return unique

    if out_fmt in ("xlsx","both"): export_xlsx(unique, f"{out_name}.xlsx", filters)
    if out_fmt in ("csv","both"):  export_csv(unique,  f"{out_name}.csv")
    return unique


def main():
    parser = argparse.ArgumentParser(description="ApplyBoard scraper")
    parser.add_argument("--debug",      action="store_true", help="Save raw HTML for inspection")
    parser.add_argument("--no-details", action="store_true", help="Skip detail pages")
    parser.add_argument("--headless",   action="store_true", help="Headless browser mode")
    args = parser.parse_args()

    email, password, filters, do_details, max_pages, out_fmt, out_name = prompt_user()
    if args.no_details: do_details = False

    asyncio.run(run(email, password, filters, do_details, max_pages,
                    out_fmt, out_name, debug=args.debug, headless=args.headless))


if __name__ == "__main__":
    main()
