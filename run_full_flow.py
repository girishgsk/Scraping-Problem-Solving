# mplads_pipeline.py
import os
import re
import time
import json
import requests
import pandas as pd
from pathlib import Path
from openpyxl import Workbook, load_workbook
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
import winsound
import os


# ---------------- CONFIG ----------------
DISTRICTS_FILE = "districts.xlsx"           # input: column "Districts"
MPLADS_MPS_FILE = "mplads_all_mps.xlsx"     # must contain "District" and "Profile Link"
SUMMARY_FILE = "hyperZones-mp-data.xlsx"    # append-only MP summary
FAILURE_FILE = "failureDistricts.xlsx"      # logged failures (channel or MP match)
CHANNELS_CSV = "channels_found.csv"         # append channel id/name found

API_BASE = os.getenv("API_BASE")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")  # Bearer token

PLAYWRIGHT_HEADLESS = False
# ----------------------------------------
# ---------- Utilities ----------
def format_date(raw_date: str) -> str:
    """
    Convert various date formats like '30 Oct 2025' or '26 Sept 2024'
    into '2025-10-30'. Returns '' if unable to parse.
    """
    if not raw_date:
        return ""

    raw_date = raw_date.strip()

    # 🔧 Normalize month abbreviations (like Sept → Sep)
    month_fixes = {
        "Sept": "Sep",
        "Mar.": "Mar",
        "Jun.": "Jun",
        "Jul.": "Jul",
        "Oct.": "Oct",
        "Nov.": "Nov",
        "Dec.": "Dec"
    }

    for wrong, right in month_fixes.items():
        if wrong in raw_date:
            raw_date = raw_date.replace(wrong, right)

    # Try multiple formats
    formats = [
        "%d %b %Y",     # 26 Sep 2024
        "%d %B %Y",     # 26 September 2024
        "%Y-%m-%d",     # 2024-09-26
        "%d/%m/%Y",     # 26/09/2024
        "%d-%m-%Y"      # 26-09-2024
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(raw_date, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    print(f"⚠️ Unrecognized date format: {raw_date}")
    return ""

def normalize_channel_name(name: str) -> str:
    """Remove spaces and hyphens from district name to form channelName."""
    if not isinstance(name, str):
        return ""
    return re.sub(r"[-\s]+", "", name).strip()

def normalize_key(s: str) -> str:
    """Normalize district strings for matching: lowercase and remove non-word chars."""
    if not isinstance(s, str):
        return ""
    return re.sub(r"[^\w]", "", s.lower())

def append_csv_row(path: str, row: dict):
    df = pd.DataFrame([row])
    if Path(path).exists():
        df.to_csv(path, mode="a", header=False, index=False)
    else:
        df.to_csv(path, index=False)

def append_failure(district: str, reason: str):
    """Append a failure row to FAILURE_FILE with District and Reason."""
    row = {"District": district, "Reason": reason}
    if Path(FAILURE_FILE).exists():
        old = pd.read_excel(FAILURE_FILE)
        new = pd.concat([old, pd.DataFrame([row])], ignore_index=True)
        new.to_excel(FAILURE_FILE, index=False)
    else:
        pd.DataFrame([row]).to_excel(FAILURE_FILE, index=False)
    print(f"⚠️ Logged failure: {district} — {reason}")

def append_summary_row(summary: dict):
    """Append MP summary to SUMMARY_FILE (create file with headers if missing)."""
    headers = [
        "MP Name", "District", "State", "Total Allocated",
        "Fund Utilization", "Works Completed", "Completion Rate",
        "Paid but Work Incomplete", "Profile Link", "Channel ID", "Channel Name"
    ]

    row = {key: summary.get(key, "") for key in headers}

    if Path(SUMMARY_FILE).exists():
        df_old = pd.read_excel(SUMMARY_FILE)
        df_new = pd.concat([df_old, pd.DataFrame([row])], ignore_index=True)
    else:
        df_new = pd.DataFrame([row], columns=headers)

    df_new.to_excel(SUMMARY_FILE, index=False)
    print(f"💾 Appended MP summary to {SUMMARY_FILE}")    

# ---------- Channel search ----------
def search_channel(channel_name: str, fallback_return_first: bool = True):
    """Call /api/circles/search/?q={channel_name}. Return dict {'id','name'} or None."""
    if not API_BASE or not ACCESS_TOKEN:
        print("⚠️ API_BASE or ACCESS_TOKEN not set; cannot search channel.")
        return None
    url = f"{API_BASE}/api/circles/search/"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    params = {"q": channel_name}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=20)
        r.raise_for_status()
        js = r.json()
        results = js.get("results", [])
        # exact match (case-insensitive)
        for c in results:
            if isinstance(c.get("name"), str) and c.get("name").lower() == channel_name.lower():
                return {"id": c.get("id"), "name": c.get("name")}
        # fallback: optionally return first
        if fallback_return_first and results:
            return {"id": results[0].get("id"), "name": results[0].get("name")}
    except Exception as e:
        print("⚠️ search_channel error:", e)
    return None

# ---------- Scraping utilities ----------
def click_inpage_tab(page, tab_text, min_y=200, timeout=10000):
    """Click the correct in-page tab by visible text (avoids header links)."""
    loc = page.locator(f"text={tab_text}")
    try:
        loc.first.wait_for(timeout=timeout)
    except PWTimeout:
        return False

    elems = loc.all()
    for el in elems:
        try:
            box = el.bounding_box()
            if not box:
                continue
            if box["y"] > min_y:
                el.click()
                return True
        except Exception:
            continue
    try:
        loc.first.click(timeout=3000)
        return True
    except Exception:
        return False


def extract_cards(page, url):
    """Scrape all visible .project-card elements."""
    cards = page.locator(".project-card")
    count = cards.count()
    results = []

    for i in range(count):
        try:
            card = cards.nth(i)
            title = card.locator(".project-title").inner_text().strip()
            category = card.locator(".project-category").inner_text().strip() if card.locator(".project-category").count() else ""
            amount = card.locator(".detail-item").nth(0).locator("span").nth(1).inner_text().strip()
            location = card.locator(".detail-item").nth(1).locator("span").inner_text().strip()
            date = card.locator(".detail-item").nth(2).locator("span").inner_text().strip()
            person_name = card.locator(".mp-info strong").inner_text().strip()
            district = card.locator(".mp-info span").inner_text().strip()
            recommendation = card.locator(".status-badge").inner_text().strip().capitalize() if card.locator(".status-badge").count() else ""
            results.append({
                "title": title,
                "category": category,
                "amount": amount,
                "location": location,
                "date": date,
                "person_name": person_name,
                "district": district,
                "recommendation": recommendation,
                "source_url": url
            })
        except Exception as e:
            print("   ⚠ skipped card", i, "due to", e)
            continue
    return results


def scrape_recommended_works_and_post(page, profile_url, circle_id, channel_name):
    """
    Open 'Projects' → 'Recommended Works', paginate (Load More / Next),
    extract all cards and POST each as a project to the API.
    """
    print(f"→ Visiting {profile_url}")
    # page.goto(profile_url, timeout=120000)
    page.goto(profile_url, timeout=10000)
    page.wait_for_load_state("networkidle")
    time.sleep(1)

    # Navigate in-page
    click_inpage_tab(page, "Projects", min_y=180)
    time.sleep(1)
    click_inpage_tab(page, "Recommended Works", min_y=200)
    time.sleep(2)

    all_results = []
    page_number = 1

    while True:
        print(f"  Scraping page {page_number}...")
        time.sleep(1)

        # Scroll to trigger lazy load
        prev_h = -1
        for _ in range(20):
            page.evaluate("window.scrollBy(0, window.innerHeight)")
            time.sleep(0.3)
            h = page.evaluate("document.body.scrollHeight")
            if h == prev_h:
                break
            prev_h = h

        try:
            page.wait_for_selector(".project-card", timeout=10000)
        except PWTimeout:
            print("  ⚠️ No project-card found on this page.")
            break

        cards_data = extract_cards(page, profile_url)
        print(f"   → Found {len(cards_data)} cards on page {page_number}")
        all_results.extend(cards_data)

        # POST each card directly
        for card in cards_data:
            payload = {
                "circle": circle_id,
                "title": f"Proposed Project for {channel_name}: {card['title']}",
                "description": "",
                "images": [],
                "videos": [],
                "political_data": {
                    "category": card.get("category") or "Normal/Others",
                    "amount": card.get("amount") or "",
                    "location": card.get("location") or "",
                    "date": format_date(card.get("date")) or "",
                    "person_name": card.get("person_name") or "",
                    "district": card.get("district") or "",
                    # "recommendation": card.get("recommendation") or "",
                    "recommendation": "recommended",
                },
            }
            try:
                post_url = f"{API_BASE}/api/posts/"
                headers = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Content-Type": "application/json"}
                resp = requests.post(post_url, headers=headers, data=json.dumps(payload), timeout=30)
                if resp.status_code in (200, 201):
                    # print(f"     ✅ posted: {payload['title']}")
                    print("✅")
                else:
                    print(f"     ❌ failed to post ({resp.status_code}): {resp.text}")
            except Exception as e:
                print("     ❌ exception posting card:", e)

        # Pagination — handle both “Next” and “Load more”
        next_btn = page.locator('button:has-text("Next"), button:has-text("Load more")')
        if next_btn.count() == 0:
            next_btn = page.locator("text=›")
        if next_btn.count() == 0:
            print("  No Next/Load more button found — stopping pagination.")
            break

        # check disabled
        try:
            disabled = next_btn.get_attribute("disabled")
        except Exception:
            disabled = None
        if disabled:
            print("  Next/Load more disabled — reached last page.")
            break

        try:
            next_btn.first.click()
            time.sleep(2)
            page_number += 1
        except Exception as e:
            print("  Failed to click next:", e)
            break

    print(f"✅ Extracted total {len(all_results)} projects from {profile_url}")
    return len(all_results)


# ---------- MP Summary Scraper ----------
def scrape_mp_summary_and_save(page, profile_url, channel_id=None, channel_name=None):
    """Scrape MP summary card and save it into SUMMARY_FILE (append)."""
    print(f"\n➡ Scraping MP Summary: {profile_url}")
    page.goto(profile_url, timeout=120000)
    page.wait_for_load_state("networkidle")
    time.sleep(2)

    def safe_text(selector, first=False):
        """Safely extract inner text from Playwright locator."""
        try:
            loc = page.locator(selector)
            if first:
                return loc.first.inner_text().strip()
            return loc.inner_text().strip()
        except Exception:
            return ""

    # --- Core summary info ---
    mp_name = safe_text(".mp-title-info h1")
    location_text = safe_text(".mp-basic-info .info-item span", first=True)

    # parse district and state
    parts = [x.strip() for x in location_text.split(",")] if location_text else []
    district = parts[0] if len(parts) > 0 else ""
    state = parts[1] if len(parts) > 1 else ""

    total_allocated = safe_text(".summary-stat-card.allocated .stat-value")
    fund_utilization = safe_text(".summary-stat-card.utilization .stat-value")
    works_completed = safe_text(".summary-stat-card.works .stat-value")
    completion_rate = safe_text(".summary-stat-card.success .stat-value")

    try:
        paid_but_incomplete = page.locator(".mp-payment-warning .warning-amount").inner_text().strip()
    except Exception:
        paid_but_incomplete = "N/A"

    summary = {
        "MP Name": mp_name,
        "District": district,
        "State": state,
        "Total Allocated": total_allocated,
        "Fund Utilization": fund_utilization,
        "Works Completed": works_completed,
        "Completion Rate": completion_rate,
        "Paid but Work Incomplete": paid_but_incomplete,
        "Profile Link": profile_url,
        "Channel ID": channel_id or "",
        "Channel Name": channel_name or "",
    }

    append_summary_row(summary)
    print(f"✅ Summary extracted for {mp_name}: {district}, {state}")
    return summary

# ---------- Main driver ----------
def main():
    # check inputs
    if not Path(DISTRICTS_FILE).exists():
        print(f"❌ {DISTRICTS_FILE} not found. Create one with header 'Districts'")
        return
    if not Path(MPLADS_MPS_FILE).exists():
        print(f"❌ {MPLADS_MPS_FILE} not found. Create MP list first.")
        return
    if not API_BASE or not ACCESS_TOKEN:
        print("❌ Please set API_BASE and ACCESS_TOKEN environment variables before running.")
        print("Example (bash): export API_BASE=https://your-domain.com; export ACCESS_TOKEN=abcd")
        return

    districts_df = pd.read_excel(DISTRICTS_FILE)
    mps_df = pd.read_excel(MPLADS_MPS_FILE)
    # prepare key for MP matching
    mps_df["_key"] = mps_df["District"].fillna("").astype(str).apply(normalize_key)

    # ensure channels CSV exists header
    if not Path(CHANNELS_CSV).exists():
        pd.DataFrame(columns=["district_input", "channel_id", "channel_name"]).to_csv(CHANNELS_CSV, index=False)

    # Playwright browser
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=PLAYWRIGHT_HEADLESS)
        context = browser.new_context()
        page = context.new_page()

        for idx, row in districts_df.iterrows():
            raw_district = str(row.get("Districts", "")).strip()
            if not raw_district:
                continue
            print("\n=== Processing district:", raw_district, "===")

            # Step 1: normalize to channelName
            channel_name = normalize_channel_name(raw_district)
            if not channel_name:
                append_failure(raw_district, "channel name normalize empty")
                continue

            # Step 1a: search channel endpoint
            channel_info = search_channel(channel_name, fallback_return_first=True)
            if not channel_info:
                append_failure(raw_district, "channel not found")
                continue

            # save channel found
            append_csv_row(CHANNELS_CSV, {"district_input": raw_district, "channel_id": channel_info["id"], "channel_name": channel_info["name"]})

            # Step 2: match district to mplads_all_mps.xlsx
            key = normalize_key(raw_district)
            matches = mps_df[mps_df["_key"] == key]
            if matches.empty:
                # try contains match (some districts may vary)
                candidates = mps_df[mps_df["_key"].str.contains(key, na=False)]
                if not candidates.empty:
                    matches = candidates

            if matches.empty:
                append_failure(raw_district, "MP row not found in mplads_all_mps.xlsx")
                continue

            mp_row = matches.iloc[0]
            profile_link = mp_row.get("Profile Link") or mp_row.get("ProfileLink") or mp_row.get("Profile_Link") or ""
            if not profile_link:
                append_failure(raw_district, "Profile Link empty in mplads_all_mps.xlsx")
                continue

            # normalize profile URL
            if profile_link.startswith("/"):
                profile_link = "https://empoweredindian.in" + profile_link

            # Step 3: Scrape MP summary and save to hyperZones file
            try:
                summary = scrape_mp_summary_and_save(page, profile_link, channel_id=channel_info["id"], channel_name=channel_info["name"])
                print("  → Saved summary for", summary.get("MP Name"))
            except Exception as e:
                append_failure(raw_district, f"summary_scrape_error: {e}")
                continue

            # Step 4: Scrape Recommended Works and POST each card
            try:
                posted = scrape_recommended_works_and_post(page, profile_link, channel_info["id"], channel_info["name"])
                print(f"  → Posted {posted} cards for {raw_district}")
            except Exception as e:
                append_failure(raw_district, f"projects_scrape_post_error: {e}")
                continue

        browser.close()
    print("\nAll districts processed.")
    winsound.PlaySound(r"C:\\Windows\\Media\\Ring03.wav", winsound.SND_FILENAME)


if __name__ == "__main__":
    main()