import os
import time
import glob
import shutil
import requests
import subprocess
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse, quote
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from selenium.webdriver.chrome.service import Service

# Try to import undetected_chromedriver for Cobalt
try:
    import undetected_chromedriver as uc
    UC_AVAILABLE = True
except ImportError:
    UC_AVAILABLE = False
    print("undetected-chromedriver not installed. Install with: pip install undetected-chromedriver")

# --- Setup folders ---
os.makedirs('downloads', exist_ok=True)

# --- Helper: extract username + videoid ---
def extract_tiktok_info(url):
    try:
        parsed = urlparse(url)
        parts = parsed.path.strip('/').split('/')
        if len(parts) >= 3 and parts[0].startswith('@') and parts[1] == 'video':
            username = parts[0][1:].strip()
            videoid = parts[2].strip()
            return username, videoid
    except Exception:
        pass
    return "unknown_user", "unknown_id"

# --- Helper: sanitize filename ---
def sanitize_filename(username, videoid):
    safe_username = "".join(c for c in username if c.isalnum() or c in (' ', '_', '-')).strip()
    safe_username = " ".join(safe_username.split()) # remove double spaces
    videoid = videoid.strip() if videoid else "unknown_id"
    return f"{safe_username} - {videoid}.mp4"

# --- Locks for thread safety ---
existing_files_lock = Lock()

# --- Helper: make unique filename if needed (checks downloads + cached existing files) ---
def make_unique_filename(filename, existing_files_set):
    name, ext = os.path.splitext(filename)
    candidate = filename
    i = 1
    with existing_files_lock:
        while candidate in existing_files_set:
            candidate = f"{name} ({i}){ext}"
            i += 1
        # register candidate to existing_files_set to avoid future collisions
        existing_files_set.add(candidate)
    return candidate

# --- Download helper ---
def download_file_from_href(href, cookies, referer, tiktok_url, output_dir='downloads', max_retries=2, allow_duplicate=False, existing_files_set=None):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Referer': referer,
    }
    session = requests.Session()
    for cookie in cookies:
        try:
            session.cookies.set(cookie['name'], cookie['value'])
        except Exception:
            pass
    username, videoid = extract_tiktok_info(tiktok_url)
    filename = sanitize_filename(username, videoid)
    # If duplicates allowed, find a unique filename across downloads + mapped dirs
    if allow_duplicate:
        if existing_files_set is None:
            existing_files_set = set(os.listdir(output_dir))
        unique_name = make_unique_filename(filename, existing_files_set)
        filepath = os.path.join(output_dir, unique_name)
    else:
        filepath = os.path.join(output_dir, filename)
    if not allow_duplicate and os.path.exists(filepath):
        print(f" File already exists, skipping download: {os.path.basename(filepath)}")
        return True
    for attempt in range(1, max_retries + 1):
        try:
            print(f" Downloading {os.path.basename(filepath)} (attempt {attempt})...")
            response = session.get(href, headers=headers, stream=True, timeout=60)
            response.raise_for_status()
            expected_size = int(response.headers.get("Content-Length", 0))
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            actual_size = os.path.getsize(filepath)
            if expected_size and actual_size + 1024 < expected_size:
                print(f"⚠️ Incomplete file ({actual_size} < {expected_size}) — retrying...")
                # remove incomplete file before retry
                try:
                    os.remove(filepath)
                except Exception:
                    pass
                continue
            if expected_size:
                print(f" ✅ Saved: {filepath} ({actual_size / 1024 / 1024:.1f} MB / expected {expected_size / 1024 / 1024:.1f} MB)")
            else:
                print(f" ✅ Saved: {filepath} ({actual_size / 1024 / 1024:.1f} MB, size unknown)")
            return True
        except Exception as e:
            print(f" Download error on attempt {attempt}: {e}")
            # remove any partial file
            try:
                if os.path.exists(filepath):
                    os.remove(filepath)
            except Exception:
                pass
        time.sleep(1)
    print(f" ❌ Failed to download complete file after {max_retries} attempts.")
    return False

# --- Read URLs ---
with open('urls.txt', 'r', encoding='utf-8') as f:
    urls = [line.strip() for line in f if line.strip()]

# --- FILTER: remove any URL containing 'photo' (case-insensitive) ---
filtered_out_urls = [u for u in urls if 'photo' in u.lower()]
urls = [u for u in urls if 'photo' not in u.lower()]
if filtered_out_urls:
    print(f"Ignoring {len(filtered_out_urls)} URL(s) containing 'photo'.")

if not urls:
    print("No URLs in urls.txt after filtering.")
    exit()

# --- Load user directory map ---
user_map = {}
if os.path.exists('user_dir_map.txt'):
    with open('user_dir_map.txt', 'r', encoding='utf-8') as f:
        for line in f:
            if ':' in line:
                u, d = line.strip().split(':', 1)
                user_map[u.strip()] = d.strip()

# --- Gather existing filenames ---
existing_files = set(os.listdir('downloads'))
base_td = r"C:\Bridge\Downloads\td"
print(f"Caching existing filenames from {base_td} (this may take a moment)...")
if os.path.exists(base_td):
    for root, dirs, files in os.walk(base_td):
        for f in files:
            existing_files.add(f)

# --- New menu: 12 options (4 actions × 3 sites) ---
sites = [
    ("TikWM", "https://www.tikwm.com/originalDownloader.html"),
    ("MusicalDown", "https://musicaldown.com/en"),
    ("Cobalt", "https://cobalt.tools")
]

# --- Selectors Mapping ---
SITE_SELECTORS = {
    "tikwm": {
        "input": (By.ID, "params"),
        "submit": (By.CSS_SELECTOR, "button.btn-submit"),
        "result": (By.CSS_SELECTOR, "a.btn.btn-success[download]")
    },
    "musicaldown": {
        "input": (By.ID, "link_url"),
        "submit": (By.CSS_SELECTOR, "form#submit-form button[type=submit], form button[type=submit]"),
        "result_hd": (By.CSS_SELECTOR, "a.download[data-event='hd_download_click'], a.btn[href*='hd=1'], a.btn[href*='video_hd']"),
        "result_sd": (By.CSS_SELECTOR, "a.download, a.btn[href*='musicaldown.com/download'], a.btn[target='_blank']")
    },
    "cobalt": {
        "input": (By.CSS_SELECTOR, "input[type='url'], input[placeholder*='URL'], input[placeholder*='link'], textarea"),
        "submit": (By.ID, "download-button"),
        "submit_fallback": (By.CSS_SELECTOR, "button, input[type=submit]"),
        "result": (By.CSS_SELECTOR, "a[href$='.mp4'], .download a, a.download")
    }
}
actions = [
    ("Start in headless mode", {"kill_before": False, "headless": True}),
    ("Start in visible mode", {"kill_before": False, "headless": False}),
    ("Kill all Chrome/Chromedriver, then start headless", {"kill_before": True, "headless": True}),
    ("Kill all Chrome/Chromedriver, then start visible", {"kill_before": True, "headless": False})
]
print("Select one option (1-12):")
menu_options = []
idx = 1
for s_name, s_url in sites:
    for action_name, params in actions:
        label = f"{idx}) {action_name} using {s_name}"
        menu_options.append({
            "index": idx,
            "site_name": s_name,
            "site_url": s_url,
            "kill_before": params["kill_before"],
            "headless": params["headless"]
        })
        print(label)
        idx += 1
choice = input("Choose option number (default 1): ").strip()
if not choice.isdigit() or int(choice) < 1 or int(choice) > len(menu_options):
    choice_idx = 1
else:
    choice_idx = int(choice)
selected = menu_options[choice_idx - 1]
use_cobalt = (selected["site_name"].lower() == "cobalt")
use_musicaldown = (selected["site_name"].lower() == "musicaldown")
use_tikwm = (selected["site_name"].lower() == "tikwm")
start_url = selected["site_url"]
kill_before_start = selected["kill_before"]
headless_mode = selected["headless"]
print(f"Selected: {selected['site_name']} — {'headless' if headless_mode else 'visible'} — kill_before={kill_before_start}")

# For Cobalt, ask about undetected (only if Cobalt selected)
use_uc = False
if use_cobalt:
    if not UC_AVAILABLE:
        print("Warning: undetected-chromedriver not available. Install it for better Cloudflare bypass: pip install undetected-chromedriver")
        use_uc = False
    else:
        use_uc_input = input("Use undetected-chromedriver for better Cloudflare compatibility? (y/n, default y): ").strip().lower()
        use_uc = (use_uc_input != 'n')

# --- Windows-only kill handling ---
def kill_chrome_processes_windows():
    # Only kill chromedriver to avoid closing user's open Chrome windows
    try:
        subprocess.call(['taskkill', '/f', '/im', 'chromedriver.exe'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        pass

if kill_before_start:
    print("Killing all Chrome and Chromedriver processes (Windows taskkill)...")
    kill_chrome_processes_windows()
    time.sleep(1.0)

# --- Setup Chrome options ---
options = webdriver.ChromeOptions()
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--disable-gpu')
options.add_argument('--remote-debugging-port=9222')

# Optimization: Disable unnecessary features for speed
options.add_experimental_option("prefs", {
    "profile.managed_default_content_settings.images": 2, # Disable images for faster load
    "download.default_directory": os.path.abspath("downloads"),
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})
options.add_argument('--disable-extensions')
options.add_argument('--disable-infobars')
options.add_argument('--disable-notifications')

# Separate profile to avoid conflicts with personal Chrome
profile_path = os.path.abspath("chrome_profile")
options.add_argument(f'--user-data-dir={profile_path}')
options.add_argument('--profile-directory=TikTokAutomator')

if headless_mode:
    # Use the new headless mode flag where supported
    options.add_argument("--headless=new")
if use_cobalt and not headless_mode:
    options.add_argument('--disable-blink-features=AutomationControlled')

driver = None
try:
    if use_cobalt and use_uc:
        driver = uc.Chrome(options=options, version_main=None) # Auto-detect Chrome version
        print("Using undetected-chromedriver!")
    else:
        service = Service('./chromedriver.exe')
        driver = webdriver.Chrome(service=service, options=options)
        print(f"ChromeDriver launched successfully ({'headless' if headless_mode else 'visible'} mode)!")
        if use_cobalt and not use_uc:
            # try to remove webdriver flag
            try:
                driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            except Exception:
                pass
except WebDriverException as e:
    print(f"Failed to launch ChromeDriver: {e}")
    exit()

driver.implicitly_wait(2)
wait = WebDriverWait(driver, 20)

# --- Helper: Robust click that handles interception ---
def robust_click(element):
    try:
        element.click()
    except Exception:
        driver.execute_script("arguments[0].click();", element)

# --- Helper: click "Do not consent" on MusicalDown whenever visible ---
def try_click_do_not_consent():
    try:
        # selector for the consent button used in your example
        btns = driver.find_elements(By.CSS_SELECTOR, "button.fc-cta-do-not-consent, button.fc-button.fc-cta-do-not-consent")
        for btn in btns:
            try:
                # ensure visible and clickable
                if btn.is_displayed():
                    print("Found 'Do not consent' button — clicking it.")
                    driver.execute_script("arguments[0].click();", btn)
                    time.sleep(0.5)
                    return True
            except Exception:
                pass
    except Exception:
        pass
    return False

# --- For Cobalt, handle initial Turnstile if needed ---
if use_cobalt:
    print(f"Navigating to {start_url}...")
    driver.get(start_url)
    time.sleep(3) # Initial load
    # Check for Turnstile
    try:
        turnstile_iframe = driver.find_element(By.CSS_SELECTOR, "iframe[src*='turnstile']")
        print("Cloudflare Turnstile detected. If in visible mode, solve it manually.")
        if not headless_mode:
            input("Press Enter after solving the Turnstile...")
        else:
            print("In headless mode, Turnstile may block. Consider visible mode or undetected-chromedriver.")
            time.sleep(10) # Wait longer in headless
    except NoSuchElementException:
        print("No Turnstile detected on initial load.")
    # Wait for URL input to be present - use more general selector
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='url'], input[placeholder*='URL'], input[placeholder*='link'], textarea")))
        print("Cobalt page loaded successfully.")
    except TimeoutException:
        print("Failed to load Cobalt input field. Check browser.")
        exit()

# --- Ask whether to check for existing files before downloading ---
check_input = input("Check for existing files before downloading? (y/n, default y): ").strip().lower()
check_exists = (check_input != 'n')

# --- Prepare list of URLs to process depending on check_exists ---
urls_to_process = []
skipped_urls = []
if check_exists:
    for url in urls:
        username, videoid = extract_tiktok_info(url)
        filename = sanitize_filename(username, videoid)
        if filename in existing_files:
            skipped_urls.append(url)
        else:
            urls_to_process.append(url)
else:
    urls_to_process = urls.copy()

print("\n=== Pre-check ===")
print(f"Total URLs loaded (after filter): {len(urls)}")
print(f"Filtered out containing 'photo': {len(filtered_out_urls)}")
print(f"Already downloaded / will be skipped: {len(skipped_urls)}")
print(f"Remaining URLs for processing: {len(urls_to_process)}")
print("================\n")

if not urls_to_process:
    print("All URLs already downloaded or nothing to process! Exiting.")
    try:
        driver.quit()
    except Exception:
        pass
    exit()

# --- Batch processing ---
total_urls = len(urls_to_process)
successful = 0
failed_urls = []
sd_saved = [] # record situations where SD was used (url, href)
max_workers = min(5, os.cpu_count() or 4)
download_executor = ThreadPoolExecutor(max_workers=max_workers)
download_futures = []
batch_size = 10
num_batches = (total_urls + batch_size - 1) // batch_size
initial_count = len(glob.glob('downloads/*.mp4'))

for batch_start in range(0, total_urls, batch_size):
    batch_end = min(batch_start + batch_size, total_urls)
    batch = urls_to_process[batch_start:batch_end]
    current_batch = (batch_start // batch_size) + 1
    print(f"\n--- Processing batch {current_batch}/{num_batches} (URLs {batch_start+1}-{batch_end} of {total_urls}) ---")

    # Load the site once per batch
    try:
        driver.get(start_url)
        # Use smarter waits instead of fixed sleep
        site_key = selected["site_name"].lower()
        wait.until(EC.presence_of_element_located(SITE_SELECTORS[site_key]["input"]))
        try_click_do_not_consent()
    except Exception as e:
        print(f" Failed to load site for batch {current_batch}: {e}")

    batch_success, batch_skipped = 0, 0
    for idx, url in enumerate(batch, 1):
        username, videoid = extract_tiktok_info(url)
        filename = sanitize_filename(username, videoid)
        filepath = os.path.join('downloads', filename)
        if check_exists and os.path.exists(filepath):
            print(f" [{idx}/{len(batch)}] File exists, skipping: {filename}")
            batch_skipped += 1
            continue
        print(f" Processing URL {idx}/{len(batch)}: {url[:120]}...")
        retries, max_retries, url_success = 0, 3, False
        while retries < max_retries and not url_success:
            retries += 1
            try:
                # Check if we need to reload (non-SPA and input field missing or it's a retry)
                site_key = selected["site_name"].lower()
                if not use_cobalt:
                    if retries > 1:
                        driver.get(start_url)
                        try_click_do_not_consent()
                    else:
                        try:
                            driver.find_element(*SITE_SELECTORS[site_key]["input"])
                        except NoSuchElementException:
                            driver.get(start_url)
                            try_click_do_not_consent()
                elif retries > 1:
                    driver.get(start_url)

                href = None
                cookies = []
                referer = driver.current_url
                if use_cobalt:
                    # Cobalt flow
                    url_input = wait.until(EC.presence_of_element_located(SITE_SELECTORS["cobalt"]["input"]))
                    url_input.clear()
                    url_input.send_keys(url)
                    # Click the download button
                    try:
                        submit_btn = wait.until(EC.element_to_be_clickable(SITE_SELECTORS["cobalt"]["submit"]))
                    except TimeoutException:
                        # Fallback to any button
                        submit_btn = wait.until(EC.element_to_be_clickable(SITE_SELECTORS["cobalt"]["submit_fallback"]))
                    robust_click(submit_btn)
                    print(f" Submitted to Cobalt (attempt {retries}). Waiting for download...")
                    # After clicking, try to find download link
                    try:
                        download_anchor = wait.until(EC.presence_of_element_located(SITE_SELECTORS["cobalt"]["result"]))
                        href = download_anchor.get_attribute('href')
                        if not href:
                            href = download_anchor.get_attribute('data-url')
                        if not href:
                            raise ValueError('No href found on download element')
                        # Optimization: only fetch cookies just before download
                        cookies = driver.get_cookies()
                        referer = driver.current_url
                        print(f" Extracted href: {href[:120]}...")
                    except TimeoutException:
                        # Fallback: check auto-download into downloads dir
                        time.sleep(10)
                        mp4_files = glob.glob('downloads/*.mp4')
                        if mp4_files:
                            latest_file = max(mp4_files, key=os.path.getctime)
                            actual_size = os.path.getsize(latest_file)
                            if actual_size > 10000:
                                expected_filename = sanitize_filename(username, videoid)
                                expected_path = os.path.join('downloads', expected_filename)
                                if latest_file != expected_path:
                                    if os.path.exists(expected_path):
                                        name, ext = os.path.splitext(expected_filename)
                                        i = 1
                                        while os.path.exists(expected_path):
                                            expected_path = os.path.join('downloads', f"{name} ({i}){ext}")
                                            i += 1
                                    os.rename(latest_file, expected_path)
                                print(f" ✅ Auto-download detected and renamed: {os.path.basename(expected_path)} ({actual_size / 1024 / 1024:.1f} MB)")
                                url_success = True
                                batch_success += 1
                                break
                        raise
                elif use_musicaldown:
                    # musicaldown flow
                    try:
                        # attempt to click consent if present before interacting
                        try_click_do_not_consent()
                        url_input = wait.until(EC.presence_of_element_located(SITE_SELECTORS["musicaldown"]["input"]))
                        url_input.clear()
                        url_input.send_keys(url)
                        # Submit the form
                        submit_btn = wait.until(EC.element_to_be_clickable(SITE_SELECTORS["musicaldown"]["submit"]))
                        robust_click(submit_btn)
                        print(f" Submitted to MusicalDown (attempt {retries}). Waiting for download link...")
                    except Exception as e:
                        raise
                    # After submitting, prefer HD anchor
                    try:
                        # Be more specific for HD to avoid accidental SD fallback
                        download_anchor = WebDriverWait(driver, 15).until(EC.element_to_be_clickable(SITE_SELECTORS["musicaldown"]["result_hd"]))
                        href = download_anchor.get_attribute('href')
                        if not href:
                            href = download_anchor.get_attribute('data-url')
                        if not href:
                            raise ValueError('No href found on HD download anchor')
                        # Optimization: only fetch cookies just before download
                        cookies = driver.get_cookies()
                        referer = driver.current_url
                        print(f" Extracted HD href: {href[:120]}...")
                    except (TimeoutException, TypeError):
                        # HD not present — fallback to any 'a.download' (SD)
                        try:
                            fallback_anchor = WebDriverWait(driver, 12).until(EC.presence_of_element_located(SITE_SELECTORS["musicaldown"]["result_sd"]))
                            href = fallback_anchor.get_attribute('href')
                            if not href:
                                href = fallback_anchor.get_attribute('data-url')
                            
                            # Optimization: only fetch cookies just before download
                            cookies = driver.get_cookies()
                            referer = driver.current_url
                            print(f" Extracted SD href (fallback): {href[:120]}...")
                            # record SD fallback to sd_saved; we'll store url + href for final report
                            sd_saved.append({"tiktok_url": url, "href": href})
                        except TimeoutException:
                            raise
                else:
                    # tikwm flow
                    try:
                        url_input = wait.until(EC.presence_of_element_located(SITE_SELECTORS["tikwm"]["input"]))
                        url_input.clear()
                        url_input.send_keys(url)
                        submit_btn = wait.until(EC.element_to_be_clickable(SITE_SELECTORS["tikwm"]["submit"]))
                        robust_click(submit_btn)
                        print(f" Submitted to TikWM (attempt {retries}). Waiting...")
                        # try catch for error box if parsing failed
                        try:
                            error_box = driver.find_element(By.CSS_SELECTOR, "div.alert.alert-danger[role='alert']")
                            if "url parsing is failed" in error_box.text.lower():
                                print(" Parse failed — skipping this URL.")
                                failed_urls.append(url)
                                break
                        except NoSuchElementException:
                            pass
                        download_link = wait.until(EC.presence_of_element_located(SITE_SELECTORS["tikwm"]["result"]))
                        href = download_link.get_attribute('href')
                        if not href or 'mp4' not in href.lower():
                            raise ValueError(f"Invalid href: {href}")
                        # Optimization: only fetch cookies just before download
                        cookies = driver.get_cookies()
                        referer = driver.current_url
                        print(f" Extracted href: {href[:120]}...")
                    except TimeoutException:
                        raise

                # Attempt to download the extracted href (only if href was found)
                if href:
                    future = download_executor.submit(download_file_from_href, href, cookies, referer, url, allow_duplicate=not check_exists, existing_files_set=existing_files)
                    download_futures.append((url, future))
                    url_success = True
                    batch_success += 1
                    
                    # For Cobalt, clear input for next use
                    if use_cobalt:
                        try:
                            url_input = driver.find_element(By.CSS_SELECTOR, "input[type='url'], input[placeholder*='URL'], input[placeholder*='link'], textarea")
                            url_input.clear()
                        except:
                            pass
                    # After successful download, stay on page to see if we can reuse it
                    break
            except TimeoutException:
                # Exponential backoff for retries
                wait_time = retries
                print(f" Timeout waiting for result, retrying in {wait_time}s...")
                time.sleep(wait_time)
            except Exception as e:
                wait_time = retries
                print(f" Error on attempt {retries}: {e}. Retrying in {wait_time}s...")
                if retries == max_retries:
                    failed_urls.append(url)
                time.sleep(wait_time)
        if not url_success:
            print(f" URL {idx} failed after {max_retries} retries")
            if url not in failed_urls:
                failed_urls.append(url)
    current_total = len(glob.glob('downloads/*.mp4')) - initial_count
    print(f"Batch {current_batch} complete ({batch_success}/{len(batch)} successful, {batch_skipped} skipped). Total processed: {current_total}/{total_urls}")
    if batch_end < total_urls:
        time.sleep(5) # Reduced batch delay

# Wait for all downloads to complete before moving files
print("\nWaiting for all downloads to finish...")
successful = 0
for url, future in download_futures:
    try:
        if future.result():
            successful += 1
        else:
            if url not in failed_urls:
                failed_urls.append(url)
    except Exception as e:
        print(f" Error processing {url}: {e}")
        if url not in failed_urls:
            failed_urls.append(url)
download_executor.shutdown(wait=True)

# --- Move Section ---
def load_user_map(map_file='user_dir_map.txt'):
    user_map = {}
    if os.path.exists(map_file):
        with open(map_file, 'r', encoding='utf-8') as f:
            for line in f:
                if ':' in line:
                    u, d = line.strip().split(':', 1)
                    user_map[u.strip()] = d.strip()
    return user_map

def save_user_map(user_map, map_file='user_dir_map.txt'):
    with open(map_file, 'w', encoding='utf-8') as f:
        for u, d in user_map.items():
            f.write(f"{u}:{d}\n")

def move_files_to_user_dirs(base_dir=r"C:\Bridge\Downloads\td"):
    user_map = load_user_map()
    downloads = [f for f in os.listdir('downloads') if f.lower().endswith('.mp4')]
    if not downloads:
        print("No files to move.")
        return
    print(f"\nMove downloaded files to user directories under {base_dir}?")
    resp = input("(y/n): ").strip().lower()
    if resp != 'y':
        return
    usernames = {}
    for f in downloads:
        if ' - ' in f:
            u = f.split(' - ')[0].strip()
            usernames.setdefault(u, []).append(f)
        else:
            usernames.setdefault('unknown_user', []).append(f)
    moved, replaced, skipped = 0, 0, 0
    # Auto-move for mapped users
    for username, files in list(usernames.items()):
        if username in user_map:
            subdir = user_map[username]
            dest_dir = os.path.join(base_dir, subdir)
            os.makedirs(dest_dir, exist_ok=True)
            print(f"\nAuto-moving {len(files)} file(s) for '{username}' to '{dest_dir}'...")
            for fname in files:
                src = os.path.join('downloads', fname)
                dst = os.path.join(dest_dir, fname)
                if os.path.exists(dst):
                    src_size = os.path.getsize(src)
                    dst_size = os.path.getsize(dst)
                    if src_size == dst_size:
                        shutil.move(src, dst)
                        replaced += 1
                        print(f" Replaced existing (same size): {fname}")
                    else:
                        skipped += 1
                        print(f" Skipped (size mismatch): {fname}")
                else:
                    shutil.move(src, dst)
                    moved += 1
                    print(f" Moved: {fname}")
            del usernames[username]
    # Ask for unmapped users
    for username, files in usernames.items():
        print(f"\n=== User: {username} ===")
        subdir = input(f"Enter directory under td for '{username}': ").strip()
        if not subdir:
            print(" Skipped (no directory provided).")
            continue
        user_map[username] = subdir
        dest_dir = os.path.join(base_dir, subdir)
        os.makedirs(dest_dir, exist_ok=True)
        for fname in files:
            src = os.path.join('downloads', fname)
            dst = os.path.join(dest_dir, fname)
            shutil.move(src, dst)
            moved += 1
            print(f" Moved: {fname}")
    save_user_map(user_map)
    print(f"\nMove summary: {moved} moved, {replaced} replaced, {skipped} skipped.")
    print("User directory mappings saved to user_dir_map.txt\n")

# --- Small File Check ---
print("\nChecking for small files (< 2KB)...")
small_files = []
for f in os.listdir('downloads'):
    if f.lower().endswith('.mp4'):
        path = os.path.join('downloads', f)
        if os.path.getsize(path) < 2048:
            small_files.append(f)

if small_files:
    print(f"⚠️ Found {len(small_files)} file(s) smaller than 2KB:")
    for sf in small_files:
        print(f" - {sf}")
else:
    print("No small files found.")

# --- Summary ---
print(f"\nAutomation complete! Successful: {successful}/{total_urls}")
failed_count = len(failed_urls)
print(f"Failed: {failed_count}/{total_urls}")
if failed_count > 0:
    resp = input(f"\nGenerate failed_urls.txt? (y/n): ").strip().lower()
    if resp == 'y':
        with open('failed_urls.txt', 'w', encoding='utf-8') as f:
            for u in failed_urls:
                f.write(u + '\n')
        print("Generated failed_urls.txt")

# SD fallback report (per your request)
if sd_saved:
    print("\n--- SD FALLBACK REPORT ---")
    print(f"{len(sd_saved)} item(s) were downloaded via SD fallback (HD not available).")
    for s in sd_saved:
        print(f" TikTok URL: {s.get('tiktok_url')}")
        print(f" SD href: {s.get('href')}")
    # Optionally write to sd_fallback.txt
    resp = input("Write SD fallback report to sd_fallback.txt? (y/n, default y): ").strip().lower()
    if resp != 'n':
        with open('sd_fallback.txt', 'w', encoding='utf-8') as f:
            for s in sd_saved:
                f.write(f"{s.
