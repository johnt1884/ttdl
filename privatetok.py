import os
import subprocess

# === Paths ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
URL_FILE = os.path.join(BASE_DIR, "urls.txt")
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads")

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# === Load URLs ===
with open(URL_FILE, "r", encoding="utf-8") as f:
    urls = [line.strip() for line in f if line.strip()]

print(f"Loaded {len(urls)} URLs")

# === Download each URL ===
for index, url in enumerate(urls, start=1):
    print(f"\n[{index}/{len(urls)}] Downloading: {url}")

    command = [
        "yt-dlp",
        "--cookies-from-browser", "firefox",
        "-f", "bestvideo+bestaudio/best",

        # 🔑 KEY: unique filename per URL
        "-o", os.path.join(DOWNLOAD_DIR, f"%(uploader)s - %(title)s ({index}).%(ext)s"),

        url
    ]

    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError:
        print(f"❌ Failed: {url}")

print("\n✅ All downloads attempted.")
