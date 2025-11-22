import os
import requests
from bs4 import BeautifulSoup
import subprocess

# ================================================
# CONFIG
# ================================================
API_URL = "https://com-courtlistener-storage.s3.amazonaws.com/?list-type=2&prefix=bulk-data/&delimiter=/"
BASE_URL = "https://storage.courtlistener.com/"
RAW_PATH = "/mnt/e/FREELANCE/legal_data_analytics/data/raw"
BZ2_PATH = f"{RAW_PATH}/pressed"
CSV_PATH = f"{RAW_PATH}/depressed"
URL_LIST_FILE = f"{RAW_PATH}/bulk_urls.txt"

os.makedirs(BZ2_PATH, exist_ok=True)
os.makedirs(CSV_PATH, exist_ok=True)


# ================================================
# 1. FIND ALL URLs AND SAVE TO bulk_urls.txt
# ================================================
def generate_url_list():
    print("\n📡 Fetching list of bulk files...")
    response = requests.get(API_URL)
    soup = BeautifulSoup(response.content, "xml")

    urls = []
    for content in soup.find_all("Contents"):
        key = content.find("Key").get_text()
        # Keep only 2025-01 files
        if "2025-01" in key and key.endswith(".bz2"):
            urls.append(BASE_URL + key)

    if not urls:
        print("❌ No files found!")
        return

    with open(URL_LIST_FILE, "w") as f:
        f.write("\n".join(urls))

    print(f"✅ URL list generated: {URL_LIST_FILE}")
    print(f"📦 Files detected: {len(urls)}")


# ================================================
# 2. DOWNLOAD USING ARIA2C (fast & parallel)
# ================================================
def download_all():
    print("\n⏬ Downloading files using aria2c...")
    cmd = [
        "aria2c",
        "-x", "16",     # connections per download
        "-s", "16",     # parallel segments
        "-j", "10",     # download 10 files at once
        "-d", BZ2_PATH, # output directory
        "-i", URL_LIST_FILE  # list of URLs
    ]

    subprocess.run(cmd, check=True)
    print("📥 Download finished!")


# ================================================
# 3. DECOMPRESS USING PBZIP2 (all cores)
# ================================================
def decompress_all():
    print("\n📦 Decompressing with pbzip2...")

    cmd = f"pbzip2 -d {BZ2_PATH}/*.bz2 --output {CSV_PATH}/"
    os.system(cmd)

    print("✔ Decompression complete!")


# ================================================
# RUN THE PIPELINE
# ================================================
if __name__ == "__main__":
    print("\n🚀 STARTING NEW EXTRACTION PIPELINE\n")

    generate_url_list()
    download_all()
    decompress_all()

    print("\n🎉 DONE — CSVs are ready!")
