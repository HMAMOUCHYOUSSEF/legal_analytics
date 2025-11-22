import requests
from bs4 import BeautifulSoup
import os
import bz2

# CONFIG
#=======
API_URL = "https://com-courtlistener-storage.s3.amazonaws.com/?list-type=2&prefix=bulk-data/&delimiter=/" # THE PAGE URL
BASE_URL = "https://storage.courtlistener.com/" # EVERY FILE HASE THIS BASE URL AT FIRST
RAW_DATA_PATH = "data/raw/pressed" # WHERE I WILL STOCK THE PRESSED FILES


#FIND THE DOWNLOAD LINK TO DOWNLOAD THE FILE
#===========================================
def find_link(api_url):
    print(f"\n📡 scanning {api_url}")
    response = requests.get(api_url) # WE SEND THE HTTP GET REQUEST TO THE API URL PAGE WHEN ALL FILES EXIST
    soup = BeautifulSoup(response.content,"xml") # WE ARE LOOKING AT FILES NOT TEXT + THE FILES ARE HOSTED ON AWS S3

    for content in soup.find_all("Contents"): # THE XML CODE CONTAINTS CONTENTS AND EACH CONTENT HAVE A KEY
        KEY = content.find("Key").get_text() # THE KEY CONTAIN THE FILE NAME, THE LAST URL NAME
        
        URL_FILE = BASE_URL + KEY # MERGE THE BASE URL WITH THE NAME OF THE FILE TO CREATE THE URL FILE
        

        if "2025-01" in URL_FILE: # WE CHOOSE ONLY FILES THAT EXTRACTED ON 2025-01
            download_file(URL_FILE) 

#THE DOWNLOAD FUNCITON
#=====================
def download_file(url_file, folder_path=RAW_DATA_PATH):

    os.makedirs(folder_path, exist_ok=True) # CREATE THE FOLDER IF NOT EXIST
    filename = url_file.split("/")[-1] # TAKE THE NAME OF THE FILE
    file_path = os.path.join(folder_path, filename)

    
    
    if os.path.exists(file_path):
    
        # CHECK FILE SIZE AND RESUME IF INCOMPLETE
        file_size = os.path.getsize(file_path)
        headers = {'Range': f'bytes={file_size}-'} if file_size > 0 else {}
        print(f"🔄 RESUMING DOWNLOAD FROM BYTE {file_size}")
    else:
        print(f"⏬ DOWNLOADING THE FILE {filename}")
        file_size = 0
        headers = {}
    attempt = 0
    while True:
        attempt += 1
        try:
            response = requests.get(url_file, stream=True, headers=headers) # THIS TIME WE SEND THE HTTP GET REQUEST TO THE URL FILE, ALLOWING US TO DOWNLAD THE FILE IN CHUNK WICH IS GOOD TO THE RAM MEMORY
            response.raise_for_status()  # CHECK IF THE REQUEST WAS SUCCESSFUL

            #LET DOWNLOAD THE FILE IN CHUNKS
            #===============================
            mode = "ab" if file_size > 0 else "wb"  # APPEND IF RESUMING, WRITE IF NEW
            with open(file_path, mode) as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            
            print(f"💹 IT IS DONE, THE FILE IS IN: {file_path}")
            return file_path

        except requests.exceptions.RequestException as e:
            if response.status_code == 416:
                print(f"✅ FILE IS ALREADY COMPLETE: {filename}")
                return file_path  # File is already fully downloaded
            print(f"❌ DOWNLOAD FAILED FOR {filename}: {e}")
            if attempt > 5:
                return None

#WE WILL DEPRESS THE FILES ON THESE FUNCTIONS
#============================================

def decompress_bz2(bz2_path):
    if not bz2_path.endswith(".bz2"):
        return None

    # Create depressed folder if it doesn't exist
    depressed_dir = os.path.join("data/raw", "depressed")
    os.makedirs(depressed_dir, exist_ok=True)
    
    # Get original filename without .bz2 and create new path in depressed folder
    original_filename = os.path.basename(bz2_path)[:-4]  # remove .bz2 from filename
    csv_path = os.path.join(depressed_dir, original_filename)


    if os.path.exists(csv_path):
        print(f"⏭ Already decompressed: {csv_path}")
        return csv_path

    print(f"📦 Decompressing: {bz2_path}")
    with bz2.open(bz2_path, "rb") as source, open(csv_path, "wb") as dest:
        dest.write(source.read())

    print(f"✔ Decompressed to: {csv_path}")
    return csv_path


def decompress_all():
    all_csvs = []

    for root, dirs, files in os.walk("data/raw"):
        for f in files:
            if f.endswith(".bz2"):
                bz2_path = os.path.join(root, f)
                csv_path = decompress_bz2(bz2_path)
                if csv_path:
                    all_csvs.append(csv_path)

    return all_csvs




#WHERE EVERY THINGS RUN
#======================
if __name__ == "__main__":
    print("\n🚀 STARTING COURTLISTENER EXTRACT PIPELINE\n")

    print("📥 Step 1 — Downloading all .bz2 bulk files...")
    find_link(API_URL)

    print("\n📦 Step 2 — Decompressing all files...")
    csv_files = decompress_all()

    print("\n🎉 DONE — All raw CSVs are ready in /data/raw/")

    








