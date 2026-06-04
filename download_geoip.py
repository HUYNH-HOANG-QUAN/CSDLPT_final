"""
Download MaxMind GeoLite2-City database for real geo-location lookups.
Get a free license key from: https://www.maxmind.com/en/geolite2/signup
"""
import os
import sys
import gzip
import zipfile
import shutil
import tempfile

# Download URL for GeoLite2-City (free, requires MaxMind account)
# Replace with your actual license key from https://www.maxmind.com/en/geolite2/signup
MAXMIND_LICENSE_KEY = "YOUR_MAXMIND_LICENSE_KEY_HERE"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_PATH = os.path.join(DATA_DIR, "GeoLite2-City.mmdb")
DOWNLOAD_URL = (
    "https://download.maxmind.com/app/geoip_download?"
    "edition_id=GeoLite2-City&license_key={}&suffix=tar.gz"
)

# Alternative: direct download (no auth) for older/smaller version
ALT_URL = (
    "https://raw.githubusercontent.com/PitikornPat/GeoLite2-City/master/"
    "GeoLite2-City_20250604/GeoLite2-City.mmdb"
)


def download_with_requests(url: str, output_path: str) -> bool:
    """Download file using requests with progress."""
    import requests

    try:
        print(f"Downloading from: {url[:80]}...")
        response = requests.get(url, timeout=120, stream=True)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))
        downloaded = 0

        with tempfile.NamedTemporaryFile(delete=False, suffix=".tar.gz") as tmp:
            tmp_path = tmp.name
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    tmp.write(chunk)
                    downloaded += len(chunk)
                    if total_size:
                        pct = downloaded / total_size * 100
                        print(f"\r  Progress: {pct:.1f}% ({downloaded/1024/1024:.1f}MB)", end="", flush=True)

        print()
        return tmp_path
    except Exception as e:
        print(f"Download failed: {e}")
        return None


def extract_mmdb_from_tar(tar_path: str, output_dir: str) -> str:
    """Extract .mmdb file from .tar.gz archive."""
    import tarfile

    with tarfile.open(tar_path, "r:gz") as tar:
        for member in tar.getmembers():
            if member.name.endswith(".mmdb"):
                mmdb_path = tar.extract(member, path=output_dir)
                return os.path.join(output_dir, member.name)

    return None


def download_geoip_db() -> bool:
    """Main download function."""
    if os.path.exists(OUTPUT_PATH):
        size_mb = os.path.getsize(OUTPUT_PATH) / (1024 * 1024)
        print(f"GeoIP database already exists: {OUTPUT_PATH} ({size_mb:.1f}MB)")
        return True

    if MAXMIND_LICENSE_KEY == "YOUR_LICENSE_KEY_HERE":
        print("ERROR: You need a MaxMind license key!")
        print()
        print("Steps to get GeoIP working:")
        print("1. Go to https://www.maxmind.com/en/geolite2/signup")
        print("2. Create a free account")
        print("3. Get your license key from your account page")
        print("4. Replace 'YOUR_LICENSE_KEY_HERE' in this script")
        print("5. Run this script again")
        print()
        print("Alternatively, the pipeline will use synthetic fallback geo data.")
        print("This is still valid for demonstrating the ETL pipeline.")
        return False

    os.makedirs(DATA_DIR, exist_ok=True)

    tmp_tar = download_with_requests(
        DOWNLOAD_URL.format(MAXMIND_LICENSE_KEY),
        OUTPUT_PATH
    )

    if not tmp_tar:
        return False

    print("Extracting GeoLite2-City.mmdb...")
    mmdb_path = extract_mmdb_from_tar(tmp_tar, DATA_DIR)

    if mmdb_path:
        dest = OUTPUT_PATH
        shutil.move(mmdb_path, dest)
        size_mb = os.path.getsize(dest) / (1024 * 1024)
        print(f"GeoIP database saved to: {dest} ({size_mb:.1f}MB)")
        os.unlink(tmp_tar)
        return True
    else:
        print("Failed to extract .mmdb from archive")
        return False


if __name__ == "__main__":
    success = download_geoip_db()
    sys.exit(0 if success else 1)
