from pathlib import Path
import csv

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "data" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

output_file = OUTPUT_DIR / "apache_raw_dataset.csv"

with output_file.open("w", encoding="utf-8", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["site", "file", "line_no", "raw_line"])

    for path in sorted(BASE_DIR.glob("*_access.log")):
        site_name = path.stem.replace("_access", "")
        with path.open("r", encoding="utf-8", errors="ignore") as src:
            for line_no, line in enumerate(src, start=1):
                writer.writerow([site_name, path.name, line_no, line.rstrip("\n")])

print(f"Saved dataset to: {output_file}")
print(f"Total rows written: {sum(1 for _ in output_file.open('r', encoding='utf-8')) - 1}")
