#!/usr/bin/env python3
"""
Download mp3/audio files from an anime music Power Ranking Google Sheet.

Setup:
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt

Usage:
    python download_pr.py <google_sheet_url>
"""

import re
import sys
import subprocess
import io
from datetime import date
from pathlib import Path

import requests
import openpyxl


def parse_sheet_id(url: str) -> str:
    """Extract Google Sheets ID from a URL or return the string as-is if it's already an ID."""
    match = re.search(r'/spreadsheets/d/([a-zA-Z0-9_-]+)', url)
    if match:
        return match.group(1)
    # Maybe it's just the raw ID
    if re.match(r'^[a-zA-Z0-9_-]+$', url):
        return url
    raise ValueError(f"Could not parse Google Sheet ID from: {url}")


def download_xlsx(sheet_id: str) -> bytes:
    """Download the Google Sheet as XLSX bytes."""
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"
    print(f"Fetching sheet: {url}")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.content


def find_header_row(ws):
    """Return (row_index, col_map) where col_map maps column names to 1-based col indices."""
    for row in ws.iter_rows():
        values = [str(c.value).strip().lower() if c.value is not None else "" for c in row]
        if "id" in values and any("anime" in v for v in values):
            col_map = {}
            for cell in row:
                if cell.value is not None:
                    col_map[str(cell.value).strip().lower()] = cell.column
            return row[0].row, col_map
    raise ValueError("Could not find header row. Expected columns: ID, Anime Name, Song Info, mp3 Links")


def parse_song_info(text: str):
    """Parse '"Song Title" by Artist' into (title, artist)."""
    if not text:
        return "", ""
    # Match: "Title" by Artist  (with optional curly/smart quotes)
    m = re.match(r'^["\u201c\u300c](.+?)["\u201d\u300d]\s+by\s+(.+)$', text.strip())
    if m:
        return m.group(1).strip(), m.group(2).strip()
    # Fallback: split on ' by '
    parts = text.split(" by ", 1)
    if len(parts) == 2:
        return parts[0].strip().strip('"'), parts[1].strip()
    return text.strip(), ""


def sanitize(name: str) -> str:
    """Replace filesystem-unsafe characters with underscores."""
    name = re.sub(r'[/\\:*?"<>|]', '_', name)
    name = re.sub(r'\s+', '_', name)
    return name.strip('_')


def get_cell_url(cell) -> str | None:
    """Extract URL from a cell's hyperlink or value."""
    if cell.hyperlink:
        target = cell.hyperlink.target if hasattr(cell.hyperlink, 'target') else str(cell.hyperlink)
        if target:
            return target
    val = str(cell.value).strip() if cell.value else ""
    if val.startswith("http"):
        return val
    return None


MIN_FILE_SIZE = 10 * 1024  # 10 KB — treat smaller files as incomplete


def already_downloaded(out_dir: Path, id_prefix: str) -> bool:
    """Check if a reasonably-sized file starting with id_prefix exists in out_dir."""
    return any(
        f.stat().st_size >= MIN_FILE_SIZE
        for f in out_dir.glob(f"{id_prefix}_*")
    )


def download_as_mp3(url: str, dest_base: Path):
    """Download any URL as mp3 using yt-dlp (works for YouTube and direct video/audio files)."""
    output_template = str(dest_base) + ".%(ext)s"
    subprocess.run(
        ["yt-dlp", "-x", "--audio-format", "mp3", "-o", output_template, url],
        check=True
    )


def find_mp3_column(col_map: dict) -> int | None:
    """Find the mp3 links column index from the header map."""
    for key in col_map:
        if "mp3" in key or "audio" in key:
            return col_map[key]
    return None


def git_root() -> Path:
    """Find the git repository root."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True, text=True, check=True
        )
        return Path(result.stdout.strip())
    except subprocess.CalledProcessError:
        return Path(__file__).parent.parent


def main():
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} <google_sheet_url>")
        sys.exit(1)

    sheet_url = sys.argv[1]
    sheet_id = parse_sheet_id(sheet_url)

    # Prompt for output directory
    today = date.today().strftime("%Y-%m-%d")
    dir_name = input(f"Output directory name [{today}]: ").strip() or today

    out_dir = git_root() / "media" / dir_name
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Saving to: {out_dir}")

    # Download and parse XLSX
    xlsx_bytes = download_xlsx(sheet_id)
    wb = openpyxl.load_workbook(io.BytesIO(xlsx_bytes))
    ws = wb.active

    header_row, col_map = find_header_row(ws)
    print(f"Found headers: {list(col_map.keys())}")

    # Locate required columns
    id_col = col_map.get("id")
    anime_col = next((col_map[k] for k in col_map if "anime" in k), None)
    song_info_col = next((col_map[k] for k in col_map if "song info" in k or "song title" in k), None)
    mp3_col = find_mp3_column(col_map)

    if not all([id_col, anime_col, song_info_col]):
        print(f"ERROR: Missing required columns. Found: {list(col_map.keys())}")
        print("Expected columns containing: id, anime, song info")
        sys.exit(1)

    if mp3_col:
        print("mp3 column found — will use mp3 links.")
    else:
        print("No mp3 column — will use Song Info links.")

    # Collect rows
    songs = []
    for row in ws.iter_rows(min_row=header_row + 1):
        id_cell = row[id_col - 1]
        if id_cell.value is None:
            continue
        try:
            song_id = int(id_cell.value)
        except (ValueError, TypeError):
            continue
        anime = str(row[anime_col - 1].value or "").strip()
        song_info_cell = row[song_info_col - 1]
        song_info_text = str(song_info_cell.value or "").strip()
        title, artist = parse_song_info(song_info_text)
        media_url = get_cell_url(row[mp3_col - 1]) if mp3_col else get_cell_url(song_info_cell)
        songs.append((song_id, anime, title, artist, media_url))

    total = len(songs)
    print(f"Found {total} songs.")

    for i, (song_id, anime, title, artist, media_url) in enumerate(songs, 1):
        id_prefix = f"{song_id:02d}"
        base_name = sanitize(f"{id_prefix}_{anime}_{title}_by_{artist}")
        print(f"[{i}/{total}] {base_name}", end="")

        if already_downloaded(out_dir, id_prefix):
            print(" (skipped, already exists)")
            continue

        if not media_url:
            print(" (skipped, no URL)")
            continue

        print()
        try:
            download_as_mp3(media_url, out_dir / base_name)
        except Exception as e:
            print(f"  ERROR: {e}")

    print("Done.")


if __name__ == "__main__":
    main()
