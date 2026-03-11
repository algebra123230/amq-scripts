#!/bin/bash
set -e

# Install ffmpeg (includes ffprobe) via Homebrew
if ! command -v brew &>/dev/null; then
    echo "Homebrew not found. Install it from https://brew.sh then re-run this script."
    exit 1
fi

if ! command -v ffmpeg &>/dev/null; then
    echo "Installing ffmpeg..."
    brew install ffmpeg
else
    echo "ffmpeg already installed: $(ffmpeg -version 2>&1 | head -1)"
fi

# Set up Python venv
python3 -m venv venv
source venv/bin/activate
pip install -q -r requirements.txt
echo "Python dependencies installed."

echo "Done. Activate the venv with: source venv/bin/activate"
