#!/usr/bin/env bash
# Builds workshop_data.zip from the local data/ folder (instructors only).
# Upload the result as asset of the GitHub release "data-v1":
#   gh release create data-v1 dist/workshop_data.zip --title "Workshop data v1" --notes "Open data for the workshop"
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p ../dist
rm -f ../dist/workshop_data.zip
zip -r -9 ../dist/workshop_data.zip mobility sample_data adventureworks -x "*.DS_Store"
ls -lh ../dist/workshop_data.zip
