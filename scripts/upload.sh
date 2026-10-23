#!/bin/zsh
# Copy finished hourly files to Google Drive every 15 minutes and free local
# disk once a file has been on Drive for KEEP_HOURS. Runs at low priority with
# a bandwidth cap so it never competes with the recorder.
#
# Needs an rclone remote (default "gdrive:") whose root is the Drive folder
# that should hold the data; see README "Google Drive".
cd "${0:A:h}/.." || exit 1
trap 'exit 0' INT TERM
REMOTE=${MEXCMM_REMOTE:-gdrive:}
KEEP_HOURS=${MEXCMM_KEEP_HOURS:-48}
while true; do
  if [[ -d data/raw ]]; then
    # Only *.parquet: files still being written end in .parquet.part.
    nice -n 10 rclone copy data/raw "${REMOTE}ham-veri" --include "*.parquet" \
      --bwlimit 4M --transfers 2 --log-level NOTICE
    # move = verify the remote copy, then delete the local one.
    nice -n 10 rclone move data/raw "${REMOTE}ham-veri" --include "*.parquet" \
      --min-age "${KEEP_HOURS}h" --log-level NOTICE
    [[ -f data/universe.json ]] && rclone copy data/universe.json "${REMOTE}ham-veri" --log-level NOTICE
    echo "$(date '+%F %T') upload pass done; local data: $(du -sh data/raw | cut -f1), free disk: $(df -h . | awk 'NR==2{print $4}')"
  fi
  sleep 900
done
