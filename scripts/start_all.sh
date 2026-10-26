#!/bin/zsh
# Start the recorders and the uploader detached from the terminal that runs
# this script, so closing or force-quitting the app that owns the terminal
# does not stop them. Logs go to data/logs/. Safe to run twice: a process
# that is already running is left alone.
cd "${0:A:h}/.." || exit 1
mkdir -p data/logs data/broken
start() {
  local name=$1 script=$2
  if pgrep -f "scripts/$script" >/dev/null; then
    echo "$name already running"
    return
  fi
  nohup "scripts/$script" >> "data/logs/$name.log" 2>&1 < /dev/null &
  disown
  echo "$name started (pid $!)"
}
# Unfinished files from a hard stop cannot be read; keep them out of uploads.
if ! pgrep -f "mexcmm.recorder|mexcmm.hl_recorder" >/dev/null; then
  find data/raw -name "*.part" -exec mv {} data/broken/ \; 2>/dev/null
fi
start mexc run.sh
sleep 3  # distinct run ids per recorder
start hl run_hl.sh
start upload upload.sh
