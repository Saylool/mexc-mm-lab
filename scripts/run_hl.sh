#!/bin/zsh
# Keep the Hyperliquid recorder running (same pattern as run.sh).
cd "${0:A:h}/.." || exit 1
trap 'exit 0' INT TERM
mkdir -p data
while true; do
  caffeinate -i .venv/bin/python -m mexcmm.hl_recorder --data data "$@"
  code=$?
  [[ $code -eq 0 ]] && exit 0
  echo "$(date '+%F %T') hl recorder exited with code $code; restarting in 10 s"
  sleep 10
done
