#!/bin/zsh
# Keep the recorder running: restart it after a crash or network loss and
# keep the Mac from idle-sleeping while it records. Ctrl-C stops it cleanly.
# A closed lid still sleeps the Mac; leave it open and on power.
cd "${0:A:h}/.." || exit 1
trap 'exit 0' INT TERM
mkdir -p data
while true; do
  caffeinate -i .venv/bin/python -m mexcmm.recorder --data data "$@"
  code=$?
  [[ $code -eq 0 ]] && exit 0
  echo "$(date '+%F %T') recorder exited with code $code; restarting in 10 s"
  sleep 10
done
