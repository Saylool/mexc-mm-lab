#!/bin/zsh
# Stop the recorders and the uploader cleanly (SIGINT lets them close their files).
pkill -INT -f "scripts/run.sh|scripts/run_hl.sh|scripts/upload.sh"
pkill -INT -f "mexcmm.recorder|mexcmm.hl_recorder"
sleep 3
pgrep -fl "mexcmm|scripts/(run|run_hl|upload).sh" || echo "all stopped"
