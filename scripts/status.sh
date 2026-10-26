#!/bin/zsh
# One-screen status: processes, last log lines, newest files, disk.
cd "${0:A:h}/.." || exit 1
for n in mexc hl upload; do
  echo "== $n: $(tail -1 data/logs/$n.log 2>/dev/null)"
done
pgrep -fl "mexcmm|scripts/(run|run_hl|upload).sh" | sed 's/^/   proc /'
echo "== disk free: $(df -h ~ | awk 'NR==2{print $4}'), local data: $(du -sh data/raw | cut -f1)"
