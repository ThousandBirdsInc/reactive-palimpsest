#!/usr/bin/env bash
# Memory profile wrapper for the load suite (docs/LOAD-TESTING.md,
# "Memory profiling").
#
#   crates/palimpsest-soak/memprofile.sh <label> <outdir> -- <command...>
#
# Runs <command>, samples VmRSS / VmHWM / VmData / thread count from
# /proc every 250 ms into <outdir>/<label>.csv, captures the command's
# output in <outdir>/<label>.out, and prints a one-line summary. Pair
# it with the suite's own `rss_*` report fields: the CSV shows the
# shape over time (ramp, plateau, leak), the report gives the
# before/after-subscribe/peak points.
#
# Example:
#   cargo build --release -p palimpsest-soak --bin palimpsest-loadsuite
#   PALIMPSEST_SUITE_JSON=1 PALIMPSEST_SUITE_SUBSCRIBERS=4000 \
#     crates/palimpsest-soak/memprofile.sh steady-4k /tmp/memprof -- \
#     target/release/palimpsest-loadsuite steady-state
set -euo pipefail
label="$1"; out="$2"; shift 2; [ "$1" = "--" ] && shift
mkdir -p "$out"
csv="$out/$label.csv"; log="$out/$label.out"
echo "t_ms,vmrss_kib,vmhwm_kib,vmdata_kib,threads" > "$csv"
start=$(date +%s%N)
"$@" > "$log" 2>&1 &
pid=$!
while kill -0 "$pid" 2>/dev/null; do
  if [ -r /proc/$pid/status ]; then
    awk -v t=$(( ($(date +%s%N) - start) / 1000000 )) '
      /^VmRSS:/ {rss=$2} /^VmHWM:/ {hwm=$2} /^VmData:/ {data=$2} /^Threads:/ {thr=$2}
      END { if (rss != "") printf "%d,%d,%d,%d,%d\n", t, rss, hwm, data, thr }' /proc/$pid/status >> "$csv" 2>/dev/null || true
  fi
  sleep 0.25
done
wait "$pid" || echo "command exited non-zero" >> "$log"
end=$(date +%s%N)
python3 - "$csv" "$label" $(( (end - start) / 1000000 )) <<'PY'
import csv, sys
rows = list(csv.DictReader(open(sys.argv[1])))
if not rows:
    print(f"{sys.argv[2]}: no samples"); sys.exit()
rss = [int(r["vmrss_kib"]) for r in rows]
hwm = max(int(r["vmhwm_kib"]) for r in rows)
print(f"{sys.argv[2]}: wall={int(sys.argv[3])/1000:.1f}s samples={len(rows)} rss_first={rss[0]//1024}MiB rss_peak={max(rss)//1024}MiB vmhwm={hwm//1024}MiB rss_last={rss[-1]//1024}MiB threads_max={max(int(r['threads']) for r in rows)}")
PY
