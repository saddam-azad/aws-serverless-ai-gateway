#!/usr/bin/env bash
# Runs one k6 scenario and saves a JSON summary plus an HTML report with charts.
#
#   ./run.sh baseline
#   ./run.sh load --load-max 200
#   ./run.sh baseline --model model-a      # real Bedrock (Claude Haiku 4.5)
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage: ./run.sh <scenario> [options]

scenarios: baseline | load | burst | streaming | quota | policy

options:
  --model <alias>      Use a real model alias instead of the tenant default
  --load-max <n>       Peak users for the `load` scenario (default 500)
  --burst-max <n>      Peak users for the `burst` scenario (default 500)
  --vus <n>            Override the user count for `streaming` and `quota`
  -h, --help           Show this help
EOF
}

SCENARIO=""
MODEL=""
LOAD_MAX=500
BURST_MAX=500
VUS=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    baseline | load | burst | streaming | quota | policy) SCENARIO="$1"; shift ;;
    --model) MODEL="${2:-}"; shift 2 ;;
    --load-max) LOAD_MAX="${2:-}"; shift 2 ;;
    --burst-max) BURST_MAX="${2:-}"; shift 2 ;;
    --vus) VUS="${2:-}"; shift 2 ;;
    -h | --help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ -z "$SCENARIO" ]]; then
  echo "a scenario is required" >&2
  usage
  exit 2
fi

if ! command -v k6 >/dev/null 2>&1; then
  echo "k6 is not installed. See https://grafana.com/docs/k6/latest/set-up/install-k6/" >&2
  exit 1
fi

cd "$(dirname "$0")"

if [[ ! -f config.json ]]; then
  echo "missing loadtest/config.json; run: node scripts/setup.ts" >&2
  exit 1
fi

mkdir -p results
stamp="$(date +%Y%m%d-%H%M%S)"
if [[ -n "$MODEL" ]]; then
  name="$SCENARIO-$MODEL-$stamp"
else
  name="$SCENARIO-$stamp"
fi

export K6_WEB_DASHBOARD=true
export K6_WEB_DASHBOARD_EXPORT="results/$name.html"
# An empty VUS lets the scenario fall back to its own default.
k6 run \
  -e SCENARIO="$SCENARIO" \
  -e MODEL="$MODEL" \
  -e LOAD_MAX="$LOAD_MAX" \
  -e BURST_MAX="$BURST_MAX" \
  -e VUS="$VUS" \
  --summary-export "results/$name.json" \
  gateway.js

echo
echo "Report: loadtest/results/$name.html"
