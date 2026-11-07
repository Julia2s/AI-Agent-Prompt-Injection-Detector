#!/usr/bin/env bash
set -euo pipefail

NAME=${1:?usage: $0 NAME bounded|unbounded WORKERS [DURATION] [SCALE_UP_AT]}
MODE=${2:?mode: bounded|unbounded}
WORKERS=${3:?workers}
DURATION=${4:-60}
SCALE_AT=${5:-0}
SCALE_TO=${SCALE_TO:-2}
DRAIN=${DRAIN:-20}
DB_USER=${DB_USER:-user}
DB_NAME=${DB_NAME:-detector_db}
MGMT=${MGMT:-http://localhost:15672}

cd "$(dirname "$0")/.."
OUT=runs/$NAME
mkdir -p "$OUT"
QUEUE="exp_${NAME//[^A-Za-z0-9_]/_}_$(date +%s)"

case "$MODE" in
  bounded)
    export QUEUE_CAPACITY=${QUEUE_CAPACITY:-50}
    export WORKER_CONCURRENCY=${WORKER_CONCURRENCY:-1}
    export PREFETCH_COUNT=$WORKER_CONCURRENCY
    SOURCE_MODE=backpressure ;;
  unbounded)
    export QUEUE_CAPACITY=0 WORKER_CONCURRENCY=${WORKER_CONCURRENCY:-1} PREFETCH_COUNT=0
    SOURCE_MODE=naive ;;
  *) echo "unknown mode: $MODE" >&2; exit 2 ;;
esac
export QUEUE_NAME=$QUEUE INFERENCE_DELAY=${INFERENCE_DELAY:-0.5}

echo ">> $NAME: mode=$MODE workers=$WORKERS duration=${DURATION}s queue=$QUEUE" \
     "capacity=$QUEUE_CAPACITY prefetch=$PREFETCH_COUNT"

wait_workers() {
  local want=$1 healthy=0
  for _ in $(seq 1 90); do
    healthy=$(docker compose ps -q worker | xargs -r docker inspect \
      -f '{{.State.Health.Status}}' | grep -c '^healthy$' || true)
    [ "$healthy" -ge "$want" ] && return 0
    sleep 2
  done
  echo "workers are not healthy (got $healthy of $want)" >&2
  return 1
}

docker compose up -d --no-deps --force-recreate --scale worker="$WORKERS" worker
wait_workers "$WORKERS"

T0=$(docker compose exec -T db psql -U "$DB_USER" -d "$DB_NAME" -tAc \
  "select to_char(now() at time zone 'utc','YYYY-MM-DD HH24:MI:SS')" | tr -d '\r')

python3 scripts/monitor.py --queue "$QUEUE" --out "$OUT/monitor.csv" \
  --duration $((DURATION + DRAIN)) --match worker --match producer --match rabbitmq &
MONITOR=$!

if [ "$SCALE_AT" -gt 0 ]; then
  ( sleep "$SCALE_AT"
    echo ">> t=${SCALE_AT}s: scaling workers to $SCALE_TO (no code change)"
    docker compose up -d --no-deps --no-recreate --scale worker="$SCALE_TO" worker ) &
fi

docker compose --profile load run --rm --no-deps -T -e LOG_LEVEL=INFO producer \
  python -m mlops.producer --batch-id "$NAME" --repeat 100000 \
  --mode "$SOURCE_MODE" --duration "$DURATION" --pad-bytes "${PAD_BYTES:-0}" \
  2>&1 | tee "$OUT/producer.log" >/dev/null || true

python3 -m scripts.extract_source_metrics "$OUT/producer.log" "$OUT/source.csv"

wait "$MONITOR" || true
wait || true

docker compose exec -T db psql -U "$DB_USER" -d "$DB_NAME" -c \
  "\copy (select finished_at, inference_ms, latency_ms from task_results where finished_at >= '$T0' order by finished_at) to stdout with csv header" \
  | tr -d '\r' > "$OUT/latency.csv"

docker compose kill worker >/dev/null
docker compose rm -f worker >/dev/null
for q in "$QUEUE" "$QUEUE.dlq"; do
  curl -s -u guest:guest -X DELETE "$MGMT/api/queues/%2F/$q" >/dev/null || true
done
echo ">> done: $OUT  ($(($(wc -l < "$OUT/latency.csv") - 1)) finished tasks)"
