#!/usr/bin/env bash

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="${TMPDIR:-/tmp}/feedsystem-zero-run"
BIN_DIR="${RUN_DIR}/bin"
LOG_DIR="${RUN_DIR}/logs"
PID_DIR="${RUN_DIR}/pids"

ACTION="start"
RESET_SEED=false
BUILD_BINARIES=true
WITH_DEPS=false
START_DEPS=true

SEED_USERS="${SEED_USERS:-10000}"
SEED_VIDEOS="${SEED_VIDEOS:-5000}"
SEED_FILE_BUCKETS="${SEED_FILE_BUCKETS:-100}"

SERVICE_NAMES=(
  account video interaction social feed notification-rpc gateway
  outbox interaction-sync social-sync feed-timeline hotrank notification-job asset-cleanup event-cleanup
)

usage() {
  cat <<'EOF'
用法:
  ./scripts/start_all.sh [start] [--seed] [--no-build] [--no-deps]
  ./scripts/start_all.sh restart [--seed] [--no-build] [--no-deps]
  ./scripts/start_all.sh stop [--with-deps]
  ./scripts/start_all.sh status

选项:
  --seed       删除并重建压测数据，默认 10000 用户、5000 视频、100 组文件资产。
  --no-build   复用 /tmp/feedsystem-zero-run/bin 中已编译的二进制。
  --no-deps    复用已运行的 MySQL、Redis、etcd、ZooKeeper、Kafka，不执行 Docker/Sudo。
  --with-deps  stop 时同时停止 Docker Compose 依赖。

可通过环境变量覆盖造数规模:
  SEED_USERS=20000 SEED_VIDEOS=8000 SEED_FILE_BUCKETS=200 \
    ./scripts/start_all.sh --seed
EOF
}

for arg in "$@"; do
  case "$arg" in
    start|restart|stop|status)
      ACTION="$arg"
      ;;
    --seed)
      RESET_SEED=true
      ;;
    --no-build)
      BUILD_BINARIES=false
      ;;
    --no-deps)
      START_DEPS=false
      ;;
    --with-deps)
      WITH_DEPS=true
      ;;
    -h|--help|help)
      usage
      exit 0
      ;;
    *)
      echo "未知参数: $arg" >&2
      usage >&2
      exit 2
      ;;
  esac
done

mkdir -p "$BIN_DIR" "$LOG_DIR" "$PID_DIR"
cd "$ROOT_DIR"

log() {
  printf '[start_all] %s\n' "$*"
}

compose_command() {
  if command -v docker-compose >/dev/null 2>&1; then
    COMPOSE=(sudo docker-compose)
    return
  fi
  if docker compose version >/dev/null 2>&1; then
    COMPOSE=(sudo docker compose)
    return
  fi
  echo "未找到 docker-compose 或 docker compose" >&2
  exit 1
}

stop_backend() {
  log "停止已有后端进程"

  local name pid_file pid
  for name in "${SERVICE_NAMES[@]}"; do
    pid_file="${PID_DIR}/${name}.pid"
    if [[ ! -f "$pid_file" ]]; then
      continue
    fi
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
      kill -TERM "$pid" 2>/dev/null || true
    fi
  done

  # 兼容此前通过 go run 或手动终端启动的进程。所有项目进程都会携带 etc yaml 参数。
  pkill -TERM -f 'apps/.*/etc/.*\.yaml' 2>/dev/null || true
  sleep 2

  local deadline=$((SECONDS + 10))
  while (( SECONDS < deadline )); do
    local alive=false
    for name in "${SERVICE_NAMES[@]}"; do
      pid_file="${PID_DIR}/${name}.pid"
      [[ -f "$pid_file" ]] || continue
      pid="$(cat "$pid_file" 2>/dev/null || true)"
      if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
        alive=true
        break
      fi
    done
    [[ "$alive" == false ]] && break
    sleep 1
  done

  for name in "${SERVICE_NAMES[@]}"; do
    pid_file="${PID_DIR}/${name}.pid"
    [[ -f "$pid_file" ]] || continue
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
      kill -KILL "$pid" 2>/dev/null || true
    fi
    rm -f "$pid_file"
  done
}

wait_port() {
  local name="$1"
  local port="$2"
  local timeout="$3"
  local deadline=$((SECONDS + timeout))

  while (( SECONDS < deadline )); do
    if (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; then
      exec 3>&- 3<&-
      log "${name} 已就绪: 127.0.0.1:${port}"
      return 0
    fi
    sleep 1
  done

  echo "等待 ${name} 端口 ${port} 超时" >&2
  return 1
}

start_process() {
  local name="$1"
  local binary="$2"
  local config="$3"
  local log_file="${LOG_DIR}/${name}.log"
  local pid_file="${PID_DIR}/${name}.pid"

  : >"$log_file"
  nohup "$binary" -f "$config" >>"$log_file" 2>&1 &
  local pid=$!
  echo "$pid" >"$pid_file"

  sleep 1
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "${name} 启动失败，最近日志:" >&2
    tail -n 30 "$log_file" >&2 || true
    return 1
  fi
  log "已启动 ${name}, pid=${pid}, log=${log_file}"
}

assert_all_running() {
  local name pid_file pid failed=false
  for name in "${SERVICE_NAMES[@]}"; do
    pid_file="${PID_DIR}/${name}.pid"
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [[ ! "$pid" =~ ^[0-9]+$ ]] || ! kill -0 "$pid" 2>/dev/null; then
      echo "服务 ${name} 未运行，日志: ${LOG_DIR}/${name}.log" >&2
      tail -n 20 "${LOG_DIR}/${name}.log" >&2 || true
      failed=true
    fi
  done
  [[ "$failed" == false ]]
}

build_binary() {
  local name="$1"
  local package="$2"
  log "编译 ${name}"
  go build -o "${BIN_DIR}/${name}" "$package"
}

build_all() {
  if [[ "$BUILD_BINARIES" == false ]]; then
    local name
    for name in "${SERVICE_NAMES[@]}"; do
      if [[ ! -x "${BIN_DIR}/${name}" ]]; then
        echo "缺少二进制 ${BIN_DIR}/${name}，请去掉 --no-build" >&2
        exit 1
      fi
    done
    return
  fi

  build_binary account ./apps/account
  build_binary video ./apps/video
  build_binary interaction ./apps/interaction
  build_binary social ./apps/social
  build_binary feed ./apps/feed
  build_binary notification-rpc ./apps/notification
  build_binary gateway ./apps/gateway
  build_binary outbox ./apps/job/outbox
  build_binary interaction-sync ./apps/job/interaction_sync
  build_binary social-sync ./apps/job/social_sync
  build_binary feed-timeline ./apps/job/feed_timeline
  build_binary hotrank ./apps/job/hotrank
  build_binary notification-job ./apps/job/notification
  build_binary asset-cleanup ./apps/job/asset_cleanup
  build_binary event-cleanup ./apps/job/event_cleanup
}

start_dependencies() {
  compose_command
  log "验证 sudo 权限"
  sudo -v

  log "启动 MySQL、Redis、etcd、ZooKeeper、Kafka"
  "${COMPOSE[@]}" -f deploy/docker-compose.yml up -d

  wait_port mysql 3308 120
  wait_port redis 6380 60
  wait_port etcd 23790 60
  wait_port kafka 9094 180

  log "创建 Kafka Topics"
  sudo bash deploy/kafka/create_topics.sh >/dev/null
}

seed_load_test_data() {
  [[ "$RESET_SEED" == true ]] || return 0
  log "重建压测数据: users=${SEED_USERS}, videos=${SEED_VIDEOS}, file_buckets=${SEED_FILE_BUCKETS}"
  go run ./tests/cmd/seed \
    -reset \
    -reset-redis \
    -users "$SEED_USERS" \
    -videos "$SEED_VIDEOS" \
    -file-buckets "$SEED_FILE_BUCKETS" \
    -upload-dir "$ROOT_DIR/uploads"
}

start_backend() {
  build_all

  start_process account "${BIN_DIR}/account" "${ROOT_DIR}/apps/account/etc/account.yaml"
  start_process video "${BIN_DIR}/video" "${ROOT_DIR}/apps/video/etc/video.yaml"
  start_process interaction "${BIN_DIR}/interaction" "${ROOT_DIR}/apps/interaction/etc/interaction.yaml"
  start_process social "${BIN_DIR}/social" "${ROOT_DIR}/apps/social/etc/social.yaml"
  start_process feed "${BIN_DIR}/feed" "${ROOT_DIR}/apps/feed/etc/feed.yaml"
  start_process notification-rpc "${BIN_DIR}/notification-rpc" "${ROOT_DIR}/apps/notification/etc/notification.yaml"

  wait_port account 9001 60
  wait_port video 9002 60
  wait_port interaction 9003 60
  wait_port social 9004 60
  wait_port feed 9005 60
  wait_port notification 9006 60

  start_process gateway "${BIN_DIR}/gateway" "${ROOT_DIR}/apps/gateway/etc/gateway.yaml"
  wait_port gateway 8888 60

  start_process outbox "${BIN_DIR}/outbox" "${ROOT_DIR}/apps/job/outbox/etc/outbox.yaml"
  start_process interaction-sync "${BIN_DIR}/interaction-sync" "${ROOT_DIR}/apps/job/interaction_sync/etc/interaction_sync.yaml"
  start_process social-sync "${BIN_DIR}/social-sync" "${ROOT_DIR}/apps/job/social_sync/etc/social_sync.yaml"
  start_process feed-timeline "${BIN_DIR}/feed-timeline" "${ROOT_DIR}/apps/job/feed_timeline/etc/feed_timeline.yaml"
  start_process hotrank "${BIN_DIR}/hotrank" "${ROOT_DIR}/apps/job/hotrank/etc/hotrank.yaml"
  start_process notification-job "${BIN_DIR}/notification-job" "${ROOT_DIR}/apps/job/notification/etc/notification.yaml"
  start_process asset-cleanup "${BIN_DIR}/asset-cleanup" "${ROOT_DIR}/apps/job/asset_cleanup/etc/asset_cleanup.yaml"
  start_process event-cleanup "${BIN_DIR}/event-cleanup" "${ROOT_DIR}/apps/job/event_cleanup/etc/event_cleanup.yaml"

  sleep 3
  assert_all_running
}

print_status() {
  local name pid_file pid state
  printf '\n%-20s %-9s %s\n' "SERVICE" "STATE" "PID"
  printf '%-20s %-9s %s\n' "--------------------" "---------" "------"
  for name in "${SERVICE_NAMES[@]}"; do
    pid_file="${PID_DIR}/${name}.pid"
    pid=""
    state="stopped"
    if [[ -f "$pid_file" ]]; then
      pid="$(cat "$pid_file" 2>/dev/null || true)"
      if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
        state="running"
      fi
    fi
    printf '%-20s %-9s %s\n' "$name" "$state" "${pid:--}"
  done
  printf '\n日志目录: %s\n' "$LOG_DIR"
}

case "$ACTION" in
  stop)
    stop_backend
    if [[ "$WITH_DEPS" == true ]]; then
      compose_command
      sudo -v
      "${COMPOSE[@]}" -f deploy/docker-compose.yml down
    fi
    print_status
    ;;
  status)
    print_status
    ;;
  start|restart)
    stop_backend
    if [[ "$START_DEPS" == true ]]; then
      start_dependencies
    else
      log "复用已运行的基础依赖，跳过 Docker Compose"
    fi
    seed_load_test_data
    start_backend
    print_status
    log "全部后端服务已启动，可开始压测"
    ;;
esac
