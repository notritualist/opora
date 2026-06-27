#!/bin/bash
# Start Qwen3.5-9b at llama-server

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR/.."

SERVER_BIN="$PROJECT_ROOT/llama.cpp/build/bin/llama-server"
MODEL_PATH="$PROJECT_ROOT/models/qwen3_5/Qwen3.5-9B-Q4_K_M.gguf"

# Check
if [[ ! -f "$SERVER_BIN" ]]; then
  echo "❌ Server not found: $SERVER_BIN" >&2
  exit 1
fi

if [[ ! -f "$MODEL_PATH" ]]; then
  echo "❌ Model not found: $MODEL_PATH" >&2
  exit 1
fi

echo "🚀 Launching the LLM model orchestrator server..."
echo "   Model: $MODEL_PATH"
echo "   Server: $SERVER_BIN"

exec "$SERVER_BIN" \
  -m "$MODEL_PATH" \
  --ctx-size 262144 \
  --n-gpu-layers -1 \
  --cache-type-k q8_0 \
  --cache-type-v q8_0 \
  --jinja \
  --flash-attn on \
  --threads 4 \
  --batch-size 2048 \
  --parallel 1 \
  --port 8081 \
  --host main-srv