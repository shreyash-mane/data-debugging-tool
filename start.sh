#!/usr/bin/env bash
# start.sh — Start backend and frontend in one command (development)
# Usage: ./start.sh

set -e

ROOT="$(cd "$(dirname "$0")" && pwd)"

echo ""
echo "╔══════════════════════════════════════╗"
echo "║       Data Debugging Tool            ║"
echo "╚══════════════════════════════════════╝"
echo ""

# ── Backend ──────────────────────────────────────────────────────────────────
BACKEND="$ROOT/backend"

if [ ! -d "$BACKEND/venv" ]; then
  echo "→ Creating Python virtual environment..."
  python3 -m venv "$BACKEND/venv"
fi

echo "→ Installing backend dependencies..."
"$BACKEND/venv/bin/pip" install -q -r "$BACKEND/requirements.txt"

echo "→ Starting FastAPI backend on http://localhost:8000"
cd "$BACKEND"
"$BACKEND/venv/bin/uvicorn" main:app --reload --port 8000 &
BACKEND_PID=$!

# ── Frontend ─────────────────────────────────────────────────────────────────
FRONTEND="$ROOT/frontend"

echo "→ Installing frontend dependencies..."
cd "$FRONTEND"
npm install --silent

echo "→ Starting Vite dev server on http://localhost:5173"
npm run dev &
FRONTEND_PID=$!

# ── Wait / cleanup ────────────────────────────────────────────────────────────
echo ""
echo "✓ Both servers started."
echo "  Frontend : http://localhost:5173"
echo "  Backend  : http://localhost:8000"
echo "  API docs : http://localhost:8000/docs"
echo ""
echo "Press Ctrl+C to stop both servers."
echo ""

trap "echo ''; echo 'Stopping...'; kill $BACKEND_PID $FRONTEND_PID 2>/dev/null; exit 0" INT TERM

wait $BACKEND_PID $FRONTEND_PID
