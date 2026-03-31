#!/usr/bin/env bash
# start.sh — Start backend and frontend in one command (development)
# Usage: ./start.sh

set -e

ROOT="$(cd "$(dirname "$0")" && pwd)"

echo ""
echo "╔══════════════════════════════════════╗"
echo "║   Data Debugging + Dataset Finder    ║"
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

# ── Dataset Finder Backend ───────────────────────────────────────────────────
FINDER_BACKEND="$ROOT/dataset-finder-backend"

echo "→ Installing Dataset Finder backend dependencies..."
cd "$FINDER_BACKEND"
npm install --silent

echo "→ Starting Dataset Finder backend on http://localhost:3001"
node server.js &
FINDER_PID=$!

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
echo "✓ All servers started."
echo "  Frontend        : http://localhost:5173"
echo "  Data Debugger   : http://localhost:8000 (API docs: /docs)"
echo "  Dataset Finder  : http://localhost:3001"
echo ""
echo "Press Ctrl+C to stop all servers."
echo ""

trap "echo ''; echo 'Stopping...'; kill $BACKEND_PID $FINDER_PID $FRONTEND_PID 2>/dev/null; exit 0" INT TERM

wait $BACKEND_PID $FINDER_PID $FRONTEND_PID
