# Data Debugging Tool

A visual, step-by-step data pipeline debugger. Upload a CSV, build a pipeline of transformation steps, run it, and inspect what happens to your data at each stage — with automatic anomaly detection and root cause explanations.

---

## Features

| Feature | Detail |
|---|---|
| Dataset upload | CSV upload with schema inference and preview |
| Pipeline builder | 11 transformation step types with a form UI |
| Step-by-step execution | Snapshots saved after every step |
| Diff engine | Row/col/null/type/stat/distribution changes between steps |
| Anomaly detection | 9 heuristic anomaly types with configurable thresholds |
| Root cause explanations | Human-readable cause + fix for each anomaly |
| Visual debugger | Step sidebar, charts, data table, diff viewer, explanation panel |
| Export | Download full anomaly report as JSON |
| Run history | Every run is stored; re-run to see changes |

---

## Supported Transformation Steps

1. **Drop Missing** — remove rows with null values (any/all, subset columns)
2. **Fill Missing** — fill nulls with a constant, mean, median, mode, ffill, bfill
3. **Rename Column** — rename one or more columns
4. **Change Data Type** — cast column to int/float/str/bool/datetime
5. **Filter Rows** — filter by column condition (==, !=, >, contains, isnull, ...)
6. **Select Columns** — keep only specified columns
7. **Sort Values** — sort by one or more columns, asc/desc
8. **Remove Duplicates** — drop duplicate rows, with subset + keep options
9. **Add Computed Column** — create a new column via arithmetic or string concat
10. **Join Dataset** — merge with a second uploaded CSV (inner/left/right/outer)
11. **Group & Aggregate** — groupby + sum/mean/count/min/max per column

> **Note on custom expressions**: Arbitrary pandas/Python expressions are intentionally not supported. Running user-provided code on a server is a significant security risk (code injection). Use the composition of supported steps instead.

---

## Tech Stack

- **Backend**: Python 3.11+, FastAPI, SQLAlchemy 2.0, SQLite, Pandas, SciPy
- **Frontend**: React 18, TypeScript, Tailwind CSS, Recharts, Zustand, React Router
- **Storage**: Local filesystem (uploads/) + SQLite (data_debugger.db)

---

## Setup & Run

### Prerequisites

- Python 3.11+
- Node.js 18+ and npm

### 1. Clone / copy the project

```bash
git clone <repo> data-debugging-tool
cd data-debugging-tool
```

### 2. Backend

```bash
cd backend

# Create a virtual environment
python -m venv venv
source venv/bin/activate       # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Start the server
uvicorn main:app --reload --port 8000
```

The backend will start at **http://localhost:8000**.  
SQLite database and `uploads/` folder are created automatically on first run.

API docs are available at http://localhost:8000/docs

### 3. Frontend

```bash
cd frontend

npm install
npm run dev
```

The frontend will start at **http://localhost:5173**.

---

## Quick Demo Walkthrough

### Scenario A — Sales join shrink + nulls

1. Upload `sample_data/sales_data.csv`
2. Also upload `sample_data/customers.csv` (this will be available for the join step)
3. Create a new pipeline for `sales_data.csv`
4. Add these steps in order:

   | Step | Type | Config |
   |---|---|---|
   | Filter completed | Filter Rows | column=status, operator===, value=completed |
   | Join customers | Join | right=customers.csv, on=customer_id, how=inner |
   | Compute revenue | Add Computed Column | new=revenue, op=multiply, col_a=amount, col_b=quantity |
   | Sort by revenue | Sort Values | columns=[revenue], ascending=false |

5. Click **Run Pipeline**
6. In the debugger, click the **Join customers** step
7. Observe: row count drops significantly (many customer_ids don't exist in customers.csv)
8. The anomaly panel will flag a **large row drop**
9. The explanation will suggest checking key alignment

### Scenario B — HR type conversion + duplicates

1. Upload `sample_data/hr_data.csv`
2. Create a new pipeline
3. Add these steps:

   | Step | Type | Config |
   |---|---|---|
   | Cast salary to float | Change Data Type | column=salary, dtype=float |
   | Remove duplicates | Remove Duplicates | keep=first |
   | Fill null salary | Fill Missing | column=salary, method=mean |
   | Group by department | Group & Aggregate | group_by=[department], aggs={salary: mean, age: mean} |

4. Run and inspect:
   - **Cast salary to float**: rows with "N/A", "NOT_AVAILABLE", "PENDING" become null → anomaly
   - **Remove duplicates**: row count drops (3 duplicates) → info
   - **Group by department**: collapses to one row per department → anomaly (unexpected data loss)

---

## Project Structure

```
data-debugging-tool/
├── backend/
│   ├── main.py                    # FastAPI routes
│   ├── database.py                # SQLAlchemy models + SQLite setup
│   ├── models.py                  # Pydantic request/response schemas
│   ├── requirements.txt
│   ├── uploads/                   # Uploaded CSV files (auto-created)
│   └── services/
│       ├── csv_service.py         # CSV load, schema inference, stats
│       ├── pipeline_service.py    # Pipeline/step CRUD
│       ├── execution_engine.py    # Step execution (11 types)
│       ├── diff_engine.py         # Before/after DataFrame diff
│       ├── anomaly_detector.py    # Heuristic anomaly detection
│       └── explanation_engine.py  # Human-readable root cause
├── frontend/
│   ├── src/
│   │   ├── App.tsx
│   │   ├── api/client.ts          # Typed API calls
│   │   ├── types/index.ts         # TypeScript types
│   │   ├── store/useAppStore.ts   # Zustand state
│   │   └── components/
│   │       ├── Layout.tsx
│   │       ├── UploadPage.tsx
│   │       ├── PipelineBuilder.tsx
│   │       ├── StepEditor.tsx     # Config forms for each step type
│   │       ├── DebuggerPage.tsx   # Main visual debugger
│   │       ├── DataTable.tsx
│   │       ├── DiffViewer.tsx
│   │       ├── AnomalyCards.tsx
│   │       ├── ExplanationPanel.tsx
│   │       └── Charts.tsx         # Recharts visualizations
├── sample_data/
│   ├── sales_data.csv
│   ├── customers.csv
│   └── hr_data.csv
└── README.md
```

---

## Anomaly Detection Thresholds

Default thresholds (configurable in `backend/services/anomaly_detector.py`):

| Anomaly | Default Threshold |
|---|---|
| Row drop → warning | 30% |
| Row drop → critical | 70% |
| Row increase (join explosion) | +50% |
| Null increase | +20% of rows |
| Column mostly null | 80% null |
| Duplicate spike | +50% duplicates |
| Mean drift | 50% shift |
| KS distribution shift | KS stat ≥ 0.3, p ≤ 0.05 |
| Category disappearance | ≥ 3 categories removed |

---

## Docker (Optional)

If you'd like to containerise the app, here's a quick Dockerfile approach:

**Backend Dockerfile** (place in `backend/`):
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

**Frontend Dockerfile** (place in `frontend/`):
```dockerfile
FROM node:20-alpine AS build
WORKDIR /app
COPY package*.json .
RUN npm install
COPY . .
RUN npm run build

FROM nginx:alpine
COPY --from=build /app/dist /usr/share/nginx/html
```

---

## Design Decisions

- **Synchronous execution**: Pipeline runs synchronously in the HTTP request. For large datasets, this could time out. The proper solution is a background task queue (Celery/ARQ), but this is beyond MVP scope.
- **Snapshots in SQLite as JSON**: Sample rows and stats are stored as JSON text in SQLite. For very large datasets, a parquet/arrow store would be more efficient.
- **SciPy KS test**: Used for numeric distribution shift detection. It's sampled at 500 rows per column for performance.
- **No auth**: Intentional for MVP. All data is visible to anyone with server access.
- **No custom expressions**: Security decision. Executing arbitrary user code requires sandboxing.

---

## License

MIT
