# Event Query UI (simple)

This UI is designed for your project:
- **event-collector** (default http://localhost:18081)
- **event-query-service** (default http://localhost:18080)

It uses **Vite proxy** so the browser never hits CORS issues.

## Run

```bash
cd /Users/mmm/Documents/user_data_platform/event-query-ui
npm i
npm run dev
```

Open:
- http://localhost:5173

## How to test (UI-only, no CLI)

1. Make sure these are running:
   - infra (kafka + redis)
   - stream-processor
   - event-collector (18081)
   - event-query-service (18080)
2. In the UI, click **Send VALID sample**.
3. Then click **Fetch event by ID** and **Fetch user events**.
4. Click **Send INVALID sample** and then **Fetch DLQ**.

## Change ports

If your ports are different, run dev server like this:

```bash
VITE_QUERY_API_BASE=http://localhost:18080 VITE_COLLECTOR_API_BASE=http://localhost:18081 npm run dev
```
