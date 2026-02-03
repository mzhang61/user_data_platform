import React, { useEffect, useMemo, useState } from 'react'
import JsonBox from './components/JsonBox'
import { tryGetJson, tryPostCollector } from './lib/http'

type Health = any

type EventPayload = {
  eventId: string
  userId: string
  eventType: string
  eventTime: string
  sessionId: string
  page: string
  device: string
  properties: Record<string, any> | null
}

export default function App() {
  const [queryHealth, setQueryHealth] = useState<any>(null)
  const [collectorHealth, setCollectorHealth] = useState<any>(null)
  const [healthErr, setHealthErr] = useState<string | null>(null)

  const [eventId, setEventId] = useState('evt-ui-1')
  const [userId, setUserId] = useState('u-ui-1')
  const [limit, setLimit] = useState(10)
  const [includeDetails, setIncludeDetails] = useState(true)

  const [lastEvent, setLastEvent] = useState<any>(null)
  const [lastUserEvents, setLastUserEvents] = useState<any>(null)
  const [lastDlq, setLastDlq] = useState<any>(null)
  const [lastErr, setLastErr] = useState<string | null>(null)

  const [seedEventId, setSeedEventId] = useState('evt-ui-1')
  const [seedUserId, setSeedUserId] = useState('u-ui-1')
  const [seedPage, setSeedPage] = useState('/prod')

  const validSample: EventPayload = useMemo(() => ({
    eventId: seedEventId,
    userId: seedUserId,
    eventType: 'page_view',
    eventTime: '2026-01-30T20:00:00Z',
    sessionId: `s-${seedUserId}`,
    page: seedPage,
    device: 'mac',
    properties: { k: 'v' },
  }), [seedEventId, seedUserId, seedPage])

  const invalidSample: EventPayload = useMemo(() => ({
    ...validSample,
    eventId: seedEventId.replace(/\s+/g, '') + '-bad',
    userId: seedUserId.replace(/\s+/g, '') + '-bad',
    eventType: 'click',
    eventTime: 'not-a-time',
    properties: null,
  }), [validSample, seedEventId, seedUserId])

  async function refreshHealth() {
    setHealthErr(null)
    const q = await tryGetJson<Health>('/health')
    const c = await (async () => {
      // collector health path is /health in your collector project (based on your logs)
      // We proxy through /collector.
      const a = await fetch('/collector/health')
      const t = await a.text()
      if (!a.ok) return { ok: false as const, error: `HTTP ${a.status}`, body: t }
      try { return { ok: true as const, data: JSON.parse(t) } } catch { return { ok: true as const, data: t } }
    })()

    if (q.ok) setQueryHealth(q.data)
    else setHealthErr(`Query service health failed: ${q.error}`)

    if (c.ok) setCollectorHealth(c.data)
    else setHealthErr((prev) => (prev ? prev + ` | Collector health failed: ${c.error}` : `Collector health failed: ${c.error}`))
  }

  useEffect(() => {
    refreshHealth()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  async function seedValid() {
    setLastErr(null)
    const r = await tryPostCollector<any>('/events', validSample)
    if (!r.ok) {
      setLastErr(`Send valid sample failed: ${r.error}\n${r.body || ''}`)
      return
    }
    setEventId(seedEventId)
    setUserId(seedUserId)
    await fetchAll(seedEventId, seedUserId)
  }

  async function seedInvalid() {
    setLastErr(null)
    const r = await tryPostCollector<any>('/events', invalidSample)
    if (!r.ok) {
      setLastErr(`Send invalid sample failed: ${r.error}\n${r.body || ''}`)
      return
    }
    await fetchDlq()
  }

  async function fetchEvent() {
    setLastErr(null)
    const r = await tryGetJson<any>(`/events/${encodeURIComponent(eventId)}`)
    if (!r.ok) {
      setLastEvent(null)
      setLastErr(`Get event failed: ${r.error}\n${r.body || ''}`)
      return
    }
    setLastEvent(r.data)
  }

  async function fetchUserEvents() {
    setLastErr(null)
    const qs = new URLSearchParams({
      limit: String(limit),
      includeDetails: String(includeDetails),
    })
    const r = await tryGetJson<any>(`/users/${encodeURIComponent(userId)}/events?${qs.toString()}`)
    if (!r.ok) {
      setLastUserEvents(null)
      setLastErr(`Get user events failed: ${r.error}\n${r.body || ''}`)
      return
    }
    setLastUserEvents(r.data)
  }

  async function fetchDlq() {
    setLastErr(null)
    const qs = new URLSearchParams({ limit: String(limit) })
    const r = await tryGetJson<any>(`/dlq?${qs.toString()}`)
    if (!r.ok) {
      setLastDlq(null)
      setLastErr(`Get DLQ failed: ${r.error}\n${r.body || ''}`)
      return
    }
    setLastDlq(r.data)
  }

  async function fetchAll(eid: string, uid: string) {
    setEventId(eid)
    setUserId(uid)
    await Promise.all([fetchEvent(), fetchUserEvents(), fetchDlq()])
  }

  return (
    <div className="container">
      <div className="h1">Event Query UI (simple)</div>
      <div className="muted">One page. One-click seed + query. Uses Vite proxy, so no CORS issues.</div>

      <div className="btnrow" style={{ marginTop: 12 }}>
        <button className="btn" onClick={refreshHealth}>Refresh status</button>
        <span className={queryHealth ? 'badge ok' : 'badge bad'}>Query API: {queryHealth ? 'UP' : 'DOWN'}</span>
        <span className={collectorHealth ? 'badge ok' : 'badge bad'}>Collector API: {collectorHealth ? 'UP' : 'DOWN'}</span>
        {healthErr ? <span className="badge bad">{healthErr}</span> : null}
      </div>

      <div className="row" style={{ marginTop: 14 }}>
        <div className="card">
          <h2>1) Seed data (no CLI)</h2>
          <label>eventId</label>
          <input value={seedEventId} onChange={(e) => setSeedEventId(e.target.value)} />
          <label>userId</label>
          <input value={seedUserId} onChange={(e) => setSeedUserId(e.target.value)} />
          <label>page</label>
          <input value={seedPage} onChange={(e) => setSeedPage(e.target.value)} />
          <div className="btnrow">
            <button className="btn primary" onClick={seedValid}>Send VALID sample</button>
            <button className="btn" onClick={seedInvalid}>Send INVALID sample (goes to DLQ)</button>
          </div>
          <small>Valid → Kafka raw topic → stream-processor → Redis. Invalid → DLQ list.</small>
          {lastErr ? <pre>{lastErr}</pre> : null}
        </div>

        <div className="card">
          <h2>2) Query</h2>
          <label>eventId</label>
          <input value={eventId} onChange={(e) => setEventId(e.target.value)} />
          <div className="btnrow">
            <button className="btn primary" onClick={fetchEvent}>Fetch event by ID</button>
          </div>

          <hr />

          <label>userId</label>
          <input value={userId} onChange={(e) => setUserId(e.target.value)} />
          <div className="kv">
            <div style={{ flex: 1 }}>
              <label>limit</label>
              <input type="number" value={limit} min={1} max={100} onChange={(e) => setLimit(Number(e.target.value))} />
            </div>
            <div style={{ width: 200 }}>
              <label>includeDetails</label>
              <select value={String(includeDetails)} onChange={(e) => setIncludeDetails(e.target.value === 'true')}>
                <option value="true">true</option>
                <option value="false">false</option>
              </select>
            </div>
          </div>
          <div className="btnrow">
            <button className="btn primary" onClick={fetchUserEvents}>Fetch user events</button>
            <button className="btn" onClick={fetchDlq}>Fetch DLQ</button>
          </div>
          <small>Tip: click “Send VALID sample” once, then click the fetch buttons.</small>
        </div>
      </div>

      <div className="row" style={{ marginTop: 14 }}>
        <div className="card">
          <h2>Result: event</h2>
          <JsonBox title="/events/{eventId}" value={lastEvent} error={lastEvent ? undefined : undefined} />
        </div>
        <div className="card">
          <h2>Result: user events</h2>
          <JsonBox title="/users/{userId}/events" value={lastUserEvents} error={lastUserEvents ? undefined : undefined} />
        </div>
        <div className="card">
          <h2>Result: DLQ</h2>
          <JsonBox title="/dlq" value={lastDlq} error={lastDlq ? undefined : undefined} />
        </div>
      </div>
    </div>
  )
}
