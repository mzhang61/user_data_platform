export type HttpResult<T> = { ok: true; data: T } | { ok: false; error: string; status?: number; body?: string }

async function readTextSafe(r: Response): Promise<string> {
  try { return await r.text() } catch { return '' }
}

export async function getJson<T>(url: string): Promise<HttpResult<T>> {
  const r = await fetch(url, { method: 'GET' })
  const text = await readTextSafe(r)
  if (!r.ok) return { ok: false, error: `HTTP ${r.status}`, status: r.status, body: text }
  try {
    const data = JSON.parse(text) as T
    return { ok: true, data }
  } catch {
    // some endpoints might already return JSON without us parsing; but fetch gives text.
    return { ok: false, error: 'Response is not JSON', status: r.status, body: text }
  }
}

export async function postJson<T>(url: string, payload: unknown): Promise<HttpResult<T>> {
  const r = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
  const text = await readTextSafe(r)
  if (!r.ok) return { ok: false, error: `HTTP ${r.status}`, status: r.status, body: text }
  try {
    const data = JSON.parse(text) as T
    return { ok: true, data }
  } catch {
    // collector might return empty or plain text
    return { ok: true, data: (text as unknown as T) }
  }
}

// Some of your backends might be mounted at /api/... (older version) or at root (newer version).
// This helper tries both, so the UI still works.
export async function tryGetJson<T>(pathNoApi: string): Promise<HttpResult<T>> {
  const a = await getJson<T>(`/query${pathNoApi}`)
  if (a.ok) return a
  if (a.status === 404) {
    const b = await getJson<T>(`/query/api${pathNoApi}`)
    return b
  }
  return a
}

export async function tryPostCollector<T>(pathNoApi: string, payload: unknown): Promise<HttpResult<T>> {
  const a = await postJson<T>(`/collector${pathNoApi}`, payload)
  if (a.ok) return a
  if (a.status === 404) {
    const b = await postJson<T>(`/collector/api${pathNoApi}`, payload)
    return b
  }
  return a
}
