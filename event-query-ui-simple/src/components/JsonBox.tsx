import React from 'react'

type Props = { title: string; value: unknown; error?: string }

export default function JsonBox({ title, value, error }: Props) {
  return (
    <div>
      <div className="kv" style={{ justifyContent: 'space-between', alignItems: 'center' }}>
        <small><b>{title}</b></small>
        {error ? <span className="badge bad">ERROR</span> : <span className="badge ok">OK</span>}
      </div>
      <pre>{error ? String(error) + (value ? `\n\n${String(value)}` : '') : JSON.stringify(value, null, 2)}</pre>
    </div>
  )
}
