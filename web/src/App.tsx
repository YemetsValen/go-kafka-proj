import { useEffect, useMemo, useRef, useState } from 'react'
import { MapContainer, TileLayer, Marker, Popup, useMap } from 'react-leaflet'
import L from 'leaflet'
import 'leaflet/dist/leaflet.css'
import './App.css'

// Fix default marker icon paths since Vite's bundler renames them.
import iconUrl from 'leaflet/dist/images/marker-icon.png'
import iconRetinaUrl from 'leaflet/dist/images/marker-icon-2x.png'
import shadowUrl from 'leaflet/dist/images/marker-shadow.png'

L.Icon.Default.mergeOptions({ iconUrl, iconRetinaUrl, shadowUrl })

// ----- domain types (mirror internal/models.Sighting) -----
type Sighting = {
  id: string
  species: string
  location: string
  latitude: number
  longitude: number
  observed_by: string
  observed_at: string
  created_at: string
  updated_at: string
  verified: boolean
  notes: { author: string; text: string; created_at: string }[]
}

type EventEnvelope = {
  type: 'created' | 'updated' | 'note_added' | 'verified' | 'deleted'
  timestamp: string
  sighting: Sighting
}

const DEFAULT_API_KEY = 'dev'

// ----- API helpers -----
async function fetchSightings(): Promise<Sighting[]> {
  const r = await fetch('/sightings')
  if (!r.ok) throw new Error(`GET /sightings failed: ${r.status}`)
  return r.json()
}

async function createSighting(input: Partial<Sighting>, apiKey: string): Promise<Sighting> {
  const r = await fetch('/sightings', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` },
    body: JSON.stringify(input),
  })
  if (!r.ok) {
    const text = await r.text()
    throw new Error(`POST /sightings failed: ${r.status} ${text}`)
  }
  return r.json()
}

// ----- helpers -----
function FlyTo({ lat, lng }: { lat: number; lng: number }) {
  const map = useMap()
  useEffect(() => {
    map.flyTo([lat, lng], Math.max(map.getZoom(), 6), { duration: 0.7 })
  }, [lat, lng, map])
  return null
}

// ----- main component -----
export default function App() {
  const [sightings, setSightings] = useState<Sighting[]>([])
  const [error, setError] = useState<string | null>(null)
  const [streamStatus, setStreamStatus] = useState<'connecting' | 'live' | 'error'>('connecting')
  const [apiKey, setApiKey] = useState<string>(
    () => localStorage.getItem('apiKey') ?? DEFAULT_API_KEY,
  )
  const [form, setForm] = useState({
    species: '',
    location: '',
    latitude: '48.5',
    longitude: '24.5',
    observed_by: '',
  })
  const [highlight, setHighlight] = useState<{ lat: number; lng: number } | null>(null)
  const esRef = useRef<EventSource | null>(null)

  // initial load
  useEffect(() => {
    fetchSightings().then(setSightings).catch((e) => setError(String(e)))
  }, [])

  // SSE live feed
  useEffect(() => {
    const es = new EventSource('/sightings/stream')
    esRef.current = es
    es.onopen = () => setStreamStatus('live')
    es.onerror = () => setStreamStatus('error')
    const handler = (kind: EventEnvelope['type']) => (evt: MessageEvent) => {
      const env = JSON.parse(evt.data) as EventEnvelope
      env.type = kind
      setSightings((prev) => {
        const next = prev.filter((s) => s.id !== env.sighting.id)
        if (kind === 'deleted') return next
        return [env.sighting, ...next]
      })
      if (kind === 'created') {
        setHighlight({ lat: env.sighting.latitude, lng: env.sighting.longitude })
      }
    }
    es.addEventListener('created', handler('created') as EventListener)
    es.addEventListener('updated', handler('updated') as EventListener)
    es.addEventListener('note_added', handler('note_added') as EventListener)
    es.addEventListener('verified', handler('verified') as EventListener)
    es.addEventListener('deleted', handler('deleted') as EventListener)
    return () => es.close()
  }, [])

  function saveApiKey(k: string) {
    setApiKey(k)
    localStorage.setItem('apiKey', k)
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setError(null)
    try {
      await createSighting(
        {
          species: form.species,
          location: form.location,
          latitude: parseFloat(form.latitude),
          longitude: parseFloat(form.longitude),
          observed_by: form.observed_by,
        },
        apiKey,
      )
      setForm((f) => ({ ...f, species: '', location: '', observed_by: '' }))
    } catch (err) {
      setError(String(err))
    }
  }

  const center = useMemo<[number, number]>(() => {
    if (sightings.length > 0) return [sightings[0].latitude, sightings[0].longitude]
    return [48.5, 24.5]
  }, [sightings])

  const streamPill = {
    connecting: { label: 'connecting…', color: '#888' },
    live: { label: 'live', color: '#1f8d3a' },
    error: { label: 'reconnecting…', color: '#b3261e' },
  }[streamStatus]

  return (
    <div className="app">
      <header className="app-header">
        <h1>Wildlife Sightings</h1>
        <span className="pill" style={{ background: streamPill.color }}>
          {streamPill.label}
        </span>
        <input
          className="api-key"
          placeholder="API key"
          value={apiKey}
          onChange={(e) => saveApiKey(e.target.value)}
          aria-label="API key for mutating endpoints"
        />
      </header>

      <main className="app-body">
        <section className="map-pane">
          <MapContainer center={center} zoom={5} scrollWheelZoom>
            <TileLayer
              attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
              url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
            />
            {highlight && <FlyTo lat={highlight.lat} lng={highlight.lng} />}
            {sightings.map((s) => (
              <Marker key={s.id} position={[s.latitude, s.longitude]}>
                <Popup>
                  <strong>{s.species}</strong>
                  {s.location && <> — {s.location}</>}
                  <br />
                  <small>by {s.observed_by}</small>
                  {s.verified && <span className="verified-badge"> verified</span>}
                </Popup>
              </Marker>
            ))}
          </MapContainer>
        </section>

        <aside className="side-pane">
          <form className="create-form" onSubmit={handleSubmit}>
            <h2>Add a sighting</h2>
            {error && <div className="error">{error}</div>}
            <label>
              Species
              <input
                required
                value={form.species}
                onChange={(e) => setForm((f) => ({ ...f, species: e.target.value }))}
                placeholder="Red Fox"
              />
            </label>
            <label>
              Observed by
              <input
                required
                value={form.observed_by}
                onChange={(e) => setForm((f) => ({ ...f, observed_by: e.target.value }))}
                placeholder="rama"
              />
            </label>
            <label>
              Location
              <input
                value={form.location}
                onChange={(e) => setForm((f) => ({ ...f, location: e.target.value }))}
                placeholder="Carpathians"
              />
            </label>
            <div className="row">
              <label>
                Lat
                <input
                  required
                  inputMode="decimal"
                  value={form.latitude}
                  onChange={(e) => setForm((f) => ({ ...f, latitude: e.target.value }))}
                />
              </label>
              <label>
                Lng
                <input
                  required
                  inputMode="decimal"
                  value={form.longitude}
                  onChange={(e) => setForm((f) => ({ ...f, longitude: e.target.value }))}
                />
              </label>
            </div>
            <button type="submit">Save</button>
          </form>

          <h2 className="feed-heading">Live feed</h2>
          <ul className="feed">
            {sightings.length === 0 && <li className="feed-empty">No sightings yet — add one!</li>}
            {sightings.slice(0, 30).map((s) => (
              <li key={s.id} className="feed-item">
                <div className="feed-row">
                  <strong>{s.species}</strong>
                  {s.verified && <span className="verified-badge">verified</span>}
                </div>
                <small>
                  by {s.observed_by} {s.location && <>· {s.location}</>}
                </small>
              </li>
            ))}
          </ul>
        </aside>
      </main>
    </div>
  )
}
