import { useEffect, useMemo, useState } from 'react'
import { MARKERS, USA_PATH, CARIBBEAN, project, type Marker } from './markers'
import JaLogo from './JaLogo'

// USA + Caribbean radar map for the auth pages.
// All animation lives in auth.css; this component is just structure +
// percentage-positioned pin overlay.

export default function RadarMap() {
  // Live "pipeline" counter — purely cosmetic.
  const [tick, setTick] = useState(1247)
  useEffect(() => {
    const id = setInterval(
      () => setTick((t) => t + Math.floor(Math.random() * 3) + 1),
      1800
    )
    return () => clearInterval(id)
  }, [])

  // Hot pins render on top of warm pins on top of cool pins.
  const ordered = useMemo<Marker[]>(() => {
    const order = { cool: 0, warm: 1, hot: 2 } as const
    return [...MARKERS].sort((a, b) => order[a.tier] - order[b.tier])
  }, [])

  const lats = [40, 30, 20]
  const lngs = [-120, -110, -100, -90, -80, -70]

  return (
    <div className="map-stage">
      {/* SVG: landmasses + graticule */}
      <svg
        className="map-svg"
        viewBox="0 0 100 88"
        preserveAspectRatio="xMidYMid meet"
      >
        <defs>
          <radialGradient id="mapVignette" cx="56%" cy="56%" r="65%">
            <stop offset="0%"   stopColor="rgba(7,16,30,0)" />
            <stop offset="85%"  stopColor="rgba(7,16,30,0)" />
            <stop offset="100%" stopColor="rgba(7,16,30,0.7)" />
          </radialGradient>
        </defs>

        <g className="graticule">
          {lats.map((L) => {
            const [, y] = project(0, L)
            return <line key={`la${L}`} x1="0" y1={y} x2="100" y2={y} />
          })}
          {lngs.map((L) => {
            const [x] = project(L, 0)
            return <line key={`lo${L}`} x1={x} y1="0" x2={x} y2="88" />
          })}
        </g>

        <path className="land-fill" d={USA_PATH} />

        {CARIBBEAN.map((c, i) => (
          <ellipse
            key={i}
            className="land-fill"
            cx={c.cx}
            cy={c.cy}
            rx={c.rx}
            ry={c.ry}
            transform={c.rot ? `rotate(${c.rot} ${c.cx} ${c.cy})` : undefined}
          />
        ))}

        <rect width="100" height="88" fill="url(#mapVignette)" />
      </svg>

      {/* Radar rings + sweep */}
      <div className="radar-rings">
        <div className="radar-ring" />
        <div className="radar-ring r2" />
        <div className="radar-ring r3" />
        <div className="radar-ring r4" />
        <div className="radar-ring radar-crosshair" />
        <div className="radar-sweep" />
      </div>

      {/* City pins (HTML overlay so we can animate labels per-pin) */}
      <div className="pin-layer">
        {ordered.map((c, i) => {
          const [x, y] = project(c.lng, c.lat)
          const pingDelay = (i * 0.41) % 6
          return (
            <div
              key={c.city}
              className="pin"
              style={{ left: `${x}%`, top: `${y}%` }}
            >
              <div className={`pin-dot ${c.tier}`}>
                {c.tier !== 'cool' && (
                  <span
                    className={`pin-ping ${c.tier === 'hot' ? 'hot' : ''}`}
                    style={{ animationDelay: `${pingDelay}s` }}
                  />
                )}
              </div>
              {c.lead && (
                <div
                  className={`pin-label ${c.pos ?? 'up'}`}
                  style={{ animationDelay: `${c.delay ?? 0}s` }}
                >
                  <span className="lead">{c.lead}</span>
                  {c.note && <span className="sub">{c.note}</span>}
                </div>
              )}
            </div>
          )
        })}
      </div>

      {/* J.A. Uniforms hallmark (top-left) */}
      <div className="ja-mark">
        <JaLogo size="lg" />
      </div>

      {/* Corner ornaments (TL omitted to make room for the JA mark) */}
      <span className="ornament-corner tr" />
      <span className="ornament-corner bl" />
      <span className="ornament-corner br" />

      {/* Top-right live status */}
      <div className="status-tag">
        <span className="dot" />
        <span>Live · USA + Caribbean</span>
      </div>

      {/* Bottom-left intel callout */}
      <div className="intel">
        <div className="intel-eyebrow">Pipeline</div>
        <div className="num">{tick.toLocaleString()}</div>
        <hr />
        <div className="intel-foot">Active leads · 147 zones</div>
      </div>

      {/* Bottom-right product brand */}
      <div className="brand-block">
        <div className="ai">AI-Powered</div>
        <div className="word">Lead Generator</div>
        <div className="desc">Hotel Intelligence</div>
      </div>
    </div>
  )
}
