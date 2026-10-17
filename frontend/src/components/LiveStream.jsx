export default function LiveStream({ stream, fps, totalEvents }) {
  return (
    <div className="main">
      <div className="card">
        <div className="card-title" style={{ justifyContent:"space-between" }}>
          <span>⚡ Real Kafka Topic: clickstream</span>
          <div style={{ display:"flex", gap:16, fontSize:11, fontFamily:"monospace" }}>
            <span style={{ color:"var(--green)" }}>● purchase</span>
            <span style={{ color:"var(--amber)" }}>● add_to_cart</span>
            <span style={{ color:"var(--purple)" }}>● wishlist</span>
            <span style={{ color:"var(--muted)" }}>● view</span>
          </div>
        </div>
        <div className="stream-feed" style={{ height:460 }}>
          {stream.map((ev,i) => (
            <div key={i} className={`stream-row ${ev.action}`}>
              <span className="stream-ts">{ev.ts}</span>
              <span className="stream-user">{ev.user}</span>
              <span className={`stream-action ${ev.action}`}>{ev.action}</span>
              <span className="stream-prod">{ev.emoji} {ev.product}</span>
            </div>
          ))}
        </div>
      </div>
      <div className="metrics-row">
        {[
          { val:`${fps}/s`,                    lbl:"Kafka Throughput" },
          { val:totalEvents.toLocaleString(),  lbl:"Total Events"     },
          { val:"Real SSE",                    lbl:"Transport"        },
          { val:"gzip",                        lbl:"Compression"      },
        ].map(m => (
          <div key={m.lbl} className="metric-card">
            <div className="metric-val">{m.val}</div>
            <div className="metric-lbl">{m.lbl}</div>
          </div>
        ))}
      </div>
    </div>
  );
}
