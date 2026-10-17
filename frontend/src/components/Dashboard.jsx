import ALSMatrix from "./ALSMatrix.jsx";
export default function Dashboard({ stream, ticks, hdfsStatus, sparkStatus }) {
  const workers = sparkStatus?.workers?.workers?.length || 0;
  const liveNodes = hdfsStatus?.jmx?.beans?.find(b => b.name?.includes("FSNamesystem"))?.NumLiveDataNodes || "—";
  return (
    <div className="main">
      <div className="metrics-row">
        {[
          { val: liveNodes,  lbl:"HDFS DataNodes",    color:"var(--cyan)"   },
          { val: workers||"—", lbl:"Spark Workers",   color:"var(--purple)" },
          { val: "Real",     lbl:"Kafka Stream",       color:"var(--green)"  },
          { val: "3-tier",   lbl:"Architecture",       color:"var(--amber)"  },
        ].map(m => (
          <div key={m.lbl} className="metric-card">
            <div className="metric-val" style={{ color:m.color }}>{m.val}</div>
            <div className="metric-lbl">{m.lbl}</div>
          </div>
        ))}
      </div>
      <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:20 }}>
        <div className="card">
          <div className="card-title">⚡ Real Kafka Clickstream</div>
          <div className="stream-feed">
            {stream.slice(0,20).map((ev,i) => (
              <div key={i} className={`stream-row ${ev.action}`}>
                <span className="stream-ts">{ev.ts}</span>
                <span className="stream-user">{ev.user}</span>
                <span className={`stream-action ${ev.action}`}>{ev.action}</span>
                <span className="stream-prod">{ev.emoji} {ev.product}</span>
              </div>
            ))}
          </div>
        </div>
        <div className="card">
          <div className="card-title">🧮 ALS Factor Matrix</div>
          <ALSMatrix ticks={ticks}/>
        </div>
      </div>
    </div>
  );
}
