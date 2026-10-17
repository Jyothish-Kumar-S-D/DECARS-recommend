import { useState } from "react";
import ALSMatrix     from "./ALSMatrix.jsx";

export default function ALSEngine({ ticks, sparkStatus, api }) {
  const [training,  setTraining]  = useState(false);
  const [trainMsg,  setTrainMsg]  = useState(null);

  const handleTrain = async () => {
    setTraining(true);
    setTrainMsg(null);
    try {
      const r    = await fetch(`${api}/api/train`, { method: "POST" });
      const data = await r.json();
      setTrainMsg(`✅ Job submitted (PID ${data.pid}) — check Spark UI at :8082`);
    } catch {
      setTrainMsg("⚠️ Could not reach backend. Is docker-compose running?");
    }
    setTraining(false);
  };

  // Parse real Spark apps from REST API
  const apps    = sparkStatus?.applications || [];
  const workers = sparkStatus?.workers || {};

  const STAGES = [
    { name: "HDFS Data Ingestion",         icon: "💾", s: "done"    },
    { name: "Clickstream Parsing (Kafka)", icon: "📡", s: "done"    },
    { name: "Feature Engineering",         icon: "⚙️",  s: "done"    },
    { name: "ALS Matrix Factorization",    icon: "🧮", s: "running" },
    { name: "Prediction Inference",        icon: "🔮", s: "queued"  },
    { name: "Result Caching (Redis)",      icon: "⚡", s: "queued"  },
  ];

  return (
    <div className="main">
      <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:20 }}>
        {/* Matrix */}
        <div className="card">
          <div className="card-title">🧮 User-Item Affinity Matrix</div>
          <ALSMatrix ticks={ticks} />
          <div style={{ marginTop:20, fontSize:12, color:"var(--muted)", lineHeight:1.7 }}>
            ALS decomposes R ≈ U × Vᵀ. Each user and item is embedded in a{" "}
            <strong style={{ color:"var(--cyan)" }}>k=50 dimensional</strong> latent space.
            Heatmap updates as new Kafka events arrive.
          </div>
        </div>

        {/* Spark status */}
        <div className="card">
          <div className="card-title" style={{ justifyContent:"space-between", display:"flex" }}>
            <span>⚙️ Spark Cluster</span>
            <a href="http://localhost:8082" target="_blank" rel="noreferrer"
              style={{ fontSize:10, color:"var(--cyan)", fontFamily:"monospace", textDecoration:"none" }}>
              Open Spark UI ↗
            </a>
          </div>

          {/* Real worker info */}
          {workers.workers?.length > 0 ? (
            <div style={{ marginBottom:16 }}>
              {workers.workers.map((w, i) => (
                <div key={i} className="hdfs-node">
                  <div className={`node-status ${w.state === "ALIVE" ? "healthy" : "warning"}`} />
                  <span className="node-name" style={{ fontSize:11 }}>{w.id?.split("-").slice(-1)[0] || `Worker-${i+1}`}</span>
                  <span className="node-role">{w.cores} cores</span>
                  <div className="bar-wrap">
                    <div className="bar-fill ok" style={{ width:`${Math.round((w.coresUsed/w.cores)*100)||0}%` }} />
                  </div>
                  <span className="node-pct">{w.coresUsed}/{w.cores}</span>
                </div>
              ))}
            </div>
          ) : (
            <div style={{ color:"var(--muted)", fontSize:12, marginBottom:16 }}>
              Connecting to Spark Master…
            </div>
          )}

          {/* Pipeline stages */}
          {STAGES.map(st => (
            <div key={st.name} className="spark-stage">
              <span className="stage-icon">{st.icon}</span>
              <span className="stage-name">{st.name}</span>
              <span className={`stage-status ${st.s}`}>{st.s.toUpperCase()}</span>
            </div>
          ))}

          {/* Train button */}
          <div style={{ marginTop:16 }}>
            <button onClick={handleTrain} disabled={training}
              style={{ width:"100%", background:"linear-gradient(90deg,rgba(0,212,255,0.12),rgba(176,106,255,0.12))", border:"1px solid rgba(0,212,255,0.3)", color:"var(--cyan)", borderRadius:8, padding:"10px 0", cursor:"pointer", fontSize:13, fontFamily:"monospace", opacity: training ? .6 : 1 }}>
              {training ? "⏳ Submitting job…" : "🚀 Run ALS Training Job"}
            </button>
            {trainMsg && <div style={{ marginTop:8, fontSize:11, color:"var(--green)" }}>{trainMsg}</div>}
          </div>
        </div>
      </div>
    </div>
  );
}
