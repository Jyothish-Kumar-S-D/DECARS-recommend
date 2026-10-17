import { useState } from "react";
import ALSMatrix     from "./ALSMatrix.jsx";
import useInterval   from "../hooks/useInterval.js";

export default function ALSEngine({ ticks, sparkStatus, api }) {
  const [training,  setTraining]  = useState(false);
  const [trainMsg,  setTrainMsg]  = useState(null);
  const [job,       setJob]       = useState(null);   // latest /api/train/status payload
  const [watching,  setWatching]  = useState(false);  // poll while the job runs

  const handleTrain = async () => {
    setTraining(true);
    setTrainMsg(null);
    try {
      const r    = await fetch(`${api}/api/train`, { method: "POST" });
      const data = await r.json();

      if (data.status === "error") {
        setTrainMsg(`⚠️ ${data.detail}`);
        setWatching(false);
      } else if (data.status === "already_running") {
        setTrainMsg(`⏳ A training job is already running (PID ${data.pid})`);
        setJob({ running: true, pid: data.pid, log_tail: "" });
        setWatching(true);
      } else {
        setTrainMsg(`✅ Job submitted (PID ${data.pid}) — live log below`);
        setJob({ running: true, pid: data.pid, log_tail: "", submitted: true });
        setWatching(true);
      }
    } catch {
      setTrainMsg("⚠️ Could not reach backend. Is docker-compose running?");
    }
    setTraining(false);
  };

  // Poll the training job while it is running (stops when it finishes)
  useInterval(async () => {
    try {
      const s = await fetch(`${api}/api/train/status?lines=40`).then(r => r.json());
      setJob(prev => ({ ...prev, ...s }));
      if (!s.running) setWatching(false);
    } catch { /* backend restarting — keep the last known state */ }
  }, watching ? 4000 : null);

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

  const jobState = (() => {
    if (!job) return null;
    if (job.running)               return { text: `⏳ Training on Spark cluster… (PID ${job.pid})`, color: "var(--amber)" };
    if (job.exit_code === 0)       return { text: "✅ Training finished (exit 0)",                  color: "var(--green)" };
    if (job.exit_code != null)     return { text: `⚠️ Training failed (exit ${job.exit_code})`,     color: "var(--red)"   };
    return { text: "Waiting for the driver…", color: "var(--muted)" };
  })();

  const summary = job?.summary || {};

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
              {workers.workers.map((w, i) => {
                // Spark Master REST (/json/) reports lowercase keys: cores, coresused
                const used  = w.coresused ?? w.coresUsed ?? 0;
                const total = w.cores || 0;
                return (
                  <div key={i} className="hdfs-node">
                    <div className={`node-status ${w.state === "ALIVE" ? "healthy" : "warning"}`} />
                    <span className="node-name" style={{ fontSize:11 }}>{w.id?.split("-").slice(-2).join(":") || `Worker-${i+1}`}</span>
                    <span className="node-role">{total} cores</span>
                    <div className="bar-wrap">
                      <div className="bar-fill ok" style={{ width:`${total ? Math.round((used / total) * 100) : 0}%` }} />
                    </div>
                    <span className="node-pct">{used}/{total}</span>
                  </div>
                );
              })}
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

          {/* Training result: state, RMSE, HDFS outputs, driver log tail */}
          {job && (
            <div style={{ marginTop:12, border:"1px solid rgba(0,212,255,0.18)", borderRadius:8, padding:"10px 12px", background:"rgba(0,212,255,0.04)" }}>
              <div style={{ fontSize:12, fontFamily:"monospace", color: jobState?.color }}>{jobState?.text}</div>

              {(summary.interactions || summary.rmse != null || summary.model_path || summary.no_data) && (
                <div style={{ marginTop:8, fontSize:11, color:"var(--muted)", lineHeight:1.8 }}>
                  {summary.no_data && (
                    <div style={{ color:"var(--amber)" }}>
                      ⚠️ No clickstream Parquet in HDFS yet — start the Kafka→HDFS streaming job first.
                    </div>
                  )}
                  {summary.interactions && <div>Interactions: <strong style={{ color:"var(--text)" }}>{summary.interactions}</strong></div>}
                  {summary.rmse != null && <div>RMSE: <strong style={{ color:"var(--amber)" }}>{summary.rmse}</strong></div>}
                  {summary.model_path && <div>Model: <span style={{ color:"var(--cyan)", fontFamily:"monospace" }}>{summary.model_path}</span></div>}
                  {summary.recs_path && <div>Recommendations: <span style={{ color:"var(--cyan)", fontFamily:"monospace" }}>{summary.recs_path}</span></div>}
                  {!job.running && summary.rmse != null && (
                    <div style={{ color:"var(--green)" }}>
                      Open the Recommendations tab (cache refreshes within 60 s) to see the new results.
                    </div>
                  )}
                </div>
              )}

              {job.log_tail && (
                <pre style={{ marginTop:10, marginBottom:0, maxHeight:170, overflow:"auto", fontSize:10, lineHeight:1.5, color:"var(--muted)", background:"rgba(0,0,0,0.35)", border:"1px solid rgba(0,212,255,0.12)", borderRadius:6, padding:"8px 10px", whiteSpace:"pre-wrap" }}>
                  {job.log_tail.trim()}
                </pre>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
