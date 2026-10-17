/**
 * HDFSCluster.jsx
 * Displays REAL HDFS cluster data fetched from the backend's
 * /api/hdfs/status endpoint (which queries the NameNode via WebHDFS).
 */
export default function HDFSCluster({ hdfsStatus }) {
  const loading = !hdfsStatus;
  const isError = hdfsStatus?.status === "error";

  // Parse real JMX data if available
  const jmxBeans  = hdfsStatus?.jmx?.beans || [];
  const nnInfo    = jmxBeans.find(b => b.name?.includes("NameNodeInfo")) || {};
  const fsInfo    = jmxBeans.find(b => b.name?.includes("FSNamesystem")) || {};
  const liveNodes = JSON.parse(nnInfo.LiveNodes || "{}");
  const nodeList  = Object.entries(liveNodes).map(([name, info]) => ({
    name,
    status:  info.adminState === "In Service" ? "healthy" : "warning",
    usage:   Math.round((info.usedSpace / (info.capacity || 1)) * 100),
    blocks:  info.numBlocks?.toLocaleString() || "—",
    role:    "Storage",
    capacity: info.capacity,
    used:    info.usedSpace,
  }));

  const totalCapacity  = fsInfo.CapacityTotal   || 0;
  const totalUsed      = fsInfo.CapacityUsed    || 0;
  const totalBlocks    = fsInfo.BlocksTotal      || 0;
  const liveNodeCount  = fsInfo.NumLiveDataNodes || nodeList.length || "—";

  const fmt = (bytes) => {
    if (!bytes) return "—";
    const tb = bytes / 1e12;
    return tb >= 1 ? `${tb.toFixed(1)} TB` : `${(bytes / 1e9).toFixed(0)} GB`;
  };

  return (
    <div className="main">
      {/* Connection status */}
      {isError && (
        <div style={{ background:"rgba(255,77,109,0.08)", border:"1px solid rgba(255,77,109,0.3)", borderRadius:10, padding:"12px 16px", fontSize:12, color:"var(--red)" }}>
          ⚠️ Cannot reach HDFS NameNode: {hdfsStatus.detail}
        </div>
      )}

      {/* KPIs */}
      <div className="metrics-row">
        {[
          { val: liveNodeCount,      lbl: "Live DataNodes",  color: "var(--cyan)"   },
          { val: fmt(totalCapacity), lbl: "HDFS Capacity",   color: "var(--purple)" },
          { val: fmt(totalUsed),     lbl: "Used Storage",    color: "var(--amber)"  },
          { val: totalBlocks ? totalBlocks.toLocaleString() : "—", lbl: "Total Blocks", color:"var(--green)" },
        ].map(m => (
          <div key={m.lbl} className="metric-card">
            <div className="metric-val" style={{ color: m.color }}>
              {loading ? <div className="spinner" style={{ width:18, height:18 }}/> : m.val}
            </div>
            <div className="metric-lbl">{m.lbl}</div>
            <div className="metric-delta" style={{ color:"var(--muted)", fontSize:9 }}>
              {loading ? "fetching…" : "Live via WebHDFS API"}
            </div>
          </div>
        ))}
      </div>

      {/* Node list */}
      <div className="card">
        <div className="card-title">💾 Live DataNode Status</div>
        {loading && <div style={{ color:"var(--muted)", fontSize:12 }}>Loading node data from NameNode…</div>}
        {!loading && nodeList.length === 0 && (
          <div style={{ color:"var(--muted)", fontSize:12 }}>
            No live DataNodes reported yet — cluster may be starting up.
          </div>
        )}
        {nodeList.map(n => (
          <div key={n.name} className="hdfs-node">
            <div className={`node-status ${n.status}`} />
            <span className="node-name">{n.name.split(":")[0]}</span>
            <span className="node-role">{n.role}</span>
            <div className="bar-wrap">
              <div className={`bar-fill ${n.usage > 80 ? "warn" : "ok"}`} style={{ width:`${n.usage}%` }}/>
            </div>
            <span className="node-pct" style={{ color: n.usage > 80 ? "var(--amber)" : "var(--text)" }}>{n.usage}%</span>
            <span className="node-blocks">{n.blocks}</span>
          </div>
        ))}
      </div>

      {/* HDFS paths */}
      <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr 1fr", gap:20 }}>
        {[
          { title:"Clickstream Events", emoji:"⚡", path:"/data/clickstream",     desc:"Parquet, partitioned by action" },
          { title:"ALS Model",          emoji:"🧮", path:"/models/als_latest",    desc:"Spark MLlib ALSModel" },
          { title:"Recommendations",    emoji:"🔮", path:"/data/recommendations", desc:"Pre-computed top-10 per user" },
        ].map(d => (
          <div key={d.title} className="card">
            <div style={{ fontSize:28, marginBottom:10 }}>{d.emoji}</div>
            <div style={{ fontWeight:600, fontSize:14, marginBottom:6 }}>{d.title}</div>
            <div style={{ fontFamily:"monospace", fontSize:11, color:"var(--cyan)", marginBottom:6 }}>{d.path}</div>
            <div style={{ fontSize:11, color:"var(--muted)" }}>{d.desc}</div>
          </div>
        ))}
      </div>
    </div>
  );
}
