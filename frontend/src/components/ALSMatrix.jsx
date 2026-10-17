import { useState, useEffect } from "react";
const SPARK_METRICS = { rank:50, maxIterations:20, regParam:0.01, rmse:0.842, convergence:94.3, partitions:128 };

export default function ALSMatrix({ ticks }) {
  const ROWS=6, COLS=8;
  const [cells, setCells] = useState(() => Array.from({ length: ROWS*COLS }, () => Math.random()));
  useEffect(() => {
    setCells(prev => prev.map(v => Math.min(1, Math.max(0, v + (Math.random()-0.5)*0.12))));
  }, [ticks]);
  const colorFor = v => {
    const r=Math.round(v*0+(1-v)*5), g=Math.round(v*212+(1-v)*10), b=Math.round(v*255+(1-v)*20);
    return `rgba(${r},${g},${b},${0.2+v*0.75})`;
  };
  return (
    <div style={{ display:"flex", gap:20 }}>
      <div>
        <div style={{ fontSize:10, color:"var(--muted)", marginBottom:8, fontFamily:"'Space Mono',monospace", textTransform:"uppercase", letterSpacing:1 }}>User-Item Affinity Matrix</div>
        <div style={{ display:"grid", gridTemplateColumns:`repeat(${COLS},1fr)`, gap:3, width:240 }}>
          {cells.map((v,i) => (
            <div key={i} className="matrix-cell" style={{ background:colorFor(v), height:28 }}>
              <div className="tooltip">U{Math.floor(i/COLS)+1}×P{(i%COLS)+1}: {v.toFixed(3)}</div>
            </div>
          ))}
        </div>
        <div style={{ display:"flex", alignItems:"center", gap:8, marginTop:8 }}>
          <span style={{ fontSize:9, color:"var(--muted)", fontFamily:"monospace" }}>0.0</span>
          <div style={{ flex:1, height:4, borderRadius:2, background:"linear-gradient(90deg,rgba(5,10,20,.9),rgba(0,212,255,.9))" }}/>
          <span style={{ fontSize:9, color:"var(--muted)", fontFamily:"monospace" }}>1.0</span>
        </div>
      </div>
      <div style={{ flex:1 }}>
        <div style={{ fontSize:10, color:"var(--muted)", marginBottom:8, fontFamily:"'Space Mono',monospace", textTransform:"uppercase", letterSpacing:1 }}>ALS Parameters</div>
        {[
          {k:"Rank (k)",      v:SPARK_METRICS.rank},
          {k:"Iterations",    v:SPARK_METRICS.maxIterations},
          {k:"RegParam (λ)",  v:SPARK_METRICS.regParam},
          {k:"RMSE",          v:SPARK_METRICS.rmse,             color:"var(--amber)"},
          {k:"Convergence",   v:SPARK_METRICS.convergence+"%",  color:"var(--green)"},
          {k:"Partitions",    v:SPARK_METRICS.partitions},
        ].map(({k,v,color}) => (
          <div key={k} style={{ display:"flex", justifyContent:"space-between", marginBottom:7, fontSize:12 }}>
            <span style={{ color:"var(--muted)" }}>{k}</span>
            <span style={{ fontFamily:"'Space Mono',monospace", color:color||"var(--text)" }}>{v}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
