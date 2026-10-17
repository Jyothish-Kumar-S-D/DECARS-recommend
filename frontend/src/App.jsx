import { useState, useEffect, useRef, useCallback } from "react";
import "./styles.css";
import Header         from "./components/Header.jsx";
import Dashboard      from "./components/Dashboard.jsx";
import LiveStream     from "./components/LiveStream.jsx";
import ALSEngine      from "./components/ALSEngine.jsx";
import HDFSCluster    from "./components/HDFSCluster.jsx";
import Recommendations from "./components/Recommendations.jsx";
import useInterval    from "./hooks/useInterval.js";

const API = import.meta.env.VITE_API_URL || "http://localhost:8000";

const TABS = [
  { id: "dashboard", label: "Dashboard",      icon: "◈" },
  { id: "stream",    label: "Live Stream",     icon: "⚡" },
  { id: "als",       label: "ALS Engine",      icon: "🧮" },
  { id: "hdfs",      label: "HDFS Cluster",    icon: "💾" },
  { id: "recos",     label: "Recommendations", icon: "🔮" },
];

export default function App() {
  const [tab,         setTab]         = useState("dashboard");
  const [stream,      setStream]      = useState([]);
  const [ticks,       setTicks]       = useState(0);
  const [totalEvents, setTotalEvents] = useState(0);
  const [fps,         setFps]         = useState(0);
  const [hdfsStatus,  setHdfsStatus]  = useState(null);
  const [sparkStatus, setSparkStatus] = useState(null);
  const [connected,   setConnected]   = useState(false);
  const fpsCounter = useRef(0);
  const esRef      = useRef(null);

  // ── Real Kafka SSE stream ─────────────────────────────────────────────────
  useEffect(() => {
    const es = new EventSource(`${API}/api/stream`);
    esRef.current = es;

    es.onopen = () => setConnected(true);

    es.onmessage = (e) => {
      const event = JSON.parse(e.data);
      const formatted = {
        ts:      new Date(event.timestamp).toLocaleTimeString("en-US", { hour12: false }),
        user:    `U${event.user_id}`,
        action:  event.action,
        product: event.product_name,
        emoji:   getEmoji(event.category),
        raw:     event,
      };
      setStream(prev => [formatted, ...prev].slice(0, 200));
      setTotalEvents(prev => prev + 1);
      fpsCounter.current += 1;
      setTicks(t => t + 1);
    };

    es.onerror = () => setConnected(false);

    return () => es.close();
  }, []);

  // FPS counter
  useInterval(() => {
    setFps(fpsCounter.current);
    fpsCounter.current = 0;
  }, 1000);

  // Poll real HDFS + Spark status every 10s
  useInterval(async () => {
    try {
      const [h, s] = await Promise.all([
        fetch(`${API}/api/hdfs/status`).then(r => r.json()),
        fetch(`${API}/api/spark/status`).then(r => r.json()),
      ]);
      setHdfsStatus(h);
      setSparkStatus(s);
    } catch { /* backend not ready yet */ }
  }, 10000);

  // Initial fetch
  useEffect(() => {
    fetch(`${API}/api/hdfs/status`).then(r => r.json()).then(setHdfsStatus).catch(() => {});
    fetch(`${API}/api/spark/status`).then(r => r.json()).then(setSparkStatus).catch(() => {});
  }, []);

  return (
    <div className="app">
      <Header totalEvents={totalEvents} fps={fps} connected={connected} />
      <div className="tabs">
        {TABS.map(t => (
          <div key={t.id} className={`tab ${tab === t.id ? "active" : ""}`} onClick={() => setTab(t.id)}>
            <span>{t.icon}</span>{t.label}
          </div>
        ))}
      </div>
      {tab === "dashboard" && <Dashboard  stream={stream} ticks={ticks} hdfsStatus={hdfsStatus} sparkStatus={sparkStatus} />}
      {tab === "stream"    && <LiveStream stream={stream} fps={fps} totalEvents={totalEvents} />}
      {tab === "als"       && <ALSEngine  ticks={ticks} sparkStatus={sparkStatus} api={API} />}
      {tab === "hdfs"      && <HDFSCluster hdfsStatus={hdfsStatus} />}
      {tab === "recos"     && <Recommendations api={API} />}
    </div>
  );
}

function getEmoji(category) {
  const map = { Electronics:"🎧", Computers:"💻", Footwear:"👟", Kitchen:"🍲", Apparel:"👖", Wearables:"⌚", Photography:"📷", Toys:"🧱", Home:"🌀" };
  return map[category] || "📦";
}
