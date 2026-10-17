export default function Header({ totalEvents, fps, connected }) {
  return (
    <div className="header">
      <div className="logo">
        <div className="logo-icon">⚡</div>
        <div>
          <div className="logo-text">BIG DATA PROJECT</div>
          <div className="logo-sub">Real Spark MLlib ALS · Real HDFS · Real Kafka</div>
        </div>
      </div>
      <div className="header-stats">
        <div className="hstat"><div className="hstat-val">{totalEvents.toLocaleString()}</div><div className="hstat-lbl">Kafka Events</div></div>
        <div className="hstat"><div className="hstat-val">{fps}/s</div><div className="hstat-lbl">Stream Rate</div></div>
      </div>
      <div className="live-badge" style={{ borderColor: connected ? "rgba(0,255,136,0.3)" : "rgba(255,77,109,0.3)", background: connected ? "rgba(0,255,136,0.1)" : "rgba(255,77,109,0.1)", color: connected ? "var(--green)" : "var(--red)" }}>
        <div className="live-dot" style={{ background: connected ? "var(--green)" : "var(--red)" }}/>
        {connected ? "KAFKA LIVE" : "CONNECTING"}
      </div>
    </div>
  );
}
