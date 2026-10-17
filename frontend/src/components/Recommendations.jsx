import { useState, useEffect, useCallback } from "react";

const USERS = [
  { id: 101, name: "Arjun M.",  avatar: "🧑‍💻", segment: "Tech Enthusiast" },
  { id: 102, name: "Priya S.",  avatar: "👩‍🎨", segment: "Home Decorator"  },
  { id: 103, name: "Carlos R.", avatar: "🧑‍🍳", segment: "Chef & Foodie"   },
  { id: 104, name: "Mei L.",    avatar: "👩‍💼", segment: "Business Pro"    },
  { id: 105, name: "Omar F.",   avatar: "🏃",  segment: "Fitness Junkie"  },
];

const PRODUCTS = [
  { id:1,  name:"Sony WH-1000XM5",       category:"Electronics",  price:349,  img:"🎧" },
  { id:2,  name:"MacBook Air M3",         category:"Computers",    price:1299, img:"💻" },
  { id:3,  name:"Nike Air Max 270",       category:"Footwear",     price:130,  img:"👟" },
  { id:4,  name:"Kindle Paperwhite",      category:"Electronics",  price:139,  img:"📖" },
  { id:5,  name:"Instant Pot Duo 7-in-1", category:"Kitchen",      price:99,   img:"🍲" },
  { id:6,  name:"Levi's 511 Slim Jeans",  category:"Apparel",      price:79,   img:"👖" },
  { id:7,  name:"Fitbit Charge 6",        category:"Wearables",    price:159,  img:"⌚" },
  { id:8,  name:"Canon EOS R50",          category:"Photography",  price:679,  img:"📷" },
  { id:9,  name:"LEGO Technic F40",       category:"Toys",         price:189,  img:"🧱" },
  { id:10, name:"Dyson V15 Detect",       category:"Home",         price:649,  img:"🌀" },
  { id:11, name:"Vitamix 5200",           category:"Kitchen",      price:449,  img:"🥤" },
  { id:12, name:'Samsung 4K QLED 65"',    category:"Electronics",  price:1199, img:"📺" },
];

const SOURCE_LABELS = {
  als_hdfs:       { label: "ALS · HDFS",      color: "var(--green)"  },
  redis_cache:    { label: "Redis Cache",      color: "var(--amber)"  },
  claude_fallback:{ label: "Claude Fallback",  color: "var(--purple)" },
};

export default function Recommendations({ api }) {
  const [selectedUser,    setSelectedUser]    = useState(USERS[0]);
  const [selectedProduct, setSelectedProduct] = useState(null);
  const [recs,            setRecs]            = useState([]);
  const [loading,         setLoading]         = useState(false);
  const [source,          setSource]          = useState(null);
  const [error,           setError]           = useState(null);

  const fetchRecs = useCallback(async (user, product) => {
    setLoading(true);
    setError(null);
    setRecs([]);
    try {
      const params = new URLSearchParams({
        user_id: user.id,
        segment: user.segment,
        ...(product ? { product_id: product.id } : {}),
      });
      const res  = await fetch(`${api}/api/recommend?${params}`);
      const data = await res.json();
      setRecs(data.recommendations || []);
      setSource(data.source);
    } catch (e) {
      setError("Backend unavailable — is docker-compose running?");
    }
    setLoading(false);
  }, [api]);

  useEffect(() => { fetchRecs(selectedUser, selectedProduct); }, [selectedUser, selectedProduct]);

  const src = SOURCE_LABELS[source] || {};

  return (
    <div className="main">
      {/* User selector */}
      <div className="card">
        <div className="card-title">👤 Select User Profile</div>
        <div className="user-strip">
          {USERS.map(u => (
            <div key={u.id} className={`user-chip ${selectedUser.id === u.id ? "active" : ""}`} onClick={() => setSelectedUser(u)}>
              <span className="user-avatar">{u.avatar}</span>
              <div><div>{u.name}</div><div className="user-seg">{u.segment}</div></div>
            </div>
          ))}
        </div>
      </div>

      {/* Product catalog */}
      <div className="card">
        <div className="card-title">📦 Product Catalog — Click to Filter</div>
        <div className="product-grid">
          {PRODUCTS.map(p => (
            <div key={p.id} className={`prod-card ${selectedProduct?.id === p.id ? "selected" : ""}`}
              onClick={() => setSelectedProduct(prev => prev?.id === p.id ? null : p)}>
              <div className="prod-emoji">{p.img}</div>
              <div className="prod-name">{p.name}</div>
              <div className="prod-cat">{p.category}</div>
              <div className="prod-price">${p.price}</div>
            </div>
          ))}
        </div>
      </div>

      {/* Recs output */}
      <div className="card">
        <div className="card-title" style={{ justifyContent:"space-between", display:"flex", alignItems:"center" }}>
          <span>🔮 Recommendations</span>
          <div style={{ display:"flex", alignItems:"center", gap:12 }}>
            {source && <span style={{ fontSize:10, fontFamily:"monospace", color: src.color, border:`1px solid ${src.color}`, borderRadius:20, padding:"2px 10px" }}>SOURCE: {src.label}</span>}
            {loading ? <div className="spinner"/> :
              <button onClick={() => fetchRecs(selectedUser, selectedProduct)}
                style={{ background:"rgba(0,212,255,0.1)", border:"1px solid rgba(0,212,255,0.3)", color:"var(--cyan)", borderRadius:6, padding:"4px 14px", cursor:"pointer", fontSize:12, fontFamily:"monospace" }}>
                ↻ Refresh
              </button>}
          </div>
        </div>

        {error && <div style={{ color:"var(--red)", fontSize:12, padding:"12px 0" }}>⚠️ {error}</div>}

        {loading ? (
          <div style={{ display:"flex", alignItems:"center", gap:12, padding:"24px 0", color:"var(--muted)", fontSize:13 }}>
            <div className="spinner"/> Querying {source === "als_hdfs" ? "HDFS ALS model" : "backend API"}…
          </div>
        ) : (
          <div className="reco-grid">
            {recs.map((r, i) => (
              <div key={r.id} className="reco-card">
                <div className="reco-badge">#{i+1}</div>
                <div style={{ fontSize:32, marginBottom:10, marginTop:6 }}>{r.img}</div>
                <div style={{ fontWeight:600, fontSize:14, marginBottom:4, paddingRight:40 }}>{r.name}</div>
                <div style={{ fontSize:11, color:"var(--muted)", textTransform:"uppercase", letterSpacing:.8, marginBottom:6 }}>{r.category}</div>
                <div style={{ fontFamily:"monospace", fontSize:16, color:"var(--amber)" }}>${r.price}</div>
                <div className="reco-score-bar">
                  <div className="reco-score-fill" style={{ width:`${Math.round(r.score * 100)}%` }}/>
                </div>
                <div style={{ display:"flex", justifyContent:"space-between", marginTop:6, fontSize:11 }}>
                  <span style={{ color:"var(--muted)" }}>Score</span>
                  <span style={{ fontFamily:"monospace", color:"var(--cyan)" }}>{r.score?.toFixed(3)}</span>
                </div>
                <div className="reco-reason">{r.reason}</div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
