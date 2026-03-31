import { useState, useEffect, useMemo, useRef } from "react";

const API_BASE = "";
const MAX_SEARCHES_PER_DAY = 5;

const FACTOR_META = {
  citationCount:     { label: "Citation Count",     icon: "📚", why: "Datasets cited in more papers have proven real-world utility.", example: "ERA5 climate dataset: cited in 4,800+ papers.", desc: "How many published papers have cited or used this dataset" },
  sourceCredibility: { label: "Source Credibility", icon: "🏛",  why: "Government, academic, or verified org sources are more trustworthy.", example: "NOAA, WHO, Stanford, Google — high credibility.", desc: "Trustworthiness of the institution or organisation that published it" },
  documentation:     { label: "Documentation",      icon: "📋", why: "Good docs mean less time debugging, more time building.", example: "HuggingFace datasets with data cards score high.", desc: "Quality of data cards, metadata, column descriptions and labels" },
  licenseClarity:    { label: "License Clarity",    icon: "⚖️", why: "Unclear licenses create legal risk in production systems.", example: "CC0 and MIT are ideal for commercial use.", desc: "Whether it has a clear, open license allowing research use" },
  communityAdoption: { label: "Community Adoption", icon: "🌐", why: "Widely used datasets have more pre-trained models and benchmarks.", example: "ImageNet: used in 100,000+ GitHub repos.", desc: "How widely used it is across the ML and research community" },
  maintenance:       { label: "Maintenance",        icon: "🔧", why: "Outdated datasets lead to model drift and stale predictions.", example: "Updated monthly vs. last updated in 2017.", desc: "How actively the dataset is maintained and kept up to date" },
  biasAndDiversity:  { label: "Bias & Diversity",   icon: "⚖",  why: "Biased training data leads to biased, unfair model outputs.", example: "Facial recognition datasets lacking diverse ethnicities.", desc: "How representative, balanced and free from demographic bias the data is" },
};

const SOURCES = ["Kaggle","HuggingFace","UCI ML Repository","PapersWithCode","Zenodo","GitHub","Government"];
const LICENSES = ["CC BY 4.0","CC0","MIT","Apache 2.0","Public Domain","Other"];
const SAMPLE_IDEAS = [
  "Detect fake news from social media posts",
  "Medical image classification for lung cancer",
  "Predict stock prices using time series data",
  "Sentiment analysis on product reviews",
  "Fraud detection in credit card transactions",
  "Object detection for autonomous driving",
];

const getToken = () => localStorage.getItem("token");
const setToken = (t) => localStorage.setItem("token", t);
const removeToken = () => localStorage.removeItem("token");

const apiFetch = async (path, options = {}) => {
  const token = getToken();
  return fetch(API_BASE + path, {
    ...options,
    headers: { "Content-Type": "application/json", ...(token ? { Authorization: `Bearer ${token}` } : {}), ...(options.headers || {}) },
  });
};

const downloadPdfWithAuth = async (id) => {
  const token = getToken();
  const res = await fetch(`${API_BASE}/api/saved/${id}/pdf`, { headers: { Authorization: `Bearer ${token}` } });
  if (!res.ok) { alert("Failed to download PDF"); return; }
  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url; a.download = `dataset-${id}.pdf`;
  document.body.appendChild(a); a.click();
  document.body.removeChild(a); URL.revokeObjectURL(url);
};

const scoreColor = (s) => s >= 8 ? "#00E5A0" : s >= 6 ? "#FFB800" : s >= 4 ? "#FF8C42" : "#FF5A5A";

const S = {
  input: { width: "100%", padding: "12px 16px", background: "rgba(10,20,50,0.85)", border: "1px solid #1e3a8a", borderRadius: 12, color: "#e0e8ff", fontSize: 14, fontFamily: "'Syne',sans-serif", outline: "none", boxSizing: "border-box", transition: "border-color 0.2s, box-shadow 0.2s" },
  btn: (v = "primary") => ({
    padding: "10px 20px", border: "none", borderRadius: 10, fontWeight: 700, fontSize: 13, fontFamily: "'Syne',sans-serif", cursor: "pointer",
    background: v === "primary" ? "linear-gradient(135deg,#1d4ed8,#3b82f6)" : v === "danger" ? "rgba(239,68,68,0.12)" : "rgba(30,58,138,0.25)",
    color: v === "danger" ? "#ef4444" : "#fff",
    border: v !== "primary" ? `1px solid ${v === "danger" ? "#ef444440" : "#1e3a8a"}` : "none",
    transition: "all 0.2s ease",
  }),
  card: { background: "rgba(8,15,40,0.92)", border: "1px solid #0d1b3e", borderRadius: 16, overflow: "hidden" },
  glass: { background: "rgba(14,24,58,0.7)", border: "1px solid rgba(30,58,138,0.4)", borderRadius: 16, backdropFilter: "blur(12px)" },
};

const GLOBAL_CSS = `
  * { box-sizing: border-box; }
  body { margin:0; padding:0; overflow-x:hidden; }
  @keyframes spin { to { transform:rotate(360deg); } }
  @keyframes fadeUp { from { opacity:0; transform:translateY(16px); } to { opacity:1; transform:translateY(0); } }
  @keyframes fadeIn { from { opacity:0; transform:translateY(-6px); } to { opacity:1; transform:translateY(0); } }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }
  .fade-up { animation: fadeUp 0.4s ease forwards; }
  .hover-lift { transition: transform 0.2s ease, box-shadow 0.2s ease; }
  .hover-lift:hover { transform: translateY(-3px); box-shadow: 0 12px 40px rgba(30,58,138,0.3); }
  .hover-glow { transition: all 0.2s ease; }
  .hover-glow:hover { box-shadow: 0 0 22px rgba(59,130,246,0.25); border-color: #3b82f6 !important; transform: scale(1.02); }
  input:focus, textarea:focus, select:focus { border-color: #3b82f6 !important; box-shadow: 0 0 0 3px rgba(59,130,246,0.12) !important; }
  ::-webkit-scrollbar { width:6px; } ::-webkit-scrollbar-track { background:#050b1a; } ::-webkit-scrollbar-thumb { background:#1e3a8a; border-radius:3px; }
  @media(max-width:640px){
    .search-row{flex-direction:column!important}
    .factor-grid{grid-template-columns:1fr 1fr!important}
    .card-inner{flex-direction:column!important}
    .filter-row{flex-direction:column!important}
    .hero-badges{flex-wrap:wrap!important}
    .how-grid{grid-template-columns:1fr!important}
  }
`;

function ProfileDropdown({ user, logout, setPage }) {
  const [open, setOpen] = useState(false);
  const ref = useRef(null);
  useEffect(() => {
    const h = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false); };
    document.addEventListener("mousedown", h);
    return () => document.removeEventListener("mousedown", h);
  }, []);
  const initials = user.name?.split(" ").map(n => n[0]).join("").toUpperCase().slice(0, 2) || "U";
  return (
    <div ref={ref} style={{ position: "relative" }}>
      <div onClick={() => setOpen(!open)} style={{ display: "flex", alignItems: "center", gap: 8, padding: "5px 12px 5px 6px", background: "rgba(30,58,138,0.2)", borderRadius: 24, border: "1px solid #1e3a8a", cursor: "pointer", transition: "all 0.2s", userSelect: "none" }}
        onMouseEnter={e => e.currentTarget.style.background = "rgba(30,58,138,0.4)"}
        onMouseLeave={e => e.currentTarget.style.background = "rgba(30,58,138,0.2)"}>
        {user.avatar
          ? <img src={user.avatar} style={{ width: 28, height: 28, borderRadius: "50%", border: "2px solid #1e3a8a" }} alt="" />
          : <div style={{ width: 28, height: 28, borderRadius: "50%", background: "linear-gradient(135deg,#1d4ed8,#3b82f6)", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 11, fontWeight: 800, color: "#fff" }}>{initials}</div>}
        <span style={{ fontSize: 13, color: "#aabcd8", fontWeight: 600, maxWidth: 100, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{user.name}</span>
        <span style={{ fontSize: 10, color: "#4a5a7a", transition: "transform 0.2s", display: "inline-block", transform: open ? "rotate(180deg)" : "rotate(0deg)" }}>▼</span>
      </div>
      {open && (
        <div style={{ position: "absolute", right: 0, top: "calc(100% + 10px)", width: 210, background: "rgba(5,11,26,0.98)", border: "1px solid #1e3a8a", borderRadius: 14, overflow: "hidden", boxShadow: "0 16px 48px rgba(0,0,0,0.6)", zIndex: 300, animation: "fadeIn 0.15s ease" }}>
          <div style={{ padding: "12px 16px", borderBottom: "1px solid #0d1b3e" }}>
            <div style={{ fontSize: 13, fontWeight: 700, color: "#e0e8ff" }}>{user.name}</div>
            <div style={{ fontSize: 11, color: "#4a5a7a", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{user.email}</div>
          </div>
          {[{ icon: "🔍", label: "Search", action: () => { setPage("search"); setOpen(false); } },
            { icon: "📚", label: "My Library", action: () => { setPage("saved"); setOpen(false); } },
            { icon: "ℹ️", label: "About", action: () => { setPage("about"); setOpen(false); } }].map(item => (
            <div key={item.label} onClick={item.action} style={{ padding: "10px 16px", display: "flex", alignItems: "center", gap: 10, cursor: "pointer", fontSize: 13, color: "#aabcd8", transition: "background 0.15s" }}
              onMouseEnter={e => e.currentTarget.style.background = "rgba(30,58,138,0.3)"}
              onMouseLeave={e => e.currentTarget.style.background = "transparent"}>
              <span>{item.icon}</span><span>{item.label}</span>
            </div>
          ))}
          <div style={{ borderTop: "1px solid #0d1b3e" }}>
            <div onClick={() => { logout(); setOpen(false); }} style={{ padding: "10px 16px", display: "flex", alignItems: "center", gap: 10, cursor: "pointer", fontSize: 13, color: "#ef4444", transition: "background 0.15s" }}
              onMouseEnter={e => e.currentTarget.style.background = "rgba(239,68,68,0.08)"}
              onMouseLeave={e => e.currentTarget.style.background = "transparent"}>
              <span>🚪</span><span>Logout</span>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function ScoreBar({ score, small, animated }) {
  const color = scoreColor(score);
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
      <div style={{ flex: 1, height: small ? 4 : 7, background: "#0d1b3e", borderRadius: 4, overflow: "hidden" }}>
        <div style={{ height: "100%", width: (score / 10 * 100) + "%", background: `linear-gradient(90deg,${color}55,${color})`, borderRadius: 4, transition: animated ? "width 0.8s ease" : "none" }} />
      </div>
      <span style={{ fontSize: small ? 11 : 13, fontWeight: 700, color, fontFamily: "monospace", minWidth: 28 }}>{score}</span>
    </div>
  );
}

export default function DatasetFinderPage() {
  const [page, setPage] = useState("search");
  const [user, setUser] = useState(null);
  const [authLoading, setAL] = useState(true);

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const token = params.get("token");
    if (token && window.location.pathname === "/auth/callback") {
      setToken(token);
      setUser({ name: decodeURIComponent(params.get("name") || ""), email: decodeURIComponent(params.get("email") || "") });
      window.history.replaceState({}, "", "/dataset-finder");
      setPage("search");
    }
  }, []);

  useEffect(() => {
    if (!getToken()) { setAL(false); return; }
    apiFetch("/auth/me").then(r => r.ok ? r.json() : null).then(u => {
      if (u) setUser(u); else removeToken();
      setAL(false);
    }).catch(() => { removeToken(); setAL(false); });
  }, []);

  const logout = () => { removeToken(); setUser(null); setPage("search"); };
  if (authLoading) return <Loading />;

  return (
    <div style={{ minHeight: "100vh", width: "100%", maxWidth: "100%", overflowX: "hidden", background: "#050b1a", color: "#e0e8ff", fontFamily: "'Syne',sans-serif" }}>
      <link href="https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=Space+Mono:wght@400;700&display=swap" rel="stylesheet" />
      <style>{GLOBAL_CSS}</style>
      <div style={{ position: "fixed", inset: 0, backgroundImage: "linear-gradient(rgba(30,58,138,0.06) 1px,transparent 1px),linear-gradient(90deg,rgba(30,58,138,0.06) 1px,transparent 1px)", backgroundSize: "40px 40px", pointerEvents: "none", zIndex: 0 }} />
      <div style={{ position: "fixed", top: "-15%", left: "-5%", width: 600, height: 600, borderRadius: "50%", background: "radial-gradient(circle,rgba(59,130,246,0.07) 0%,transparent 70%)", pointerEvents: "none", zIndex: 0 }} />
      <div style={{ position: "fixed", bottom: "-20%", right: "-5%", width: 700, height: 700, borderRadius: "50%", background: "radial-gradient(circle,rgba(30,58,138,0.08) 0%,transparent 70%)", pointerEvents: "none", zIndex: 0 }} />

      <nav style={{ position: "sticky", top: 0, zIndex: 100, background: "rgba(5,11,26,0.95)", borderBottom: "1px solid #0d1b3e", padding: "10px 28px", display: "flex", alignItems: "center", justifyContent: "space-between", backdropFilter: "blur(14px)", gap: 8 }}>
        <span onClick={() => setPage("search")} style={{ fontWeight: 800, fontSize: 18, color: "#6fa3ef", cursor: "pointer", letterSpacing: 0.5, display: "flex", alignItems: "center", gap: 8 }}>
          <span style={{ fontSize: 22 }}>⬡</span> DatasetFinder
        </span>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          {user ? <ProfileDropdown user={user} logout={logout} setPage={setPage} /> : (
            <>
              <button onClick={() => setPage("login")} style={{ ...S.btn("ghost"), padding: "7px 16px", fontSize: 13 }}>Login</button>
              <button onClick={() => setPage("register")} style={{ ...S.btn("primary"), padding: "7px 16px", fontSize: 13 }}
                onMouseEnter={e => e.currentTarget.style.transform = "scale(1.04)"}
                onMouseLeave={e => e.currentTarget.style.transform = "scale(1)"}>Sign Up</button>
            </>
          )}
        </div>
      </nav>

      <div style={{ position: "relative", zIndex: 1 }}>
        {page === "search"   && <SearchPage user={user} setPage={setPage} />}
        {page === "login"    && <LoginPage setUser={setUser} setPage={setPage} />}
        {page === "register" && <RegisterPage setUser={setUser} setPage={setPage} />}
        {page === "saved"    && <SavedPage user={user} setPage={setPage} />}
        {page === "about"    && <AboutPage setPage={setPage} />}
      </div>
    </div>
  );
}

// ════════════════════════════════════════════════════════════════════════════
// SEARCH PAGE
// ════════════════════════════════════════════════════════════════════════════
function SearchPage({ user, setPage }) {
  const [mode, setMode] = useState("search");
  const [query, setQuery] = useState("");
  const [ideaQuery, setIdeaQuery] = useState("");
  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [expanded, setExpanded] = useState(null);
  const [searched, setSearched] = useState("");
  const [rateInfo, setRateInfo] = useState(null);
  const [saveMsg, setSaveMsg] = useState({});
  const [collections, setCols] = useState([]);
  const [queryType, setQueryType] = useState(null);
  const [searchMode, setSearchMode] = useState(null);
  const [showFilters, setShowFilters] = useState(false);
  const [sortBy, setSortBy] = useState("reliability");
  const [filterSource, setFilterSource] = useState("");
  const [filterLicense, setFilterLicense] = useState("");
  const [filterMinScore, setFilterMinScore] = useState(0);
  const [expandedFactor, setExpandedFactor] = useState(null);
  const [hoveredSample, setHoveredSample] = useState(null);

  useEffect(() => {
    apiFetch("/api/rate-limit").then(r => r.json()).then(setRateInfo).catch(() => {});
    if (user) apiFetch("/api/collections").then(r => r.json()).then(d => Array.isArray(d) ? setCols(d) : setCols([])).catch(() => {});
  }, [user]);

  const search = async (liveSearch = false) => {
    const q = mode === "idea" ? ideaQuery : query;
    if (!q.trim() || loading) return;
    setLoading(true); setError(null); setResults(null); setSearched(q); setExpanded(null); setQueryType(null); setSearchMode(null);
    try {
      const res = await apiFetch("/api/search", { method: "POST", body: JSON.stringify({ query: q.trim(), liveSearch: liveSearch === true }) });
      const data = await res.json();
      if (!res.ok) { setError(data.error || "Search failed."); return; }
      setResults(data.datasets);
      setQueryType(data.queryType);
      setSearchMode(data.mode);
      setRateInfo({ remaining: data.remaining, used: data.used, max: data.max });
    } catch (err) { setError("Search failed: " + err.message); }
    finally { setLoading(false); }
  };

  const filteredResults = useMemo(() => {
    if (!results) return null;
    let f = [...results];
    if (filterSource) f = f.filter(d => d.source?.toLowerCase().includes(filterSource.toLowerCase()));
    if (filterLicense) f = f.filter(d => d.license?.toLowerCase().includes(filterLicense.toLowerCase()));
    if (filterMinScore > 0) f = f.filter(d => d.reliabilityScore >= filterMinScore);
    f.sort((a, b) => sortBy === "reliability" ? (b.reliabilityScore || 0) - (a.reliabilityScore || 0) : sortBy === "year" ? (b.year || 0) - (a.year || 0) : (b.sizeBytes || 0) - (a.sizeBytes || 0));
    return f;
  }, [results, sortBy, filterSource, filterLicense, filterMinScore]);

  const saveDataset = async (ds, collectionId = null) => {
    if (!user) { setPage("login"); return; }
    setSaveMsg(p => ({ ...p, [ds.name]: "saving" }));
    const res = await apiFetch("/api/saved", { method: "POST", body: JSON.stringify({ dataset: ds, collectionId }) });
    const data = await res.json();
    setSaveMsg(p => ({ ...p, [ds.name]: res.ok ? "saved" : (data.error || "Error") }));
    setTimeout(() => setSaveMsg(p => ({ ...p, [ds.name]: null })), 3000);
  };

  const remaining = rateInfo?.remaining ?? MAX_SEARCHES_PER_DAY;
  const pillColor = remaining > 2 ? "#00E5A0" : remaining > 0 ? "#FFB800" : "#FF5A5A";
  const rankColors = ["#FFD700", "#C0C0C0", "#CD7F32"];
  const rankLabels = ["BEST MATCH", "2ND BEST", "3RD BEST"];

  return (
    <div style={{ maxWidth: 960, width: "100%", margin: "0 auto", padding: "48px 20px 100px" }}>

      {/* HERO */}
      <div style={{ textAlign: "center", marginBottom: 48 }} className="fade-up">
        <div className="hero-badges" style={{ display: "flex", justifyContent: "center", alignItems: "center", gap: 10, marginBottom: 20, flexWrap: "wrap" }}>
          <div style={{ padding: "5px 14px", background: "rgba(30,58,138,0.3)", border: "1px solid #1e3a8a", borderRadius: 20, fontSize: 11, color: "#6fa3ef", fontFamily: "'Space Mono',monospace", letterSpacing: 1.5, display: "flex", alignItems: "center", gap: 6 }}>
            <span style={{ width: 6, height: 6, borderRadius: "50%", background: "#6fa3ef", display: "inline-block", animation: "pulse 2s infinite" }} />
            AI-POWERED DATASET DECISION ENGINE
          </div>
          {rateInfo && (
            <div style={{ padding: "5px 12px", background: "rgba(0,0,0,0.4)", border: `1px solid ${pillColor}55`, borderRadius: 20, fontSize: 11, color: pillColor, fontFamily: "'Space Mono',monospace" }}>
              {rateInfo.remaining}/{rateInfo.max} SEARCHES LEFT TODAY
            </div>
          )}
        </div>

        <h1 style={{ fontSize: "clamp(30px,5.5vw,58px)", fontWeight: 800, lineHeight: 1.05, margin: "0 0 16px", background: "linear-gradient(135deg,#ffffff 0%,#93c5fd 40%,#3b82f6 100%)", WebkitBackgroundClip: "text", WebkitTextFillColor: "transparent" }}>
          Stop Guessing.<br />Start Building.
        </h1>
        <p style={{ color: "#8899bb", fontSize: 15, maxWidth: 520, margin: "0 auto 8px", lineHeight: 1.8 }}>
          Find datasets that actually work for your project — ranked by reliability, research usage, and real-world performance.
        </p>
        <p style={{ color: "#4a5a7a", fontSize: 13, maxWidth: 460, margin: "0 auto 20px", lineHeight: 1.6 }}>
          Don't waste hours on Kaggle or Google. Tell us what you want to build — we'll find the right dataset.
          {!user && <span> <span onClick={() => setPage("register")} style={{ color: "#3b82f6", cursor: "pointer", textDecoration: "underline", fontWeight: 600 }}>Sign up free</span> to save results.</span>}
        </p>

        {/* Trust badges */}
        <div style={{ display: "flex", justifyContent: "center", gap: 8, flexWrap: "wrap", marginBottom: 28 }}>
          {[["🧠","AI-Powered Ranking"],["📊","7 Reliability Factors"],["🔬","Research-Backed"],["👥","Used by Researchers"]].map(([icon, label]) => (
            <div key={label} style={{ padding: "4px 12px", background: "rgba(14,24,58,0.7)", border: "1px solid #1e3a8a33", borderRadius: 20, fontSize: 11, color: "#6b7a9a", display: "flex", alignItems: "center", gap: 5 }}>
              <span>{icon}</span><span>{label}</span>
            </div>
          ))}
        </div>

        {/* Mode toggle */}
        <div style={{ display: "inline-flex", background: "rgba(10,18,40,0.9)", border: "1px solid #1e3a8a", borderRadius: 12, padding: 4, marginBottom: 20, gap: 4 }}>
          {[["search", "🔍 Search Dataset"], ["idea", "💡 Find for My Idea"]].map(([m, label]) => (
            <button key={m} onClick={() => setMode(m)}
              style={{ padding: "8px 20px", borderRadius: 9, border: "none", fontFamily: "'Syne',sans-serif", fontWeight: 700, fontSize: 13, cursor: "pointer", transition: "all 0.2s", background: mode === m ? "linear-gradient(135deg,#1d4ed8,#3b82f6)" : "transparent", color: mode === m ? "#fff" : "#6b7a9a" }}>
              {label}
            </button>
          ))}
        </div>

        {/* Inputs */}
        {mode === "search" ? (
          <div className="search-row" style={{ maxWidth: 700, margin: "0 auto 8px", display: "flex", gap: 10 }}>
            <div style={{ flex: 1, position: "relative" }}>
              <input value={query} onChange={e => setQuery(e.target.value)} onKeyDown={e => e.key === "Enter" && search()}
                placeholder='e.g. "climate data" or "mental health AND depression"'
                disabled={rateInfo?.remaining === 0}
                style={{ ...S.input, paddingLeft: 44, opacity: rateInfo?.remaining === 0 ? 0.5 : 1 }} />
              <span style={{ position: "absolute", left: 14, top: "50%", transform: "translateY(-50%)", fontSize: 18, opacity: 0.35 }}>⌕</span>
            </div>
            <button onClick={() => search(false)} disabled={loading || !query.trim() || rateInfo?.remaining === 0}
              style={{ ...S.btn("primary"), whiteSpace: "nowrap", padding: "12px 24px", fontSize: 14, opacity: (loading || rateInfo?.remaining === 0) ? 0.5 : 1 }}
              onMouseEnter={e => { if (!loading) e.currentTarget.style.transform = "scale(1.03)"; }}
              onMouseLeave={e => e.currentTarget.style.transform = "scale(1)"}>
              {loading ? "Searching..." : "Find My Dataset →"}
            </button>
          </div>
        ) : (
          <div style={{ maxWidth: 700, margin: "0 auto 8px" }}>
            <div style={{ position: "relative", marginBottom: 10 }}>
              <textarea value={ideaQuery} onChange={e => setIdeaQuery(e.target.value)}
                placeholder="e.g. I want to detect depression from Reddit posts in Hindi using NLP..."
                rows={3}
                style={{ ...S.input, resize: "none", lineHeight: 1.6, paddingLeft: 44 }} />
              <span style={{ position: "absolute", left: 14, top: 14, fontSize: 18, opacity: 0.35 }}>💡</span>
            </div>
            <div style={{ display: "flex", gap: 7, justifyContent: "center", flexWrap: "wrap", marginBottom: 10 }}>
              {SAMPLE_IDEAS.map(s => (
                <button key={s} onClick={() => setIdeaQuery(s)}
                  style={{ padding: "5px 12px", background: hoveredSample === s ? "rgba(30,58,138,0.4)" : "rgba(10,20,50,0.8)", border: hoveredSample === s ? "1px solid #3b82f6" : "1px solid #1e3a8a", borderRadius: 20, color: hoveredSample === s ? "#c8d8f0" : "#6fa3ef", fontSize: 11, cursor: "pointer", fontFamily: "'Syne',sans-serif", transition: "all 0.2s" }}
                  onMouseEnter={() => setHoveredSample(s)} onMouseLeave={() => setHoveredSample(null)}>
                  {s}
                </button>
              ))}
            </div>
            <button onClick={() => search(false)} disabled={loading || !ideaQuery.trim() || rateInfo?.remaining === 0}
              style={{ ...S.btn("primary"), padding: "12px 32px", fontSize: 14, width: "100%", opacity: (loading || rateInfo?.remaining === 0) ? 0.5 : 1 }}>
              {loading ? "Analyzing your idea..." : "🧠 Find Best Dataset for My Idea →"}
            </button>
          </div>
        )}

        {/* How it works — only on empty state */}
        {!results && !loading && (
          <div className="how-grid" style={{ maxWidth: 700, margin: "36px auto 0", display: "grid", gridTemplateColumns: "repeat(3,1fr)", gap: 12 }}>
            {[["1","Describe your problem","Type what you want to build or research","🎯"],
              ["2","AI finds best datasets","Ranked by reliability, citations & fit","🤖"],
              ["3","Make a confident choice","Get explanations, not just a list","✅"]].map(([n, title, sub]) => (
              <div key={n} className="hover-lift" style={{ ...S.glass, padding: "16px 14px", textAlign: "center" }}>
                <div style={{ width: 34, height: 34, borderRadius: "50%", background: "linear-gradient(135deg,#1d4ed8,#3b82f6)", display: "flex", alignItems: "center", justifyContent: "center", margin: "0 auto 10px", fontSize: 15, fontWeight: 800, color: "#fff" }}>{n}</div>
                <div style={{ fontSize: 13, fontWeight: 700, color: "#e0e8ff", marginBottom: 4 }}>{title}</div>
                <div style={{ fontSize: 11, color: "#6b7a9a", lineHeight: 1.5 }}>{sub}</div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Interactive reliability factors */}
      {!results && !loading && (
        <div style={{ marginBottom: 48 }}>
          <div style={{ textAlign: "center", marginBottom: 16 }}>
            <p style={{ fontSize: 10, color: "#4a5a7a", fontFamily: "'Space Mono',monospace", letterSpacing: 2, margin: "0 0 6px" }}>HOW WE SCORE DATASETS</p>
            <h2 style={{ fontSize: 20, fontWeight: 800, color: "#e0e8ff", margin: 0 }}>7 Reliability Factors — Click Any to Explore</h2>
          </div>
          <div className="factor-grid" style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 10 }}>
            {Object.entries(FACTOR_META).map(([k, f]) => {
              const isOpen = expandedFactor === k;
              return (
                <div key={k} onClick={() => setExpandedFactor(isOpen ? null : k)}
                  className="hover-glow"
                  style={{ ...S.card, padding: "14px", cursor: "pointer", border: isOpen ? "1px solid #3b82f6" : "1px solid #0d1b3e", transition: "all 0.2s" }}>
                  <div style={{ fontSize: 22, marginBottom: 6 }}>{f.icon}</div>
                  <div style={{ fontSize: 12, fontWeight: 700, color: "#c8d8f0", marginBottom: 3 }}>{f.label}</div>
                  <div style={{ fontSize: 10, color: "#4a5a7a", lineHeight: 1.4 }}>{f.desc}</div>
                  {isOpen && (
                    <div style={{ marginTop: 10, paddingTop: 10, borderTop: "1px solid #1e3a8a", animation: "fadeUp 0.2s ease" }}>
                      <div style={{ fontSize: 11, color: "#6fa3ef", fontWeight: 600, marginBottom: 4 }}>💡 Why it matters</div>
                      <div style={{ fontSize: 11, color: "#8899bb", lineHeight: 1.5, marginBottom: 6 }}>{f.why}</div>
                      <div style={{ fontSize: 10, color: "#FFB800", background: "rgba(255,184,0,0.07)", border: "1px solid rgba(255,184,0,0.2)", borderRadius: 6, padding: "5px 8px" }}>📌 {f.example}</div>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      )}

      {rateInfo?.remaining === 0 && (
        <div style={{ maxWidth: 700, margin: "0 auto 24px", padding: "14px 18px", background: "rgba(255,184,0,0.07)", border: "1px solid rgba(255,184,0,0.3)", borderRadius: 12, textAlign: "center", color: "#FFB800", fontSize: 13 }}>
          🔒 You've used all {rateInfo.max} free searches today. Come back tomorrow!
        </div>
      )}

      {loading && (
        <div style={{ textAlign: "center", padding: "70px 0" }}>
          <div style={{ position: "relative", display: "inline-block", width: 60, height: 60, marginBottom: 18 }}>
            <div style={{ position: "absolute", inset: 0, border: "3px solid #1e3a8a", borderTopColor: "#3b82f6", borderRadius: "50%", animation: "spin 0.8s linear infinite" }} />
            <div style={{ position: "absolute", inset: 9, border: "2px solid #0d1b3e", borderBottomColor: "#6fa3ef", borderRadius: "50%", animation: "spin 1.3s linear infinite reverse" }} />
          </div>
          <p style={{ margin: "0 0 6px", color: "#6fa3ef", fontFamily: "'Space Mono',monospace", fontSize: 12, letterSpacing: 2 }}>
            {mode === "idea" ? "🧠 ANALYZING YOUR IDEA..." : `⚡ SEARCHING FOR "${searched.toUpperCase()}"`}
          </p>
          <p style={{ margin: 0, color: "#3a4a6a", fontFamily: "'Space Mono',monospace", fontSize: 10 }}>Usually takes 2–5 seconds</p>
        </div>
      )}

      {error && (
        <div style={{ padding: 16, background: "rgba(255,90,90,0.08)", border: "1px solid rgba(255,90,90,0.3)", borderRadius: 12, color: "#ff8a8a", fontSize: 13, display: "flex", gap: 10, alignItems: "center" }}>
          <span style={{ fontSize: 20 }}>⚠️</span> {error}
        </div>
      )}

      {/* RESULTS */}
      {filteredResults && (
        <div className="fade-up">
          <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 16, flexWrap: "wrap" }}>
            <span style={{ fontFamily: "'Space Mono',monospace", fontSize: 11, color: "#6b7a9a", letterSpacing: 2 }}>
              {queryType === "AND" ? "🔗 AND SEARCH —" : queryType === "OR" ? "🔀 OR SEARCH —" : "TOP DATASETS FOR"}
            </span>
            <span style={{ padding: "3px 12px", background: "rgba(59,130,246,0.15)", border: "1px solid #3b82f6", borderRadius: 20, fontSize: 12, color: "#6fa3ef", fontWeight: 700 }}>"{searched}"</span>
            {searchMode === "cached" && <span style={{ padding: "3px 9px", background: "rgba(255,184,0,0.1)", border: "1px solid #FFB80044", borderRadius: 20, fontSize: 10, color: "#FFB800", fontFamily: "'Space Mono',monospace" }}>⚡ CACHED</span>}
            {searchMode === "fast" && <span style={{ padding: "3px 9px", background: "rgba(0,229,160,0.1)", border: "1px solid #00E5A044", borderRadius: 20, fontSize: 10, color: "#00E5A0", fontFamily: "'Space Mono',monospace" }}>⚡ FAST</span>}
            {searchMode === "live" && <span style={{ padding: "3px 9px", background: "rgba(59,130,246,0.1)", border: "1px solid #3b82f644", borderRadius: 20, fontSize: 10, color: "#6fa3ef", fontFamily: "'Space Mono',monospace" }}>🌐 LIVE</span>}
            <button onClick={() => search(true)} disabled={loading}
              style={{ marginLeft: "auto", padding: "5px 14px", background: "rgba(30,58,138,0.3)", border: "1px solid #1e3a8a", borderRadius: 20, color: "#6fa3ef", fontSize: 11, cursor: "pointer", fontFamily: "'Space Mono',monospace", transition: "all 0.2s" }}
              onMouseEnter={e => e.currentTarget.style.background = "rgba(30,58,138,0.5)"}
              onMouseLeave={e => e.currentTarget.style.background = "rgba(30,58,138,0.3)"}>
              🌐 Get Latest Data
            </button>
          </div>

          {/* Filters */}
          <div style={{ marginBottom: 20 }}>
            <button onClick={() => setShowFilters(!showFilters)} style={{ ...S.btn("ghost"), padding: "6px 14px", fontSize: 12 }}>
              {showFilters ? "▲ Hide Filters" : "▼ Filter & Sort"}
              {(filterSource || filterLicense || filterMinScore > 0 || sortBy !== "reliability") && <span style={{ marginLeft: 8, padding: "1px 7px", background: "rgba(59,130,246,0.2)", borderRadius: 10, fontSize: 10, color: "#6fa3ef" }}>Active</span>}
            </button>
            {showFilters && (
              <div style={{ marginTop: 10, padding: "14px 16px", ...S.glass }}>
                <div className="filter-row" style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
                  {[
                    ["SORT BY", <select value={sortBy} onChange={e => setSortBy(e.target.value)} style={{ ...S.input, padding: "7px 10px", fontSize: 12 }}>
                      <option value="reliability">Reliability ↓</option>
                      <option value="year">Year (Newest)</option>
                      <option value="size">Size ↓</option>
                    </select>],
                    ["SOURCE", <select value={filterSource} onChange={e => setFilterSource(e.target.value)} style={{ ...S.input, padding: "7px 10px", fontSize: 12 }}>
                      <option value="">All Sources</option>
                      {SOURCES.map(s => <option key={s} value={s}>{s}</option>)}
                    </select>],
                    ["LICENSE", <select value={filterLicense} onChange={e => setFilterLicense(e.target.value)} style={{ ...S.input, padding: "7px 10px", fontSize: 12 }}>
                      <option value="">All Licenses</option>
                      {LICENSES.map(l => <option key={l} value={l}>{l}</option>)}
                    </select>],
                    [`MIN SCORE: ${filterMinScore}/10`, <input type="range" min="0" max="10" step="0.5" value={filterMinScore} onChange={e => setFilterMinScore(Number(e.target.value))} style={{ width: "100%", accentColor: "#3b82f6" }} />],
                  ].map(([label, ctrl]) => (
                    <div key={label} style={{ flex: 1, minWidth: 140 }}>
                      <label style={{ fontSize: 9, color: "#6b7a9a", fontFamily: "'Space Mono',monospace", letterSpacing: 1, display: "block", marginBottom: 5 }}>{label}</label>
                      {ctrl}
                    </div>
                  ))}
                </div>
                <button onClick={() => { setSortBy("reliability"); setFilterSource(""); setFilterLicense(""); setFilterMinScore(0); }} style={{ ...S.btn("ghost"), padding: "5px 12px", fontSize: 11, marginTop: 10 }}>✕ Reset</button>
              </div>
            )}
          </div>

          <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
            {filteredResults.map((ds, i) => {
              const isExp = expanded === i;
              const sc = ds.reliabilityScore;
              const rColor = scoreColor(sc);
              const msg = saveMsg[ds.name];
              const sortedPapers = [...(ds.papers || [])].sort((a, b) => (b.year || 0) - (a.year || 0));

              return (
                <div key={i} className="hover-lift" style={{ ...S.card, border: `1px solid ${i === 0 ? "#1e3a8a" : "#0d1b3e"}` }}>
                  {/* Rank bar */}
                  <div style={{ padding: "7px 20px", background: i === 0 ? "linear-gradient(90deg,rgba(30,58,138,0.6),transparent)" : "rgba(8,14,34,0.5)", display: "flex", alignItems: "center", gap: 10, borderBottom: "1px solid #0d1b3e" }}>
                    <span style={{ fontSize: 14, fontWeight: 900, color: rankColors[Math.min(i, 2)], fontFamily: "monospace" }}>#{i + 1}</span>
                    <span style={{ fontSize: 10, fontWeight: 700, color: rankColors[Math.min(i, 2)], fontFamily: "'Space Mono',monospace", letterSpacing: 2 }}>{rankLabels[Math.min(i, 2)]}</span>
                    <div style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: 8 }}>
                      <span style={{ fontSize: 10, color: "#4a5a7a" }}>Reliability</span>
                      <div style={{ width: 90 }}><ScoreBar score={sc} small animated /></div>
                    </div>
                  </div>

                  <div style={{ padding: "16px 20px" }}>
                    <div className="card-inner" style={{ display: "flex", justifyContent: "space-between", gap: 16, marginBottom: 12 }}>
                      <div style={{ flex: 1 }}>
                        <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6, flexWrap: "wrap" }}>
                          <h2 style={{ margin: 0, fontSize: 18, fontWeight: 800, color: "#e0e8ff" }}>{ds.name}</h2>
                          <span style={{ padding: "2px 9px", background: "rgba(59,130,246,0.12)", border: "1px solid rgba(59,130,246,0.25)", borderRadius: 6, fontSize: 10, color: "#6fa3ef", fontWeight: 600 }}>{ds.source}</span>
                          {ds.year && <span style={{ fontSize: 11, color: "#4a5a7a", fontFamily: "monospace" }}>{ds.year}</span>}
                        </div>
                        <p style={{ margin: 0, color: "#8899bb", fontSize: 13, lineHeight: 1.7 }}>{ds.description}</p>
                      </div>
                      <div style={{ textAlign: "center", flexShrink: 0, padding: "12px 16px", background: "rgba(4,9,22,0.8)", border: `1px solid ${rColor}33`, borderRadius: 12 }}>
                        <div style={{ fontSize: 28, fontWeight: 900, color: rColor, fontFamily: "monospace", lineHeight: 1 }}>{sc}</div>
                        <div style={{ fontSize: 9, color: rColor, fontFamily: "'Space Mono',monospace", letterSpacing: 1 }}>/10</div>
                        <div style={{ fontSize: 9, color: rColor, marginTop: 2, fontWeight: 700 }}>{sc >= 8 ? "Excellent" : sc >= 6 ? "Good" : sc >= 4 ? "Fair" : "Poor"}</div>
                      </div>
                    </div>

                    {/* Metadata */}
                    <div style={{ display: "flex", gap: 7, flexWrap: "wrap", marginBottom: 10 }}>
                      {[["📦 SIZE", ds.size], ["🗂 FORMAT", ds.format], ["📈 CITATIONS", (ds.citedInPapers || 0).toLocaleString()], ["⚖️ LICENSE", ds.license], ["🔄 UPDATED", ds.lastUpdated]].map(([l, v]) => (
                        <div key={l} style={{ padding: "5px 10px", background: "#080f28", border: "1px solid #0d1b3e", borderRadius: 8, transition: "border-color 0.2s" }}
                          onMouseEnter={e => e.currentTarget.style.borderColor = "#1e3a8a"}
                          onMouseLeave={e => e.currentTarget.style.borderColor = "#0d1b3e"}>
                          <div style={{ fontSize: 9, color: "#4a5a7a", fontFamily: "'Space Mono',monospace", letterSpacing: 1, marginBottom: 1 }}>{l}</div>
                          <div style={{ fontSize: 11, fontWeight: 600, color: "#aabcd8" }}>{v}</div>
                        </div>
                      ))}
                    </div>

                    {/* Tags */}
                    {ds.tags && ds.tags.length > 0 && (
                      <div style={{ display: "flex", gap: 5, flexWrap: "wrap", marginBottom: 12 }}>
                        {ds.tags.slice(0, 6).map(tag => (
                          <span key={tag} style={{ padding: "2px 8px", background: "rgba(30,58,138,0.2)", border: "1px solid #1e3a8a33", borderRadius: 20, fontSize: 10, color: "#6b7a9a" }}>#{tag}</span>
                        ))}
                      </div>
                    )}

                    {/* Decision badges */}
                    <div style={{ display: "flex", gap: 7, flexWrap: "wrap", marginBottom: 12 }}>
                      {[
                        { label: ds.sizeBytes > 10e9 ? "Large Dataset" : ds.sizeBytes > 1e9 ? "Medium Dataset" : "Small Dataset", color: ds.sizeBytes > 10e9 ? "#FF8C42" : "#00E5A0" },
                        { label: sc >= 8 ? "Beginner Friendly" : sc >= 6 ? "Intermediate" : "Advanced Users", color: sc >= 8 ? "#00E5A0" : sc >= 6 ? "#FFB800" : "#FF8C42" },
                        { label: ds.format?.includes("CSV") ? "Easy to Load" : "Requires Parsing", color: ds.format?.includes("CSV") ? "#00E5A0" : "#FFB800" },
                      ].map(({ label, color }) => (
                        <span key={label} style={{ padding: "3px 10px", background: `${color}11`, border: `1px solid ${color}44`, borderRadius: 20, fontSize: 11, color, fontWeight: 600 }}>{label}</span>
                      ))}
                    </div>

                    <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                      <button onClick={() => setExpanded(isExp ? null : i)} style={{ ...S.btn("ghost"), padding: "6px 14px", fontSize: 12 }}
                        onMouseEnter={e => e.currentTarget.style.background = "rgba(30,58,138,0.4)"}
                        onMouseLeave={e => e.currentTarget.style.background = "rgba(30,58,138,0.25)"}>
                        {isExp ? "▲ Hide Analysis" : "▼ Full Analysis"}
                      </button>
                      <button onClick={() => saveDataset(ds)} style={{ ...S.btn(msg === "saved" ? "ghost" : "primary"), padding: "6px 14px", fontSize: 12 }}>
                        {msg === "saving" ? "Saving..." : msg === "saved" ? "✓ Saved!" : msg || "💾 Save Dataset"}
                      </button>
                      {user && collections.length > 0 && (
                        <select onChange={e => { if (e.target.value) saveDataset(ds, e.target.value); e.target.value = ""; }}
                          style={{ padding: "6px 10px", background: "#0a1228", border: "1px solid #1e3a8a", borderRadius: 9, color: "#6fa3ef", fontSize: 11, cursor: "pointer" }}>
                          <option value="">+ Collection</option>
                          {collections.map(c => <option key={c._id} value={c._id}>{c.name}</option>)}
                        </select>
                      )}
                    </div>
                  </div>

                  {isExp && (
                    <div style={{ borderTop: "1px solid #0d1b3e", background: "rgba(4,9,22,0.8)", animation: "fadeUp 0.2s ease" }}>

                      {/* Is this right for you? */}
                      <div style={{ padding: "16px 20px", borderBottom: "1px solid #0d1b3e" }}>
                        <h3 style={{ margin: "0 0 12px", fontSize: 11, fontFamily: "'Space Mono',monospace", color: "#6fa3ef", letterSpacing: 2 }}>🎯 IS THIS DATASET RIGHT FOR YOU?</h3>
                        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10, marginBottom: 10 }}>
                          <div style={{ padding: "12px 14px", background: "rgba(0,229,160,0.05)", border: "1px solid rgba(0,229,160,0.2)", borderRadius: 10 }}>
                            <div style={{ fontSize: 11, fontWeight: 700, color: "#00E5A0", marginBottom: 6 }}>✔ Best For</div>
                            {(ds.tags || ["General ML research"]).slice(0, 3).map((t, ti) => (
                              <div key={ti} style={{ fontSize: 12, color: "#8899bb", marginBottom: 3 }}>• {t} tasks</div>
                            ))}
                          </div>
                          <div style={{ padding: "12px 14px", background: "rgba(255,90,90,0.04)", border: "1px solid rgba(255,90,90,0.15)", borderRadius: 10 }}>
                            <div style={{ fontSize: 11, fontWeight: 700, color: "#ff8a8a", marginBottom: 6 }}>⚠ Known Issues</div>
                            <div style={{ fontSize: 12, color: "#8899bb", marginBottom: 3 }}>• May require preprocessing</div>
                            {(ds.reliabilityFactors?.biasAndDiversity || 10) < 7 && <div style={{ fontSize: 12, color: "#8899bb", marginBottom: 3 }}>• Potential representation bias</div>}
                            {ds.lastUpdated && parseInt(ds.lastUpdated) < 2022 && <div style={{ fontSize: 12, color: "#8899bb" }}>• Data may be outdated</div>}
                          </div>
                        </div>
                      </div>

                      {/* Reliability breakdown */}
                      <div style={{ padding: "16px 20px", borderBottom: "1px solid #0d1b3e" }}>
                        <h3 style={{ margin: "0 0 12px", fontSize: 11, fontFamily: "'Space Mono',monospace", color: "#6fa3ef", letterSpacing: 2 }}>📊 RELIABILITY BREAKDOWN</h3>
                        <div className="factor-grid" style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 6 }}>
                          {Object.entries(FACTOR_META).map(([key, meta]) => {
                            const val = (ds.reliabilityFactors || {})[key] || 0;
                            return (
                              <div key={key} style={{ padding: "8px 10px", background: "#080f2a", border: "1px solid #0d1b3e", borderRadius: 8 }}>
                                <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
                                  <span style={{ fontSize: 11, fontWeight: 600, color: "#c8d8f0" }}>{meta.icon} {meta.label}</span>
                                  <span style={{ fontSize: 12, fontWeight: 800, color: scoreColor(val), fontFamily: "monospace" }}>{val}/10</span>
                                </div>
                                <ScoreBar score={val} small animated />
                              </div>
                            );
                          })}
                        </div>
                      </div>

                      {/* Reliability reasons */}
                      <div style={{ padding: "16px 20px", borderBottom: "1px solid #0d1b3e" }}>
                        <h3 style={{ margin: "0 0 10px", fontSize: 11, fontFamily: "'Space Mono',monospace", color: "#6fa3ef", letterSpacing: 2 }}>🛡 KEY RELIABILITY NOTES</h3>
                        <div style={{ display: "flex", flexDirection: "column", gap: 5 }}>
                          {(ds.reliabilityReasons || []).map((r, ri) => (
                            <div key={ri} style={{ display: "flex", gap: 8, padding: "8px 10px", background: "#080f2a", border: "1px solid #0d1b3e", borderRadius: 7 }}>
                              <span style={{ color: "#00E5A0", flexShrink: 0 }}>✓</span>
                              <span style={{ fontSize: 12, color: "#8899bb", lineHeight: 1.5 }}>{r}</span>
                            </div>
                          ))}
                        </div>
                      </div>

                      {/* Papers */}
                      <div style={{ padding: "16px 20px", borderBottom: "1px solid #0d1b3e" }}>
                        <h3 style={{ margin: "0 0 10px", fontSize: 11, fontFamily: "'Space Mono',monospace", color: "#6fa3ef", letterSpacing: 2 }}>📚 RESEARCH PAPERS USING THIS DATASET</h3>
                        <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                          {sortedPapers.map((p, pi) => (
                            <div key={pi} style={{ padding: "10px 12px", background: "#080f2a", border: "1px solid #0d1b3e", borderRadius: 8, transition: "border-color 0.2s" }}
                              onMouseEnter={e => e.currentTarget.style.borderColor = "#1e3a8a"}
                              onMouseLeave={e => e.currentTarget.style.borderColor = "#0d1b3e"}>
                              <div style={{ fontSize: 12, fontWeight: 700, color: "#c8d8f0", marginBottom: 3, lineHeight: 1.5 }}>{p.title}</div>
                              <div style={{ fontSize: 11, color: "#6b7a9a", marginBottom: 5 }}>{p.authors} · {p.year}</div>
                              <div style={{ display: "flex", gap: 6 }}>
                                <span style={{ fontSize: 10, padding: "2px 7px", background: "rgba(59,130,246,0.1)", borderRadius: 4, color: "#6fa3ef" }}>{p.venue}</span>
                                <span style={{ fontSize: 10, padding: "2px 7px", background: "rgba(0,229,160,0.1)", borderRadius: 4, color: "#00E5A0" }}>⭐ {(p.citations || 0).toLocaleString()} citations</span>
                              </div>
                            </div>
                          ))}
                        </div>
                      </div>

                      <div style={{ padding: "14px 20px" }}>
                        <a href={ds.url} target="_blank" rel="noopener noreferrer"
                          style={{ display: "block", padding: "12px", background: "linear-gradient(135deg,#1d4ed8,#3b82f6)", borderRadius: 10, color: "#fff", textDecoration: "none", textAlign: "center", fontWeight: 700, fontSize: 14, transition: "opacity 0.2s" }}
                          onMouseEnter={e => e.currentTarget.style.opacity = "0.85"}
                          onMouseLeave={e => e.currentTarget.style.opacity = "1"}>
                          View Dataset Source & Download →
                        </a>
                      </div>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Empty state */}
      {!loading && !results && !error && mode === "search" && (
        <div style={{ textAlign: "center", marginTop: 48 }}>
          <p style={{ color: "#4a5a7a", fontSize: 11, fontFamily: "'Space Mono',monospace", marginBottom: 12, letterSpacing: 1 }}>— OR TRY A SAMPLE SEARCH —</p>
          <div style={{ display: "flex", justifyContent: "center", gap: 8, flexWrap: "wrap" }}>
            {SAMPLE_IDEAS.map(s => (
              <button key={s} onClick={() => setQuery(s)}
                style={{ padding: "7px 14px", background: hoveredSample === s ? "rgba(30,58,138,0.4)" : "rgba(10,20,50,0.8)", border: hoveredSample === s ? "1px solid #3b82f6" : "1px solid #1e3a8a", borderRadius: 20, color: hoveredSample === s ? "#c8d8f0" : "#6fa3ef", fontSize: 11, cursor: "pointer", fontFamily: "'Syne',sans-serif", transition: "all 0.2s", transform: hoveredSample === s ? "translateY(-2px)" : "none" }}
                onMouseEnter={() => setHoveredSample(s)} onMouseLeave={() => setHoveredSample(null)}>
                {s}
              </button>
            ))}
          </div>
          <p style={{ color: "#2a3a5a", fontSize: 10, fontFamily: "'Space Mono',monospace", marginTop: 12 }}>SUPPORTS AND/OR QUERIES → "mental health AND depression"</p>
        </div>
      )}
    </div>
  );
}

// ════════════════════════════════════════════════════════════════════════════
// ABOUT PAGE
// ════════════════════════════════════════════════════════════════════════════
function AboutPage({ setPage }) {
  return (
    <div style={{ maxWidth: 720, margin: "0 auto", padding: "50px 20px 80px" }}>
      <div style={{ ...S.card, padding: 40 }}>
        <div style={{ textAlign: "center", marginBottom: 36 }}>
          <div style={{ fontSize: 44, marginBottom: 12 }}>⬡</div>
          <h1 style={{ margin: "0 0 8px", fontSize: 30, fontWeight: 800, background: "linear-gradient(135deg,#e0e8ff,#6fa3ef)", WebkitBackgroundClip: "text", WebkitTextFillColor: "transparent" }}>DatasetFinder</h1>
          <p style={{ color: "#6b7a9a", fontSize: 14, margin: "0 0 4px" }}>AI-Powered Dataset Decision Engine</p>
          <p style={{ color: "#4a5a7a", fontSize: 12, fontStyle: "italic", margin: 0 }}>"We don't just find datasets — we help you choose the right one."</p>
        </div>
        {[
          { icon: "🤖", title: "How It Works", body: "DatasetFinder uses Claude AI with real-time Serper web search to find relevant, reliable datasets. Results are scored across 7 scientific reliability factors for transparent, research-backed rankings." },
          { icon: "📊", title: "7-Factor Reliability System", body: "Every dataset is scored /10 across: Citation Count, Source Credibility, Documentation, License Clarity, Community Adoption, Maintenance, and Bias & Diversity." },
          { icon: "💡", title: "Idea-Based Search", body: 'Use "Find Dataset for My Idea" to describe your project in plain English. Our AI extracts your task type, domain, and requirements, then matches and explains the best datasets for your use case.' },
          { icon: "🔒", title: "Your Data", body: "Your saved datasets and collections are stored securely in MongoDB. We never sell your data. Guest users can search up to 5 times per day for free." },
        ].map(item => (
          <div key={item.title} className="hover-lift" style={{ display: "flex", gap: 16, marginBottom: 16, padding: 18, background: "rgba(4,9,22,0.6)", border: "1px solid #0d1b3e", borderRadius: 12 }}>
            <span style={{ fontSize: 24, flexShrink: 0 }}>{item.icon}</span>
            <div>
              <div style={{ fontSize: 15, fontWeight: 700, color: "#e0e8ff", marginBottom: 5 }}>{item.title}</div>
              <div style={{ fontSize: 13, color: "#8899bb", lineHeight: 1.65 }}>{item.body}</div>
            </div>
          </div>
        ))}
        <div style={{ textAlign: "center", marginTop: 28 }}>
          <button onClick={() => setPage("search")} style={{ ...S.btn("primary"), padding: "12px 32px", fontSize: 14 }}>🔍 Start Finding Datasets</button>
        </div>
      </div>
    </div>
  );
}

// ════════════════════════════════════════════════════════════════════════════
// LOGIN
// ════════════════════════════════════════════════════════════════════════════
function LoginPage({ setUser, setPage }) {
  const [email, setEmail] = useState(""); const [password, setPassword] = useState("");
  const [error, setError] = useState(""); const [loading, setLoading] = useState(false);
  const login = async () => {
    setLoading(true); setError("");
    try {
      const res = await apiFetch("/auth/login", { method: "POST", body: JSON.stringify({ email, password }) });
      const data = await res.json();
      if (!res.ok) { setError(data.error || "Login failed"); return; }
      setToken(data.token); setUser(data.user); setPage("search");
    } catch { setError("Network error"); } finally { setLoading(false); }
  };
  return (
    <div style={{ maxWidth: 420, margin: "50px auto", padding: "0 20px" }}>
      <div style={{ ...S.card, padding: 32 }}>
        <h2 style={{ margin: "0 0 5px", fontSize: 24, fontWeight: 800, color: "#e0e8ff" }}>Welcome back</h2>
        <p style={{ margin: "0 0 22px", color: "#6b7a9a", fontSize: 13 }}>Sign in to access your saved datasets</p>
        {error && <div style={{ padding: "10px 14px", background: "rgba(255,90,90,0.1)", border: "1px solid rgba(255,90,90,0.3)", borderRadius: 9, color: "#ff8a8a", fontSize: 12, marginBottom: 16 }}>{error}</div>}
        <div style={{ display: "flex", flexDirection: "column", gap: 10, marginBottom: 16 }}>
          <input value={email} onChange={e => setEmail(e.target.value)} placeholder="Email address" type="email" style={S.input} />
          <input value={password} onChange={e => setPassword(e.target.value)} placeholder="Password" type="password" style={S.input} onKeyDown={e => e.key === "Enter" && login()} />
        </div>
        <button onClick={login} disabled={loading} style={{ ...S.btn("primary"), width: "100%", marginBottom: 14, padding: "12px" }}>{loading ? "Signing in..." : "Sign In"}</button>
        <div style={{ textAlign: "center", marginBottom: 14 }}><span style={{ color: "#4a5a7a", fontSize: 11 }}>or continue with</span></div>
        <div style={{ display: "flex", gap: 8, marginBottom: 16 }}>
          <a href={`${API_BASE}/auth/google`} style={{ ...S.btn("ghost"), flex: 1, textAlign: "center", textDecoration: "none", display: "block", padding: "10px", fontSize: 12 }}>🔵 Google</a>
          <a href={`${API_BASE}/auth/github`} style={{ ...S.btn("ghost"), flex: 1, textAlign: "center", textDecoration: "none", display: "block", padding: "10px", fontSize: 12 }}>⚫ GitHub</a>
        </div>
        <p style={{ textAlign: "center", color: "#6b7a9a", fontSize: 12, margin: 0 }}>No account? <span onClick={() => setPage("register")} style={{ color: "#6fa3ef", cursor: "pointer", textDecoration: "underline" }}>Sign up free</span></p>
      </div>
    </div>
  );
}

// ════════════════════════════════════════════════════════════════════════════
// REGISTER
// ════════════════════════════════════════════════════════════════════════════
function RegisterPage({ setUser, setPage }) {
  const [name, setName] = useState(""); const [email, setEmail] = useState("");
  const [password, setPassword] = useState(""); const [error, setError] = useState(""); const [loading, setLoading] = useState(false);
  const register = async () => {
    setLoading(true); setError("");
    try {
      const res = await apiFetch("/auth/register", { method: "POST", body: JSON.stringify({ name, email, password }) });
      const data = await res.json();
      if (!res.ok) { setError(data.error || "Registration failed"); return; }
      setToken(data.token); setUser(data.user); setPage("search");
    } catch { setError("Network error"); } finally { setLoading(false); }
  };
  return (
    <div style={{ maxWidth: 420, margin: "50px auto", padding: "0 20px" }}>
      <div style={{ ...S.card, padding: 32 }}>
        <h2 style={{ margin: "0 0 5px", fontSize: 24, fontWeight: 800, color: "#e0e8ff" }}>Create account</h2>
        <p style={{ margin: "0 0 22px", color: "#6b7a9a", fontSize: 13 }}>Save datasets, create collections, add notes</p>
        {error && <div style={{ padding: "10px 14px", background: "rgba(255,90,90,0.1)", border: "1px solid rgba(255,90,90,0.3)", borderRadius: 9, color: "#ff8a8a", fontSize: 12, marginBottom: 16 }}>{error}</div>}
        <div style={{ display: "flex", flexDirection: "column", gap: 10, marginBottom: 16 }}>
          <input value={name} onChange={e => setName(e.target.value)} placeholder="Full name" style={S.input} />
          <input value={email} onChange={e => setEmail(e.target.value)} placeholder="Email address" type="email" style={S.input} />
          <input value={password} onChange={e => setPassword(e.target.value)} placeholder="Password (min 6 chars)" type="password" style={S.input} onKeyDown={e => e.key === "Enter" && register()} />
        </div>
        <button onClick={register} disabled={loading} style={{ ...S.btn("primary"), width: "100%", marginBottom: 14, padding: "12px" }}>{loading ? "Creating..." : "Create Account"}</button>
        <div style={{ textAlign: "center", marginBottom: 14 }}><span style={{ color: "#4a5a7a", fontSize: 11 }}>or continue with</span></div>
        <div style={{ display: "flex", gap: 8, marginBottom: 16 }}>
          <a href={`${API_BASE}/auth/google`} style={{ ...S.btn("ghost"), flex: 1, textAlign: "center", textDecoration: "none", display: "block", padding: "10px", fontSize: 12 }}>🔵 Google</a>
          <a href={`${API_BASE}/auth/github`} style={{ ...S.btn("ghost"), flex: 1, textAlign: "center", textDecoration: "none", display: "block", padding: "10px", fontSize: 12 }}>⚫ GitHub</a>
        </div>
        <p style={{ textAlign: "center", color: "#6b7a9a", fontSize: 12, margin: 0 }}>Have an account? <span onClick={() => setPage("login")} style={{ color: "#6fa3ef", cursor: "pointer", textDecoration: "underline" }}>Sign in</span></p>
      </div>
    </div>
  );
}

// ════════════════════════════════════════════════════════════════════════════
// SAVED PAGE
// ════════════════════════════════════════════════════════════════════════════
function SavedPage({ user, setPage }) {
  const [collections, setCols] = useState([]); const [datasets, setDatasets] = useState([]);
  const [activeCol, setActiveCol] = useState(null); const [newColName, setNewColName] = useState("");
  const [editingNote, setEditNote] = useState(null); const [noteText, setNoteText] = useState("");
  const [loading, setLoading] = useState(true); const [expanded, setExpanded] = useState(null);

  useEffect(() => { if (!user) { setPage("login"); return; } loadAll(); }, [user]);

  const loadAll = async () => {
    setLoading(true);
    const [cols, ds] = await Promise.all([apiFetch("/api/collections").then(r => r.json()), apiFetch("/api/saved").then(r => r.json())]);
    setCols(Array.isArray(cols) ? cols : []); setDatasets(Array.isArray(ds) ? ds : []); setLoading(false);
  };
  const createCol = async () => {
    if (!newColName.trim()) return;
    const res = await apiFetch("/api/collections", { method: "POST", body: JSON.stringify({ name: newColName.trim() }) });
    const col = await res.json(); setCols(p => [col, ...p]); setNewColName("");
  };
  const deleteCol = async (id) => { await apiFetch(`/api/collections/${id}`, { method: "DELETE" }); setCols(p => p.filter(c => c._id !== id)); setDatasets(p => p.filter(d => d.collectionId !== id)); if (activeCol === id) setActiveCol(null); };
  const deleteDs = async (id) => { await apiFetch(`/api/saved/${id}`, { method: "DELETE" }); setDatasets(p => p.filter(d => d._id !== id)); };
  const saveNote = async (id) => { await apiFetch(`/api/saved/${id}/notes`, { method: "PUT", body: JSON.stringify({ notes: noteText }) }); setDatasets(p => p.map(d => d._id === id ? { ...d, notes: noteText } : d)); setEditNote(null); };

  const filtered = activeCol ? datasets.filter(d => d.collectionId === activeCol) : datasets;
  if (loading) return <Loading />;

  return (
    <div style={{ maxWidth: 960, width: "100%", margin: "0 auto", padding: "40px 20px 80px" }}>
      <div style={{ display: "flex", alignItems: "center", gap: 14, marginBottom: 28 }}>
        <h1 style={{ margin: 0, fontSize: 26, fontWeight: 800, color: "#e0e8ff" }}>📚 My Library</h1>
        <span style={{ padding: "3px 12px", background: "rgba(59,130,246,0.15)", border: "1px solid #3b82f6", borderRadius: 20, fontSize: 11, color: "#6fa3ef" }}>{datasets.length} saved</span>
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "210px 1fr", gap: 16 }}>
        <div>
          <div style={{ ...S.card, padding: 14, marginBottom: 10 }}>
            <p style={{ margin: "0 0 8px", fontSize: 10, color: "#6b7a9a", fontFamily: "'Space Mono',monospace", letterSpacing: 1 }}>COLLECTIONS</p>
            <div onClick={() => setActiveCol(null)} style={{ padding: "7px 10px", borderRadius: 8, cursor: "pointer", background: !activeCol ? "rgba(59,130,246,0.15)" : "transparent", border: !activeCol ? "1px solid #3b82f640" : "1px solid transparent", marginBottom: 3, fontSize: 12, color: !activeCol ? "#6fa3ef" : "#8899bb", transition: "all 0.15s" }}>📋 All ({datasets.length})</div>
            {collections.map(col => (
              <div key={col._id} onClick={() => setActiveCol(col._id)} style={{ padding: "7px 10px", borderRadius: 8, cursor: "pointer", background: activeCol === col._id ? "rgba(59,130,246,0.15)" : "transparent", border: activeCol === col._id ? "1px solid #3b82f640" : "1px solid transparent", marginBottom: 3, display: "flex", justifyContent: "space-between", alignItems: "center", transition: "all 0.15s" }}>
                <span style={{ fontSize: 12, color: activeCol === col._id ? "#6fa3ef" : "#8899bb" }}>📁 {col.name} ({datasets.filter(d => d.collectionId === col._id).length})</span>
                <button onClick={e => { e.stopPropagation(); deleteCol(col._id); }} style={{ background: "none", border: "none", color: "#4a5a7a", cursor: "pointer", fontSize: 11 }}>✕</button>
              </div>
            ))}
          </div>
          <div style={{ ...S.card, padding: 12 }}>
            <p style={{ margin: "0 0 7px", fontSize: 10, color: "#6b7a9a", fontFamily: "'Space Mono',monospace" }}>NEW COLLECTION</p>
            <input value={newColName} onChange={e => setNewColName(e.target.value)} placeholder="Collection name..." style={{ ...S.input, fontSize: 11, padding: "7px 9px", marginBottom: 7 }} onKeyDown={e => e.key === "Enter" && createCol()} />
            <button onClick={createCol} style={{ ...S.btn("primary"), width: "100%", padding: "7px", fontSize: 11 }}>+ Create</button>
          </div>
        </div>
        <div>
          {filtered.length === 0 ? (
            <div style={{ ...S.card, padding: 40, textAlign: "center" }}>
              <div style={{ fontSize: 36, marginBottom: 12 }}>📭</div>
              <p style={{ color: "#4a5a7a", fontSize: 14, margin: "0 0 16px" }}>No datasets saved yet.</p>
              <button onClick={() => setPage("search")} style={S.btn("primary")}>🔍 Search Datasets</button>
            </div>
          ) : (
            <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
              {filtered.map(ds => {
                const isExp = expanded === ds._id;
                const rColor = scoreColor(ds.reliabilityScore);
                return (
                  <div key={ds._id} className="hover-lift" style={S.card}>
                    <div style={{ padding: "14px 18px" }}>
                      <div style={{ display: "flex", alignItems: "center", gap: 7, marginBottom: 4, flexWrap: "wrap" }}>
                        <h3 style={{ margin: 0, fontSize: 15, fontWeight: 700, color: "#e0e8ff" }}>{ds.name}</h3>
                        <span style={{ padding: "1px 7px", background: "rgba(59,130,246,0.12)", border: "1px solid rgba(59,130,246,0.25)", borderRadius: 4, fontSize: 10, color: "#6fa3ef" }}>{ds.source}</span>
                        <span style={{ fontSize: 12, fontWeight: 800, color: rColor }}>{ds.reliabilityScore}/10</span>
                      </div>
                      <p style={{ margin: "0 0 8px", color: "#8899bb", fontSize: 12, lineHeight: 1.55 }}>{ds.description?.slice(0, 130)}...</p>
                      {ds.notes && <div style={{ padding: "5px 10px", background: "rgba(255,184,0,0.07)", border: "1px solid rgba(255,184,0,0.2)", borderRadius: 6, fontSize: 11, color: "#FFB800", marginBottom: 6 }}>📝 {ds.notes}</div>}
                      <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
                        <button onClick={() => setExpanded(isExp ? null : ds._id)} style={{ ...S.btn("ghost"), padding: "4px 10px", fontSize: 10 }}>{isExp ? "▲ Hide" : "▼ Details"}</button>
                        <button onClick={() => { setEditNote(ds._id); setNoteText(ds.notes || ""); }} style={{ ...S.btn("ghost"), padding: "4px 10px", fontSize: 10 }}>📝 {ds.notes ? "Edit Note" : "Add Note"}</button>
                        <button onClick={() => downloadPdfWithAuth(ds._id)} style={{ ...S.btn("ghost"), padding: "4px 10px", fontSize: 10 }}>📄 PDF</button>
                        <button onClick={() => deleteDs(ds._id)} style={{ ...S.btn("danger"), padding: "4px 10px", fontSize: 10 }}>🗑 Remove</button>
                      </div>
                      {editingNote === ds._id && (
                        <div style={{ marginTop: 10 }}>
                          <textarea value={noteText} onChange={e => setNoteText(e.target.value)} placeholder="Add your notes..." style={{ ...S.input, height: 72, resize: "vertical" }} />
                          <div style={{ display: "flex", gap: 7, marginTop: 7 }}>
                            <button onClick={() => saveNote(ds._id)} style={{ ...S.btn("primary"), padding: "5px 12px", fontSize: 11 }}>Save</button>
                            <button onClick={() => setEditNote(null)} style={{ ...S.btn("ghost"), padding: "5px 12px", fontSize: 11 }}>Cancel</button>
                          </div>
                        </div>
                      )}
                    </div>
                    {isExp && (
                      <div style={{ borderTop: "1px solid #0d1b3e", padding: "12px 18px", background: "rgba(4,9,22,0.7)" }}>
                        <div style={{ display: "flex", gap: 7, flexWrap: "wrap", marginBottom: 10 }}>
                          {[["SIZE", ds.size], ["FORMAT", ds.format], ["LICENSE", ds.license], ["UPDATED", ds.lastUpdated]].map(([l, v]) => (
                            <div key={l} style={{ padding: "5px 9px", background: "#0a1228", border: "1px solid #0d1b3e", borderRadius: 6 }}>
                              <div style={{ fontSize: 9, color: "#4a5a7a", fontFamily: "'Space Mono',monospace" }}>{l}</div>
                              <div style={{ fontSize: 11, color: "#aabcd8", fontWeight: 600 }}>{v}</div>
                            </div>
                          ))}
                        </div>
                        <a href={ds.url} target="_blank" rel="noopener noreferrer" style={{ display: "block", marginTop: 8, padding: "9px", background: "linear-gradient(135deg,#1d4ed8,#3b82f6)", borderRadius: 8, color: "#fff", textDecoration: "none", textAlign: "center", fontWeight: 700, fontSize: 12 }}>
                          View Dataset Source →
                        </a>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function Loading() {
  return (
    <div style={{ display: "flex", justifyContent: "center", alignItems: "center", height: "60vh", flexDirection: "column", gap: 16 }}>
      <div style={{ position: "relative", width: 48, height: 48 }}>
        <div style={{ position: "absolute", inset: 0, border: "3px solid #1e3a8a", borderTopColor: "#3b82f6", borderRadius: "50%", animation: "spin 0.8s linear infinite" }} />
        <div style={{ position: "absolute", inset: 9, border: "2px solid #0d1b3e", borderBottomColor: "#6fa3ef", borderRadius: "50%", animation: "spin 1.2s linear infinite reverse" }} />
      </div>
      <style>{`@keyframes spin { to { transform:rotate(360deg); } }`}</style>
    </div>
  );
}
