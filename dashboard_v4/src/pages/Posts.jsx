import { useState, useEffect, useCallback, useRef, useMemo } from "react";
import { fetchPosts, fmt, ago, getSentiment, sentColor, priorityBadge, TIME_RANGES } from "../api.js";

const POLL = 8000;

function TimeRangePicker({ value, onChange }) {
  return (
    <div style={{ display: "flex", gap: 3, flexWrap: "wrap" }}>
      {TIME_RANGES.map(r => (
        <button key={r.label} onClick={() => onChange(r)} style={{
          padding: "3px 9px", borderRadius: 4, fontSize: 9, cursor: "pointer",
          fontFamily: "'JetBrains Mono', monospace", fontWeight: 600,
          border: "1px solid transparent", transition: "all 0.15s",
          background: value.label === r.label ? "#ff4500" : "rgba(255,255,255,0.04)",
          color:      value.label === r.label ? "#fff"    : "#4a5568",
        }}>{r.label}</button>
      ))}
    </div>
  );
}

// Plain div row — no framer-motion, no re-animation on every poll
function PostRow({ post, selected, onClick, prevScore }) {
  const badge    = priorityBadge(post.poll_priority);
  const momentum = post.momentum || 0;
  const score    = post.current_score || 0;
  const delta    = prevScore != null ? score - prevScore : 0;
  const sent     = getSentiment(post);
  const vel      = post.score_velocity || 0;

  return (
    <div onClick={onClick} style={{
      padding: "10px 14px",
      borderBottom: "1px solid rgba(255,255,255,0.04)",
      cursor: "pointer",
      background: selected ? "rgba(255,69,0,0.06)" : "transparent",
      borderLeft: selected ? "2px solid #ff4500" : "2px solid transparent",
      position: "relative",
      transition: "background 0.1s",
    }}
    onMouseEnter={e => { if (!selected) e.currentTarget.style.background = "rgba(255,255,255,0.02)"; }}
    onMouseLeave={e => { if (!selected) e.currentTarget.style.background = "transparent"; }}
    >
      <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 3 }}>
        <span style={{ color: "#ff4500", fontSize: 9, letterSpacing: "0.05em" }}>r/{post.subreddit}</span>
        <span className="badge" style={{ background: badge.bg, color: badge.color }}>{badge.label}</span>
        {vel > 0.01 && (
          <span style={{ background: "rgba(62,207,116,0.12)", border: "1px solid rgba(62,207,116,0.25)",
            color: "#3ecf74", fontSize: 8, padding: "1px 5px", borderRadius: 3,
            fontFamily: "'JetBrains Mono', monospace" }}>
            ▲{vel.toFixed(2)}/s
          </span>
        )}
        {post.is_trending && (
          <span style={{ background: "rgba(246,173,85,0.12)", border: "1px solid rgba(246,173,85,0.25)",
            color: "#f6ad55", fontSize: 8, padding: "1px 5px", borderRadius: 3 }}>TRENDING</span>
        )}
        <span style={{ marginLeft: "auto", color: sentColor(sent), fontSize: 9 }}>
          {sent > 0 ? "+" : ""}{sent.toFixed(2)}
        </span>
      </div>
      <div style={{
        fontFamily: "'Syne', sans-serif", fontSize: 11, fontWeight: 600,
        lineHeight: 1.35, color: "#dde1e8", marginBottom: 4,
        overflow: "hidden", display: "-webkit-box",
        WebkitLineClamp: 2, WebkitBoxOrient: "vertical",
      }}>{post.title}</div>
      <div style={{ display: "flex", gap: 10, color: "#4a5568", fontSize: 9, alignItems: "center" }}>
        <span style={{ color: delta > 0 ? "#3ecf74" : delta < 0 ? "#f56565" : "#4a5568" }}>
          ▲{fmt(score)}{delta !== 0 && <span style={{ marginLeft: 2 }}>({delta > 0 ? "+" : ""}{delta})</span>}
        </span>
        <span>💬{fmt(post.current_comments)}</span>
        <span>{ago(post.created_utc)}</span>
        <span style={{ marginLeft: "auto", color: "#2d3748" }}>{momentum.toFixed(1)}/min</span>
      </div>
    </div>
  );
}

function PostDetail({ post, onClose }) {
  if (!post) return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "center",
      justifyContent: "center", height: "100%", color: "#2d3748", gap: 10 }}>
      <div style={{ fontSize: 28 }}>◌</div>
      <div style={{ fontSize: 10, letterSpacing: "0.1em" }}>SELECT A POST</div>
    </div>
  );

  const sent  = getSentiment(post);
  const badge = priorityBadge(post.poll_priority);
  const vel   = post.score_velocity || 0;

  const rows = [
    ["Score",        `▲ ${fmt(post.current_score)}`],
    ["Comments",     `💬 ${fmt(post.current_comments)}`],
    ["Upvote Ratio", `${Math.round((post.current_ratio || 0) * 100)}%`],
    ["Velocity",     vel > 0 ? `▲${vel.toFixed(4)}/s` : "—"],
    ["Momentum",     `${(post.momentum || 0).toFixed(3)}/min`],
    ["Engagement",   fmt(post.engagement_score)],
    ["Age",          `${Math.round(post.age_minutes || 0)}m`],
    ["Author",       `u/${post.author || "—"}`],
    ["Priority",     post.poll_priority || "—"],
    ["Trending",     post.is_trending ? "✓ Yes" : "✗ No"],
    ["Trending Score", (post.trending_score || 0).toFixed(4)],
    ["First seen",   ago(post.first_seen_at || post.created_utc)],
  ];

  return (
    <div style={{ padding: 18, height: "100%", overflowY: "auto" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 14 }}>
        <div>
          <div style={{ color: "#ff4500", fontSize: 9, marginBottom: 5, letterSpacing: "0.05em" }}>r/{post.subreddit}</div>
          <span className="badge" style={{ background: badge.bg, color: badge.color }}>{badge.label}</span>
        </div>
        <button onClick={onClose} style={{ background: "none", border: "none", color: "#4a5568", cursor: "pointer", fontSize: 14, padding: 4 }}>✕</button>
      </div>

      <div style={{ fontFamily: "'Syne', sans-serif", fontSize: 14, fontWeight: 700,
        lineHeight: 1.45, color: "#dde1e8", marginBottom: 18 }}>{post.title}</div>

      {vel > 0 && (
        <div className="card" style={{ marginBottom: 10, padding: 12 }}>
          <div style={{ display: "flex", justifyContent: "space-between", fontSize: 9, color: "#4a5568", marginBottom: 6 }}>
            <span>VELOCITY</span>
            <span style={{ color: "#3ecf74", fontWeight: 700 }}>▲{vel.toFixed(4)}/sec</span>
          </div>
          <div style={{ height: 3, background: "rgba(255,255,255,0.04)", borderRadius: 2, overflow: "hidden" }}>
            <div style={{ height: "100%", width: `${Math.min(vel * 20, 100)}%`,
              background: "linear-gradient(90deg, #3ecf74, #63b3ed)", borderRadius: 2,
              transition: "width 0.6s ease" }} />
          </div>
        </div>
      )}

      <div className="card" style={{ marginBottom: 14 }}>
        {rows.map(([label, val], i) => (
          <div key={label} style={{
            display: "flex", justifyContent: "space-between", alignItems: "center",
            padding: "8px 14px",
            borderBottom: i < rows.length - 1 ? "1px solid rgba(255,255,255,0.04)" : "none",
            fontSize: 10,
          }}>
            <span style={{ color: "#4a5568" }}>{label}</span>
            <span style={{ fontFamily: "'JetBrains Mono', monospace", fontWeight: 500 }}>{val}</span>
          </div>
        ))}
      </div>

      <div className="card" style={{ padding: 14, marginBottom: 14 }}>
        <div style={{ display: "flex", justifyContent: "space-between", fontSize: 9, color: "#4a5568", marginBottom: 8 }}>
          <span>SENTIMENT</span>
          <span style={{ color: sentColor(sent), fontWeight: 700 }}>{sent > 0 ? "+" : ""}{sent.toFixed(3)}</span>
        </div>
        <div style={{ height: 5, borderRadius: 3,
          background: "linear-gradient(90deg, #f56565 0%, #2d3748 50%, #3ecf74 100%)", position: "relative" }}>
          <div style={{
            position: "absolute", top: "50%", transform: "translate(-50%, -50%)",
            left: `${((sent + 1) / 2) * 100}%`,
            width: 12, height: 12, borderRadius: "50%",
            background: "#0d1117", border: "2px solid #ff4500",
            transition: "left 0.4s ease",
          }} />
        </div>
        <div style={{ display: "flex", justifyContent: "space-between", fontSize: 8, color: "#2d3748", marginTop: 5 }}>
          <span>NEG</span><span>NEUTRAL</span><span>POS</span>
        </div>
      </div>

      <a href={`https://reddit.com/r/${post.subreddit}/comments/${post.id}`}
        target="_blank" rel="noopener noreferrer"
        style={{
          display: "block", padding: "9px 14px",
          background: "rgba(255,69,0,0.08)", border: "1px solid rgba(255,69,0,0.2)",
          borderRadius: 8, color: "#ff6b35", fontSize: 10, textDecoration: "none",
          textAlign: "center", fontFamily: "'Syne', sans-serif", fontWeight: 600,
          letterSpacing: "0.08em",
        }}>VIEW ON REDDIT ↗</a>
    </div>
  );
}

export default function PostsPage({ onCountChange }) {
  const [posts,     setPosts]    = useState([]);
  const [selected,  setSelected] = useState(null);
  const [search,    setSearch]   = useState("");
  const [subFilter, setSub]      = useState("all");
  const [sortBy,    setSort]     = useState("score");
  const [loading,   setLoading]  = useState(true);
  const [timeRange, setTimeRange] = useState(TIME_RANGES[3]);
  const prevScores = useRef({});
  const abortRef   = useRef(null);

  const load = useCallback(async () => {
    if (abortRef.current) abortRef.current.abort();
    abortRef.current = new AbortController();
    try {
      const res  = await fetchPosts(null, timeRange.hours);
      const data = res.data?.results ?? res.data ?? [];
      // Only update prevScores, don't cause re-render storm
      const next = {};
      data.forEach(p => { next[p.id] = prevScores.current[p.id] ?? p.current_score; });
      setPosts(prev => {
        prevScores.current = next;
        return data;
      });
      onCountChange?.(data.length);
      setLoading(false);
    } catch (e) {
      if (e.name !== "CanceledError") { console.error(e); setLoading(false); }
    }
  }, [timeRange, onCountChange]);

  useEffect(() => {
    setLoading(true);
    load();
    const t = setInterval(load, POLL);
    return () => { clearInterval(t); abortRef.current?.abort(); };
  }, [load]);

  const subreddits = useMemo(() =>
    ["all", ...new Set(posts.map(p => p.subreddit).filter(Boolean))].sort(),
    [posts]
  );

  const filtered = useMemo(() => {
    let list = posts;
    if (subFilter !== "all") list = list.filter(p => p.subreddit === subFilter);
    if (search) {
      const q = search.toLowerCase();
      list = list.filter(p =>
        p.title?.toLowerCase().includes(q) ||
        p.subreddit?.toLowerCase().includes(q) ||
        p.author?.toLowerCase().includes(q)
      );
    }
    return [...list].sort((a, b) => {
      if (sortBy === "score")     return (b.current_score || 0) - (a.current_score || 0);
      if (sortBy === "comments")  return (b.current_comments || 0) - (a.current_comments || 0);
      if (sortBy === "momentum")  return (b.momentum || 0) - (a.momentum || 0);
      if (sortBy === "velocity")  return (b.score_velocity || 0) - (a.score_velocity || 0);
      if (sortBy === "trending")  return (b.trending_score || 0) - (a.trending_score || 0);
      if (sortBy === "sentiment") return getSentiment(b) - getSentiment(a);
      if (sortBy === "new")       return new Date(b.created_utc) - new Date(a.created_utc);
      return 0;
    });
  }, [posts, subFilter, search, sortBy]);

  const hotPosts      = useMemo(() => [...posts].sort((a,b) => (b.momentum||0)-(a.momentum||0)).slice(0,5), [posts]);
  const trendingCount = useMemo(() => posts.filter(p => p.is_trending).length, [posts]);
  const fastestPost   = useMemo(() => [...posts].sort((a,b) => (b.score_velocity||0)-(a.score_velocity||0))[0], [posts]);
  const topScore      = useMemo(() => [...posts].sort((a,b) => (b.current_score||0)-(a.current_score||0))[0], [posts]);

  const inputStyle = {
    background: "var(--surface)", border: "1px solid rgba(255,255,255,0.06)",
    borderRadius: 6, color: "var(--text)", fontFamily: "'JetBrains Mono', monospace",
    fontSize: 10, outline: "none", padding: "6px 10px",
  };

  return (
    <div style={{ display: "grid", gridTemplateColumns: "280px 1fr 280px", height: "100%", overflow: "hidden" }}>

      {/* LEFT: FEED */}
      <div style={{ borderRight: "1px solid rgba(255,255,255,0.04)", display: "flex", flexDirection: "column", overflow: "hidden" }}>
        <div style={{ padding: "12px 12px 10px", borderBottom: "1px solid rgba(255,255,255,0.04)", flexShrink: 0 }}>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 7 }}>
            <span style={{ fontFamily: "'Syne', sans-serif", fontSize: 9, fontWeight: 700,
              letterSpacing: "0.14em", textTransform: "uppercase", color: "#4a5568" }}>Live Feed</span>
            <span style={{ color: "#ff4500", fontSize: 9 }}>{filtered.length}</span>
          </div>
          <div style={{ marginBottom: 7 }}>
            <TimeRangePicker value={timeRange} onChange={setTimeRange} />
          </div>
          <input style={{ ...inputStyle, width: "100%", marginBottom: 6 }}
            placeholder="Search posts, subs, authors…"
            value={search} onChange={e => setSearch(e.target.value)} />
          <div style={{ display: "flex", gap: 5 }}>
            <select style={{ ...inputStyle, flex: 1 }} value={subFilter} onChange={e => setSub(e.target.value)}>
              {subreddits.map(s => <option key={s} value={s}>{s === "all" ? "All subs" : `r/${s}`}</option>)}
            </select>
            <select style={{ ...inputStyle, flex: 1 }} value={sortBy} onChange={e => setSort(e.target.value)}>
              <option value="score">Score</option>
              <option value="comments">Comments</option>
              <option value="momentum">Momentum</option>
              <option value="velocity">Velocity ▲</option>
              <option value="trending">Trending</option>
              <option value="sentiment">Sentiment</option>
              <option value="new">Newest</option>
            </select>
          </div>
        </div>
        <div style={{ flex: 1, overflowY: "auto" }}>
          {loading ? (
            Array(8).fill(0).map((_, i) => (
              <div key={i} style={{ padding: "10px 14px", borderBottom: "1px solid rgba(255,255,255,0.04)" }}>
                <div className="skeleton" style={{ height: 8, width: "35%", marginBottom: 7 }} />
                <div className="skeleton" style={{ height: 11, width: "88%", marginBottom: 4 }} />
                <div className="skeleton" style={{ height: 8, width: "55%"  }} />
              </div>
            ))
          ) : filtered.length === 0 ? (
            <div style={{ display: "flex", flexDirection: "column", alignItems: "center",
              justifyContent: "center", height: 180, color: "#2d3748", gap: 8 }}>
              <div style={{ fontSize: 24 }}>◌</div>
              <div style={{ fontSize: 10 }}>No posts found</div>
            </div>
          ) : filtered.map(p => (
            <PostRow key={p.id} post={p} selected={selected?.id === p.id}
              prevScore={prevScores.current[p.id]}
              onClick={() => setSelected(sel => sel?.id === p.id ? null : p)} />
          ))}
        </div>
      </div>

      {/* CENTER */}
      <div style={{ overflowY: "auto", padding: "18px 18px" }}>

        {/* KPI strip */}
        <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 8, marginBottom: 16 }}>
          {[
            { label: "Posts",        val: fmt(posts.length),       sub: `last ${timeRange.label}`,                         col: "#dde1e8" },
            { label: "Trending",     val: trendingCount,           sub: "active",                                          col: "#f6ad55" },
            { label: "Top Score",    val: fmt(topScore?.current_score), sub: topScore?.subreddit ? `r/${topScore.subreddit}` : "—", col: "#3ecf74" },
            { label: "Fastest ▲",   val: `${(fastestPost?.score_velocity||0).toFixed(2)}/s`,
              sub: fastestPost ? `r/${fastestPost.subreddit}` : "—", col: "#63b3ed" },
          ].map((m) => (
            <div key={m.label} className="card" style={{ padding: "14px 16px", position: "relative", overflow: "hidden" }}>
              <div style={{ position: "absolute", top: 0, left: 0, right: 0, height: 2,
                background: `linear-gradient(90deg, ${m.col}, transparent)` }} />
              <div style={{ fontSize: 9, color: "#4a5568", letterSpacing: "0.1em",
                textTransform: "uppercase", fontFamily: "'Syne', sans-serif", marginBottom: 5 }}>{m.label}</div>
              <div style={{ fontFamily: "'Syne', sans-serif", fontSize: 22, fontWeight: 800, color: m.col, lineHeight: 1 }}>{m.val}</div>
              <div style={{ fontSize: 9, color: "#2d3748", marginTop: 3 }}>{m.sub}</div>
            </div>
          ))}
        </div>

        {/* Momentum leaders */}
        <div className="card" style={{ marginBottom: 14 }}>
          <div className="card-head">
            <span className="card-title">🔥 Momentum Leaders</span>
            <span className="card-badge">score/age · {timeRange.label}</span>
          </div>
          {hotPosts.map((p, i) => (
            <div key={p.id} onClick={() => setSelected(sel => sel?.id === p.id ? null : p)}
              style={{ display: "grid", gridTemplateColumns: "28px 1fr auto", gap: 10,
                padding: "10px 16px", alignItems: "center", cursor: "pointer",
                borderBottom: i < hotPosts.length - 1 ? "1px solid rgba(255,255,255,0.04)" : "none" }}
              onMouseEnter={e => e.currentTarget.style.background = "rgba(255,69,0,0.04)"}
              onMouseLeave={e => e.currentTarget.style.background = "transparent"}
            >
              <div style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 16,
                color: i === 0 ? "#ff4500" : i === 1 ? "#ff6b35" : "#2d3748" }}>{i+1}</div>
              <div>
                <div style={{ fontSize: 9, color: "#ff4500", marginBottom: 2 }}>r/{p.subreddit}</div>
                <div style={{ fontFamily: "'Syne', sans-serif", fontSize: 11, fontWeight: 600,
                  overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{p.title}</div>
              </div>
              <div style={{ textAlign: "right" }}>
                <div style={{ color: "#3ecf74", fontSize: 12, fontWeight: 700 }}>{(p.momentum||0).toFixed(2)}</div>
                <div style={{ color: "#2d3748", fontSize: 9 }}>▲{fmt(p.current_score)}</div>
              </div>
            </div>
          ))}
        </div>

        {/* Table — no per-row animation */}
        <div className="card">
          <div className="card-head">
            <span className="card-title">All Posts</span>
            <span className="card-badge">{filtered.length} shown</span>
          </div>
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 10 }}>
              <thead>
                <tr style={{ borderBottom: "1px solid rgba(255,255,255,0.06)" }}>
                  {["#","Title","Sub","Score","Comments","Velocity","Momentum","Sentiment","Age"].map(h => (
                    <th key={h} style={{ padding: "9px 10px", textAlign: "left",
                      fontFamily: "'Syne', sans-serif", fontSize: 8, fontWeight: 700,
                      letterSpacing: "0.12em", textTransform: "uppercase", color: "#2d3748", whiteSpace: "nowrap" }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {filtered.slice(0, 80).map((p, i) => {
                  const sent = getSentiment(p);
                  const vel  = p.score_velocity || 0;
                  return (
                    <tr key={p.id} onClick={() => setSelected(sel => sel?.id === p.id ? null : p)}
                      style={{ borderBottom: "1px solid rgba(255,255,255,0.03)", cursor: "pointer",
                        background: selected?.id === p.id ? "rgba(255,69,0,0.06)" : "transparent" }}
                      onMouseEnter={e => { if (selected?.id !== p.id) e.currentTarget.style.background = "rgba(255,255,255,0.02)"; }}
                      onMouseLeave={e => { if (selected?.id !== p.id) e.currentTarget.style.background = "transparent"; }}
                    >
                      <td style={{ padding: "7px 10px", color: "#2d3748", fontWeight: 700 }}>{i+1}</td>
                      <td style={{ padding: "7px 10px", maxWidth: 180, overflow: "hidden",
                        textOverflow: "ellipsis", whiteSpace: "nowrap",
                        fontFamily: "'Syne', sans-serif", fontWeight: 600, fontSize: 10 }}>{p.title}</td>
                      <td style={{ padding: "7px 10px", color: "#ff6b35", whiteSpace: "nowrap", fontSize: 9 }}>r/{p.subreddit}</td>
                      <td style={{ padding: "7px 10px", color: "#3ecf74", whiteSpace: "nowrap" }}>▲{fmt(p.current_score)}</td>
                      <td style={{ padding: "7px 10px", whiteSpace: "nowrap" }}>💬{fmt(p.current_comments)}</td>
                      <td style={{ padding: "7px 10px", color: vel > 0 ? "#3ecf74" : "#2d3748",
                        whiteSpace: "nowrap", fontFamily: "'JetBrains Mono', monospace" }}>
                        {vel > 0 ? `▲${vel.toFixed(3)}/s` : "—"}
                      </td>
                      <td style={{ padding: "7px 10px", color: "#f6ad55", whiteSpace: "nowrap" }}>{(p.momentum||0).toFixed(2)}</td>
                      <td style={{ padding: "7px 10px", color: sentColor(sent), whiteSpace: "nowrap" }}>{sent.toFixed(2)}</td>
                      <td style={{ padding: "7px 10px", color: "#4a5568", whiteSpace: "nowrap" }}>{ago(p.created_utc)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      {/* RIGHT: DETAIL */}
      <div style={{ borderLeft: "1px solid rgba(255,255,255,0.04)", overflowY: "auto" }}>
        <PostDetail post={selected} onClose={() => setSelected(null)} />
      </div>
    </div>
  );
}
