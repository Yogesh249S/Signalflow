import { useState, useEffect, useCallback, useRef } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { fetchPosts, fmt, ago, getSentiment, sentColor, priorityBadge, TIME_RANGES } from "../api.js";

const POLL = 30000;

// ── Time range picker ─────────────────────────────────────────────────────────
function TimeRangePicker({ value, onChange }) {
  return (
    <div style={{ display: "flex", gap: 3, flexWrap: "wrap" }}>
      {TIME_RANGES.map(r => (
        <button
          key={r.label}
          onClick={() => onChange(r)}
          style={{
            padding: "3px 9px", borderRadius: 4, fontSize: 9, cursor: "pointer",
            fontFamily: "'JetBrains Mono', monospace", fontWeight: 600,
            border: "1px solid transparent", transition: "all 0.15s",
            background: value.label === r.label ? "#ff4500" : "rgba(255,255,255,0.04)",
            color:      value.label === r.label ? "#fff"    : "#4a5568",
          }}
        >
          {r.label}
        </button>
      ))}
    </div>
  );
}

// ── Post card ─────────────────────────────────────────────────────────────────
function PostCard({ post, selected, onClick, prevScore }) {
  const badge    = priorityBadge(post.poll_priority);
  const momentum = post.momentum || 0;
  const score    = post.current_score || 0;
  const delta    = score - (prevScore ?? score);
  const sent     = getSentiment(post);
  const vel      = post.velocity || post.score_velocity || 0;

  return (
    <motion.div
      layout
      initial={{ opacity: 0, x: -6 }}
      animate={{ opacity: 1, x: 0 }}
      onClick={onClick}
      style={{
        padding: "11px 16px",
        borderBottom: "1px solid rgba(255,255,255,0.04)",
        cursor: "pointer",
        background: selected ? "rgba(255,69,0,0.06)" : "transparent",
        borderLeft: selected ? "2px solid #ff4500" : "2px solid transparent",
        transition: "background 0.15s",
        position: "relative",
      }}
      onMouseEnter={e => { if (!selected) e.currentTarget.style.background = "rgba(255,255,255,0.02)"; }}
      onMouseLeave={e => { if (!selected) e.currentTarget.style.background = "transparent"; }}
    >
      {/* momentum glow bar */}
      <div style={{
        position: "absolute", left: 0, top: 0, bottom: 0, width: 2,
        background: `rgba(255,69,0,${Math.min(momentum / 8, 0.9)})`,
      }} />

      <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 4 }}>
        <span style={{ color: "#ff4500", fontSize: 10, letterSpacing: "0.05em" }}>r/{post.subreddit}</span>
        <span className="badge" style={{ background: badge.bg, color: badge.color }}>{badge.label}</span>
        {vel > 0.5 && (
          <span style={{ marginLeft: 2, background: "rgba(62,207,116,0.12)", border: "1px solid rgba(62,207,116,0.3)",
            color: "#3ecf74", fontSize: 8, padding: "1px 6px", borderRadius: 3, fontFamily: "'JetBrains Mono', monospace" }}>
            ▲{vel.toFixed(1)}/s
          </span>
        )}
        <span style={{ marginLeft: "auto", color: sentColor(sent), fontSize: 9 }}>
          {sent > 0 ? "+" : ""}{sent.toFixed(2)}
        </span>
      </div>

      <div style={{
        fontFamily: "'Syne', sans-serif", fontSize: 12, fontWeight: 600,
        lineHeight: 1.38, color: "#dde1e8", marginBottom: 5,
        overflow: "hidden", display: "-webkit-box",
        WebkitLineClamp: 2, WebkitBoxOrient: "vertical",
      }}>
        {post.title}
      </div>

      <div style={{ display: "flex", gap: 12, color: "#4a5568", fontSize: 10, alignItems: "center" }}>
        <span style={{ color: delta > 0 ? "#3ecf74" : delta < 0 ? "#f56565" : "#4a5568" }}>
          ▲ {fmt(score)}{delta !== 0 && <span style={{ marginLeft: 3 }}>({delta > 0 ? "+" : ""}{delta})</span>}
        </span>
        <span>💬 {fmt(post.current_comments)}</span>
        <span>{ago(post.created_utc)}</span>
        <span style={{ marginLeft: "auto", color: "#2d3748" }}>≈{(momentum).toFixed(1)}/min</span>
      </div>
    </motion.div>
  );
}

// ── Post detail panel ─────────────────────────────────────────────────────────
function PostDetail({ post, onClose }) {
  if (!post) return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "center",
      justifyContent: "center", height: "100%", color: "#2d3748", gap: 10 }}>
      <div style={{ fontSize: 32 }}>◌</div>
      <div style={{ fontSize: 11, letterSpacing: "0.08em" }}>SELECT A POST</div>
    </div>
  );

  const sent  = getSentiment(post);
  const badge = priorityBadge(post.poll_priority);
  const vel   = post.velocity || post.score_velocity || 0;

  const rows = [
    ["Score",       `▲ ${fmt(post.current_score)}`],
    ["Comments",    `💬 ${fmt(post.current_comments)}`],
    ["Upvote Ratio",`${Math.round((post.current_ratio || 0) * 100)}%`],
    ["Velocity",    `▲${vel.toFixed(3)}/s`],
    ["Momentum",    `${(post.momentum || 0).toFixed(3)}/min`],
    ["Engagement",  fmt(post.engagement_score)],
    ["Age",         `${Math.round(post.age_minutes || 0)} min`],
    ["Author",      `u/${post.author || "—"}`],
    ["Priority",    post.poll_priority || "—"],
    ["Trending",    post.is_trending ? "✓ Yes" : "✗ No"],
    ["First seen",  ago(post.first_seen_at || post.created_utc)],
  ];

  return (
    <motion.div
      key={post.id}
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      style={{ padding: 20, height: "100%", overflowY: "auto" }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 16 }}>
        <div>
          <div style={{ color: "#ff4500", fontSize: 10, marginBottom: 6, letterSpacing: "0.05em" }}>
            r/{post.subreddit}
          </div>
          <span className="badge" style={{ background: badge.bg, color: badge.color }}>{badge.label}</span>
        </div>
        <button onClick={onClose}
          style={{ background: "none", border: "none", color: "#4a5568", cursor: "pointer", fontSize: 16, lineHeight: 1, padding: 4 }}>
          ✕
        </button>
      </div>

      <div style={{ fontFamily: "'Syne', sans-serif", fontSize: 15, fontWeight: 700,
        lineHeight: 1.45, color: "#dde1e8", marginBottom: 20 }}>
        {post.title}
      </div>

      {/* Velocity bar */}
      {vel > 0 && (
        <div className="card" style={{ marginBottom: 12, padding: 14 }}>
          <div style={{ display: "flex", justifyContent: "space-between", fontSize: 10, color: "#4a5568", marginBottom: 8 }}>
            <span>VELOCITY</span>
            <span style={{ color: "#3ecf74", fontWeight: 700 }}>▲{vel.toFixed(3)}/sec</span>
          </div>
          <div style={{ height: 4, background: "rgba(255,255,255,0.04)", borderRadius: 2, overflow: "hidden" }}>
            <motion.div
              initial={{ width: 0 }}
              animate={{ width: `${Math.min(vel * 10, 100)}%` }}
              transition={{ duration: 0.8, ease: "easeOut" }}
              style={{ height: "100%", background: "linear-gradient(90deg, #3ecf74, #63b3ed)", borderRadius: 2 }}
            />
          </div>
        </div>
      )}

      {/* Stats rows */}
      <div className="card" style={{ marginBottom: 16 }}>
        {rows.map(([label, val], i) => (
          <div key={label} style={{
            display: "flex", justifyContent: "space-between", alignItems: "center",
            padding: "9px 16px",
            borderBottom: i < rows.length - 1 ? "1px solid rgba(255,255,255,0.04)" : "none",
            fontSize: 11,
          }}>
            <span style={{ color: "#4a5568" }}>{label}</span>
            <span style={{ fontFamily: "'JetBrains Mono', monospace", fontWeight: 500 }}>{val}</span>
          </div>
        ))}
      </div>

      {/* Sentiment bar */}
      <div className="card" style={{ padding: 16, marginBottom: 16 }}>
        <div style={{ display: "flex", justifyContent: "space-between", fontSize: 10, color: "#4a5568", marginBottom: 10 }}>
          <span>SENTIMENT</span>
          <span style={{ color: sentColor(sent), fontWeight: 700 }}>
            {sent > 0 ? "+" : ""}{sent.toFixed(3)}
          </span>
        </div>
        <div style={{ height: 6, borderRadius: 3,
          background: "linear-gradient(90deg, #f56565 0%, #2d3748 50%, #3ecf74 100%)", position: "relative" }}>
          <div style={{
            position: "absolute", top: "50%", transform: "translate(-50%, -50%)",
            left: `${((sent + 1) / 2) * 100}%`,
            width: 14, height: 14, borderRadius: "50%",
            background: "#0d1117", border: "2px solid #ff4500",
            transition: "left 0.5s ease",
          }} />
        </div>
        <div style={{ display: "flex", justifyContent: "space-between", fontSize: 9, color: "#2d3748", marginTop: 6 }}>
          <span>NEGATIVE</span><span>NEUTRAL</span><span>POSITIVE</span>
        </div>
      </div>

      {post.permalink && (
        <a
          href={`https://reddit.com${post.permalink}`}
          target="_blank" rel="noopener noreferrer"
          style={{
            display: "block", marginTop: 4, padding: "10px 16px",
            background: "rgba(255,69,0,0.08)", border: "1px solid rgba(255,69,0,0.2)",
            borderRadius: 8, color: "#ff6b35", fontSize: 11, textDecoration: "none",
            textAlign: "center", fontFamily: "'Syne', sans-serif", fontWeight: 600,
            letterSpacing: "0.08em",
          }}
        >
          VIEW ON REDDIT ↗
        </a>
      )}
    </motion.div>
  );
}

// ── Page ──────────────────────────────────────────────────────────────────────
export default function PostsPage() {
  const [posts,      setPosts]    = useState([]);
  const [selected,   setSelected] = useState(null);
  const [search,     setSearch]   = useState("");
  const [subFilter,  setSub]      = useState("all");
  const [sortBy,     setSort]     = useState("score");
  const [loading,    setLoading]  = useState(true);
  const [total,      setTotal]    = useState(0);
  const [timeRange,  setTimeRange] = useState(TIME_RANGES[3]); // 3h default
  const prevScores = useRef({});

  const load = useCallback(async () => {
    try {
      const res  = await fetchPosts(null, timeRange.hours);
      const data = res.data?.results ?? res.data ?? [];
      prevScores.current = Object.fromEntries(
        data.map(p => [p.id, prevScores.current[p.id] ?? p.current_score])
      );
      setPosts(data);
      setTotal(res.data?.count ?? data.length);
      setLoading(false);
    } catch (e) { console.error(e); setLoading(false); }
  }, [timeRange]);
/*
  useEffect(() => {
    load();
    const t = setInterval(load, POLL);
    return () => clearInterval(t);
  }, [load]);
*/

  useEffect(() => {
    const ws = new WebSocket("ws://localhost:8000/ws/posts/");
    
    ws.onmessage = (e) => {
      const updates = JSON.parse(e.data);
      setPosts(prev => {
        const map = Object.fromEntries(prev.map(p => [p.id, p]));
        updates.forEach(u => { map[u.id] = { ...map[u.id], ...u }; });
        return Object.values(map);
      });
    };

    ws.onerror = () => {
      // fall back to polling if WebSocket fails
      const t = setInterval(load, 30000);
      return () => clearInterval(t);
    };

    // initial load still needed
    load();
    
    return () => ws.close();
  }, [timeRange]);

  const subreddits = ["all", ...new Set(posts.map(p => p.subreddit).filter(Boolean))].sort();

  const filtered = posts
    .filter(p => {
      if (subFilter !== "all" && p.subreddit !== subFilter) return false;
      if (search && !p.title?.toLowerCase().includes(search.toLowerCase()) &&
          !p.subreddit?.toLowerCase().includes(search.toLowerCase()) &&
          !p.author?.toLowerCase().includes(search.toLowerCase())) return false;
      return true;
    })
    .sort((a, b) => {
      if (sortBy === "score")    return (b.current_score || 0) - (a.current_score || 0);
      if (sortBy === "comments") return (b.current_comments || 0) - (a.current_comments || 0);
      if (sortBy === "momentum") return (b.momentum || 0) - (a.momentum || 0);
      if (sortBy === "velocity") return (b.velocity || b.score_velocity || 0) - (a.velocity || a.score_velocity || 0);
      if (sortBy === "new")      return (b.created_utc || 0) - (a.created_utc || 0);
      if (sortBy === "sentiment") return getSentiment(b) - getSentiment(a);
      return 0;
    });

  const hotPosts      = [...posts].sort((a,b)=>(b.momentum||0)-(a.momentum||0)).slice(0,5);
  const trendingCount = posts.filter(p => p.is_trending).length;
  const fastestPost   = [...posts].sort((a,b)=>(b.velocity||b.score_velocity||0)-(a.velocity||a.score_velocity||0))[0];

  const inputStyle = {
    background: "var(--surface)", border: "1px solid rgba(255,255,255,0.06)",
    borderRadius: 6, color: "var(--text)", fontFamily: "'JetBrains Mono', monospace",
    fontSize: 11, outline: "none", padding: "7px 10px", transition: "border-color 0.2s",
  };

  return (
    <div className="page-scroll" style={{ display: "grid", gridTemplateColumns: "300px 1fr 300px", height: "100%", overflow: "hidden" }}>

      {/* ── LEFT: FEED ── */}
      <div style={{ borderRight: "1px solid rgba(255,255,255,0.04)", display: "flex", flexDirection: "column", overflow: "hidden" }}>
        <div style={{ padding: "14px 14px 10px", borderBottom: "1px solid rgba(255,255,255,0.04)", flexShrink: 0 }}>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 8 }}>
            <span style={{ fontFamily: "'Syne', sans-serif", fontSize: 10, fontWeight: 700,
              letterSpacing: "0.14em", textTransform: "uppercase", color: "#4a5568" }}>
              Live Feed
            </span>
            <span style={{ color: "#ff4500", fontSize: 10 }}>{filtered.length} / {total}</span>
          </div>

          {/* Time range */}
          <div style={{ marginBottom: 8 }}>
            <TimeRangePicker value={timeRange} onChange={r => setTimeRange(r)} />
          </div>

          <input
            style={{ ...inputStyle, width: "100%", marginBottom: 8 }}
            placeholder="Search posts, subs, authors…"
            value={search}
            onChange={e => setSearch(e.target.value)}
            onFocus={e => e.target.style.borderColor = "rgba(255,69,0,0.4)"}
            onBlur={e => e.target.style.borderColor = "rgba(255,255,255,0.06)"}
          />
          <div style={{ display: "flex", gap: 6 }}>
            <select style={{ ...inputStyle, flex: 1 }} value={subFilter} onChange={e => setSub(e.target.value)}>
              {subreddits.map(s => <option key={s} value={s}>{s === "all" ? "All subs" : `r/${s}`}</option>)}
            </select>
            <select style={{ ...inputStyle, flex: 1 }} value={sortBy} onChange={e => setSort(e.target.value)}>
              <option value="score">Score</option>
              <option value="comments">Comments</option>
              <option value="momentum">Momentum</option>
              <option value="velocity">Velocity ▲</option>
              <option value="new">Newest</option>
              <option value="sentiment">Sentiment</option>
            </select>
          </div>
        </div>

        <div style={{ flex: 1, overflowY: "auto" }}>
          {loading ? (
            Array(8).fill(0).map((_, i) => (
              <div key={i} style={{ padding: "12px 16px", borderBottom: "1px solid rgba(255,255,255,0.04)" }}>
                <div className="skeleton" style={{ height: 10, width: "40%", marginBottom: 8 }} />
                <div className="skeleton" style={{ height: 12, width: "90%", marginBottom: 4 }} />
                <div className="skeleton" style={{ height: 10, width: "60%" }} />
              </div>
            ))
          ) : filtered.length === 0 ? (
            <div style={{ display: "flex", flexDirection: "column", alignItems: "center",
              justifyContent: "center", height: 200, color: "#2d3748", gap: 8 }}>
              <div style={{ fontSize: 28 }}>◌</div>
              <div style={{ fontSize: 11 }}>No posts in last {timeRange.label}</div>
            </div>
          ) : (
            <AnimatePresence initial={false}>
              {filtered.map(p => (
                <PostCard
                  key={p.id}
                  post={p}
                  selected={selected?.id === p.id}
                  prevScore={prevScores.current[p.id]}
                  onClick={() => setSelected(sel => sel?.id === p.id ? null : p)}
                />
              ))}
            </AnimatePresence>
          )}
        </div>
      </div>

      {/* ── CENTER ── */}
      <div style={{ overflowY: "auto", padding: "20px 20px" }}>

        {/* KPI strip */}
        <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 10, marginBottom: 20 }}>
          {[
            { label: "Posts",         val: fmt(total),         sub: `last ${timeRange.label}`, col: "#dde1e8" },
            { label: "Trending Now",  val: trendingCount,      sub: "posts",                   col: "#f6ad55" },
            { label: "Top Score",     val: fmt(posts.sort((a,b)=>(b.current_score||0)-(a.current_score||0))[0]?.current_score), sub: "upvotes", col: "#3ecf74" },
            { label: "Fastest ▲",
              val: `${((fastestPost?.velocity || fastestPost?.score_velocity || 0)).toFixed(1)}/s`,
              sub: fastestPost?.subreddit ? `r/${fastestPost.subreddit}` : "—",
              col: "#63b3ed" },
          ].map((m, i) => (
            <motion.div key={m.label} className="card"
              initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: i*0.05 }}
              style={{ padding: "16px 18px", position: "relative", overflow: "hidden" }}>
              <div style={{ position: "absolute", top: 0, left: 0, right: 0, height: 2,
                background: `linear-gradient(90deg, ${m.col}, transparent)` }} />
              <div style={{ fontSize: 10, color: "#4a5568", letterSpacing: "0.1em", textTransform: "uppercase",
                marginBottom: 6, fontFamily: "'Syne', sans-serif" }}>{m.label}</div>
              <div style={{ fontFamily: "'Syne', sans-serif", fontSize: 26, fontWeight: 800, color: m.col, lineHeight: 1 }}>{m.val}</div>
              <div style={{ fontSize: 10, color: "#2d3748", marginTop: 4 }}>{m.sub}</div>
            </motion.div>
          ))}
        </div>

        {/* Momentum leaders */}
        <div className="card" style={{ marginBottom: 16 }}>
          <div className="card-head">
            <span className="card-title">🔥 Momentum Leaders</span>
            <span className="card-badge">score / age · {timeRange.label}</span>
          </div>
          {hotPosts.map((p, i) => (
            <motion.div key={p.id}
              initial={{ opacity: 0, x: 10 }} animate={{ opacity: 1, x: 0 }} transition={{ delay: i*0.06 }}
              onClick={() => setSelected(sel => sel?.id === p.id ? null : p)}
              style={{ display: "grid", gridTemplateColumns: "32px 1fr auto", gap: 12,
                padding: "12px 18px", alignItems: "center", cursor: "pointer",
                borderBottom: i < hotPosts.length-1 ? "1px solid rgba(255,255,255,0.04)" : "none" }}
              onMouseEnter={e => e.currentTarget.style.background = "rgba(255,69,0,0.04)"}
              onMouseLeave={e => e.currentTarget.style.background = "transparent"}
            >
              <div style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 18,
                color: i === 0 ? "#ff4500" : i === 1 ? "#ff6b35" : "#4a5568" }}>{i+1}</div>
              <div>
                <div style={{ fontSize: 10, color: "#ff4500", marginBottom: 3 }}>r/{p.subreddit}</div>
                <div style={{ fontFamily: "'Syne', sans-serif", fontSize: 12, fontWeight: 600,
                  lineHeight: 1.35, overflow: "hidden", display: "-webkit-box",
                  WebkitLineClamp: 1, WebkitBoxOrient: "vertical" }}>{p.title}</div>
              </div>
              <div style={{ textAlign: "right" }}>
                <div style={{ color: "#3ecf74", fontSize: 13, fontWeight: 700 }}>{(p.momentum||0).toFixed(2)}</div>
                <div style={{ color: "#2d3748", fontSize: 10 }}>▲{fmt(p.current_score)}</div>
              </div>
            </motion.div>
          ))}
        </div>

        {/* Full table */}
        <div className="card">
          <div className="card-head">
            <span className="card-title">All Posts</span>
            <span className="card-badge">{filtered.length} shown · {timeRange.label}</span>
          </div>
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
              <thead>
                <tr style={{ borderBottom: "1px solid rgba(255,255,255,0.06)" }}>
                  {["#","Title","Sub","Score","Comments","Velocity","Momentum","Sent.","Age","Author"].map(h => (
                    <th key={h} style={{ padding: "10px 12px", textAlign: "left",
                      fontFamily: "'Syne', sans-serif", fontSize: 9, fontWeight: 700,
                      letterSpacing: "0.12em", textTransform: "uppercase", color: "#2d3748", whiteSpace: "nowrap" }}>
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {filtered.slice(0,60).map((p, i) => {
                  const sent = getSentiment(p);
                  const vel  = p.velocity || p.score_velocity || 0;
                  return (
                    <motion.tr key={p.id}
                      initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ delay: i*0.005 }}
                      onClick={() => setSelected(sel => sel?.id === p.id ? null : p)}
                      style={{ borderBottom: "1px solid rgba(255,255,255,0.03)", cursor: "pointer",
                        background: selected?.id === p.id ? "rgba(255,69,0,0.06)" : "transparent" }}
                      onMouseEnter={e => { if (selected?.id !== p.id) e.currentTarget.style.background = "rgba(255,255,255,0.02)"; }}
                      onMouseLeave={e => { if (selected?.id !== p.id) e.currentTarget.style.background = "transparent"; }}
                    >
                      <td style={{ padding: "8px 12px", color: "#2d3748", fontWeight: 700 }}>{i+1}</td>
                      <td style={{ padding: "8px 12px", maxWidth: 200, overflow: "hidden",
                        textOverflow: "ellipsis", whiteSpace: "nowrap",
                        fontFamily: "'Syne', sans-serif", fontWeight: 600, fontSize: 11 }}>{p.title}</td>
                      <td style={{ padding: "8px 12px", color: "#ff6b35", whiteSpace: "nowrap" }}>r/{p.subreddit}</td>
                      <td style={{ padding: "8px 12px", color: "#3ecf74", whiteSpace: "nowrap" }}>▲{fmt(p.current_score)}</td>
                      <td style={{ padding: "8px 12px", whiteSpace: "nowrap" }}>💬{fmt(p.current_comments)}</td>
                      <td style={{ padding: "8px 12px", color: "#3ecf74", whiteSpace: "nowrap", fontFamily: "'JetBrains Mono', monospace" }}>
                        {vel > 0 ? `▲${vel.toFixed(2)}/s` : "—"}
                      </td>
                      <td style={{ padding: "8px 12px", color: "#f6ad55", whiteSpace: "nowrap" }}>{(p.momentum||0).toFixed(2)}</td>
                      <td style={{ padding: "8px 12px", color: sentColor(sent), whiteSpace: "nowrap" }}>{sent.toFixed(2)}</td>
                      <td style={{ padding: "8px 12px", color: "#4a5568", whiteSpace: "nowrap" }}>{ago(p.created_utc)}</td>
                      <td style={{ padding: "8px 12px", color: "#4a5568", whiteSpace: "nowrap" }}>u/{p.author || "—"}</td>
                    </motion.tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      {/* ── RIGHT: POST DETAIL ── */}
      <div style={{ borderLeft: "1px solid rgba(255,255,255,0.04)", overflowY: "auto" }}>
        <PostDetail post={selected} onClose={() => setSelected(null)} />
      </div>

    </div>
  );
}
