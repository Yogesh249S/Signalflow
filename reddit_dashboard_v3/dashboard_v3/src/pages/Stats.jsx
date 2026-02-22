import { useState, useEffect, useCallback } from "react";
import { motion, AnimatePresence } from "framer-motion";
import {
  BarChart, Bar, AreaChart, Area, LineChart, Line,
  ScatterChart, Scatter, XAxis, YAxis, Tooltip,
  ResponsiveContainer, CartesianGrid, Cell,
  PieChart, Pie, Legend,
} from "recharts";
import {
  fetchPosts, fetchStats, fetchActivityTimeline, fetchKeywords,
  fetchVelocityLeaders, fmt, getSentiment, sentColor,
  TIME_RANGES,
} from "../api.js";
import CustomTooltip from "../components/Tooltip.jsx";

const POLL = 60000;

// ── Sub-components ────────────────────────────────────────────────────────────

function KpiCard({ label, val, sub, color, delay = 0 }) {
  return (
    <motion.div
      className="card"
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay }}
      style={{ padding: "20px 22px", position: "relative", overflow: "hidden" }}
    >
      <div style={{ position: "absolute", top: 0, left: 0, right: 0, height: 2,
        background: `linear-gradient(90deg, ${color || "#ff4500"}, transparent)` }} />
      <div style={{ fontSize: 10, color: "#4a5568", letterSpacing: "0.12em",
        textTransform: "uppercase", fontFamily: "'Syne', sans-serif", marginBottom: 8 }}>
        {label}
      </div>
      <div style={{ fontFamily: "'Syne', sans-serif", fontSize: 30, fontWeight: 800,
        color: color || "#dde1e8", lineHeight: 1 }}>
        {val}
      </div>
      {sub && <div style={{ fontSize: 10, color: "#4a5568", marginTop: 6 }}>{sub}</div>}
    </motion.div>
  );
}

function TimeRangePicker({ value, onChange }) {
  return (
    <div style={{ display: "flex", gap: 4 }}>
      {TIME_RANGES.map(r => (
        <button
          key={r.label}
          onClick={() => onChange(r)}
          style={{
            padding: "4px 10px", borderRadius: 5, fontSize: 10, cursor: "pointer",
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

// ── Main ──────────────────────────────────────────────────────────────────────
export default function StatsPage() {
  const [posts,      setPosts]    = useState([]);
  const [stats,      setStats]    = useState(null);
  const [timeline,   setTimeline] = useState([]);
  const [keywords,   setKeywords] = useState([]);
  const [velocity,   setVelocity] = useState([]);
  const [loading,    setLoading]  = useState(true);
  const [activeChart, setChart]   = useState("engagement");
  const [timeRange,  setTimeRange] = useState(TIME_RANGES[3]); // default 3h

  const load = useCallback(async () => {
    try {
      const [pr, sr] = await Promise.all([
        fetchPosts(null, timeRange.hours),
        fetchStats(Math.max(1, Math.ceil(timeRange.hours / 24))),
      ]);
      setPosts(pr.data?.results ?? pr.data ?? []);
      setStats(sr.data);
      setLoading(false);
    } catch (e) { console.error(e); setLoading(false); }
  }, [timeRange]);

  const loadExtras = useCallback(async () => {
    try {
      const [tl, kw, vl] = await Promise.all([
        fetchActivityTimeline(timeRange.hours),
        fetchKeywords(Math.min(timeRange.hours, 12)),
        fetchVelocityLeaders(Math.min(timeRange.hours, 2)),
      ]);
      setTimeline(tl.data?.results ?? tl.data ?? []);
      setKeywords(kw.data?.results ?? kw.data ?? []);
      setVelocity(vl.data?.results ?? vl.data ?? []);
    } catch (e) {
      // endpoints may not exist yet — fall back to client-side derivation
      setTimeline([]);
      setKeywords([]);
      setVelocity([]);
    }
  }, [timeRange]);

  useEffect(() => {
    load();
    loadExtras();
    const t1 = setInterval(load, POLL);
    const t2 = setInterval(loadExtras, POLL * 2);
    return () => { clearInterval(t1); clearInterval(t2); };
  }, [load, loadExtras]);

  // ── Derived data ─────────────────────────────────────────────────────────────
  const overallSent = posts.length
    ? posts.reduce((s, p) => s + getSentiment(p), 0) / posts.length
    : 0;

  const topPosts     = [...posts].sort((a,b) => (b.current_score||0)-(a.current_score||0)).slice(0,20);
  const trendingNow  = posts.filter(p => p.is_trending);
  const fastestRising = [...posts].sort((a,b)=>(b.velocity||b.score_velocity||0)-(a.velocity||a.score_velocity||0))[0];

  // subreddit breakdown
  const subMap = {};
  posts.forEach(p => {
    const s = p.subreddit || "unknown";
    if (!subMap[s]) subMap[s] = { name: s, posts: 0, score: 0, comments: 0, pos: 0, neg: 0, neu: 0 };
    subMap[s].posts++;
    subMap[s].score    += p.current_score    || 0;
    subMap[s].comments += p.current_comments || 0;
    const sent = getSentiment(p);
    if (sent >  0.05) subMap[s].pos++;
    else if (sent < -0.05) subMap[s].neg++;
    else subMap[s].neu++;
  });
  const subData = Object.values(subMap).sort((a,b)=>b.score-a.score).slice(0,12);

  // sentiment buckets
  const sentBuckets = { Positive: 0, Neutral: 0, Negative: 0 };
  posts.forEach(p => {
    const s = getSentiment(p);
    if (s > 0.05) sentBuckets.Positive++;
    else if (s < -0.05) sentBuckets.Negative++;
    else sentBuckets.Neutral++;
  });
  const sentPieData  = Object.entries(sentBuckets).map(([name, value]) => ({ name, value }));
  const PIE_COLORS   = { Positive: "#3ecf74", Neutral: "#4a5568", Negative: "#f56565" };

  // scatter
  const scatterData  = posts.map(p => ({
    x: Math.round(p.age_minutes || 0),
    y: Math.round(p.engagement_score || 0),
    name: p.title?.slice(0, 40),
  }));

  // momentum leaders
  const momentumData = [...posts]
    .sort((a,b)=>(b.momentum||0)-(a.momentum||0)).slice(0,15)
    .map(p => ({ name: `r/${p.subreddit}`, momentum: parseFloat((p.momentum||0).toFixed(2)) }));

  // velocity leaders — prefer API data, fall back to client-side sort
  const velocityLeaders = (velocity.length ? velocity : [...posts]
    .sort((a,b)=>(b.velocity||b.score_velocity||0)-(a.velocity||a.score_velocity||0)))
    .slice(0,10);

  // activity timeline — fall back to client-side hour bucketing
  const timelineData = (() => {
    if (timeline.length) return timeline;
    const buckets = {};
    posts.forEach(p => {
      if (!p.created_utc) return;
      const ts = p.created_utc > 1e12 ? p.created_utc/1000 : p.created_utc;
      const h  = new Date(ts * 1000);
      h.setMinutes(0, 0, 0);
      const key = h.toISOString();
      if (!buckets[key]) buckets[key] = { hour: key, posts: 0, score: 0 };
      buckets[key].posts++;
      buckets[key].score += p.current_score || 0;
    });
    return Object.values(buckets)
      .sort((a,b) => new Date(a.hour) - new Date(b.hour))
      .map(b => ({
        ...b,
        label: new Date(b.hour).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }),
      }));
  })();

  // keyword data — fall back to client-side extraction
  const keywordData = (() => {
    if (keywords.length) return keywords.slice(0,30);
    const freq = {};
    posts.forEach(p => {
      (p.title || "").toLowerCase()
        .split(/\W+/)
        .filter(w => w.length > 4)
        .forEach(w => { freq[w] = (freq[w] || 0) + 1; });
    });
    return Object.entries(freq)
      .sort((a,b) => b[1]-a[1])
      .slice(0,20)
      .map(([word, count]) => ({ word, count }));
  })();

  const CHARTS = ["engagement", "velocity", "timeline", "sentiment", "subreddits", "keywords", "scatter"];

  const tabStyle = (t) => ({
    padding: "6px 14px", borderRadius: 6, fontSize: 10, cursor: "pointer",
    fontFamily: "'Syne', sans-serif", fontWeight: 700, letterSpacing: "0.08em",
    border: "1px solid transparent", transition: "all 0.15s",
    background: activeChart === t ? "#ff4500" : "transparent",
    color:      activeChart === t ? "#fff"    : "#4a5568",
  });

  if (loading) return (
    <div style={{ display: "flex", alignItems: "center", justifyContent: "center",
      height: "100%", color: "#2d3748", fontSize: 12 }}>
      Loading analytics…
    </div>
  );

  return (
    <div className="page-scroll" style={{ padding: "24px 28px" }}>

      {/* ── Time range selector ─────────────────────────────────────────────── */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 20 }}>
        <div style={{ fontFamily: "'Syne', sans-serif", fontSize: 11, fontWeight: 700,
          letterSpacing: "0.14em", textTransform: "uppercase", color: "#4a5568" }}>
          TIME WINDOW
        </div>
        <TimeRangePicker value={timeRange} onChange={r => { setTimeRange(r); }} />
      </div>

      {/* ── KPI strip ──────────────────────────────────────────────────────── */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(6, 1fr)", gap: 10, marginBottom: 20 }}>
        <KpiCard label="Posts Tracked"   val={fmt(posts.length)}
          sub={`last ${timeRange.label}`}              color="#ff4500" delay={0}    />
        <KpiCard label="Trending Now"    val={trendingNow.length}
          sub="active posts"                           color="#f6ad55" delay={0.04} />
        <KpiCard label="Top Score"       val={fmt(topPosts[0]?.current_score)}
          sub="upvotes"                                color="#3ecf74" delay={0.08} />
        <KpiCard label="Fastest Rising"
          val={`▲${((fastestRising?.velocity || fastestRising?.score_velocity || 0)).toFixed(1)}/s`}
          sub={fastestRising?.subreddit ? `r/${fastestRising.subreddit}` : "—"}
          color="#63b3ed" delay={0.12} />
        <KpiCard label="Active Authors"  val={fmt(stats?.overview?.active_users ?? new Set(posts.map(p=>p.author)).size)}
          sub="unique"                                 color="#a78bfa" delay={0.16} />
        <KpiCard label="Sentiment"
          val={overallSent > 0.05 ? "POS" : overallSent < -0.05 ? "NEG" : "NEU"}
          sub={`${Math.round(sentBuckets.Positive/(posts.length||1)*100)}% pos · ${Math.round(sentBuckets.Negative/(posts.length||1)*100)}% neg`}
          color={sentColor(overallSent)} delay={0.20} />
      </div>

      {/* ── Main chart + sidebar ────────────────────────────────────────────── */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 300px", gap: 16, marginBottom: 16 }}>

        {/* Chart panel */}
        <div className="card">
          <div className="card-head" style={{ paddingBottom: 14 }}>
            <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
              {CHARTS.map(t => (
                <button key={t} style={tabStyle(t)} onClick={() => setChart(t)}>
                  {t.charAt(0).toUpperCase() + t.slice(1)}
                </button>
              ))}
            </div>
          </div>
          <div style={{ padding: 20 }}>
            <AnimatePresence mode="wait">

              {/* ENGAGEMENT */}
              {activeChart === "engagement" && (
                <motion.div key="eng" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
                  <div style={{ fontFamily: "'Syne', sans-serif", fontSize: 13, fontWeight: 700, marginBottom: 4 }}>
                    Engagement Score — Top 20
                  </div>
                  <div style={{ color: "#4a5568", fontSize: 10, marginBottom: 16 }}>
                    score + (comments × 2) · last {timeRange.label}
                  </div>
                  <ResponsiveContainer width="100%" height={380}>
                    <BarChart data={topPosts} layout="vertical" margin={{ left: 0, right: 20 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,69,0,0.06)" horizontal={false} />
                      <XAxis type="number" stroke="#2d3748" tick={{ fill: "#4a5568", fontSize: 10 }} />
                      <YAxis type="category" dataKey="title" width={200} tick={{ fill: "#4a5568", fontSize: 10 }}
                        tickFormatter={v => v?.slice(0,28) + (v?.length > 28 ? "…" : "")} />
                      <Tooltip content={<CustomTooltip />} />
                      <Bar dataKey="engagement_score" name="Engagement" radius={[0,4,4,0]}>
                        {topPosts.map((_, i) => <Cell key={i} fill={`rgba(255,69,0,${1 - i*0.04})`} />)}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </motion.div>
              )}

              {/* VELOCITY LEADERBOARD — new */}
              {activeChart === "velocity" && (
                <motion.div key="vel" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
                  <div style={{ fontFamily: "'Syne', sans-serif", fontSize: 13, fontWeight: 700, marginBottom: 4 }}>
                    Rising Right Now
                  </div>
                  <div style={{ color: "#4a5568", fontSize: 10, marginBottom: 20 }}>
                    upvotes/sec · posts gaining score fastest in last {timeRange.label}
                  </div>
                  <div style={{ display: "flex", flexDirection: "column", gap: 0 }}>
                    {velocityLeaders.map((p, i) => {
                      const vel = p.velocity || p.score_velocity || 0;
                      const maxVel = velocityLeaders[0]?.velocity || velocityLeaders[0]?.score_velocity || 1;
                      const pct = Math.max((vel / maxVel) * 100, 2);
                      return (
                        <motion.div
                          key={p.id}
                          initial={{ opacity: 0, x: -10 }}
                          animate={{ opacity: 1, x: 0 }}
                          transition={{ delay: i * 0.04 }}
                          style={{
                            padding: "12px 16px",
                            borderBottom: "1px solid rgba(255,255,255,0.04)",
                            position: "relative", overflow: "hidden",
                          }}
                        >
                          {/* velocity bar background */}
                          <div style={{
                            position: "absolute", inset: 0,
                            width: `${pct}%`,
                            background: `rgba(255,69,0,${0.04 + (i === 0 ? 0.06 : 0)})`,
                            borderRight: "1px solid rgba(255,69,0,0.15)",
                            transition: "width 0.6s ease",
                          }} />
                          <div style={{ position: "relative", display: "flex", alignItems: "center", gap: 12 }}>
                            <span style={{
                              fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 16,
                              color: i < 3 ? "#ff4500" : "#2d3748", minWidth: 24,
                            }}>#{i+1}</span>
                            <div style={{ flex: 1, minWidth: 0 }}>
                              <div style={{ fontSize: 9, color: "#ff6b35", marginBottom: 3 }}>r/{p.subreddit}</div>
                              <div style={{
                                fontFamily: "'Syne', sans-serif", fontSize: 12, fontWeight: 600,
                                overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
                              }}>{p.title}</div>
                            </div>
                            <div style={{ textAlign: "right", flexShrink: 0 }}>
                              <div style={{ color: "#3ecf74", fontSize: 15, fontWeight: 700, fontFamily: "'JetBrains Mono', monospace" }}>
                                ▲{vel.toFixed(2)}<span style={{ fontSize: 9, color: "#4a5568" }}>/s</span>
                              </div>
                              <div style={{ color: "#2d3748", fontSize: 10 }}>▲{fmt(p.current_score)}</div>
                            </div>
                          </div>
                        </motion.div>
                      );
                    })}
                    {velocityLeaders.length === 0 && (
                      <div style={{ textAlign: "center", padding: 40, color: "#2d3748", fontSize: 11 }}>
                        No velocity data yet — posts need at least one refresh cycle
                      </div>
                    )}
                  </div>
                </motion.div>
              )}

              {/* ACTIVITY TIMELINE — new */}
              {activeChart === "timeline" && (
                <motion.div key="tl" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
                  <div style={{ fontFamily: "'Syne', sans-serif", fontSize: 13, fontWeight: 700, marginBottom: 4 }}>
                    Activity Timeline
                  </div>
                  <div style={{ color: "#4a5568", fontSize: 10, marginBottom: 16 }}>
                    posts ingested per hour · last {timeRange.label}
                  </div>
                  <ResponsiveContainer width="100%" height={180}>
                    <BarChart data={timelineData} margin={{ left: 0, right: 10, bottom: 20 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,69,0,0.06)" vertical={false} />
                      <XAxis dataKey="label" stroke="#2d3748" tick={{ fill: "#4a5568", fontSize: 9 }}
                        angle={-35} textAnchor="end" />
                      <YAxis stroke="#2d3748" tick={{ fill: "#4a5568", fontSize: 10 }} />
                      <Tooltip content={<CustomTooltip />} />
                      <Bar dataKey="posts" name="New Posts" fill="#ff4500" opacity={0.8} radius={[3,3,0,0]} />
                    </BarChart>
                  </ResponsiveContainer>
                  <div style={{ marginTop: 24, fontFamily: "'Syne', sans-serif", fontSize: 12, fontWeight: 700, marginBottom: 12, color: "#4a5568" }}>
                    AVG SCORE PER HOUR
                  </div>
                  <ResponsiveContainer width="100%" height={140}>
                    <AreaChart data={timelineData} margin={{ left: 0, right: 10 }}>
                      <defs>
                        <linearGradient id="scoreGrad" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="0%"   stopColor="#3ecf74" stopOpacity={0.3} />
                          <stop offset="100%" stopColor="#3ecf74" stopOpacity={0.02} />
                        </linearGradient>
                      </defs>
                      <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,69,0,0.06)" vertical={false} />
                      <XAxis dataKey="label" stroke="#2d3748" tick={{ fill: "#4a5568", fontSize: 9 }} angle={-35} textAnchor="end" />
                      <YAxis stroke="#2d3748" tick={{ fill: "#4a5568", fontSize: 10 }} />
                      <Tooltip content={<CustomTooltip />} />
                      <Area dataKey="score" name="Avg Score" stroke="#3ecf74" strokeWidth={2}
                        fill="url(#scoreGrad)" dot={false} />
                    </AreaChart>
                  </ResponsiveContainer>
                </motion.div>
              )}

              {/* SENTIMENT */}
              {activeChart === "sentiment" && (
                <motion.div key="sent" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
                  <div style={{ fontFamily: "'Syne', sans-serif", fontSize: 13, fontWeight: 700, marginBottom: 4 }}>
                    Sentiment Distribution
                  </div>
                  <div style={{ color: "#4a5568", fontSize: 10, marginBottom: 16 }}>
                    VADER compound score · last {timeRange.label}
                  </div>
                  <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20, alignItems: "center" }}>
                    <ResponsiveContainer width="100%" height={280}>
                      <PieChart>
                        <Pie data={sentPieData} dataKey="value" nameKey="name"
                          cx="50%" cy="50%" outerRadius={110} innerRadius={60} paddingAngle={3}>
                          {sentPieData.map(e => <Cell key={e.name} fill={PIE_COLORS[e.name]} />)}
                        </Pie>
                        <Tooltip content={<CustomTooltip />} />
                        <Legend iconType="circle" iconSize={8}
                          formatter={v => <span style={{ fontSize: 11, color: "#4a5568" }}>{v}</span>} />
                      </PieChart>
                    </ResponsiveContainer>
                    <div>
                      {Object.entries(sentBuckets).map(([k, v]) => (
                        <div key={k} style={{ marginBottom: 18 }}>
                          <div style={{ display: "flex", justifyContent: "space-between", fontSize: 11, marginBottom: 6 }}>
                            <span style={{ color: PIE_COLORS[k], fontFamily: "'Syne', sans-serif", fontWeight: 700 }}>{k}</span>
                            <span style={{ color: "#4a5568" }}>{v} posts ({posts.length ? Math.round(v/posts.length*100) : 0}%)</span>
                          </div>
                          <div style={{ height: 6, background: "rgba(255,255,255,0.04)", borderRadius: 3, overflow: "hidden" }}>
                            <motion.div
                              initial={{ width: 0 }}
                              animate={{ width: `${posts.length ? (v/posts.length*100) : 0}%` }}
                              transition={{ duration: 0.8, ease: "easeOut" }}
                              style={{ height: "100%", background: PIE_COLORS[k], borderRadius: 3 }}
                            />
                          </div>
                        </div>
                      ))}
                      {/* sentiment by subreddit breakdown */}
                      <div style={{ marginTop: 20, padding: "14px 16px", background: "rgba(255,255,255,0.02)",
                        borderRadius: 8, border: "1px solid rgba(255,255,255,0.04)" }}>
                        <div style={{ fontSize: 10, color: "#4a5568", marginBottom: 10, fontFamily: "'Syne', sans-serif", letterSpacing: "0.1em" }}>
                          BY SUBREDDIT
                        </div>
                        {subData.slice(0,5).map(s => (
                          <div key={s.name} style={{ marginBottom: 8 }}>
                            <div style={{ display: "flex", justifyContent: "space-between", fontSize: 10, marginBottom: 3 }}>
                              <span style={{ color: "#ff6b35" }}>r/{s.name}</span>
                              <span style={{ color: "#4a5568" }}>
                                <span style={{ color: "#3ecf74" }}>{s.pos}+</span>
                                {" · "}
                                <span style={{ color: "#f56565" }}>{s.neg}-</span>
                              </span>
                            </div>
                            <div style={{ height: 4, background: "rgba(255,255,255,0.04)", borderRadius: 2, overflow: "hidden", display: "flex" }}>
                              <div style={{ width: `${(s.pos/(s.posts||1))*100}%`, background: "#3ecf74", opacity: 0.8 }} />
                              <div style={{ width: `${(s.neu/(s.posts||1))*100}%`, background: "#4a5568", opacity: 0.5 }} />
                              <div style={{ width: `${(s.neg/(s.posts||1))*100}%`, background: "#f56565", opacity: 0.8 }} />
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  </div>
                </motion.div>
              )}

              {/* SUBREDDITS */}
              {activeChart === "subreddits" && (
                <motion.div key="sub" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
                  <div style={{ fontFamily: "'Syne', sans-serif", fontSize: 13, fontWeight: 700, marginBottom: 4 }}>
                    Subreddit Activity
                  </div>
                  <div style={{ color: "#4a5568", fontSize: 10, marginBottom: 16 }}>
                    post count & cumulative score · last {timeRange.label}
                  </div>
                  <ResponsiveContainer width="100%" height={380}>
                    <BarChart data={subData} margin={{ left: 0, right: 20, bottom: 30 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,69,0,0.06)" vertical={false} />
                      <XAxis dataKey="name" stroke="#2d3748" tick={{ fill: "#4a5568", fontSize: 10 }}
                        tickFormatter={v => `r/${v}`} angle={-35} textAnchor="end" />
                      <YAxis yAxisId="l" stroke="#2d3748" tick={{ fill: "#4a5568", fontSize: 10 }} />
                      <YAxis yAxisId="r" orientation="right" stroke="#2d3748" tick={{ fill: "#4a5568", fontSize: 10 }} />
                      <Tooltip content={<CustomTooltip />} />
                      <Bar yAxisId="l" dataKey="score"  name="Total Score" fill="#ff4500" opacity={0.85} radius={[4,4,0,0]} />
                      <Bar yAxisId="r" dataKey="posts"  name="Posts"       fill="#7c3aed" opacity={0.7}  radius={[4,4,0,0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </motion.div>
              )}

              {/* KEYWORDS — new */}
              {activeChart === "keywords" && (
                <motion.div key="kw" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
                  <div style={{ fontFamily: "'Syne', sans-serif", fontSize: 13, fontWeight: 700, marginBottom: 4 }}>
                    Trending Keywords
                  </div>
                  <div style={{ color: "#4a5568", fontSize: 10, marginBottom: 20 }}>
                    most frequent terms in post titles · last {timeRange.label}
                  </div>
                  <ResponsiveContainer width="100%" height={200}>
                    <BarChart data={keywordData.slice(0,15)} layout="vertical" margin={{ left: 0, right: 20 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,69,0,0.06)" horizontal={false} />
                      <XAxis type="number" stroke="#2d3748" tick={{ fill: "#4a5568", fontSize: 10 }} />
                      <YAxis type="category" dataKey="word" width={100} tick={{ fill: "#4a5568", fontSize: 10 }} />
                      <Tooltip content={<CustomTooltip />} />
                      <Bar dataKey="count" name="Frequency" radius={[0,4,4,0]}>
                        {keywordData.slice(0,15).map((_, i) => (
                          <Cell key={i} fill={`rgba(99,179,237,${1 - i*0.06})`} />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                  {/* keyword bubbles */}
                  <div style={{ marginTop: 24, display: "flex", flexWrap: "wrap", gap: 8 }}>
                    {keywordData.map((k, i) => {
                      const maxCount = keywordData[0]?.count || 1;
                      const scale = 0.7 + (k.count / maxCount) * 0.9;
                      return (
                        <motion.div
                          key={k.word}
                          initial={{ opacity: 0, scale: 0.8 }}
                          animate={{ opacity: 1, scale: 1 }}
                          transition={{ delay: i * 0.02 }}
                          style={{
                            padding: "5px 12px", borderRadius: 20,
                            background: `rgba(99,179,237,${0.05 + (k.count/maxCount)*0.15})`,
                            border: `1px solid rgba(99,179,237,${0.1 + (k.count/maxCount)*0.3})`,
                            color: `rgba(99,179,237,${0.5 + (k.count/maxCount)*0.5})`,
                            fontSize: Math.round(9 + scale * 4),
                            fontFamily: "'JetBrains Mono', monospace",
                            fontWeight: k.count === keywordData[0]?.count ? 700 : 400,
                          }}
                        >
                          {k.word}
                          <span style={{ marginLeft: 5, opacity: 0.5, fontSize: 9 }}>{k.count}</span>
                        </motion.div>
                      );
                    })}
                  </div>
                </motion.div>
              )}

              {/* SCATTER */}
              {activeChart === "scatter" && (
                <motion.div key="scat" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
                  <div style={{ fontFamily: "'Syne', sans-serif", fontSize: 13, fontWeight: 700, marginBottom: 4 }}>
                    Age vs Engagement
                  </div>
                  <div style={{ color: "#4a5568", fontSize: 10, marginBottom: 16 }}>
                    post age (minutes) × engagement score · last {timeRange.label}
                  </div>
                  <ResponsiveContainer width="100%" height={380}>
                    <ScatterChart margin={{ left: 0, right: 20 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,69,0,0.06)" />
                      <XAxis dataKey="x" name="Age (min)" stroke="#2d3748" tick={{ fill: "#4a5568", fontSize: 10 }}
                        label={{ value: "Age (min)", position: "insideBottom", offset: -5, fill: "#2d3748", fontSize: 10 }} />
                      <YAxis dataKey="y" name="Engagement" stroke="#2d3748" tick={{ fill: "#4a5568", fontSize: 10 }} />
                      <Tooltip content={<CustomTooltip />} cursor={{ stroke: "rgba(255,69,0,0.3)" }} />
                      <Scatter data={scatterData} fill="#ff4500" opacity={0.6} />
                    </ScatterChart>
                  </ResponsiveContainer>
                </motion.div>
              )}

            </AnimatePresence>
          </div>
        </div>

        {/* ── Right sidebar ─────────────────────────────────────────────────── */}
        <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>

          {/* Top Authors */}
          <div className="card">
            <div className="card-head">
              <span className="card-title">Top Authors</span>
              <span className="card-badge">last {timeRange.label}</span>
            </div>
            {(stats?.users ?? []).slice(0,8).map((u, i) => (
              <div key={u.author} style={{
                display: "flex", alignItems: "center", gap: 10,
                padding: "9px 18px", borderBottom: "1px solid rgba(255,255,255,0.04)", fontSize: 11,
              }}>
                <span style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 12,
                  color: i < 3 ? "#ff4500" : "#2d3748", minWidth: 22 }}>{i+1}</span>
                <span style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                  u/{u.author}
                </span>
                <span style={{ color: "#ff4500" }}>▲{fmt(u.total_score)}</span>
                <span style={{ color: "#2d3748" }}>{u.posts}p</span>
              </div>
            ))}
            {!stats?.users?.length && (
              <div style={{ padding: "16px 18px", color: "#2d3748", fontSize: 11 }}>No author data yet</div>
            )}
          </div>

          {/* Subreddit rankings */}
          <div className="card">
            <div className="card-head">
              <span className="card-title">Subreddit Rankings</span>
              <span className="card-badge">by posts</span>
            </div>
            {subData.slice(0,8).map((s, i) => (
              <div key={s.name} style={{
                display: "flex", alignItems: "center", gap: 10,
                padding: "9px 18px", borderBottom: "1px solid rgba(255,255,255,0.04)", fontSize: 11,
              }}>
                <span style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 12,
                  color: i < 3 ? "#ff6b35" : "#2d3748", minWidth: 22 }}>{i+1}</span>
                <span style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", color: "#ff6b35" }}>
                  r/{s.name}
                </span>
                <span style={{ color: "#4a5568" }}>{fmt(s.posts)} posts</span>
              </div>
            ))}
          </div>

          {/* Most upvoted / commented */}
          {[
            { label: "Most Upvoted",   post: stats?.posts?.most_upvoted,   val: p => `▲${fmt(p.current_score)}`, col: "#3ecf74" },
            { label: "Most Commented", post: stats?.posts?.most_commented,  val: p => `💬${fmt(p.current_comments)}`, col: "#63b3ed" },
          ].map(({ label, post, val, col }) => post ? (
            <div key={label} className="card">
              <div className="card-head">
                <span className="card-title">{label}</span>
                <span className="card-badge" style={{ color: col }}>{val(post)}</span>
              </div>
              <div style={{ padding: "14px 18px" }}>
                <div style={{ color: "#ff4500", fontSize: 9, marginBottom: 6 }}>r/{post.subreddit__name ?? post.subreddit}</div>
                <div style={{ fontFamily: "'Syne', sans-serif", fontSize: 12, fontWeight: 700,
                  lineHeight: 1.45, color: "#dde1e8", marginBottom: 8 }}>
                  {post.title?.slice(0,100)}{post.title?.length > 100 ? "…" : ""}
                </div>
                <div style={{ fontSize: 10, color: "#4a5568" }}>by u/{post.author}</div>
              </div>
            </div>
          ) : null)}

        </div>
      </div>

    </div>
  );
}
