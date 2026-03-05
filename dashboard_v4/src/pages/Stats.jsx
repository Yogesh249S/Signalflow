import { useState, useEffect, useCallback, useMemo } from "react";
import {
  BarChart, Bar, AreaChart, Area,
  ScatterChart, Scatter, XAxis, YAxis, Tooltip,
  ResponsiveContainer, CartesianGrid, Cell,
  PieChart, Pie, Legend,
} from "recharts";
import {
  fetchPosts, fetchStats, fetchActivityTimeline,
  fmt, getSentiment, sentColor, TIME_RANGES,
} from "../api.js";

const POLL = 20000;

const CT = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{
      background: "#0d1117", border: "1px solid rgba(255,69,0,0.2)",
      borderRadius: 6, padding: "8px 12px", fontSize: 10,
    }}>
      {label && <div style={{ color: "#4a5568", marginBottom: 4 }}>{label}</div>}
      {payload.map((e, i) => (
        <div key={i} style={{ color: e.color || "#dde1e8" }}>
          {e.name}: {typeof e.value === "number" ? e.value.toFixed(2) : e.value}
        </div>
      ))}
    </div>
  );
};

function KpiCard({ label, val, sub, color }) {
  return (
    <div className="card" style={{ padding: "18px 20px", position: "relative", overflow: "hidden" }}>
      <div style={{
        position: "absolute", top: 0, left: 0, right: 0, height: 2,
        background: `linear-gradient(90deg, ${color || "#ff4500"}, transparent)`,
      }} />
      <div style={{
        fontSize: 9, color: "#4a5568", letterSpacing: "0.12em",
        textTransform: "uppercase", fontFamily: "'Syne', sans-serif", marginBottom: 6,
      }}>{label}</div>
      <div style={{
        fontFamily: "'Syne', sans-serif", fontSize: 26, fontWeight: 800,
        color: color || "#dde1e8", lineHeight: 1,
      }}>{val}</div>
      {sub && <div style={{ fontSize: 9, color: "#4a5568", marginTop: 4 }}>{sub}</div>}
    </div>
  );
}

function TimeRangePicker({ value, onChange }) {
  return (
    <div style={{ display: "flex", gap: 4 }}>
      {TIME_RANGES.map(r => (
        <button key={r.label} onClick={() => onChange(r)} style={{
          padding: "4px 10px", borderRadius: 5, fontSize: 9, cursor: "pointer",
          fontFamily: "'JetBrains Mono', monospace", fontWeight: 600,
          border: "1px solid transparent", transition: "all 0.15s",
          background: value.label === r.label ? "#ff4500" : "rgba(255,255,255,0.04)",
          color:      value.label === r.label ? "#fff"    : "#4a5568",
        }}>{r.label}</button>
      ))}
    </div>
  );
}

const CHARTS = ["engagement", "velocity", "timeline", "sentiment", "subreddits", "keywords", "scatter"];

// Stop words excluded from title-based keyword fallback
const STOP = new Set([
  "that","this","with","from","have","they","will","been","were","your",
  "what","when","about","just","than","then","there","their","which",
  "would","could","should","after","before","these","those","some","more",
  "into","over","also","here","like","very","even","much","such","only",
  "dont","cant","wont","isnt","arent","wasnt","hasnt","didnt","doesnt",
]);

export default function StatsPage() {
  const [posts,     setPosts]    = useState([]);
  const [stats,     setStats]    = useState(null);
  // FIX: timeline now sourced from /api/stats/timeline/ (server-side, first_seen_at-based)
  // instead of being computed client-side from created_utc (which breaks short windows)
  const [timeline,  setTimeline] = useState([]);
  const [loading,   setLoading]  = useState(true);
  const [chart,     setChart]    = useState("engagement");
  const [timeRange, setTR]       = useState(TIME_RANGES[3]);

  const load = useCallback(async () => {
    try {
      const [pr, sr, tr] = await Promise.all([
        fetchPosts(null, timeRange.hours),
        fetchStats(Math.max(1, Math.ceil(timeRange.hours / 24))),
        fetchActivityTimeline(timeRange.hours),  // FIX: was declared but never called
      ]);
      setPosts(pr.data?.results ?? pr.data ?? []);
      setStats(sr.data);
      setTimeline(Array.isArray(tr.data) ? tr.data : []);
      setLoading(false);
    } catch (e) { console.error(e); setLoading(false); }
  }, [timeRange]);

  useEffect(() => {
    setLoading(true);
    load();
    const t = setInterval(load, POLL);
    return () => clearInterval(t);
  }, [load]);

  // ── Derived data ─────────────────────────────────────────────────────────────
  const topPosts      = useMemo(() => [...posts].sort((a,b) => (b.current_score||0)-(a.current_score||0)).slice(0,20), [posts]);
  const trendingNow   = useMemo(() => posts.filter(p => p.is_trending), [posts]);
  const fastestRising = useMemo(() => [...posts].sort((a,b) => (b.score_velocity||0)-(a.score_velocity||0))[0], [posts]);
  const overallSent   = useMemo(() => posts.length ? posts.reduce((s,p) => s + getSentiment(p), 0) / posts.length : 0, [posts]);

  const subData = useMemo(() => {
    const m = {};
    posts.forEach(p => {
      const s = p.subreddit || "unknown";
      if (!m[s]) m[s] = { name: s, posts: 0, score: 0, comments: 0, pos: 0, neg: 0, neu: 0 };
      m[s].posts++; m[s].score += p.current_score||0; m[s].comments += p.current_comments||0;
      const sent = getSentiment(p);
      if (sent > 0.05) m[s].pos++; else if (sent < -0.05) m[s].neg++; else m[s].neu++;
    });
    return Object.values(m).sort((a,b) => b.score-a.score).slice(0,12);
  }, [posts]);

  const sentBuckets = useMemo(() => {
    const b = { Positive: 0, Neutral: 0, Negative: 0 };
    posts.forEach(p => {
      const s = getSentiment(p);
      if (s > 0.05) b.Positive++; else if (s < -0.05) b.Negative++; else b.Neutral++;
    });
    return b;
  }, [posts]);

  const sentPieData = useMemo(() =>
    Object.entries(sentBuckets).map(([name, value]) => ({ name, value })), [sentBuckets]);
  const PIE_COLORS = { Positive: "#3ecf74", Neutral: "#4a5568", Negative: "#f56565" };

  const scatterData = useMemo(() =>
    posts.map(p => ({
      x: Math.round(p.age_minutes||0),
      y: Math.round(p.engagement_score||0),
      name: p.title?.slice(0,40),
    })), [posts]);

  const velocityLeaders = useMemo(() =>
    [...posts].sort((a,b) => (b.score_velocity||0)-(a.score_velocity||0)).slice(0,10),
    [posts]);

  // FIX: Use server-side timeline (based on first_seen_at/captured_at in post_metrics_history).
  // If server timeline is empty, fall back to bucketing by first_seen_at from post objects.
  // OLD BUG: was bucketing by created_utc — Reddit top posts were created hours/days ago,
  // so ALL of them fell outside a 30m or 1h window → empty chart or identical slots.
  const timelineData = useMemo(() => {
    if (timeline.length > 0) {
      // Server-side data: normalise field names across possible API shapes
      return timeline.map(b => ({
        ...b,
        label: b.label || new Date(b.hour || b.bucket || b.ts || b.captured_at)
          .toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }),
        posts: b.posts ?? b.post_count ?? 0,
        score: b.score ?? b.avg_score ?? 0,
      }));
    }

    // Client-side fallback using first_seen_at (ingestion time), NOT created_utc
    const buckets = {};
    posts.forEach(p => {
      // first_seen_at = when the poller first discovered the post → within our window
      // created_utc   = when Reddit published it → can be days ago for hot posts
      const ts = p.first_seen_at || p.created_utc;
      if (!ts) return;
      const h = new Date(ts);
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
  }, [timeline, posts]);

  // FIX: Keywords now pulled from post_nlp_features.keywords (JSONB) stored per-post.
  // OLD BUG: was doing naive word-frequency over raw titles → identical results for any
  // time range since the same top-100 posts dominate every window, and stop words leaked in.
  const keywordData = useMemo(() => {
    const freq = {};
    let hasNlpKeywords = false;

    posts.forEach(p => {
      const nlp = p.keywords;
      // keywords field is JSONB: could be array ["ai","gpu",...] or object {"ai":3,"gpu":2}
      if (nlp && typeof nlp === "object") {
        hasNlpKeywords = true;
        const kws = Array.isArray(nlp) ? nlp : Object.keys(nlp);
        kws.forEach(w => {
          if (w && typeof w === "string" && w.length > 2) {
            // If object, weight by stored frequency; if array, count occurrences
            const weight = Array.isArray(nlp) ? 1 : (nlp[w] || 1);
            freq[w] = (freq[w] || 0) + weight;
          }
        });
        return; // skip title fallback for this post
      }

      // Fallback: extract from title with aggressive stop-word filtering
      (p.title || "").toLowerCase().split(/\W+/)
        .filter(w => w.length > 4 && !STOP.has(w) && !/^\d+$/.test(w))
        .slice(0, 8) // cap per-post contribution
        .forEach(w => { freq[w] = (freq[w] || 0) + 1; });
    });

    return {
      data: Object.entries(freq).sort((a,b) => b[1]-a[1]).slice(0,25).map(([word, count]) => ({ word, count })),
      fromNlp: hasNlpKeywords,
    };
  }, [posts]);

  const tabStyle = t => ({
    padding: "5px 12px", borderRadius: 5, fontSize: 9, cursor: "pointer",
    fontFamily: "'Syne', sans-serif", fontWeight: 700, letterSpacing: "0.08em",
    border: "1px solid transparent", transition: "all 0.15s",
    background: chart === t ? "#ff4500" : "transparent",
    color:      chart === t ? "#fff"    : "#4a5568",
  });

  if (loading) return (
    <div style={{
      display: "flex", alignItems: "center", justifyContent: "center",
      height: "100%", color: "#2d3748", fontSize: 11,
    }}>Loading analytics…</div>
  );

  const allNeutral = posts.length > 0 && posts.every(p => getSentiment(p) === 0);

  return (
    <div className="page-scroll" style={{ padding: "20px 24px" }}>

      {/* Time range */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 16 }}>
        <div style={{
          fontFamily: "'Syne', sans-serif", fontSize: 10, fontWeight: 700,
          letterSpacing: "0.14em", textTransform: "uppercase", color: "#4a5568",
        }}>TIME WINDOW</div>
        <TimeRangePicker value={timeRange} onChange={setTR} />
      </div>

      {/* KPI strip */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(6,1fr)", gap: 8, marginBottom: 16 }}>
        <KpiCard label="Posts"
          val={fmt(posts.length)} sub={`last ${timeRange.label}`} color="#ff4500" />
        <KpiCard label="Trending"
          val={trendingNow.length} sub="active posts" color="#f6ad55" />
        <KpiCard label="Top Score"
          val={fmt(topPosts[0]?.current_score)} sub="upvotes" color="#3ecf74" />
        <KpiCard label="Fastest ▲"
          val={`▲${(fastestRising?.score_velocity||0).toFixed(2)}/s`}
          sub={fastestRising ? `r/${fastestRising.subreddit}` : "—"} color="#63b3ed" />
        <KpiCard label="Authors"
          val={fmt(new Set(posts.map(p=>p.author)).size)} sub="unique" color="#a78bfa" />
        <KpiCard label="Sentiment"
          val={overallSent > 0.05 ? "POS" : overallSent < -0.05 ? "NEG" : "NEU"}
          sub={`${Math.round(sentBuckets.Positive/(posts.length||1)*100)}% pos`}
          color={sentColor(overallSent)} />
      </div>

      {/* NLP not-yet-processed notice */}
      {allNeutral && (
        <div style={{
          marginBottom: 12, padding: "10px 16px", borderRadius: 8,
          background: "rgba(246,173,85,0.08)", border: "1px solid rgba(246,173,85,0.2)",
          fontSize: 10, color: "#f6ad55", display: "flex", alignItems: "center", gap: 8,
        }}>
          <span>⚠</span>
          <span>
            All sentiment scores are 0.00 — NLP features haven't been processed yet or
            the processor is still catching up. Sentiment & keywords will populate once
            posts complete their first refresh cycle through the processing pipeline.
          </span>
        </div>
      )}

      {/* Chart + sidebar */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 280px", gap: 14, marginBottom: 14 }}>
        <div className="card">
          <div className="card-head" style={{ paddingBottom: 12 }}>
            <div style={{ display: "flex", gap: 3, flexWrap: "wrap" }}>
              {CHARTS.map(t => (
                <button key={t} style={tabStyle(t)} onClick={() => setChart(t)}>
                  {t.charAt(0).toUpperCase() + t.slice(1)}
                </button>
              ))}
            </div>
          </div>
          <div style={{ padding: 18 }}>

            {chart === "engagement" && (
              <div>
                <div style={{ fontFamily: "'Syne', sans-serif", fontSize: 12, fontWeight: 700, marginBottom: 3 }}>
                  Engagement Score — Top 20
                </div>
                <div style={{ color: "#4a5568", fontSize: 9, marginBottom: 14 }}>
                  score + (comments × 2) · last {timeRange.label}
                </div>
                <ResponsiveContainer width="100%" height={360}>
                  <BarChart data={topPosts} layout="vertical" margin={{ left: 0, right: 20 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,69,0,0.06)" horizontal={false} />
                    <XAxis type="number" stroke="#2d3748" tick={{ fill: "#4a5568", fontSize: 9 }} />
                    <YAxis type="category" dataKey="title" width={190}
                      tick={{ fill: "#4a5568", fontSize: 9 }}
                      tickFormatter={v => v?.slice(0,26) + (v?.length > 26 ? "…" : "")} />
                    <Tooltip content={<CT />} />
                    <Bar dataKey="engagement_score" name="Engagement" radius={[0,4,4,0]}>
                      {topPosts.map((_, i) => <Cell key={i} fill={`rgba(255,69,0,${1 - i*0.04})`} />)}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
            )}

            {chart === "velocity" && (
              <div>
                <div style={{ fontFamily: "'Syne', sans-serif", fontSize: 12, fontWeight: 700, marginBottom: 3 }}>
                  Rising Right Now
                </div>
                <div style={{ color: "#4a5568", fontSize: 9, marginBottom: 16 }}>
                  upvotes/sec · fastest gaining posts · last {timeRange.label}
                </div>
                {velocityLeaders.every(p => (p.score_velocity||0) === 0) ? (
                  <div style={{
                    textAlign: "center", padding: "40px 20px",
                    color: "#2d3748", fontSize: 10, lineHeight: 1.9,
                  }}>
                    <div style={{ fontSize: 24, marginBottom: 10 }}>◌</div>
                    <div>No velocity data yet.</div>
                    <div>Posts need <strong style={{ color: "#4a5568" }}>two refresh cycles</strong> to compute velocity.</div>
                    <div style={{ fontSize: 9, color: "#2d3748", marginTop: 6 }}>
                      Check back in ~60s after the processor runs.
                    </div>
                  </div>
                ) : velocityLeaders.map((p, i) => {
                  const vel    = p.score_velocity || 0;
                  const maxVel = velocityLeaders[0]?.score_velocity || 1;
                  const pct    = Math.max((vel / maxVel) * 100, 2);
                  return (
                    <div key={p.id} style={{
                      padding: "10px 14px", borderBottom: "1px solid rgba(255,255,255,0.04)",
                      position: "relative", overflow: "hidden",
                    }}>
                      <div style={{
                        position: "absolute", inset: 0, width: `${pct}%`,
                        background: `rgba(255,69,0,${0.03 + (i === 0 ? 0.05 : 0)})`,
                        borderRight: "1px solid rgba(255,69,0,0.12)", transition: "width 0.5s ease",
                      }} />
                      <div style={{ position: "relative", display: "flex", alignItems: "center", gap: 10 }}>
                        <span style={{
                          fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 14,
                          color: i < 3 ? "#ff4500" : "#2d3748", minWidth: 22,
                        }}>#{i+1}</span>
                        <div style={{ flex: 1, minWidth: 0 }}>
                          <div style={{ fontSize: 8, color: "#ff6b35", marginBottom: 2 }}>r/{p.subreddit}</div>
                          <div style={{
                            fontFamily: "'Syne', sans-serif", fontSize: 11, fontWeight: 600,
                            overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
                          }}>{p.title}</div>
                        </div>
                        <div style={{ textAlign: "right", flexShrink: 0 }}>
                          <div style={{
                            color: "#3ecf74", fontSize: 13, fontWeight: 700,
                            fontFamily: "'JetBrains Mono', monospace",
                          }}>
                            ▲{vel.toFixed(3)}<span style={{ fontSize: 8, color: "#4a5568" }}>/s</span>
                          </div>
                          <div style={{ color: "#2d3748", fontSize: 9 }}>▲{fmt(p.current_score)}</div>
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            )}

            {chart === "timeline" && (
              <div>
                <div style={{ fontFamily: "'Syne', sans-serif", fontSize: 12, fontWeight: 700, marginBottom: 3 }}>
                  Activity Timeline
                </div>
                <div style={{ color: "#4a5568", fontSize: 9, marginBottom: 14 }}>
                  posts ingested per hour · last {timeRange.label}
                  {timeline.length > 0
                    ? <span style={{ color: "#3ecf74", marginLeft: 6 }}>● server-side (accurate)</span>
                    : <span style={{ color: "#f6ad55", marginLeft: 6 }}>● client fallback via first_seen_at</span>
                  }
                </div>
                {timelineData.length === 0 ? (
                  <div style={{
                    textAlign: "center", padding: "40px 20px",
                    color: "#2d3748", fontSize: 10, lineHeight: 1.8,
                  }}>
                    <div style={{ fontSize: 24, marginBottom: 10 }}>◌</div>
                    No activity recorded in this time window.
                    <div style={{ fontSize: 9, marginTop: 6 }}>
                      Try a wider range — posts seen before this window won't appear.
                    </div>
                  </div>
                ) : (
                  <>
                    <ResponsiveContainer width="100%" height={170}>
                      <BarChart data={timelineData} margin={{ left: 0, right: 10, bottom: 18 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,69,0,0.06)" vertical={false} />
                        <XAxis dataKey="label" stroke="#2d3748"
                          tick={{ fill: "#4a5568", fontSize: 8 }} angle={-30} textAnchor="end" />
                        <YAxis stroke="#2d3748" tick={{ fill: "#4a5568", fontSize: 9 }} />
                        <Tooltip content={<CT />} />
                        <Bar dataKey="posts" name="Ingested Posts" fill="#ff4500" opacity={0.8} radius={[3,3,0,0]} />
                      </BarChart>
                    </ResponsiveContainer>
                    <div style={{
                      marginTop: 20, fontFamily: "'Syne', sans-serif", fontSize: 10,
                      fontWeight: 700, marginBottom: 10, color: "#4a5568",
                    }}>AVG SCORE / HOUR</div>
                    <ResponsiveContainer width="100%" height={130}>
                      <AreaChart data={timelineData} margin={{ left: 0, right: 10 }}>
                        <defs>
                          <linearGradient id="sg" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="0%"   stopColor="#3ecf74" stopOpacity={0.3} />
                            <stop offset="100%" stopColor="#3ecf74" stopOpacity={0.02} />
                          </linearGradient>
                        </defs>
                        <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,69,0,0.06)" vertical={false} />
                        <XAxis dataKey="label" stroke="#2d3748"
                          tick={{ fill: "#4a5568", fontSize: 8 }} angle={-30} textAnchor="end" />
                        <YAxis stroke="#2d3748" tick={{ fill: "#4a5568", fontSize: 9 }} />
                        <Tooltip content={<CT />} />
                        <Area dataKey="score" name="Avg Score" stroke="#3ecf74" strokeWidth={2}
                          fill="url(#sg)" dot={false} />
                      </AreaChart>
                    </ResponsiveContainer>
                  </>
                )}
              </div>
            )}

            {chart === "sentiment" && (
              <div>
                <div style={{ fontFamily: "'Syne', sans-serif", fontSize: 12, fontWeight: 700, marginBottom: 3 }}>
                  Sentiment Distribution
                </div>
                <div style={{ color: "#4a5568", fontSize: 9, marginBottom: 14 }}>
                  VADER compound score from post_nlp_features · last {timeRange.label}
                </div>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20, alignItems: "center" }}>
                  <ResponsiveContainer width="100%" height={260}>
                    <PieChart>
                      <Pie data={sentPieData} dataKey="value" nameKey="name"
                        cx="50%" cy="50%" outerRadius={105} innerRadius={55} paddingAngle={3}>
                        {sentPieData.map(e => <Cell key={e.name} fill={PIE_COLORS[e.name]} />)}
                      </Pie>
                      <Tooltip content={<CT />} />
                      <Legend iconType="circle" iconSize={7}
                        formatter={v => <span style={{ fontSize: 10, color: "#4a5568" }}>{v}</span>} />
                    </PieChart>
                  </ResponsiveContainer>
                  <div>
                    {Object.entries(sentBuckets).map(([k, v]) => (
                      <div key={k} style={{ marginBottom: 16 }}>
                        <div style={{ display: "flex", justifyContent: "space-between", fontSize: 10, marginBottom: 5 }}>
                          <span style={{ color: PIE_COLORS[k], fontFamily: "'Syne', sans-serif", fontWeight: 700 }}>{k}</span>
                          <span style={{ color: "#4a5568" }}>
                            {v} ({posts.length ? Math.round(v/posts.length*100) : 0}%)
                          </span>
                        </div>
                        <div style={{ height: 5, background: "rgba(255,255,255,0.04)", borderRadius: 3, overflow: "hidden" }}>
                          <div style={{
                            height: "100%",
                            width: `${posts.length ? (v/posts.length*100) : 0}%`,
                            background: PIE_COLORS[k], borderRadius: 3, transition: "width 0.6s ease",
                          }} />
                        </div>
                      </div>
                    ))}
                    <div style={{
                      marginTop: 16, padding: "12px 14px",
                      background: "rgba(255,255,255,0.02)", borderRadius: 8,
                      border: "1px solid rgba(255,255,255,0.04)",
                    }}>
                      <div style={{
                        fontSize: 9, color: "#4a5568", marginBottom: 8,
                        fontFamily: "'Syne', sans-serif", letterSpacing: "0.1em",
                      }}>BY SUBREDDIT</div>
                      {subData.slice(0,5).map(s => (
                        <div key={s.name} style={{ marginBottom: 7 }}>
                          <div style={{ display: "flex", justifyContent: "space-between", fontSize: 9, marginBottom: 3 }}>
                            <span style={{ color: "#ff6b35" }}>r/{s.name}</span>
                            <span style={{ color: "#4a5568" }}>
                              <span style={{ color: "#3ecf74" }}>{s.pos}+</span> ·{" "}
                              <span style={{ color: "#f56565" }}>{s.neg}-</span>
                            </span>
                          </div>
                          <div style={{ height: 3, background: "rgba(255,255,255,0.04)", borderRadius: 2, overflow: "hidden", display: "flex" }}>
                            <div style={{ width: `${(s.pos/(s.posts||1))*100}%`, background: "#3ecf74", opacity: 0.8 }} />
                            <div style={{ width: `${(s.neu/(s.posts||1))*100}%`, background: "#4a5568", opacity: 0.5 }} />
                            <div style={{ width: `${(s.neg/(s.posts||1))*100}%`, background: "#f56565", opacity: 0.8 }} />
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              </div>
            )}

            {chart === "subreddits" && (
              <div>
                <div style={{ fontFamily: "'Syne', sans-serif", fontSize: 12, fontWeight: 700, marginBottom: 3 }}>
                  Subreddit Activity
                </div>
                <div style={{ color: "#4a5568", fontSize: 9, marginBottom: 14 }}>
                  post count & cumulative score · last {timeRange.label}
                </div>
                <ResponsiveContainer width="100%" height={360}>
                  <BarChart data={subData} margin={{ left: 0, right: 20, bottom: 28 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,69,0,0.06)" vertical={false} />
                    <XAxis dataKey="name" stroke="#2d3748" tick={{ fill: "#4a5568", fontSize: 9 }}
                      tickFormatter={v => `r/${v}`} angle={-30} textAnchor="end" />
                    <YAxis yAxisId="l" stroke="#2d3748" tick={{ fill: "#4a5568", fontSize: 9 }} />
                    <YAxis yAxisId="r" orientation="right" stroke="#2d3748" tick={{ fill: "#4a5568", fontSize: 9 }} />
                    <Tooltip content={<CT />} />
                    <Bar yAxisId="l" dataKey="score" name="Total Score" fill="#ff4500" opacity={0.85} radius={[4,4,0,0]} />
                    <Bar yAxisId="r" dataKey="posts" name="Posts"       fill="#7c3aed" opacity={0.7}  radius={[4,4,0,0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            )}

            {chart === "keywords" && (
              <div>
                <div style={{ fontFamily: "'Syne', sans-serif", fontSize: 12, fontWeight: 700, marginBottom: 3 }}>
                  Trending Keywords
                </div>
                <div style={{ color: "#4a5568", fontSize: 9, marginBottom: 16 }}>
                  {keywordData.fromNlp
                    ? <span style={{ color: "#3ecf74" }}>● from NLP feature extraction (post_nlp_features)</span>
                    : <span style={{ color: "#f6ad55" }}>● title-based fallback — NLP keywords not yet populated</span>
                  }{" · "}last {timeRange.label}
                </div>
                {keywordData.data.length === 0 ? (
                  <div style={{ textAlign: "center", padding: 40, color: "#2d3748", fontSize: 10 }}>
                    No keyword data in this window
                  </div>
                ) : (
                  <>
                    <ResponsiveContainer width="100%" height={190}>
                      <BarChart data={keywordData.data.slice(0,15)} layout="vertical" margin={{ left: 0, right: 20 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,69,0,0.06)" horizontal={false} />
                        <XAxis type="number" stroke="#2d3748" tick={{ fill: "#4a5568", fontSize: 9 }} />
                        <YAxis type="category" dataKey="word" width={90} tick={{ fill: "#4a5568", fontSize: 9 }} />
                        <Tooltip content={<CT />} />
                        <Bar dataKey="count" name="Frequency" radius={[0,4,4,0]}>
                          {keywordData.data.slice(0,15).map((_, i) => (
                            <Cell key={i} fill={`rgba(99,179,237,${1-i*0.06})`} />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                    <div style={{ marginTop: 20, display: "flex", flexWrap: "wrap", gap: 6 }}>
                      {keywordData.data.map((k) => {
                        const maxC = keywordData.data[0]?.count || 1;
                        return (
                          <div key={k.word} style={{
                            padding: "4px 10px", borderRadius: 20,
                            background: `rgba(99,179,237,${0.04 + (k.count/maxC)*0.14})`,
                            border: `1px solid rgba(99,179,237,${0.08 + (k.count/maxC)*0.28})`,
                            color: `rgba(99,179,237,${0.4 + (k.count/maxC)*0.6})`,
                            fontSize: Math.round(8 + (k.count/maxC)*5),
                            fontFamily: "'JetBrains Mono', monospace",
                            fontWeight: k.count === keywordData.data[0]?.count ? 700 : 400,
                          }}>
                            {k.word}
                            <span style={{ marginLeft: 4, opacity: 0.5, fontSize: 8 }}>{k.count}</span>
                          </div>
                        );
                      })}
                    </div>
                  </>
                )}
              </div>
            )}

            {chart === "scatter" && (
              <div>
                <div style={{ fontFamily: "'Syne', sans-serif", fontSize: 12, fontWeight: 700, marginBottom: 3 }}>
                  Age vs Engagement
                </div>
                <div style={{ color: "#4a5568", fontSize: 9, marginBottom: 14 }}>
                  post age (min) × engagement score · last {timeRange.label}
                </div>
                <ResponsiveContainer width="100%" height={360}>
                  <ScatterChart margin={{ left: 0, right: 20 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,69,0,0.06)" />
                    <XAxis dataKey="x" name="Age (min)" stroke="#2d3748"
                      tick={{ fill: "#4a5568", fontSize: 9 }}
                      label={{ value: "Age (min)", position: "insideBottom", offset: -5, fill: "#2d3748", fontSize: 9 }} />
                    <YAxis dataKey="y" name="Engagement" stroke="#2d3748" tick={{ fill: "#4a5568", fontSize: 9 }} />
                    <Tooltip content={<CT />} cursor={{ stroke: "rgba(255,69,0,0.3)" }} />
                    <Scatter data={scatterData} fill="#ff4500" opacity={0.55} />
                  </ScatterChart>
                </ResponsiveContainer>
              </div>
            )}

          </div>
        </div>

        {/* Right sidebar */}
        <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
          <div className="card">
            <div className="card-head">
              <span className="card-title">Top Authors</span>
              <span className="card-badge">last {timeRange.label}</span>
            </div>
            {(stats?.users ?? []).slice(0,8).map((u, i) => (
              <div key={u.author} style={{
                display: "flex", alignItems: "center", gap: 8,
                padding: "8px 16px", borderBottom: "1px solid rgba(255,255,255,0.04)", fontSize: 10,
              }}>
                <span style={{
                  fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 11,
                  color: i < 3 ? "#ff4500" : "#2d3748", minWidth: 20,
                }}>{i+1}</span>
                <span style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                  u/{u.author}
                </span>
                <span style={{ color: "#ff4500" }}>▲{fmt(u.total_score)}</span>
                <span style={{ color: "#2d3748" }}>{u.posts}p</span>
              </div>
            ))}
            {!stats?.users?.length && (
              <div style={{ padding: "14px 16px", color: "#2d3748", fontSize: 10 }}>No author data yet</div>
            )}
          </div>

          <div className="card">
            <div className="card-head">
              <span className="card-title">Subreddit Rankings</span>
              <span className="card-badge">by posts</span>
            </div>
            {subData.slice(0,8).map((s, i) => (
              <div key={s.name} style={{
                display: "flex", alignItems: "center", gap: 8,
                padding: "8px 16px", borderBottom: "1px solid rgba(255,255,255,0.04)", fontSize: 10,
              }}>
                <span style={{
                  fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 11,
                  color: i < 3 ? "#ff6b35" : "#2d3748", minWidth: 20,
                }}>{i+1}</span>
                <span style={{
                  flex: 1, overflow: "hidden", textOverflow: "ellipsis",
                  whiteSpace: "nowrap", color: "#ff6b35",
                }}>r/{s.name}</span>
                <span style={{ color: "#4a5568" }}>{s.posts}p</span>
              </div>
            ))}
          </div>

          {[
            { label: "Most Upvoted",   post: stats?.posts?.most_upvoted,   val: p => `▲${fmt(p.current_score)}`,     col: "#3ecf74" },
            { label: "Most Commented", post: stats?.posts?.most_commented,  val: p => `💬${fmt(p.current_comments)}`, col: "#63b3ed" },
          ].map(({ label, post, val, col }) => post ? (
            <div key={label} className="card">
              <div className="card-head">
                <span className="card-title">{label}</span>
                <span className="card-badge" style={{ color: col }}>{val(post)}</span>
              </div>
              <div style={{ padding: "12px 16px" }}>
                <div style={{ color: "#ff4500", fontSize: 8, marginBottom: 5 }}>
                  r/{post.subreddit__name ?? post.subreddit}
                </div>
                <div style={{
                  fontFamily: "'Syne', sans-serif", fontSize: 11, fontWeight: 700,
                  lineHeight: 1.4, color: "#dde1e8", marginBottom: 6,
                }}>
                  {post.title?.slice(0,90)}{post.title?.length > 90 ? "…" : ""}
                </div>
                <div style={{ fontSize: 9, color: "#4a5568" }}>by u/{post.author}</div>
              </div>
            </div>
          ) : null)}
        </div>
      </div>

    </div>
  );
}
