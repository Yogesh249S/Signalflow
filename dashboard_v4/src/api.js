import axios from "axios";

const api = axios.create({ baseURL: "http://localhost:8080/api" });

export const setAuthToken = (token) => {
  if (token) api.defaults.headers.common["Authorization"] = `Bearer ${token}`;
  else delete api.defaults.headers.common["Authorization"];
};
const saved = sessionStorage.getItem("sf_token");
if (saved) setAuthToken(saved);

export const TIME_RANGES = [
  { label: "30m", hours: 0.5 },
  { label: "1h",  hours: 1   },
  { label: "2h",  hours: 2   },
  { label: "3h",  hours: 3   },
  { label: "6h",  hours: 6   },
  { label: "12h", hours: 12  },
  { label: "24h", hours: 24  },
];

export const rangeToIso = (hours) => new Date(Date.now() - hours * 3600 * 1000).toISOString();
const today   = () => new Date().toISOString().split("T")[0];
const daysAgo = (n) => { const d = new Date(); d.setDate(d.getDate() - n); return d.toISOString().split("T")[0]; };

export const fetchPosts  = (cursor, hours = 24) =>
  api.get(`/posts/?page_size=100${cursor ? `&cursor=${cursor}` : ""}`);
export const fetchStats  = (days = 1) =>
  api.get(`/stats/?start=${daysAgo(days - 1)}&end=${today()}`);
export const fetchActivityTimeline = (hours = 24) =>
  api.get(`/stats/timeline/?hours=${hours}`).catch(() => ({ data: [] }));
export const fetchKeywords = (hours = 6) =>
  api.get(`/stats/keywords/?hours=${hours}`).catch(() => ({ data: [] }));
export const login = (username, password) =>
  api.post("/token/", { username, password });

export default api;

export const fmt = n => {
  if (n == null) return "—";
  if (n >= 1_000_000) return `${(n/1_000_000).toFixed(1)}M`;
  if (n >= 1_000)     return `${(n/1_000).toFixed(1)}k`;
  return String(Math.round(n));
};

export const ago = (utc) => {
  if (!utc) return "—";
  let diff;
  if (typeof utc === "string" && utc.includes("T")) {
    diff = Math.floor((Date.now() - new Date(utc).getTime()) / 1000);
  } else {
    const ts = typeof utc === "string" ? Number(utc) : utc;
    if (!ts || isNaN(ts)) return "—";
    const secs = ts > 1e12 ? Math.floor(ts / 1000) : ts;
    diff = Math.floor(Date.now() / 1000 - secs);
  }
  if (diff < 60) return "just now";
  const m = Math.floor(diff / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  return `${Math.floor(h / 24)}d ago`;
};

export const getSentiment = p => p?.sentiment_score ?? p?.sentiment ?? p?.compound ?? 0;
export const sentColor    = s => s > 0.2 ? "#3ecf74" : s < -0.2 ? "#f56565" : "#4a5568";
export const priorityBadge = p => ({
  aggressive: { label: "HOT",    bg: "#ff4500", color: "#fff"    },
  normal:     { label: "ACTIVE", bg: "#7c3aed", color: "#e9d5ff" },
  slow:       { label: "COOL",   bg: "#1e3a5f", color: "#63b3ed" },
  inactive:   { label: "OLD",    bg: "#1a202c", color: "#4a5568" },
}[p] ?? { label: "—", bg: "#1a202c", color: "#4a5568" });
