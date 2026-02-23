import { NavLink, Link } from "react-router-dom";
import { useState, useEffect } from "react";

export default function Nav({ postCount = 0 }) {
  const [time, setTime] = useState(new Date().toLocaleTimeString());
  useEffect(() => {
    const t = setInterval(() => setTime(new Date().toLocaleTimeString()), 1000);
    return () => clearInterval(t);
  }, []);

  return (
    <nav className="nav">
      <Link to="/posts" className="nav-logo">
        <div className="nav-logo-dot" />
        SIGNAL·FLOW
      </Link>
      <div className="nav-links">
        <NavLink to="/posts" className={({ isActive }) => `nav-link${isActive ? " active" : ""}`}>Posts</NavLink>
        <NavLink to="/stats" className={({ isActive }) => `nav-link${isActive ? " active" : ""}`}>Analytics</NavLink>
      </div>
      <div className="nav-right">
        {postCount > 0 && <span style={{ color: "var(--muted)" }}>{postCount} posts</span>}
        <span>{time}</span>
        <div className="live-pill"><div className="live-dot" /> LIVE</div>
      </div>
    </nav>
  );
}
