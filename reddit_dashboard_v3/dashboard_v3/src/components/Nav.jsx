import { NavLink, Link } from "react-router-dom";
import { useState, useEffect } from "react";

export default function Nav() {
  const [time, setTime] = useState(new Date().toLocaleTimeString());
  useEffect(() => {
    const t = setInterval(() => setTime(new Date().toLocaleTimeString()), 1000);
    return () => clearInterval(t);
  }, []);

  return (
    <nav className="nav">
      <Link to="/posts" className="nav-logo">
        <div className="nav-logo-dot" />
        REDDIT·PULSE
      </Link>

      <div className="nav-links">
        <NavLink
          to="/posts"
          className={({ isActive }) => `nav-link${isActive ? " active" : ""}`}
        >
          Posts
        </NavLink>
        <NavLink
          to="/stats"
          className={({ isActive }) => `nav-link${isActive ? " active" : ""}`}
        >
          Stats
        </NavLink>
      </div>

      <div className="nav-right">
        <span>{time}</span>
        <div className="live-pill">
          <div className="live-dot" /> LIVE
        </div>
      </div>
    </nav>
  );
}
