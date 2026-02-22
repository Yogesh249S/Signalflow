import { Routes, Route, Navigate } from "react-router-dom";
import Nav from "./components/Nav.jsx";
import PostsPage from "./pages/Posts.jsx";
import StatsPage from "./pages/Stats.jsx";

export default function App() {
  return (
    <div className="shell">
      <Nav />
      <Routes>
        <Route path="/"       element={<Navigate to="/posts" replace />} />
        <Route path="/posts"  element={<PostsPage />} />
        <Route path="/stats"  element={<StatsPage />} />
      </Routes>
    </div>
  );
}
