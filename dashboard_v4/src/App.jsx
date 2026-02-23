import { useState } from "react";
import { Routes, Route, Navigate } from "react-router-dom";
import Nav from "./components/Nav.jsx";
import PostsPage from "./pages/Posts.jsx";
import StatsPage from "./pages/Stats.jsx";

export default function App() {
  const [postCount, setPostCount] = useState(0);
  return (
    <div className="shell">
      <Nav postCount={postCount} />
      <Routes>
        <Route path="/"      element={<Navigate to="/posts" replace />} />
        <Route path="/posts" element={<PostsPage onCountChange={setPostCount} />} />
        <Route path="/stats" element={<StatsPage />} />
      </Routes>
    </div>
  );
}
