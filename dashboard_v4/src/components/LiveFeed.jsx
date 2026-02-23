import { motion } from "framer-motion";

export default function LiveFeed({ posts }) {
  return (
    <div className="bg-[#11161c] border border-gray-800 rounded-2xl shadow-lg p-4">
      <h2 className="text-lg mb-4 font-semibold">🔥 Live Feed</h2>

      <div className="space-y-2">
        {posts.map((p) => (
          <motion.div
            layout
            key={p.id}
            className="p-3 rounded bg-[#1a2128] hover:bg-[#202a33] transition"
            style={{
              borderLeft:
                p.momentum > 2
                  ? "3px solid #22c55e"
                  : "3px solid transparent",
            }}
          >
            <div className="text-sm font-medium">{p.title}</div>

            <div className="text-xs text-gray-400 mt-1">
              Score: {p.current_score} | Momentum: {p.momentum}
            </div>
          </motion.div>
        ))}
      </div>
    </div>
  );
}
