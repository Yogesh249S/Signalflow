import { ScatterChart, Scatter, XAxis, YAxis, Tooltip } from "recharts";

export default function TrendChart({ posts }) {
  return (
    <div className="bg-[#11161c] border border-gray-800 rounded-2xl shadow-lg p-4">
      <h2 className="text-lg mb-4 font-semibold">📈 Engagement Trends</h2>

      <ScatterChart width={600} height={350}>
        <XAxis dataKey="age_minutes" stroke="#aaa" />
        <YAxis dataKey="engagement_score" stroke="#aaa" />
        <Tooltip />
        <Scatter data={posts} fill="#22c55e" />
      </ScatterChart>
    </div>
  );
}
