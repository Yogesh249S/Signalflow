export default function StatsSidebar({ stats }) {
  if (!stats || !stats.overview) return null;

  return (
    <div className="bg-[#11161c] border border-gray-800 rounded-2xl shadow-lg p-4">
      <h2 className="text-lg mb-4 font-semibold">📊 Daily Stats</h2>

      <div className="space-y-2 text-sm">
        <div className="flex justify-between">
          <span>Total Posts</span>
          <span>{stats.overview.total_posts}</span>
        </div>

        <div className="flex justify-between">
          <span>Avg Score</span>
          <span>{stats.overview.avg_score}</span>
        </div>
      </div>
    </div>
  );
}
