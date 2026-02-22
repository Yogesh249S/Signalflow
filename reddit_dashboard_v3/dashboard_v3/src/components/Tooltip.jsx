export default function CustomTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null;
  return (
    <div style={{
      background: "#0d1117",
      border: "1px solid rgba(255,69,0,0.3)",
      borderRadius: 8,
      padding: "10px 14px",
      fontFamily: "'JetBrains Mono', monospace",
      fontSize: 11,
      boxShadow: "0 8px 32px rgba(0,0,0,0.4)"
    }}>
      {label && <div style={{ color: "#4a5568", marginBottom: 6, fontSize: 10 }}>{label}</div>}
      {payload.map((p, i) => (
        <div key={i} style={{ color: p.color || "#dde1e8", display: "flex", gap: 8, alignItems: "center" }}>
          <span style={{ color: "#4a5568" }}>{p.name}:</span>
          <strong>{typeof p.value === "number" ? p.value.toLocaleString() : p.value}</strong>
        </div>
      ))}
    </div>
  );
}
