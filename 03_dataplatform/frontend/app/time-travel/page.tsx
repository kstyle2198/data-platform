"use client";

import { useCallback, useEffect, useState } from "react";

type Snapshot = {
  snapshot_id: string;
  parent_id?: string | null;
  committed_at?: string;
  operation?: string;
  summary?: Record<string, string> | null;
  is_current?: boolean;
};

type SnapshotResponse = {
  table: string;
  current_snapshot_id: string | null;
  count: number;
  snapshots: Snapshot[];
};

type QueryResult = {
  table: string;
  snapshot_id: string;
  historical_count: number;
  current_count: number;
  returned_rows: number;
  limit: number;
  columns: string[];
  rows: Record<string, unknown>[];
};

type ClearResult = {
  before_count: number;
  deleted_count: number;
  after_count: number;
  snapshot_id: string | null;
  history_preserved: boolean;
};

const TABLE_NAME = "local.usage_db.usage_events";

const styles: Record<string, React.CSSProperties> = {
  page: {
    minHeight: "100vh",
    background: "#f5f7fb",
    color: "#172033",
    padding: "32px 20px 56px",
    fontFamily:
      'Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
  },
  container: { maxWidth: 1240, margin: "0 auto" },
  header: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 20,
    flexWrap: "wrap",
    marginBottom: 28,
  },
  eyebrow: {
    margin: "0 0 8px",
    color: "#5965d8",
    fontSize: 12,
    fontWeight: 800,
    letterSpacing: "0.12em",
    textTransform: "uppercase",
  },
  title: {
    margin: 0,
    fontSize: "clamp(28px, 4vw, 38px)",
    lineHeight: 1.15,
    letterSpacing: "-0.04em",
    fontWeight: 800,
  },
  subtitle: { margin: "10px 0 0", color: "#697386", fontSize: 15, lineHeight: 1.6 },
  card: {
    background: "#fff",
    border: "1px solid #e5e9f2",
    borderRadius: 18,
    padding: 24,
    marginBottom: 20,
    boxShadow: "0 8px 28px rgba(22, 34, 66, 0.045)",
    minWidth: 0,
  },
  sectionHeader: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 12,
    flexWrap: "wrap",
    marginBottom: 16,
  },
  sectionTitle: { margin: 0, fontSize: 18, fontWeight: 750, letterSpacing: "-0.02em" },
  sectionDescription: { margin: "6px 0 0", color: "#7a8497", fontSize: 13, lineHeight: 1.5 },
  label: { display: "grid", gap: 7, color: "#4c566b", fontSize: 13, fontWeight: 650 },
  input: {
    width: "100%",
    boxSizing: "border-box",
    border: "1px solid #dce2ee",
    borderRadius: 10,
    padding: "11px 12px",
    background: "#fff",
    color: "#172033",
    outlineColor: "#747df0",
    fontSize: 14,
  },
  primaryButton: {
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    gap: 8,
    border: "1px solid #5965d8",
    borderRadius: 10,
    padding: "11px 16px",
    background: "#5965d8",
    color: "#fff",
    fontWeight: 700,
    fontSize: 13,
    cursor: "pointer",
    transition: "background 150ms ease",
  },
  secondaryButton: {
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    gap: 8,
    border: "1px solid #dce2ee",
    borderRadius: 10,
    padding: "10px 14px",
    background: "#fff",
    color: "#344054",
    fontWeight: 700,
    fontSize: 13,
    cursor: "pointer",
  },
  tableWrap: { width: "100%", overflow: "auto", border: "1px solid #edf0f6", borderRadius: 12 },
  table: { width: "100%", borderCollapse: "separate", borderSpacing: 0, fontSize: 13 },
  th: {
    position: "sticky",
    top: 0,
    zIndex: 1,
    background: "#f7f8fc",
    color: "#697386",
    fontSize: 11,
    fontWeight: 800,
    letterSpacing: "0.06em",
    textTransform: "uppercase",
    textAlign: "left",
    padding: "13px 15px",
    borderBottom: "1px solid #e8ecf4",
    whiteSpace: "nowrap",
  },
  td: {
    padding: "13px 15px",
    borderBottom: "1px solid #eff2f7",
    color: "#3f485b",
    verticalAlign: "middle",
    whiteSpace: "nowrap",
  },
  metricLabel: { color: "#7a8497", fontSize: 12, fontWeight: 700, margin: 0 },
  metricValue: { margin: "8px 0 0", color: "#172033", fontSize: 28, lineHeight: 1.1, fontWeight: 800 },
  muted: { color: "#7a8497" },
};

function formatDate(value?: string) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("ko-KR", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(date);
}

function shortId(value: string, length = 20) {
  return value.length > length ? `${value.slice(0, length)}…` : value;
}

function Badge({
  children,
  tone = "neutral",
}: {
  children: React.ReactNode;
  tone?: "neutral" | "success" | "info" | "danger";
}) {
  const palette = {
    neutral: { background: "#f0f2f7", color: "#596579" },
    success: { background: "#e8f8ef", color: "#18804a" },
    info: { background: "#eef0ff", color: "#505bd0" },
    danger: { background: "#fff0f0", color: "#bd3434" },
  }[tone];

  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        borderRadius: 999,
        padding: "5px 9px",
        fontSize: 11,
        lineHeight: 1,
        fontWeight: 800,
        whiteSpace: "nowrap",
        ...palette,
      }}
    >
      {children}
    </span>
  );
}

export default function TimeTravelPage() {
  const [token, setToken] = useState("");
  const [snapshots, setSnapshots] = useState<Snapshot[]>([]);
  const [currentSnapshotId, setCurrentSnapshotId] = useState<string | null>(null);
  const [selectedId, setSelectedId] = useState("");
  const [limit, setLimit] = useState("100");
  const [result, setResult] = useState<QueryResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [clearConfirmation, setClearConfirmation] = useState("");
  const [clearing, setClearing] = useState(false);
  const [clearResult, setClearResult] = useState<ClearResult | null>(null);

  const apiFetch = useCallback(async (path: string, accessToken: string) => {
    const response = await fetch(path, {
      headers: { Authorization: `Bearer ${accessToken}` },
      cache: "no-store",
    });
    const body = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(body.detail || `HTTP ${response.status}`);
    }
    return body;
  }, []);

  const loadSnapshots = useCallback(
    async (accessToken: string) => {
      if (!accessToken) return;
      setLoading(true);
      setError("");
      setMessage("");
      try {
        const data: SnapshotResponse = await apiFetch("/api/iceberg/snapshots", accessToken);
        const loadedSnapshots = data.snapshots || [];
        setSnapshots(loadedSnapshots);
        setCurrentSnapshotId(data.current_snapshot_id || null);

        // Preserve the user's current selection when it still exists.
        const previous = loadedSnapshots.find(
          (snapshot) =>
            !snapshot.is_current &&
            snapshot.snapshot_id !== data.current_snapshot_id,
        );
        const fallbackSelection =
          previous?.snapshot_id ||
          data.current_snapshot_id ||
          loadedSnapshots[0]?.snapshot_id ||
          "";
        setSelectedId((current) =>
          loadedSnapshots.some((snapshot) => snapshot.snapshot_id === current)
            ? current
            : fallbackSelection,
        );

        if (loadedSnapshots.length === 0) {
          setMessage("스냅샷이 없습니다. Usage 이벤트를 발행한 뒤 Spark 커밋을 확인하세요.");
        }
      } catch (e) {
        setError(e instanceof Error ? e.message : "스냅샷 조회에 실패했습니다.");
      } finally {
        setLoading(false);
      }
    },
    [apiFetch],
  );

  useEffect(() => {
    const accessToken =
      sessionStorage.getItem("access_token") || sessionStorage.getItem("token");
    if (!accessToken) {
      setError("로그인 토큰이 없습니다. 메인 화면에서 로그인한 뒤 다시 열어주세요.");
      return;
    }
    setToken(accessToken);
    void loadSnapshots(accessToken);
  }, [loadSnapshots]);

  async function runTimeTravel() {
    if (!token || !selectedId) {
      setError("조회할 스냅샷을 선택하세요.");
      return;
    }
    const parsedLimit = Number(limit);
    if (!Number.isInteger(parsedLimit) || parsedLimit < 1 || parsedLimit > 200) {
      setError("조회 행 수는 1~200 사이여야 합니다.");
      return;
    }

    setLoading(true);
    setError("");
    setMessage("");
    setResult(null);
    try {
      const query = new URLSearchParams({
        snapshot_id: selectedId,
        limit: String(parsedLimit),
      });
      const data: QueryResult = await apiFetch(
        `/api/iceberg/time-travel?${query.toString()}`,
        token,
      );
      setResult(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Time Travel 조회에 실패했습니다.");
    } finally {
      setLoading(false);
    }
  }

  async function clearAllRows() {
    if (clearConfirmation !== "DELETE ALL ROWS") {
      setError('확인 문구 "DELETE ALL ROWS"를 정확히 입력하세요.');
      return;
    }
    if (!token) {
      setError("로그인이 필요합니다.");
      return;
    }

    setClearing(true);
    setError("");
    setMessage("");
    setResult(null);
    setClearResult(null);
    try {
      const response = await fetch("/api/iceberg/clear", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ confirmation: clearConfirmation }),
        cache: "no-store",
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(data.detail || `HTTP ${response.status}`);

      setClearResult(data);
      setClearConfirmation("");
      setMessage("테이블 데이터 삭제가 완료되었습니다. 과거 스냅샷은 유지됩니다.");
      await loadSnapshots(token);
    } catch (e) {
      setError(e instanceof Error ? e.message : "데이터 삭제에 실패했습니다.");
    } finally {
      setClearing(false);
    }
  }

  const selectedSnapshot = snapshots.find((snapshot) => snapshot.snapshot_id === selectedId);
  const historicalSnapshots = snapshots.filter((snapshot) => !snapshot.is_current).length;

  return (
    <main style={styles.page}>
      <div style={styles.container}>
        <header style={styles.header}>
          <div>
            <p style={styles.eyebrow}>Data Platform / Apache Iceberg</p>
            <h1 style={styles.title}>Time Travel Explorer</h1>
            <p style={styles.subtitle}>
              스냅샷을 선택하고 특정 시점의 테이블 데이터를 조회합니다.
            </p>
          </div>
          <a
            href="/"
            style={{ ...styles.secondaryButton, textDecoration: "none", padding: "11px 15px" }}
          >
            <span aria-hidden="true">←</span> 메인 모니터
          </a>
        </header>

        {(error || message) && (
          <div
            role={error ? "alert" : "status"}
            style={{
              padding: "13px 16px",
              marginBottom: 20,
              borderRadius: 12,
              border: `1px solid ${error ? "#f3caca" : "#bce8cd"}`,
              background: error ? "#fff5f5" : "#f0fff5",
              color: error ? "#a52a2a" : "#176b3a",
              fontSize: 13,
              lineHeight: 1.5,
            }}
          >
            {error || message}
          </div>
        )}

        <section style={styles.card}>
          <div style={styles.sectionHeader}>
            <div>
              <h2 style={styles.sectionTitle}>Table overview</h2>
              <p style={styles.sectionDescription}>현재 테이블 및 스냅샷 상태</p>
            </div>
            <button
              type="button"
              style={{ ...styles.secondaryButton, opacity: loading || !token ? 0.6 : 1 }}
              disabled={loading || !token}
              onClick={() => void loadSnapshots(token)}
            >
              {loading ? "불러오는 중…" : "↻ 스냅샷 새로고침"}
            </button>
          </div>

          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(210px, 1fr))",
              gap: 14,
            }}
          >
            <div style={{ background: "#f8f9fd", border: "1px solid #edf0f6", borderRadius: 13, padding: 17 }}>
              <p style={styles.metricLabel}>TABLE</p>
              <p style={{ margin: "8px 0 0", fontSize: 14, fontWeight: 750, overflowWrap: "anywhere" }}>
                {TABLE_NAME}
              </p>
            </div>
            <div style={{ background: "#f8f9fd", border: "1px solid #edf0f6", borderRadius: 13, padding: 17 }}>
              <p style={styles.metricLabel}>TOTAL SNAPSHOTS</p>
              <p style={styles.metricValue}>{snapshots.length}</p>
            </div>
            <div style={{ background: "#f8f9fd", border: "1px solid #edf0f6", borderRadius: 13, padding: 17 }}>
              <p style={styles.metricLabel}>HISTORICAL SNAPSHOTS</p>
              <p style={styles.metricValue}>{historicalSnapshots}</p>
            </div>
            <div style={{ background: "#f8f9fd", border: "1px solid #edf0f6", borderRadius: 13, padding: 17, minWidth: 0 }}>
              <p style={styles.metricLabel}>CURRENT SNAPSHOT</p>
              <p
                title={currentSnapshotId || "없음"}
                style={{
                  margin: "8px 0 0",
                  color: "#505bd0",
                  fontSize: 14,
                  fontWeight: 750,
                  overflowWrap: "anywhere",
                }}
              >
                {currentSnapshotId ? shortId(currentSnapshotId, 28) : "없음"}
              </p>
            </div>
          </div>
        </section>

        <section style={styles.card}>
          <div style={styles.sectionHeader}>
            <div>
              <h2 style={styles.sectionTitle}>Snapshot history</h2>
              <p style={styles.sectionDescription}>
                항목을 선택하면 아래 Time Travel 조회에 반영됩니다.
              </p>
            </div>
            <Badge tone="info">{snapshots.length} snapshots</Badge>
          </div>

          {/* Keep history in a bounded scroll region so it cannot grow indefinitely. */}
          <div
            style={{
              ...styles.tableWrap,
              maxHeight: 340,
              overflowY: "auto",
              overscrollBehavior: "contain",
            }}
          >
            <table style={styles.table}>
              <thead>
                <tr>
                  <th style={styles.th}>Snapshot ID</th>
                  <th style={styles.th}>Committed at</th>
                  <th style={styles.th}>Operation</th>
                  <th style={styles.th}>Status</th>
                </tr>
              </thead>
              <tbody>
                {snapshots.map((snapshot) => {
                  const selected = selectedId === snapshot.snapshot_id;
                  const current = Boolean(snapshot.is_current) ||
                    snapshot.snapshot_id === currentSnapshotId;
                  return (
                    <tr
                      key={snapshot.snapshot_id}
                      onClick={() => setSelectedId(snapshot.snapshot_id)}
                      style={{
                        background: selected ? "#f0f2ff" : "#fff",
                        cursor: "pointer",
                      }}
                    >
                      <td style={{ ...styles.td, minWidth: 190 }}>
                        <button
                          type="button"
                          title={snapshot.snapshot_id}
                          onClick={(event) => {
                            event.stopPropagation();
                            setSelectedId(snapshot.snapshot_id);
                          }}
                          style={{
                            padding: 0,
                            border: 0,
                            background: "transparent",
                            color: selected ? "#4d58cb" : "#30394d",
                            fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace",
                            fontSize: 12,
                            fontWeight: selected ? 800 : 600,
                            cursor: "pointer",
                            textAlign: "left",
                          }}
                        >
                          {selected ? "● " : ""}{shortId(snapshot.snapshot_id, 24)}
                        </button>
                      </td>
                      <td style={styles.td}>{formatDate(snapshot.committed_at)}</td>
                      <td style={styles.td}>{snapshot.operation || "—"}</td>
                      <td style={styles.td}>
                        <Badge tone={current ? "success" : selected ? "info" : "neutral"}>
                          {current ? "CURRENT" : selected ? "SELECTED" : "HISTORICAL"}
                        </Badge>
                      </td>
                    </tr>
                  );
                })}
                {snapshots.length === 0 && (
                  <tr>
                    <td style={{ ...styles.td, padding: 28, textAlign: "center" }} colSpan={4}>
                      {loading ? "스냅샷을 불러오는 중입니다…" : "조회된 스냅샷이 없습니다."}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
          <p style={{ margin: "10px 2px 0", color: "#8992a3", fontSize: 12 }}>
            목록 영역 안에서 스크롤할 수 있습니다. 전체 페이지 길이는 스냅샷 수에 따라 늘어나지 않습니다.
          </p>
        </section>

        <section style={styles.card}>
          <div style={styles.sectionHeader}>
            <div>
              <h2 style={styles.sectionTitle}>Time Travel query</h2>
              <p style={styles.sectionDescription}>선택한 스냅샷 시점의 데이터를 조회합니다.</p>
            </div>
            {selectedSnapshot && (
              <Badge tone={selectedSnapshot.is_current ? "success" : "info"}>
                {selectedSnapshot.is_current ? "현재 스냅샷" : "과거 스냅샷 선택됨"}
              </Badge>
            )}
          </div>

          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
              gap: 14,
              alignItems: "end",
            }}
          >
            <label style={{ ...styles.label, gridColumn: "span 2" }}>
              Snapshot
              <select
                value={selectedId}
                onChange={(event) => setSelectedId(event.target.value)}
                style={styles.input}
              >
                <option value="">스냅샷 선택</option>
                {snapshots.map((snapshot) => (
                  <option key={snapshot.snapshot_id} value={snapshot.snapshot_id}>
                    {snapshot.snapshot_id}
                    {snapshot.is_current || snapshot.snapshot_id === currentSnapshotId ? " (CURRENT)" : ""}
                  </option>
                ))}
              </select>
            </label>
            <label style={styles.label}>
              조회 행 수
              <select value={limit} onChange={(event) => setLimit(event.target.value)} style={styles.input}>
                {[10, 50, 100, 200].map((n) => (
                  <option key={n} value={n}>{n} rows</option>
                ))}
              </select>
            </label>
            <button
              type="button"
              style={{
                ...styles.primaryButton,
                minHeight: 42,
                opacity: loading || !selectedId ? 0.55 : 1,
              }}
              disabled={loading || !selectedId}
              onClick={() => void runTimeTravel()}
            >
              {loading ? "조회 중…" : "과거 데이터 조회 →"}
            </button>
          </div>
        </section>

        {result && (
          <>
            <section
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(auto-fit, minmax(170px, 1fr))",
                gap: 14,
                marginBottom: 20,
              }}
            >
              {[
                { label: "HISTORICAL ROW COUNT", value: result.historical_count },
                { label: "CURRENT ROW COUNT", value: result.current_count },
                { label: "RETURNED ROWS", value: result.returned_rows },
              ].map((metric) => (
                <div key={metric.label} style={{ ...styles.card, marginBottom: 0, padding: 20 }}>
                  <p style={styles.metricLabel}>{metric.label}</p>
                  <p style={styles.metricValue}>{metric.value.toLocaleString()}</p>
                </div>
              ))}
            </section>

            <section style={styles.card}>
              <div style={styles.sectionHeader}>
                <div>
                  <h2 style={styles.sectionTitle}>Query result</h2>
                  <p style={styles.sectionDescription}>
                    Snapshot ID: <code>{result.snapshot_id}</code>
                  </p>
                </div>
                <Badge tone="neutral">{result.returned_rows} rows returned</Badge>
              </div>
              <div style={styles.tableWrap}>
                <table style={styles.table}>
                  <thead>
                    <tr>
                      {result.columns.map((column) => <th key={column} style={styles.th}>{column}</th>)}
                    </tr>
                  </thead>
                  <tbody>
                    {result.rows.map((row, index) => (
                      <tr key={index}>
                        {result.columns.map((column) => {
                          const value = row[column];
                          return (
                            <td key={column} style={styles.td}>
                              {value == null
                                ? <span style={{ color: "#9aa2b1", fontStyle: "italic" }}>NULL</span>
                                : typeof value === "object"
                                  ? JSON.stringify(value)
                                  : String(value)}
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                    {result.rows.length === 0 && (
                      <tr>
                        <td style={{ ...styles.td, padding: 28, textAlign: "center" }} colSpan={Math.max(result.columns.length, 1)}>
                          데이터가 없습니다.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </section>
          </>
        )}

        <section style={{ ...styles.card, border: "1px solid #f0cccc" }}>
          <div style={styles.sectionHeader}>
            <div>
              <h2 style={{ ...styles.sectionTitle, color: "#b83232" }}>학습용 데이터 초기화</h2>
              <p style={styles.sectionDescription}>
                {TABLE_NAME} 테이블의 모든 행을 삭제합니다. 실행 전 아래 내용을 확인하세요.
              </p>
            </div>
            <Badge tone="danger">주의: 데이터 삭제</Badge>
          </div>

          <ul style={{ margin: "0 0 18px", paddingLeft: 20, color: "#626d80", fontSize: 13, lineHeight: 1.9 }}>
            <li>현재 테이블 데이터 행은 0건이 됩니다.</li>
            <li>삭제 커밋은 새로운 Iceberg 스냅샷을 생성합니다.</li>
            <li>기존 스냅샷은 유지되어 과거 데이터 조회가 가능합니다.</li>
            <li>Kafka, LDAP 및 다른 테이블은 삭제하지 않습니다.</li>
          </ul>

          <label style={{ ...styles.label, maxWidth: 420 }}>
            계속하려면 <code>DELETE ALL ROWS</code>를 입력하세요.
            <input
              value={clearConfirmation}
              onChange={(event) => setClearConfirmation(event.target.value)}
              placeholder="DELETE ALL ROWS"
              autoComplete="off"
              style={styles.input}
            />
          </label>
          <button
            type="button"
            style={{
              ...styles.primaryButton,
              marginTop: 12,
              background: "#c43d3d",
              borderColor: "#c43d3d",
              opacity: clearing || clearConfirmation !== "DELETE ALL ROWS" ? 0.5 : 1,
              cursor: clearing || clearConfirmation !== "DELETE ALL ROWS" ? "not-allowed" : "pointer",
            }}
            disabled={clearing || clearConfirmation !== "DELETE ALL ROWS"}
            onClick={() => void clearAllRows()}
          >
            {clearing ? "삭제 처리 중…" : "전체 데이터 삭제"}
          </button>

          {clearResult && (
            <div
              style={{
                marginTop: 18,
                padding: 17,
                borderRadius: 12,
                border: "1px solid #d8efdf",
                background: "#f4fbf6",
                display: "grid",
                gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))",
                gap: 14,
              }}
            >
              <div><p style={styles.metricLabel}>삭제 전 행 수</p><p style={{ ...styles.metricValue, fontSize: 22 }}>{clearResult.before_count}</p></div>
              <div><p style={styles.metricLabel}>삭제된 행 수</p><p style={{ ...styles.metricValue, fontSize: 22 }}>{clearResult.deleted_count}</p></div>
              <div><p style={styles.metricLabel}>삭제 후 행 수</p><p style={{ ...styles.metricValue, fontSize: 22 }}>{clearResult.after_count}</p></div>
              <div>
                <p style={styles.metricLabel}>과거 스냅샷 보존</p>
                <p style={{ margin: "8px 0 0", fontSize: 14, fontWeight: 800, color: clearResult.history_preserved ? "#18804a" : "#bd3434" }}>
                  {clearResult.history_preserved ? "예" : "아니요"}
                </p>
              </div>
              <div style={{ gridColumn: "1 / -1", overflowWrap: "anywhere", fontSize: 12, color: "#596579" }}>
                삭제 커밋 Snapshot ID: <code>{clearResult.snapshot_id ?? "—"}</code>
              </div>
            </div>
          )}
        </section>
      </div>
    </main>
  );
}
