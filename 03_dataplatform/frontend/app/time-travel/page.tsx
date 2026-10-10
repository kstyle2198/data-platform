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

export default function TimeTravelPage() {
  const [token, setToken] = useState("");
  const [snapshots, setSnapshots] = useState<Snapshot[]>([]);
  const [currentSnapshotId, setCurrentSnapshotId] =
    useState<string | null>(null);
  const [selectedId, setSelectedId] = useState("");
  const [limit, setLimit] = useState("100");
  const [result, setResult] = useState<QueryResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");

  const apiFetch = useCallback(
    async (path: string, accessToken: string) => {
      const response = await fetch(path, {
        headers: {
          Authorization: `Bearer ${accessToken}`,
        },
        cache: "no-store",
      });

      const body = await response.json().catch(() => ({}));

      if (!response.ok) {
        throw new Error(
          body.detail || `HTTP ${response.status}`,
        );
      }

      return body;
    },
    [],
  );

  const loadSnapshots = useCallback(
    async (accessToken: string) => {
      setLoading(true);
      setError("");
      setMessage("");

      try {
        const data: SnapshotResponse = await apiFetch(
          "/api/iceberg/snapshots",
          accessToken,
        );

        setSnapshots(data.snapshots || []);
        setCurrentSnapshotId(
          data.current_snapshot_id || null,
        );

        // 가능한 경우 현재 스냅샷이 아닌 과거 스냅샷을
        // 기본 선택해 Time Travel을 바로 테스트합니다.
        const previous = data.snapshots.find(
          (snapshot) => !snapshot.is_current,
        );

        const initial =
          previous?.snapshot_id ||
          data.current_snapshot_id ||
          data.snapshots[0]?.snapshot_id ||
          "";

        setSelectedId(initial);

        if (data.snapshots.length === 0) {
          setMessage(
            "스냅샷이 없습니다. 먼저 Usage 이벤트를 발행하고 Spark 커밋을 확인하세요.",
          );
        }
      } catch (e) {
        setError(
          e instanceof Error
            ? e.message
            : "스냅샷 조회에 실패했습니다.",
        );
      } finally {
        setLoading(false);
      }
    },
    [apiFetch],
  );

  useEffect(() => {
    const accessToken =
      sessionStorage.getItem("access_token") ||
      sessionStorage.getItem("token");

    if (!accessToken) {
      setError(
        "로그인 토큰이 없습니다. 메인 화면에서 로그인한 뒤 다시 열어주세요.",
      );
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

    if (
      !Number.isInteger(parsedLimit) ||
      parsedLimit < 1 ||
      parsedLimit > 200
    ) {
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
      setError(
        e instanceof Error
          ? e.message
          : "Time Travel 조회에 실패했습니다.",
      );
    } finally {
      setLoading(false);
    }
  }

  const panel: React.CSSProperties = {
    border: "1px solid #d1d5db",
    borderRadius: 10,
    padding: 16,
    marginBottom: 16,
  };

  const button: React.CSSProperties = {
    padding: "9px 14px",
    border: "1px solid #9ca3af",
    borderRadius: 7,
    cursor: "pointer",
    background: "#f9fafb",
  };

  const cell: React.CSSProperties = {
    padding: "9px 12px",
    borderBottom: "1px solid #e5e7eb",
    textAlign: "left",
    whiteSpace: "nowrap",
  };

  return (
    <main
      style={{
        maxWidth: 1200,
        margin: "0 auto",
        padding: 24,
        color: "#111827",
      }}
    >
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          gap: 12,
          flexWrap: "wrap",
          alignItems: "center",
          marginBottom: 24,
        }}
      >
        <div>
          <h1 style={{ fontSize: 28, fontWeight: 700 }}>
            Iceberg Time Travel
          </h1>
          <p style={{ color: "#6b7280", marginTop: 6 }}>
            과거 스냅샷을 선택해 해당 시점의 데이터를 조회합니다.
          </p>
        </div>

        <a href="/" style={{ color: "#2563eb" }}>
          ← 메인 모니터
        </a>
      </div>

      <section style={panel}>
        <h2 style={{ fontSize: 18, fontWeight: 600 }}>
          Table Information
        </h2>
        <p style={{ marginTop: 10 }}>
          <strong>Table:</strong>{" "}
          local.usage_db.usage_events
        </p>
        <p style={{ marginTop: 6 }}>
          <strong>Current Snapshot:</strong>{" "}
          {currentSnapshotId || "없음"}
        </p>

        <div
          style={{
            display: "flex",
            gap: 10,
            flexWrap: "wrap",
            marginTop: 16,
          }}
        >
          <button
            style={button}
            disabled={loading || !token}
            onClick={() => void loadSnapshots(token)}
          >
            스냅샷 새로고침
          </button>
        </div>
      </section>

      <section style={panel}>
        <h2 style={{ fontSize: 18, fontWeight: 600 }}>
          Snapshot History
        </h2>

        <div
          style={{
            overflowX: "auto",
            marginTop: 12,
          }}
        >
          <table
            style={{
              width: "100%",
              borderCollapse: "collapse",
            }}
          >
            <thead>
              <tr>
                {[
                  "Snapshot ID",
                  "Committed At",
                  "Operation",
                  "Status",
                ].map((name) => (
                  <th key={name} style={cell}>
                    {name}
                  </th>
                ))}
              </tr>
            </thead>

            <tbody>
              {snapshots.map((snapshot) => (
                <tr key={snapshot.snapshot_id}>
                  <td style={cell}>
                    <button
                      style={{
                        ...button,
                        background:
                          selectedId === snapshot.snapshot_id
                            ? "#dbeafe"
                            : "#fff",
                      }}
                      onClick={() =>
                        setSelectedId(snapshot.snapshot_id)
                      }
                    >
                      {snapshot.snapshot_id}
                    </button>
                  </td>
                  <td style={cell}>
                    {snapshot.committed_at || "-"}
                  </td>
                  <td style={cell}>
                    {snapshot.operation || "-"}
                  </td>
                  <td style={cell}>
                    {snapshot.is_current
                      ? "CURRENT"
                      : "HISTORICAL"}
                  </td>
                </tr>
              ))}

              {snapshots.length === 0 && (
                <tr>
                  <td style={cell} colSpan={4}>
                    {loading
                      ? "스냅샷 조회 중..."
                      : "조회된 스냅샷이 없습니다."}
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>

      <section style={panel}>
        <h2 style={{ fontSize: 18, fontWeight: 600 }}>
          Time Travel Query
        </h2>

        <div
          style={{
            display: "flex",
            gap: 12,
            alignItems: "end",
            flexWrap: "wrap",
            marginTop: 14,
          }}
        >
          <label style={{ display: "grid", gap: 6 }}>
            Snapshot
            <select
              value={selectedId}
              onChange={(e) => setSelectedId(e.target.value)}
              style={{ padding: 9, minWidth: 250 }}
            >
              <option value="">스냅샷 선택</option>
              {snapshots.map((snapshot) => (
                <option
                  key={snapshot.snapshot_id}
                  value={snapshot.snapshot_id}
                >
                  {snapshot.snapshot_id}
                  {snapshot.is_current ? " (CURRENT)" : ""}
                </option>
              ))}
            </select>
          </label>

          <label style={{ display: "grid", gap: 6 }}>
            조회 행 수
            <select
              value={limit}
              onChange={(e) => setLimit(e.target.value)}
              style={{ padding: 9 }}
            >
              {[10, 50, 100, 200].map((n) => (
                <option key={n} value={n}>
                  {n}
                </option>
              ))}
            </select>
          </label>

          <button
            style={button}
            disabled={loading || !selectedId}
            onClick={() => void runTimeTravel()}
          >
            {loading ? "조회 중..." : "과거 데이터 조회"}
          </button>
        </div>

        {error && (
          <p
            role="alert"
            style={{ color: "#b91c1c", marginTop: 14 }}
          >
            {error}
          </p>
        )}

        {message && (
          <p style={{ marginTop: 14 }}>{message}</p>
        )}
      </section>

      {result && (
        <>
          <section
            style={{
              ...panel,
              display: "flex",
              gap: 24,
              flexWrap: "wrap",
            }}
          >
            <div>
              <p style={{ color: "#6b7280" }}>
                Historical Row Count
              </p>
              <strong style={{ fontSize: 24 }}>
                {result.historical_count}
              </strong>
            </div>

            <div>
              <p style={{ color: "#6b7280" }}>
                Current Row Count
              </p>
              <strong style={{ fontSize: 24 }}>
                {result.current_count}
              </strong>
            </div>

            <div>
              <p style={{ color: "#6b7280" }}>
                Returned Rows
              </p>
              <strong style={{ fontSize: 24 }}>
                {result.returned_rows}
              </strong>
            </div>
          </section>

          <section style={panel}>
            <h2 style={{ fontSize: 18, fontWeight: 600 }}>
              Query Result
            </h2>
            <p
              style={{
                color: "#6b7280",
                marginTop: 6,
                marginBottom: 12,
              }}
            >
              Snapshot ID: {result.snapshot_id}
            </p>

            <div style={{ overflowX: "auto" }}>
              <table
                style={{
                  width: "100%",
                  borderCollapse: "collapse",
                }}
              >
                <thead>
                  <tr>
                    {result.columns.map((column) => (
                      <th key={column} style={cell}>
                        {column}
                      </th>
                    ))}
                  </tr>
                </thead>

                <tbody>
                  {result.rows.map((row, index) => (
                    <tr key={index}>
                      {result.columns.map((column) => (
                        <td key={column} style={cell}>
                          {row[column] == null
                            ? "NULL"
                            : typeof row[column] === "object"
                              ? JSON.stringify(row[column])
                              : String(row[column])}
                        </td>
                      ))}
                    </tr>
                  ))}

                  {result.rows.length === 0 && (
                    <tr>
                      <td
                        style={cell}
                        colSpan={Math.max(
                          result.columns.length,
                          1,
                        )}
                      >
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
    </main>
  );
}