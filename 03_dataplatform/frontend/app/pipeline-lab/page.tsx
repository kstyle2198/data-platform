"use client";

import { useCallback, useEffect, useRef, useState } from "react";

type PipelineStage = { status?: string; topic?: string; partition?: number; offset?: number; error?: string };
type PipelineEvent = {
  event_id: string;
  created_at?: string;
  event?: { service?: string; usage_type?: string; quantity?: number; timestamp?: string };
  fastapi?: PipelineStage;
  kafka?: PipelineStage;
  spark?: PipelineStage;
  iceberg?: PipelineStage;
  minio?: PipelineStage;
};
type TopicPartition = { partition: number; leader: number | null; replicas: number[]; isr: number[]; beginning_offset: number | null; latest_offset: number | null; log_size: number | null };
type Topic = { topic: string; partition_count: number; partitions: TopicPartition[] };
type Snapshot = { snapshot_id: string; committed_at?: string; operation?: string; is_current?: boolean };
type IcebergResult = { table?: string; snapshot_id?: string; current_count?: number; historical_count?: number; returned_rows?: number; columns?: string[]; rows?: Record<string, unknown>[] };

async function api<T>(path: string, token: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers);
  headers.set("Accept", "application/json");
  headers.set("Authorization", `Bearer ${token}`);
  if (init.body) headers.set("Content-Type", "application/json");
  const response = await fetch(`/api${path}`, { ...init, headers, cache: "no-store" });
  if (!response.ok) {
    let detail = `HTTP ${response.status}`;
    try { detail = (await response.json()).detail || detail; } catch {}
    throw new Error(detail);
  }
  return response.json() as Promise<T>;
}

const services = ["api-gateway", "billing", "search", "recommendation", "storage"];
const usageTypes = ["request", "read", "write", "upload", "download"];
const stageNames = ["fastapi", "kafka", "spark", "iceberg", "minio"] as const;

function statusLabel(status?: string) {
  return ({ completed: "완료", processing: "처리 중", waiting: "대기", failed: "실패" } as Record<string, string>)[status || ""] || status || "미확인";
}

export default function PipelineLabPage() {
  const [token, setToken] = useState("");
  const [events, setEvents] = useState<PipelineEvent[]>([]);
  const [topics, setTopics] = useState<Topic[]>([]);
  const [snapshots, setSnapshots] = useState<Snapshot[]>([]);
  const [selectedSnapshot, setSelectedSnapshot] = useState("");
  const [iceberg, setIceberg] = useState<IcebergResult | null>(null);
  const [running, setRunning] = useState(false);
  const [intervalSeconds, setIntervalSeconds] = useState(3);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [busy, setBusy] = useState(false);
  const [isAdmin, setIsAdmin] = useState(false);
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const runningRef = useRef(false);

  const refresh = useCallback(async (activeToken: string) => {
    if (!activeToken) return;
    const results = await Promise.allSettled([
      api<{ events: PipelineEvent[] }>("/pipeline?limit=100", activeToken),
      api<{ topics: Topic[] }>("/monitoring/kafka/topics", activeToken),
      api<{ snapshots: Snapshot[] }>("/iceberg/snapshots", activeToken),
    ]);
    if (results[0].status === "fulfilled") setEvents(results[0].value.events || []);
    if (results[1].status === "fulfilled") setTopics(results[1].value.topics || []);
    if (results[2].status === "fulfilled") {
      const next = results[2].value.snapshots || [];
      setSnapshots(next);
      setSelectedSnapshot((current) => current && next.some((s) => s.snapshot_id === current)
        ? current
        : next.find((s) => s.is_current)?.snapshot_id || next[0]?.snapshot_id || "");
    }
    const failed = results.find((r) => r.status === "rejected");
    if (failed?.status === "rejected") setError(failed.reason instanceof Error ? failed.reason.message : "일부 모니터링 데이터를 조회하지 못했습니다.");
  }, []);

  const loadTimeTravel = useCallback(async (activeToken: string, snapshotId: string) => {
    if (!activeToken || !snapshotId) { setIceberg(null); return; }
    try {
      const result = await api<IcebergResult>(`/iceberg/time-travel?snapshot_id=${encodeURIComponent(snapshotId)}&limit=50`, activeToken);
      setIceberg(result);
    } catch (e) { setError(e instanceof Error ? e.message : "Iceberg 데이터 조회 실패"); }
  }, []);

  useEffect(() => {
    const saved = sessionStorage.getItem("access_token") || "";
    setToken(saved);
    if (!saved) return;
    void api<{ roles?: string[] }>("/me", saved).then((me) => setIsAdmin((me.roles || []).some((r) => ["ldap-admin", "data-admin", "platform-admin", "realm-admin"].includes(r)))).catch(() => setError("로그인이 필요합니다. 메인 화면에서 다시 로그인하세요."));
    void refresh(saved);
    const timer = window.setInterval(() => void refresh(saved), 5000);
    return () => window.clearInterval(timer);
  }, [refresh]);

  useEffect(() => { void loadTimeTravel(token, selectedSnapshot); }, [token, selectedSnapshot, loadTimeTravel]);
  useEffect(() => () => { if (timerRef.current) clearInterval(timerRef.current); runningRef.current = false; }, []);

  const publishRandomEvent = useCallback(async () => {
    if (!runningRef.current || !token) return;
    try {
      const service = services[Math.floor(Math.random() * services.length)];
      const usageType = usageTypes[Math.floor(Math.random() * usageTypes.length)];
      const quantity = Math.floor(Math.random() * 20) + 1;
      await api("/usage", token, { method: "POST", body: JSON.stringify({ service, usage_type: usageType, quantity }) });
      setNotice(`랜덤 수집 이벤트 발행: ${service} / ${usageType} / 수량 ${quantity}`);
      setError("");
      await refresh(token);
    } catch (e) {
      setError(e instanceof Error ? e.message : "랜덤 이벤트 발행 실패");
      stopSimulation();
    }
  }, [token, refresh]);

  function stopSimulation() {
    runningRef.current = false;
    setRunning(false);
    if (timerRef.current) clearInterval(timerRef.current);
    timerRef.current = null;
  }

  function startSimulation() {
    if (!token || runningRef.current) return;
    runningRef.current = true;
    setRunning(true);
    setError("");
    setNotice(`랜덤 이벤트 생성 시작 · ${intervalSeconds}초 간격`);
    void publishRandomEvent();
    timerRef.current = setInterval(() => void publishRandomEvent(), intervalSeconds * 1000);
  }

  async function resetLab() {
    if (!isAdmin) { setError("전체 초기화는 관리자 역할이 필요합니다."); return; }
    const confirmed = window.confirm("초기화할까요? Iceberg 현재 데이터와 API 파이프라인 상태가 삭제됩니다. Kafka 로그와 Iceberg Time Travel 스냅샷 이력은 보존됩니다.");
    if (!confirmed) return;
    stopSimulation();
    setBusy(true);
    setError("");
    try {
      const result = await api<{ message: string; iceberg?: { deleted_count?: number }; cleared_pipeline_events?: number; notes?: string[] }>("/learning/reset", token, { method: "POST", body: JSON.stringify({}) });
      setNotice(`${result.message} 파이프라인 이벤트 ${result.cleared_pipeline_events ?? 0}건, Iceberg 행 ${result.iceberg?.deleted_count ?? 0}건 초기화.`);
      setIceberg(null);
      await refresh(token);
    } catch (e) { setError(e instanceof Error ? e.message : "초기화 실패"); }
    finally { setBusy(false); }
  }

  if (!token) return <main className="login-page"><section className="login-card"><div className="brand-mark">DP</div><h1>Pipeline Learning Lab</h1><p className="muted">먼저 메인 화면에서 로그인한 후 접근하세요.</p><a className="primary full" href="/">메인 화면으로 이동</a></section></main>;

  const partitions = topics.reduce((n, t) => n + t.partition_count, 0);
  const currentRows = iceberg?.rows || [];
  const columns = iceberg?.columns || (currentRows[0] ? Object.keys(currentRows[0]) : []);

  return (
    <main className="app-shell">
      <aside className="sidebar">
        <div className="brand"><div className="brand-mark">DP</div><div><strong>Data Platform</strong><span>Learning Workspace</span></div></div>
        <p className="nav-label">LEARNING</p>
        <a href="/pipeline-lab" className="nav-item active">Pipeline Learning Lab</a>
        <a href="/" className="nav-item">Pipeline & Kafka Monitoring</a>
        <a href="/time-travel" className="nav-item">Iceberg Time Travel</a>
        {isAdmin && <a href="/identity/ldap" className="nav-item">OpenLDAP Users</a>}
        <div className="sidebar-bottom"><div className="user-avatar">L</div><div className="user-meta"><strong>Learning Lab</strong><span>{isAdmin ? "Administrator" : "Authenticated user"}</span></div><a className="icon-button" href="/" title="메인 화면">↗</a></div>
      </aside>

      <section className="main-area">
        <header className="topbar"><div><p className="eyebrow">HANDS-ON DATA ENGINEERING</p><h1>Pipeline Learning Lab</h1><p className="muted">랜덤 이벤트 → Kafka → Spark → Iceberg / MinIO</p></div><div className="topbar-right"><span className="live-indicator">● LIVE</span><button className="secondary" onClick={() => void refresh(token)}>새로고침</button></div></header>
        {error && <div className="alert error dismissible">{error}<button onClick={() => setError("")}>닫기</button></div>}
        {notice && <div className="alert success dismissible">{notice}<button onClick={() => setNotice("")}>닫기</button></div>}

        <section className="metrics-grid">
          <article className="metric-card"><span className="metric-label">Pipeline Events</span><strong className="metric-value">{events.length}</strong><span className="metric-note">메모리 상태에 보관된 최근 이벤트</span></article>
          <article className="metric-card"><span className="metric-label">Kafka Topics</span><strong className="metric-value">{topics.length}</strong><span className="metric-note">내부 토픽 제외</span></article>
          <article className="metric-card"><span className="metric-label">Partitions</span><strong className="metric-value">{partitions}</strong><span className="metric-note">조회된 전체 파티션</span></article>
          <article className="metric-card"><span className="metric-label">Iceberg Current Rows</span><strong className="metric-value">{iceberg?.current_count ?? "-"}</strong><span className="metric-note">선택 스냅샷 조회 시 갱신</span></article>
        </section>

        <section className="panel">
          <div className="panel-heading"><div><p className="eyebrow">EVENT GENERATOR</p><h2>1. 랜덤 수집 이벤트 생성</h2><p className="muted">기존 POST /usage API를 호출해 실제 Kafka 파이프라인에 이벤트를 발행합니다.</p></div><span className={running ? "status processing" : "status waiting"}>{running ? "실행 중" : "중지됨"}</span></div>
          <div className="usage-form"><label>발행 간격<select value={intervalSeconds} disabled={running} onChange={(e) => setIntervalSeconds(Number(e.target.value))}><option value={1}>1초</option><option value={3}>3초</option><option value={5}>5초</option><option value={10}>10초</option></select></label><div className="form-actions"><button className="primary" onClick={startSimulation} disabled={running || !token}>시작</button><button className="secondary" onClick={stopSimulation} disabled={!running}>중지</button><button className="danger" onClick={() => void resetLab()} disabled={busy || !isAdmin}>{busy ? "초기화 중..." : "전체 초기화"}</button></div></div>
          <p className="muted small">초기화는 관리자만 실행할 수 있습니다. Kafka 토픽 로그는 Spark 스트리밍 안정성을 위해 보존되며, Iceberg DELETE로 기존 스냅샷 이력도 남습니다.</p>
        </section>

        <section className="panel"><div className="panel-heading"><div><p className="eyebrow">END-TO-END</p><h2>2. 파이프라인 상태</h2></div></div>
          <div className="table-wrap pipeline-status-scroll"><table><thead><tr><th>Event ID</th><th>수집 데이터</th>{stageNames.map((s) => <th key={s}>{s}</th>)}<th>Kafka 위치</th></tr></thead><tbody>{events.map((event) => <tr key={event.event_id}><td className="mono">{event.event_id.slice(0, 8)}…</td><td>{event.event?.service || "-"} / {event.event?.usage_type || "-"} × {event.event?.quantity ?? "-"}</td>{stageNames.map((s) => <td key={s}><span className={`status ${event[s]?.status || "waiting"}`}>{statusLabel(event[s]?.status)}</span></td>)}<td>{event.kafka?.topic || "-"} / P{event.kafka?.partition ?? "-"} / O{event.kafka?.offset ?? "-"}</td></tr>)}{events.length === 0 && <tr><td colSpan={8} className="empty">아직 파이프라인 이벤트가 없습니다. 시작 버튼을 눌러 보세요.</td></tr>}</tbody></table></div>
        </section>

        <section className="panel"><div className="panel-heading"><div><p className="eyebrow">BROKER METADATA</p><h2>3. Kafka 토픽 및 파티션</h2><p className="muted">오프셋 차이(Log Size)는 현재 브로커에 남아 있는 레코드 수의 근사치입니다.</p></div></div>
          <div className="table-wrapper"><table><thead><tr><th>Topic</th><th>Partition</th><th>Leader</th><th>Replicas</th><th>ISR</th><th>Beginning</th><th>Latest</th><th>Log Size</th></tr></thead><tbody>{topics.flatMap((topic) => topic.partitions.map((part) => <tr key={`${topic.topic}-${part.partition}`}><td>{topic.topic}</td><td>{part.partition}</td><td>{part.leader ?? "-"}</td><td>{part.replicas.join(", ") || "-"}</td><td>{part.isr.join(", ") || "-"}</td><td>{part.beginning_offset ?? "-"}</td><td>{part.latest_offset ?? "-"}</td><td>{part.log_size ?? "-"}</td></tr>))}{topics.length === 0 && <tr><td colSpan={8} className="empty">Kafka 메타데이터가 없습니다.</td></tr>}</tbody></table></div>
        </section>

        <section className="panel"><div className="panel-heading"><div><p className="eyebrow">ICEBERG SNAPSHOTS</p><h2>4. Iceberg 데이터와 Time Travel</h2><p className="muted">스냅샷을 선택하면 해당 시점의 레코드를 조회합니다.</p></div></div>
          <div className="filter-row"><label>Snapshot<select value={selectedSnapshot} onChange={(e) => setSelectedSnapshot(e.target.value)}><option value="">스냅샷 선택</option>{snapshots.map((s) => <option key={s.snapshot_id} value={s.snapshot_id}>{s.snapshot_id}{s.is_current ? " (현재)" : ""} · {s.operation || "operation"}</option>)}</select></label><span className="muted small">스냅샷 {snapshots.length}개</span></div>
          {iceberg && <div className="metrics-grid compact-grid"><article className="metric-card"><span className="metric-label">Selected Snapshot</span><strong className="metric-value compact">{iceberg.snapshot_id || selectedSnapshot}</strong></article><article className="metric-card"><span className="metric-label">Rows at Snapshot</span><strong className="metric-value">{iceberg.historical_count ?? "-"}</strong></article><article className="metric-card"><span className="metric-label">Current Rows</span><strong className="metric-value">{iceberg.current_count ?? "-"}</strong></article></div>}
          <div className="table-wrapper"><table><thead><tr>{columns.map((col) => <th key={col}>{col}</th>)}</tr></thead><tbody>{currentRows.map((row, i) => <tr key={String(row.event_id ?? i)}>{columns.map((col) => <td key={col}>{row[col] == null ? "-" : typeof row[col] === "object" ? JSON.stringify(row[col]) : String(row[col])}</td>)}</tr>)}{currentRows.length === 0 && <tr><td colSpan={Math.max(columns.length, 1)} className="empty">선택한 스냅샷에 표시할 행이 없거나 스냅샷을 선택하지 않았습니다.</td></tr>}</tbody></table></div>
        </section>
      </section>
    </main>
  );
}
