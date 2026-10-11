"use client";

import { useCallback, useEffect, useState } from "react";

type Partition = { partition: number; leader: number; replicas: number[]; isr: number[]; beginning_offset: number | null; latest_offset: number | null; log_size: number };
type Topic = { topic: string; partition_count: number; partitions: Partition[] };
type PipelineEvent = { event_id: string; created_at?: string; kafka?: { topic?: string; partition?: number; offset?: number; status?: string }; spark?: { status?: string }; iceberg?: { status?: string }; minio?: { status?: string }; fastapi?: { status?: string } };
type Group = { group_id: string; protocol_type: string; state: string };

async function api<T>(path: string, token: string): Promise<T> {
  const response = await fetch(`/api${path}`, { headers: { Accept: "application/json", Authorization: `Bearer ${token}` }, cache: "no-store" });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.detail || `HTTP ${response.status}`);
  return data as T;
}

function fmt(value: unknown) { return value === null || value === undefined ? "—" : String(value); }

export default function KafkaMonitoringPage() {
  const [token, setToken] = useState("");
  const [topics, setTopics] = useState<Topic[]>([]);
  const [groups, setGroups] = useState<Group[]>([]);
  const [events, setEvents] = useState<PipelineEvent[]>([]);
  const [selectedTopic, setSelectedTopic] = useState("");
  const [filter, setFilter] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [updatedAt, setUpdatedAt] = useState("");

  const refresh = useCallback(async (accessToken: string) => {
    setLoading(true); setError("");
    try {
      const [topicData, groupData, eventData] = await Promise.all([
        api<{ topics: Topic[] }>("/monitoring/kafka/topics", accessToken),
        api<{ groups: Group[] }>("/monitoring/kafka/consumer-groups", accessToken),
        api<{ events: PipelineEvent[] }>("/pipeline?limit=100", accessToken),
      ]);
      setTopics(topicData.topics || []); setGroups(groupData.groups || []); setEvents(eventData.events || []);
      setSelectedTopic((current) => current && topicData.topics.some((topic) => topic.topic === current) ? current : (topicData.topics[0]?.topic || ""));
      setUpdatedAt(new Date().toLocaleString());
    } catch (e) { setError(e instanceof Error ? e.message : "Kafka 모니터링 조회에 실패했습니다."); }
    finally { setLoading(false); }
  }, []);

  useEffect(() => {
    const accessToken = sessionStorage.getItem("access_token") || sessionStorage.getItem("token") || "";
    if (!accessToken) { setError("로그인이 필요합니다. 메인 화면에서 로그인한 뒤 다시 열어주세요."); return; }
    setToken(accessToken); void refresh(accessToken);
    const timer = window.setInterval(() => void refresh(accessToken), 10000);
    return () => window.clearInterval(timer);
  }, [refresh]);

  const chosen = topics.find((topic) => topic.topic === selectedTopic);
  const filteredEvents = events.filter((event) => (!selectedTopic || event.kafka?.topic === selectedTopic) && (!filter || event.event_id?.toLowerCase().includes(filter.toLowerCase()) || event.kafka?.topic?.toLowerCase().includes(filter.toLowerCase())));
  const totalPartitions = topics.reduce((sum, topic) => sum + topic.partition_count, 0);
  const totalLogSize = topics.reduce((sum, topic) => sum + topic.partitions.reduce((n, part) => n + (part.log_size || 0), 0), 0);

  return <main className="app-shell">
    <aside className="sidebar"><div className="brand"><div className="brand-mark">DP</div><div><strong>Data Platform</strong><span>Operations Console</span></div></div>
      <p className="nav-label">MONITORING</p><a href="/" className="nav-item">Pipeline Flow</a><a href="/monitoring/kafka" className="nav-item active">Kafka Processing</a><a href="/time-travel" className="nav-item">Iceberg Time Travel</a>
      <div className="sidebar-bottom"><div className="user-avatar">K</div><div className="user-meta"><strong>Kafka Monitor</strong><span>Authenticated session</span></div><a className="icon-button" href="/" title="메인으로">↗</a></div>
    </aside>
    <section className="main-area"><header className="topbar"><div><p className="eyebrow">OBSERVABILITY / KAFKA</p><h1>Kafka Processing Monitor</h1><p className="muted">토픽 · 파티션 · 오프셋 · 처리 이력</p></div><div className="topbar-actions"><span className="muted small">최근 갱신: {updatedAt || "—"}</span><button className="secondary" disabled={loading || !token} onClick={() => void refresh(token)}>{loading ? "조회 중…" : "새로고침"}</button></div></header>
      {error && <div className="alert error">{error}</div>}
      <div className="stats-grid"><article className="stat-card"><span>토픽 수</span><strong>{topics.length}</strong><small>브로커 메타데이터 기준</small></article><article className="stat-card"><span>파티션 수</span><strong>{totalPartitions}</strong><small>조회된 전체 토픽</small></article><article className="stat-card"><span>로그 레코드 범위</span><strong>{totalLogSize.toLocaleString()}</strong><small>latest offset − beginning offset</small></article><article className="stat-card"><span>Consumer Groups</span><strong>{groups.length}</strong><small>그룹 메타데이터</small></article></div>
      <section className="panel"><div className="panel-heading"><div><h2>토픽 및 파티션</h2><p className="muted">Kafka 브로커에서 직접 조회한 메타데이터입니다.</p></div><select value={selectedTopic} onChange={(e) => setSelectedTopic(e.target.value)} aria-label="토픽 선택"><option value="">전체 토픽</option>{topics.map((topic) => <option key={topic.topic} value={topic.topic}>{topic.topic}</option>)}</select></div>
        <div className="table-wrap"><table><thead><tr><th>토픽</th><th>Partition</th><th>Leader</th><th>Replicas</th><th>ISR</th><th>Beginning</th><th>Latest</th><th>Log size</th></tr></thead><tbody>{topics.filter((topic) => !selectedTopic || topic.topic === selectedTopic).flatMap((topic) => topic.partitions.map((part) => <tr key={`${topic.topic}-${part.partition}`}><td><strong>{topic.topic}</strong></td><td>{part.partition}</td><td>{fmt(part.leader)}</td><td>{part.replicas.join(", ") || "—"}</td><td>{part.isr.join(", ") || "—"}</td><td>{fmt(part.beginning_offset)}</td><td>{fmt(part.latest_offset)}</td><td>{part.log_size.toLocaleString()}</td></tr>))}{topics.length === 0 && <tr><td colSpan={8} className="empty-state">조회할 토픽이 없습니다.</td></tr>}</tbody></table></div>
      </section>
      <section className="panel"><div className="panel-heading"><div><h2>Consumer Groups</h2><p className="muted">그룹 목록만으로는 Lag을 확정할 수 없습니다. 커밋 오프셋 기반 조회는 별도 지표입니다.</p></div></div><div className="table-wrap"><table><thead><tr><th>Group ID</th><th>Protocol</th><th>상태</th></tr></thead><tbody>{groups.map((group) => <tr key={group.group_id}><td><strong>{group.group_id}</strong></td><td>{group.protocol_type || "—"}</td><td>{group.state}</td></tr>)}{groups.length === 0 && <tr><td colSpan={3} className="empty-state">Consumer Group 정보가 없습니다.</td></tr>}</tbody></table></div></section>
      <section className="panel"><div className="panel-heading"><div><h2>이벤트 처리 내역</h2><p className="muted">현재 FastAPI가 보유한 최근 pipeline 상태 이력입니다.</p></div><input value={filter} onChange={(e) => setFilter(e.target.value)} placeholder="event ID 또는 topic 검색" /></div><div className="table-wrap"><table><thead><tr><th>Event ID</th><th>Topic</th><th>Partition</th><th>Offset</th><th>FastAPI</th><th>Spark</th><th>Iceberg</th><th>MinIO</th></tr></thead><tbody>{filteredEvents.map((event) => <tr key={event.event_id}><td><code>{event.event_id}</code></td><td>{event.kafka?.topic || "—"}</td><td>{fmt(event.kafka?.partition)}</td><td>{fmt(event.kafka?.offset)}</td><td>{event.fastapi?.status || "—"}</td><td>{event.spark?.status || "—"}</td><td>{event.iceberg?.status || "—"}</td><td>{event.minio?.status || "—"}</td></tr>)}{filteredEvents.length === 0 && <tr><td colSpan={8} className="empty-state">조건에 맞는 이벤트가 없습니다.</td></tr>}</tbody></table></div></section>
      <p className="muted small">자동 갱신 10초 · API 오류가 발생하면 마지막 성공 데이터 대신 오류를 표시합니다. 이벤트 처리 이력은 현재 백엔드 메모리 보관 정책의 영향을 받습니다.</p>
    </section>
  </main>;
}
