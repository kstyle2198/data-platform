
"use client";

import { FormEvent, useCallback, useEffect, useState } from "react";

type User = {
  username?: string;
  name?: string;
  email?: string;
  roles?: string[];
};

type PipelineEvent = {
  event_id: string;
  created_at?: string;
  event?: Record<string, unknown>;
  fastapi?: { status?: string };
  kafka?: { status?: string; topic?: string; partition?: number; offset?: number };
  spark?: { status?: string };
  iceberg?: { status?: string };
  minio?: { status?: string };
};

type KafkaPartition = {
  partition: number;
  leader: number | null;
  replicas: number[];
  isr: number[];
  beginning_offset: number | null;
  latest_offset: number | null;
  log_size: number | null;
};

type KafkaTopic = {
  topic: string;
  partition_count: number;
  partitions: KafkaPartition[];
};

type KafkaConsumerGroup = {
  group_id: string;
  protocol_type: string;
  state: string;
};

type LdapUser = {
  username: string;
  full_name: string;
  email: string;
  department: string;
};

type ApiError = { detail?: string; message?: string };

const stages = ["fastapi", "kafka", "spark", "iceberg", "minio"] as const;

async function request<T>(
  path: string,
  token: string,
  init: RequestInit = {}
): Promise<T> {
  const headers = new Headers(init.headers);
  headers.set("Accept", "application/json");

  if (init.body) {
    headers.set("Content-Type", "application/json");
  }

  if (token) {
    headers.set("Authorization", `Bearer ${token}`);
  }

  const response = await fetch(`/api${path}`, {
    ...init,
    headers,
    cache: "no-store"
  });

  if (!response.ok) {
    let message = `HTTP ${response.status}`;

    try {
      const error = (await response.json()) as ApiError;
      message = error.detail || error.message || message;
    } catch {
      // 응답이 JSON이 아닌 경우 기본 HTTP 오류를 사용
    }

    if (response.status === 401) {
      throw new Error(`인증이 만료되었거나 유효하지 않습니다. 다시 로그인하세요. (${message})`);
    }

    throw new Error(message);
  }

  return response.json() as Promise<T>;
}

function pretty(value: unknown) {
  return JSON.stringify(value, null, 2);
}

function statusLabel(status?: string) {
  switch (status) {
    case "completed": return "완료";
    case "processing": return "처리 중";
    case "waiting": return "대기";
    case "failed": return "실패";
    default: return status || "미확인";
  }
}

function statusClass(status?: string) {
  if (status === "completed") return "status completed";
  if (status === "failed") return "status failed";
  if (status === "processing") return "status processing";
  return "status waiting";
}

export default function Home() {
  const [token, setToken] = useState("");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [user, setUser] = useState<User | null>(null);

  const [service, setService] = useState("api");
  const [usageType, setUsageType] = useState("request");
  const [quantity, setQuantity] = useState("1");

  const [events, setEvents] = useState<PipelineEvent[]>([]);
  const [selectedEvent, setSelectedEvent] = useState("");
  const [pipelineDetail, setPipelineDetail] = useState<PipelineEvent | null>(null);
  const [kafkaTopics, setKafkaTopics] = useState<KafkaTopic[]>([]);
  const [consumerGroups, setConsumerGroups] = useState<KafkaConsumerGroup[]>([]);
  const [selectedTopic, setSelectedTopic] = useState("");
  const [kafkaFilter, setKafkaFilter] = useState("");
  const [kafkaError, setKafkaError] = useState("");
  const [kafkaUpdatedAt, setKafkaUpdatedAt] = useState("");

  const [ldapUsers, setLdapUsers] = useState<LdapUser[]>([]);
  const [newUser, setNewUser] = useState({
    username: "",
    full_name: "",
    email: "",
    department: "",
    password: ""
  });

  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [lastRefresh, setLastRefresh] = useState("");

  const isAdmin = user?.roles?.includes("ldap-admin") ?? false;

  const logout = useCallback(() => {
    sessionStorage.removeItem("access_token");
    setToken("");
    setUser(null);
    setEvents([]);
    setSelectedEvent("");
    setPipelineDetail(null);
    setLdapUsers([]);
    setMessage("로그아웃했습니다.");
    setError("");
  }, []);

  const refresh = useCallback(async (activeToken: string) => {
    if (!activeToken) return;

    try {
      const [me, pipeline] = await Promise.all([
        request<User>("/me", activeToken),
        request<{ count: number; events: PipelineEvent[] }>(
          "/pipeline?limit=100",
          activeToken
        )
      ]);

      setUser(me);
      setEvents(pipeline.events || []);
      setLastRefresh(new Date().toLocaleTimeString());

      if (selectedEvent) {
        try {
          const detail = await request<PipelineEvent>(
            `/pipeline/${encodeURIComponent(selectedEvent)}`,
            activeToken
          );
          setPipelineDetail(detail);
        } catch {
          // 선택된 이벤트의 상세 정보가 더 이상 조회되지 않을 수 있음
        }
      }
    } catch (e) {
      const text = e instanceof Error ? e.message : "상태 조회 실패";
      setError(text);

      if (text.includes("다시 로그인")) {
        sessionStorage.removeItem("access_token");
        setToken("");
        setUser(null);
        return;
      }
    }

    // Kafka 조회 실패가 Pipeline 상태 조회까지 중단시키지 않도록 분리합니다.
    try {
      const [topicData, groupData] = await Promise.all([
        request<{ count?: number; topics: KafkaTopic[] }>(
          "/monitoring/kafka/topics",
          activeToken
        ),
        request<{ count?: number; groups: KafkaConsumerGroup[] }>(
          "/monitoring/kafka/consumer-groups",
          activeToken
        )
      ]);

      const nextTopics = topicData.topics || [];
      setKafkaTopics(nextTopics);
      setConsumerGroups(groupData.groups || []);
      setSelectedTopic((current) =>
        current && nextTopics.some((topic) => topic.topic === current)
          ? current
          : ""
      );
      setKafkaUpdatedAt(new Date().toLocaleTimeString());
      setKafkaError("");
    } catch (e) {
      setKafkaError(
        e instanceof Error ? e.message : "Kafka 모니터링 조회 실패"
      );
    }
  }, [selectedEvent]);

  useEffect(() => {
    const saved = sessionStorage.getItem("access_token");
    if (saved) setToken(saved);
  }, []);

  useEffect(() => {
    if (!token) return;

    void refresh(token);
    const timer = window.setInterval(() => void refresh(token), 5000);

    return () => window.clearInterval(timer);
  }, [token, refresh]);

  async function handleLogin(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setBusy(true);
    setError("");
    setMessage("");

    try {
      const result = await request<{
        access_token: string;
        token_type: string;
        expires_in?: number;
      }>("/login", "", {
        method: "POST",
        body: JSON.stringify({ username, password })
      });

      sessionStorage.setItem("access_token", result.access_token);
      setToken(result.access_token);
      setPassword("");
      setMessage("로그인했습니다.");
    } catch (e) {
      setError(e instanceof Error ? e.message : "로그인 실패");
    } finally {
      setBusy(false);
    }
  }

  async function handleUsage(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setBusy(true);
    setError("");
    setMessage("");

    try {
      const result = await request<{
        message: string;
        event_id: string;
        kafka?: { topic?: string; partition?: number; offset?: number };
      }>("/usage", token, {
        method: "POST",
        body: JSON.stringify({
          service,
          usage_type: usageType,
          quantity: Number(quantity)
        })
      });

      setSelectedEvent(result.event_id);
      setMessage(
        `이벤트 발행 완료: ${result.event_id} · Kafka offset ${result.kafka?.offset ?? "-"}`
      );
      await refresh(token);
    } catch (e) {
      setError(e instanceof Error ? e.message : "이벤트 발행 실패");
    } finally {
      setBusy(false);
    }
  }

  async function inspectEvent(eventId: string) {
    setSelectedEvent(eventId);
    setError("");

    try {
      const detail = await request<PipelineEvent>(
        `/pipeline/${encodeURIComponent(eventId)}`,
        token
      );
      setPipelineDetail(detail);
    } catch (e) {
      setError(e instanceof Error ? e.message : "파이프라인 조회 실패");
    }
  }

  async function loadLdapUsers() {
    setBusy(true);
    setError("");

    try {
      const result = await request<{ count: number; users: LdapUser[] }>(
        "/admin/ldap/users",
        token
      );
      setLdapUsers(result.users || []);
      setMessage(`LDAP 사용자 ${result.count}명을 조회했습니다.`);
    } catch (e) {
      setError(e instanceof Error ? e.message : "LDAP 사용자 조회 실패");
    } finally {
      setBusy(false);
    }
  }

  async function createLdapUser(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setBusy(true);
    setError("");
    setMessage("");

    try {
      await request("/admin/ldap/users", token, {
        method: "POST",
        body: JSON.stringify(newUser)
      });

      setNewUser({
        username: "",
        full_name: "",
        email: "",
        department: "",
        password: ""
      });

      setMessage("LDAP 사용자를 등록했습니다.");
      await loadLdapUsers();
    } catch (e) {
      setError(e instanceof Error ? e.message : "LDAP 등록 실패");
    } finally {
      setBusy(false);
    }
  }

  if (!token || !user) {
    return (
      <main className="login-page">
        <form className="login-card" onSubmit={handleLogin}>
          <div className="brand-mark">DP</div>
          <p className="eyebrow">DATA PLATFORM</p>
          <h1>Pipeline Monitor</h1>
          <p className="muted">
            Keycloak 인증으로 데이터 파이프라인에 접속합니다.
          </p>

          <label>
            사용자 이름
            <input
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              autoComplete="username"
              required
            />
          </label>

          <label>
            비밀번호
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              autoComplete="current-password"
              required
            />
          </label>

          {error && <p className="alert error">{error}</p>}
          {message && <p className="alert success">{message}</p>}

          <button className="primary full" disabled={busy}>
            {busy ? "로그인 중..." : "로그인"}
          </button>
          <p className="muted small">
            인증은 기존 FastAPI /login API와 Keycloak을 사용합니다.
          </p>
        </form>
      </main>
    );
  }

  return (
    <main className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark">DP</div>
          <div>
            <strong>Data Platform</strong>
            <span>Operations Console</span>
          </div>
        </div>

        <p className="nav-label">MONITORING</p>
        <a href="/" className="nav-item active">Pipeline & Kafka Monitoring</a>
        <a href="/pipeline-lab" className="nav-item">Pipeline Learning Lab</a>
        <a href="/time-travel" className="nav-item">Iceberg Time Travel</a>

        <p className="nav-label">WORKSPACE</p>
        <a href="#overview" className="nav-item">Overview</a>
        <a href="#usage" className="nav-item">Usage Events</a>
        <a href="#pipeline" className="nav-item">Pipeline Status</a>
        {isAdmin && <a href="/identity/ldap" className="nav-item">OpenLDAP Users</a>}

        <div className="sidebar-bottom">
          <div className="user-avatar">
            {(user.name || user.username || "U").slice(0, 1).toUpperCase()}
          </div>
          <div className="user-meta">
            <strong>{user.name || user.username}</strong>
            <span>{isAdmin ? "Administrator" : "Authenticated user"}</span>
          </div>
          <button className="icon-button" onClick={logout} title="로그아웃">
            ↪
          </button>
        </div>
      </aside>

      <section className="main-area">
        <header className="topbar">
          <div>
            <p className="eyebrow">OBSERVABILITY / DATA ENGINEERING</p>
            <h1>Pipeline & Kafka Monitoring</h1>
          </div>
          <div className="topbar-right">
            <span className="live-indicator">● LIVE</span>
            <button className="secondary" onClick={() => void refresh(token)}>
              새로고침
            </button>
          </div>
        </header>

        {error && (
          <div className="alert error dismissible">
            {error}
            <button onClick={() => setError("")}>닫기</button>
          </div>
        )}
        {message && (
          <div className="alert success dismissible">
            {message}
            <button onClick={() => setMessage("")}>닫기</button>
          </div>
        )}

        <section id="overview" className="metrics-grid">
          <article className="metric-card">
            <span className="metric-label">Recent Events</span>
            <strong className="metric-value">{events.length}</strong>
            <span className="metric-note">최근 조회 목록</span>
          </article>
          <article className="metric-card">
            <span className="metric-label">Completed</span>
            <strong className="metric-value">
              {events.filter((event) =>
                stages.every((stage) => event[stage]?.status === "completed")
              ).length}
            </strong>
            <span className="metric-note">전체 단계 완료</span>
          </article>
          <article className="metric-card">
            <span className="metric-label">Failed</span>
            <strong className="metric-value">
              {events.filter((event) =>
                stages.some((stage) => event[stage]?.status === "failed")
              ).length}
            </strong>
            <span className="metric-note">실패 단계가 있는 이벤트</span>
          </article>
          <article className="metric-card">
            <span className="metric-label">Last Refresh</span>
            <strong className="metric-value compact">{lastRefresh || "-"}</strong>
            <span className="metric-note">5초 간격 자동 갱신</span>
          </article>
        </section>

        <section className="panel architecture-panel">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">END-TO-END FLOW</p>
              <h2>Pipeline Architecture</h2>
            </div>
            <span className="muted small">Event ID 기반 추적</span>
          </div>
          <div className="flow">
            {[
              ["Keycloak", "Authentication"],
              ["FastAPI", "Usage API"],
              ["Kafka", "Event Stream"],
              ["Spark", "Streaming"],
              ["Iceberg", "Table"],
              ["MinIO", "Object Storage"]
            ].map(([name, description], index) => (
              <div className="flow-step" key={name}>
                <div className="flow-icon">{["◈", "⌘", "≋", "ϟ", "▤", "▧"][index]}</div>
                <strong>{name}</strong>
                <span>{description}</span>
                {index < 5 && <span className="flow-arrow">→</span>}
              </div>
            ))}
          </div>
        </section>

        <section id="usage" className="panel">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">EVENT PRODUCER</p>
              <h2>Usage Event 발행</h2>
            </div>
          </div>

          <form className="usage-form" onSubmit={handleUsage}>
            <label>
              Service
              <select value={service} onChange={(e) => setService(e.target.value)}>
                <option value="api">API</option>
                <option value="search">Search</option>
                <option value="storage">Storage</option>
                <option value="llm">LLM</option>
              </select>
            </label>

            <label>
              Usage Type
              <select value={usageType} onChange={(e) => setUsageType(e.target.value)}>
                <option value="request">Request</option>
                <option value="token">Token</option>
                <option value="gb">GB</option>
                <option value="minute">Minute</option>
              </select>
            </label>

            <label>
              Quantity
              <input
                type="number"
                min="1"
                value={quantity}
                onChange={(e) => setQuantity(e.target.value)}
                required
              />
            </label>

            <button className="primary" disabled={busy}>이벤트 발행</button>
          </form>
        </section>

        <section id="pipeline" className="panel">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">REAL-TIME OBSERVABILITY</p>
              <h2>Recent Pipeline Events</h2>
            </div>
            <span className="muted small">{events.length} events</span>
          </div>

          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Event ID</th>
                  <th>Created At</th>
                  {stages.map((stage) => <th key={stage}>{stage}</th>)}
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {events.map((event) => (
                  <tr
                    key={event.event_id}
                    className={selectedEvent === event.event_id ? "selected-row" : ""}
                  >
                    <td className="mono">{event.event_id.slice(0, 12)}…</td>
                    <td>{event.created_at ? new Date(event.created_at).toLocaleTimeString() : "-"}</td>
                    {stages.map((stage) => (
                      <td key={stage}>
                        <span className={statusClass(event[stage]?.status)}>
                          {statusLabel(event[stage]?.status)}
                        </span>
                      </td>
                    ))}
                    <td>
                      <button className="text-button" onClick={() => void inspectEvent(event.event_id)}>
                        상세
                      </button>
                    </td>
                  </tr>
                ))}
                {events.length === 0 && (
                  <tr><td colSpan={8} className="empty">표시할 이벤트가 없습니다.</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </section>


        {pipelineDetail && (
          <section className="panel pipeline-detail-panel">
            <div className="panel-heading">
              <div>
                <p className="eyebrow">EVENT TRACE</p>
                <h2>Pipeline Detail</h2>
                <p className="muted small">
                  Event ID: {pipelineDetail.event_id || selectedEvent}
                </p>
              </div>
              <button
                className="secondary"
                onClick={() => setPipelineDetail(null)}
              >
                닫기
              </button>
            </div>

            <div className="detail-grid">
              {stages.map((stage, index) => {
                const status = pipelineDetail[stage]?.status;
                const normalized = (status || "unknown").toLowerCase();

                const state =
                  ["completed", "complete", "success", "succeeded"].includes(normalized)
                    ? "completed"
                    : ["processing", "running", "in_progress"].includes(normalized)
                      ? "processing"
                      : ["failed", "error"].includes(normalized)
                        ? "failed"
                        : ["waiting", "pending", "queued"].includes(normalized)
                          ? "waiting"
                          : "unknown";

                return (
                  <div
                    className={`stage-card stage-${state}`}
                    key={stage}
                  >
                    <div className="stage-card-header">
                      <span className="stage-number">{index + 1}</span>
                      <span className={`status status-${state}`}>
                        {statusLabel(status)}
                      </span>
                    </div>

                    <span className="stage-name">{stage}</span>

                    <div className="stage-indicator">
                      <span className="stage-indicator-dot" />
                      <span>
                        {state === "completed" && "처리 완료"}
                        {state === "processing" && "처리 진행 중"}
                        {state === "waiting" && "처리 대기"}
                        {state === "failed" && "오류 발생"}
                        {state === "unknown" && "상태 확인 필요"}
                      </span>
                    </div>

                    {stage === "kafka" && (
                      <div className="stage-metadata">
                        <div>
                          <span>Topic</span>
                          <strong>{pipelineDetail.kafka?.topic || "-"}</strong>
                        </div>
                        <div>
                          <span>Partition</span>
                          <strong>{pipelineDetail.kafka?.partition ?? "-"}</strong>
                        </div>
                        <div>
                          <span>Offset</span>
                          <strong>{pipelineDetail.kafka?.offset ?? "-"}</strong>
                        </div>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>

    <details className="raw-json-expander">
      <summary>
        <span className="raw-json-title">
          <span className="raw-json-icon">{ }</span>
          <span>
            <strong>Raw Pipeline JSON</strong>
            <small>전체 이벤트 및 단계별 원본 응답</small>
          </span>
        </span>
        <span className="expander-label">펼치기</span>
      </summary>

      <pre className="json-view">
        {pretty(pipelineDetail)}
      </pre>
    </details>
  </section>
)}





        <section id="kafka" className="panel">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">KAFKA / BROKER METADATA</p>
              <h2>Kafka Topics & Partitions</h2>
              <p className="muted small">최근 갱신: {kafkaUpdatedAt || "-"} · 5초 간격 자동 갱신</p>
            </div>
            <label>
              토픽 필터
              <select value={selectedTopic} onChange={(e) => setSelectedTopic(e.target.value)} aria-label="Kafka 토픽 선택">
                <option value="">전체 토픽</option>
                {kafkaTopics.map((topic) => <option key={topic.topic} value={topic.topic}>{topic.topic}</option>)}
              </select>
            </label>
          </div>

          {kafkaError && <div className="alert error">{`Kafka 모니터링: ${kafkaError}`}</div>}

          <div className="metrics-grid">
            <article className="metric-card"><span className="metric-label">Kafka Topics</span><strong className="metric-value">{kafkaTopics.length}</strong><span className="metric-note">내부 토픽 제외</span></article>
            <article className="metric-card"><span className="metric-label">Partitions</span><strong className="metric-value">{kafkaTopics.reduce((sum, topic) => sum + topic.partition_count, 0)}</strong><span className="metric-note">조회된 파티션 합계</span></article>
            <article className="metric-card"><span className="metric-label">Log Size</span><strong className="metric-value">{kafkaTopics.filter((topic) => !selectedTopic || topic.topic === selectedTopic).reduce((sum, topic) => sum + topic.partitions.reduce((n, part) => n + (part.log_size ?? 0), 0), 0).toLocaleString()}</strong><span className="metric-note">Beginning 대비 Latest 오프셋 범위</span></article>
            <article className="metric-card"><span className="metric-label">Consumer Groups</span><strong className="metric-value">{consumerGroups.length}</strong><span className="metric-note">브로커에서 조회한 그룹</span></article>
          </div>

          <div className="table-wrap">
            <table>
              <thead><tr><th>Topic</th><th>Partition</th><th>Leader</th><th>Replicas</th><th>ISR</th><th>Beginning</th><th>Latest</th><th>Log Size</th></tr></thead>
              <tbody>
                {kafkaTopics.filter((topic) => !selectedTopic || topic.topic === selectedTopic).flatMap((topic) => topic.partitions.map((part) => (
                  <tr key={`${topic.topic}-${part.partition}`}><td className="mono">{topic.topic}</td><td>{part.partition}</td><td>{part.leader ?? "-"}</td><td>{part.replicas.join(", ") || "-"}</td><td>{part.isr.join(", ") || "-"}</td><td>{part.beginning_offset ?? "-"}</td><td>{part.latest_offset ?? "-"}</td><td>{part.log_size?.toLocaleString() ?? "-"}</td></tr>
                )))}
                {kafkaTopics.filter((topic) => !selectedTopic || topic.topic === selectedTopic).length === 0 && <tr><td colSpan={8} className="empty">표시할 Kafka 토픽이 없습니다.</td></tr>}
              </tbody>
            </table>
          </div>

          <h3>Consumer Groups</h3>
          <p className="muted small">그룹 메타데이터만으로 Consumer Lag을 확정할 수 없습니다. 커밋 오프셋 기반 Lag은 별도 지표입니다.</p>
          <div className="table-wrap">
            <table>
              <thead><tr><th>Group ID</th><th>Protocol Type</th><th>State</th></tr></thead>
              <tbody>
                {consumerGroups.map((group) => <tr key={group.group_id}><td className="mono">{group.group_id}</td><td>{group.protocol_type || "-"}</td><td>{group.state || "unknown"}</td></tr>)}
                {consumerGroups.length === 0 && <tr><td colSpan={3} className="empty">조회된 Consumer Group이 없습니다.</td></tr>}
              </tbody>
            </table>
          </div>

          <h3>Kafka 관련 파이프라인 이벤트</h3>
          <div className="panel-heading">
            <p className="muted small">최근 파이프라인 이벤트에서 Kafka 토픽과 오프셋을 함께 확인합니다.</p>
            <input value={kafkaFilter} onChange={(e) => setKafkaFilter(e.target.value)} placeholder="Event ID 또는 Topic 검색" aria-label="Kafka 이벤트 검색" />
          </div>
          <div className="table-wrap">
            <table>
              <thead><tr><th>Event ID</th><th>Created At</th><th>Topic</th><th>Partition</th><th>Offset</th><th>FastAPI</th><th>Spark</th><th>Iceberg</th><th>MinIO</th></tr></thead>
              <tbody>
                {events.filter((event) => {
                  const query = kafkaFilter.trim().toLowerCase();
                  return (!selectedTopic || event.kafka?.topic === selectedTopic) && (!query || event.event_id.toLowerCase().includes(query) || (event.kafka?.topic || "").toLowerCase().includes(query));
                }).map((event) => (
                  <tr key={event.event_id}>
                    <td className="mono">{event.event_id.slice(0, 12)}…</td>
                    <td>{event.created_at ? new Date(event.created_at).toLocaleTimeString() : "-"}</td>
                    <td>{event.kafka?.topic || "-"}</td><td>{event.kafka?.partition ?? "-"}</td><td>{event.kafka?.offset ?? "-"}</td>
                    <td><span className={statusClass(event.fastapi?.status)}>{statusLabel(event.fastapi?.status)}</span></td>
                    <td><span className={statusClass(event.spark?.status)}>{statusLabel(event.spark?.status)}</span></td>
                    <td><span className={statusClass(event.iceberg?.status)}>{statusLabel(event.iceberg?.status)}</span></td>
                    <td><span className={statusClass(event.minio?.status)}>{statusLabel(event.minio?.status)}</span></td>
                  </tr>
                ))}
                {events.filter((event) => {
                  const query = kafkaFilter.trim().toLowerCase();
                  return (!selectedTopic || event.kafka?.topic === selectedTopic) && (!query || event.event_id.toLowerCase().includes(query) || (event.kafka?.topic || "").toLowerCase().includes(query));
                }).length === 0 && <tr><td colSpan={9} className="empty">조건에 맞는 이벤트가 없습니다.</td></tr>}
              </tbody>
            </table>
          </div>
        </section>

        <footer className="footer">
          Data Platform Monitor · Next.js Frontend · FastAPI Backend
        </footer>
      </section>
    </main>
  );
}
