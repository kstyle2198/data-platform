
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
  const [parquet, setParquet] = useState<Record<string, unknown> | null>(null);

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
    setParquet(null);
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
          "/pipeline?limit=20",
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
          // 선택된 이벤트가 최근 상태 목록에서 사라졌을 수 있음
        }
      }
    } catch (e) {
      const text = e instanceof Error ? e.message : "상태 조회 실패";
      setError(text);

      if (text.includes("다시 로그인")) {
        sessionStorage.removeItem("access_token");
        setToken("");
        setUser(null);
      }
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

  async function loadParquet(eventId?: string) {
    setBusy(true);
    setError("");

    try {
      const path = eventId
        ? `/iceberg/parquet/${encodeURIComponent(eventId)}?limit=100`
        : "/iceberg/parquet?limit=50";

      const result = await request<Record<string, unknown>>(path, token);
      setParquet(result);
      setMessage("Parquet 데이터를 조회했습니다.");
    } catch (e) {
      setError(e instanceof Error ? e.message : "Parquet 조회 실패");
    } finally {
      setBusy(false);
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

        <p className="nav-label">WORKSPACE</p>
        <a href="#overview" className="nav-item active">Overview</a>
        <a href="#usage" className="nav-item">Usage Events</a>
        <a href="#pipeline" className="nav-item">Pipeline Status</a>
        <a href="#storage" className="nav-item">Parquet Data</a>
        
        {isAdmin && <a href="#ldap" className="nav-item">LDAP Users</a>}

        <a href="/time-travel">Iceberg Time Travel</a>

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
            <h1>Usage Data Pipeline</h1>
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
                      <button className="text-button" onClick={() => void loadParquet(event.event_id)}>
                        데이터
                      </button>
                    </td>
                  </tr>
                ))}
                {events.length === 0 && (
                  <tr><td colSpan={7} className="empty">표시할 이벤트가 없습니다.</td></tr>
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


        <section id="storage" className="panel">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">ICEBERG / MINIO</p>
              <h2>Parquet Data Explorer</h2>
            </div>
            <div className="button-row">
              <button className="secondary" disabled={busy} onClick={() => void loadParquet()}>
                최근 데이터
              </button>
              <button
                className="secondary"
                disabled={busy || !selectedEvent}
                onClick={() => void loadParquet(selectedEvent)}
              >
                선택 이벤트 데이터
              </button>
            </div>
          </div>

          {parquet ? (
            <>
              <div className="metadata-grid">
                {["bucket", "prefix", "files_scanned", "row_count"].map((key) => (
                  <div className="metadata-card" key={key}>
                    <span>{key}</span>
                    <strong>{String(parquet[key] ?? "-")}</strong>
                  </div>
                ))}
              </div>
              <h3>Parquet Rows / Metadata</h3>
              <pre className="json-view">{pretty(parquet)}</pre>
            </>
          ) : (
            <p className="muted">조회 버튼을 눌러 MinIO에 저장된 Parquet 데이터를 확인하세요.</p>
          )}
        </section>

        {isAdmin && (
          <section id="ldap" className="panel">
            <div className="panel-heading">
              <div>
                <p className="eyebrow">IDENTITY MANAGEMENT</p>
                <h2>OpenLDAP 사용자 관리</h2>
                <p className="muted small">ldap-admin 역할이 있는 사용자만 이용할 수 있습니다.</p>
              </div>
              <button className="secondary" disabled={busy} onClick={() => void loadLdapUsers()}>
                사용자 목록 조회
              </button>
            </div>

            <form className="ldap-form" onSubmit={createLdapUser}>
              <label>
                Username
                <input
                  value={newUser.username}
                  onChange={(e) => setNewUser({ ...newUser, username: e.target.value })}
                  minLength={3}
                  required
                />
              </label>
              <label>
                이름
                <input
                  value={newUser.full_name}
                  onChange={(e) => setNewUser({ ...newUser, full_name: e.target.value })}
                  required
                />
              </label>
              <label>
                이메일
                <input
                  type="email"
                  value={newUser.email}
                  onChange={(e) => setNewUser({ ...newUser, email: e.target.value })}
                  required
                />
              </label>
              <label>
                부서
                <input
                  value={newUser.department}
                  onChange={(e) => setNewUser({ ...newUser, department: e.target.value })}
                />
              </label>
              <label>
                초기 비밀번호
                <input
                  type="password"
                  value={newUser.password}
                  onChange={(e) => setNewUser({ ...newUser, password: e.target.value })}
                  minLength={1}
                  required
                />
              </label>
              <button className="primary" disabled={busy}>LDAP 사용자 등록</button>
            </form>

            <div className="table-wrap">
              <table>
                <thead>
                  <tr><th>Username</th><th>이름</th><th>이메일</th><th>부서</th></tr>
                </thead>
                <tbody>
                  {ldapUsers.map((item) => (
                    <tr key={item.username}>
                      <td>{item.username}</td>
                      <td>{item.full_name}</td>
                      <td>{item.email}</td>
                      <td>{item.department || "-"}</td>
                    </tr>
                  ))}
                  {ldapUsers.length === 0 && (
                    <tr><td colSpan={4} className="empty">사용자 목록을 조회하세요.</td></tr>
                  )}
                </tbody>
              </table>
            </div>
          </section>
        )}

        <footer className="footer">
          Data Platform Monitor · Next.js Frontend · FastAPI Backend
        </footer>
      </section>
    </main>
  );
}
