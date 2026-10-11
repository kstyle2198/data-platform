"use client";

import { FormEvent, useCallback, useEffect, useState } from "react";

type User = {
  username?: string;
  name?: string;
  email?: string;
  roles?: string[];
};

type LdapUser = {
  username: string;
  full_name: string;
  email: string;
  department: string;
};

type ApiError = { detail?: string; message?: string };

async function request<T>(path: string, token: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers);
  headers.set("Accept", "application/json");
  if (init.body) headers.set("Content-Type", "application/json");
  if (token) headers.set("Authorization", `Bearer ${token}`);
  const response = await fetch(`/api${path}`, { ...init, headers, cache: "no-store" });
  if (!response.ok) {
    let message = `HTTP ${response.status}`;
    try {
      const body = (await response.json()) as ApiError;
      message = body.detail || body.message || message;
    } catch { /* use HTTP status */ }
    throw new Error(message);
  }
  return response.json() as Promise<T>;
}

export default function LdapUsersPage() {
  const [token, setToken] = useState("");
  const [user, setUser] = useState<User | null>(null);
  const [users, setUsers] = useState<LdapUser[]>([]);
  const [newUser, setNewUser] = useState({ username: "", full_name: "", email: "", department: "", password: "" });
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [loaded, setLoaded] = useState(false);

  const isAdmin = user?.roles?.includes("ldap-admin") ?? false;

  const loadUsers = useCallback(async (activeToken: string) => {
    setBusy(true);
    setError("");
    try {
      const result = await request<{ count: number; users: LdapUser[] }>("/admin/ldap/users", activeToken);
      setUsers(result.users || []);
      setMessage(`LDAP 사용자 ${result.count}명을 조회했습니다.`);
      setLoaded(true);
    } catch (e) {
      setError(e instanceof Error ? e.message : "LDAP 사용자 조회 실패");
    } finally {
      setBusy(false);
    }
  }, []);

  useEffect(() => {
    const saved = sessionStorage.getItem("access_token") || "";
    setToken(saved);
    if (!saved) return;
    void request<User>("/me", saved)
      .then((currentUser) => {
        setUser(currentUser);
        if (currentUser.roles?.includes("ldap-admin")) void loadUsers(saved);
      })
      .catch((e) => setError(e instanceof Error ? e.message : "사용자 인증 확인 실패"));
  }, [loadUsers]);

  async function createUser(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setBusy(true);
    setError("");
    setMessage("");
    try {
      await request("/admin/ldap/users", token, { method: "POST", body: JSON.stringify(newUser) });
      setNewUser({ username: "", full_name: "", email: "", department: "", password: "" });
      setMessage("OpenLDAP 사용자를 등록했습니다.");
      await loadUsers(token);
    } catch (e) {
      setError(e instanceof Error ? e.message : "LDAP 사용자 등록 실패");
    } finally {
      setBusy(false);
    }
  }

  function logout() {
    sessionStorage.removeItem("access_token");
    window.location.href = "/";
  }

  if (!token || !user) {
    return (
      <main className="login-page">
        <section className="login-card">
          <div className="brand-mark">DP</div>
          <p className="eyebrow">IDENTITY MANAGEMENT</p>
          <h1>OpenLDAP 사용자 관리</h1>
          <p className="muted">먼저 메인 화면에서 로그인해 주세요.</p>
          {error && <p className="error-message">{error}</p>}
          <a className="primary full button-link" href="/">메인 화면으로 이동</a>
        </section>
      </main>
    );
  }

  return (
    <main className="app-shell">
      <aside className="sidebar">
        <div className="brand"><div className="brand-mark">DP</div><div><strong>Data Platform</strong><span>Operations Console</span></div></div>
        <p className="nav-label">MONITORING</p>
        <a href="/" className="nav-item">Pipeline Flow</a>
        <a href="/monitoring/kafka" className="nav-item">Kafka Processing</a>
        <a href="/time-travel" className="nav-item">Iceberg Time Travel</a>
        {isAdmin && <><p className="nav-label">IDENTITY</p><a href="/identity/ldap" className="nav-item active">OpenLDAP Users</a></>}
        <div className="sidebar-bottom">
          <div className="user-avatar">{(user.name || user.username || "U").slice(0, 1).toUpperCase()}</div>
          <div className="user-meta"><strong>{user.name || user.username}</strong><span>ldap-admin</span></div>
          <button className="icon-button" onClick={logout} title="로그아웃">↪</button>
        </div>
      </aside>

      <section className="main-area">
        <header className="topbar"><div><p className="eyebrow">IDENTITY MANAGEMENT</p><h1>OpenLDAP 사용자 관리</h1><p className="muted">ldap-admin 역할을 보유한 사용자만 접근할 수 있습니다.</p></div></header>
        {error && <div className="notice error" role="alert">{error}</div>}
        {message && <div className="notice success" role="status">{message}</div>}
        {!isAdmin ? (
          <section className="panel"><h2>접근 권한이 없습니다</h2><p className="muted">이 페이지는 Keycloak의 <code>ldap-admin</code> realm role이 필요합니다.</p><a href="/" className="secondary button-link">Pipeline Flow로 돌아가기</a></section>
        ) : (
          <>
            <section className="panel">
              <div className="panel-heading"><div><p className="eyebrow">DIRECTORY</p><h2>사용자 디렉터리</h2><p className="muted small">현재 OpenLDAP의 사용자 목록을 조회합니다.</p></div><button className="secondary" disabled={busy} onClick={() => void loadUsers(token)}>{busy ? "조회 중..." : "사용자 목록 새로고침"}</button></div>
              <div className="metadata-grid"><div className="metadata-card"><span>등록 사용자</span><strong>{loaded ? users.length : "—"}</strong></div><div className="metadata-card"><span>접근 권한</span><strong>ldap-admin</strong></div></div>
              <div className="table-wrap"><table><thead><tr><th>Username</th><th>이름</th><th>이메일</th><th>부서</th></tr></thead><tbody>
                {users.map((item) => <tr key={item.username}><td>{item.username}</td><td>{item.full_name || "-"}</td><td>{item.email || "-"}</td><td>{item.department || "-"}</td></tr>)}
                {users.length === 0 && <tr><td colSpan={4} className="empty">{loaded ? "등록된 사용자가 없습니다." : "사용자 목록을 조회하세요."}</td></tr>}
              </tbody></table></div>
            </section>
            <section className="panel">
              <div className="panel-heading"><div><p className="eyebrow">PROVISIONING</p><h2>사용자 등록</h2><p className="muted small">신규 사용자를 OpenLDAP에 등록합니다.</p></div></div>
              <form className="ldap-form" onSubmit={createUser}>
                <label>Username<input value={newUser.username} onChange={(e) => setNewUser({ ...newUser, username: e.target.value })} minLength={3} autoComplete="off" required /></label>
                <label>이름<input value={newUser.full_name} onChange={(e) => setNewUser({ ...newUser, full_name: e.target.value })} required /></label>
                <label>이메일<input type="email" value={newUser.email} onChange={(e) => setNewUser({ ...newUser, email: e.target.value })} required /></label>
                <label>부서<input value={newUser.department} onChange={(e) => setNewUser({ ...newUser, department: e.target.value })} /></label>
                <label>초기 비밀번호<input type="password" value={newUser.password} onChange={(e) => setNewUser({ ...newUser, password: e.target.value })} autoComplete="new-password" required /></label>
                <button className="primary" disabled={busy}>{busy ? "처리 중..." : "OpenLDAP 사용자 등록"}</button>
              </form>
            </section>
          </>
        )}
        <footer className="footer">Data Platform Monitor · Identity Administration</footer>
      </section>
    </main>
  );
}
