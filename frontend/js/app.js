/**
 * Shared ResolveX frontend helpers (Flask session API).
 * Assumes Flask serves this site on the same origin (default http://localhost:5000).
 */
const API = ""; // same-origin

async function api(path, opts = {}) {
  const res = await fetch(`${API}${path}`, {
    credentials: "include",
    headers: { "Content-Type": "application/json", ...(opts.headers || {}) },
    ...opts,
  });
  let data = {};
  try {
    data = await res.json();
  } catch {
    /* empty */
  }
  if (!res.ok) {
    const err = new Error(data.error || res.statusText || "Request failed");
    err.data = data;
    err.status = res.status;
    throw err;
  }
  return data;
}

async function requireStudent() {
  const me = await api("/api/me");
  if (!me.logged_in) {
    window.location.href = "/login.html";
    return null;
  }
  localStorage.setItem("rx_student_id", String(me.user.id));
  localStorage.setItem("rx_student_name", me.user.name);
  return me.user;
}

function initThemeToggle() {
  const saved = localStorage.getItem("rx_theme") || "light";
  document.documentElement.setAttribute("data-theme", saved);
  document.querySelectorAll('[data-theme-toggle="1"]').forEach((el) => {
    el.checked = saved === "dark";
    el.addEventListener("change", () => {
      const t = el.checked ? "dark" : "light";
      localStorage.setItem("rx_theme", t);
      document.documentElement.setAttribute("data-theme", t);
    });
  });
}

async function logoutStudent() {
  await api("/api/logout", { method: "POST" });
  localStorage.removeItem("rx_student_id");
  localStorage.removeItem("rx_student_name");
  window.location.href = "/login.html";
}

function toggleSidebarMenu(btn) {
  const layout = btn.closest(".rx-layout");
  if (!layout) return;
  const collapsed = layout.classList.toggle("rx-menu-collapsed");
  btn.setAttribute("aria-expanded", String(!collapsed));
}

function mountStudentSidebar(active) {
  const name =
    localStorage.getItem("rx_student_name") || sessionStorage.getItem("rx_name") || "Student";
  const item = (key, href, icon, label) =>
    `<a class="rx-nav-item ${active === key ? "active" : ""}" href="${href}"><span class="rx-nav-ico" aria-hidden="true">${icon}</span><span>${label}</span></a>`;
  return `
  <aside class="rx-sidebar">
    <div class="rx-sidebar-head">
      <button type="button" class="rx-icon-btn" aria-label="Menu">☰</button>
      <div class="rx-brand-row">
        <span class="rx-logo-mark" img="student-grievance-system/resolveX_logo.jpg" aria-hidden="true"></span>
        <span class="rx-brand-title">ResolveX</span>
      </div>
    </div>
    <nav class="rx-nav-stack">
      ${item("home", "/home.html", "🏠", "Home")}
      ${item("dash", "/dashboard.html", "▦", "Dashboard")}
      ${item("raise", "/raise_grievance.html", "⊕", "Raise Grievances")}
      ${item("complaints", "/my_complaints.html", "💼", "My Complaints")}
      ${item("discussion", "/discussion.html", "💭", "Discussion")}
      ${item("faq", "/faq.html", "❓", "FAQ")}
      ${item("faculty", "/faculty_feedback.html", "📋", "Faculty Feedback")}
      ${item("profile", "/profile.html", "👤", "Profile")}
      ${item("settings", "/settings.html", "⚙", "Settings")}
    </nav>
    <div class="rx-sidebar-foot">
      <div class="rx-sidebar-user">
        <span class="rx-nav-ico" aria-hidden="true">🎓</span>
        <span class="rx-sidebar-user-name">${name}</span>
      </div>
      <button type="button" class="btn btn-outline-danger btn-sm w-100 mt-2" id="rx-signout">Sign Out</button>
    </div>
  </aside>`;
}

document.addEventListener("DOMContentLoaded", () => {
  initThemeToggle();
  const so = document.getElementById("rx-signout");
  if (so) so.addEventListener("click", () => logoutStudent());
});

document.addEventListener("click", (e) => {
  const btn = e.target.closest(".rx-icon-btn");
  if (!btn || btn.getAttribute("aria-label") !== "Menu") return;
  toggleSidebarMenu(btn);
});
