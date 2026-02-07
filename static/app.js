const form = document.getElementById("add-form");
const bulkForm = document.getElementById("bulk-form");
const message = document.getElementById("form-message");
const bulkMessage = document.getElementById("bulk-message");
const totalsEl = document.getElementById("totals");
const campaignBody = document.getElementById("campaign-body");
const postBody = document.getElementById("post-body");
const lastUpdated = document.getElementById("last-updated");
const viewsSort = document.getElementById("views-sort");
const updateNowBtn = document.getElementById("update-now");
const deleteAllBtn = document.getElementById("delete-all");

const numberFmt = new Intl.NumberFormat("en-US");
let currentPosts = [];

function formatNumber(value) {
  return numberFmt.format(value || 0);
}

function formatMetric(value) {
  if (value === null || value === undefined || value === "") return "-";
  return numberFmt.format(value);
}

function formatDate(iso) {
  if (!iso) return "-";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString();
}

function setMessage(text, isError = false) {
  message.textContent = text;
  message.className = `message ${isError ? "error" : "ok"}`;
}

function setBulkMessage(text, isError = false) {
  bulkMessage.textContent = text;
  bulkMessage.className = `message ${isError ? "error" : "ok"}`;
}

function renderTotals(totals) {
  totalsEl.innerHTML = [
    ["Posts", totals.posts],
    ["Views", totals.views],
    ["Likes", totals.likes],
    ["Comments", totals.comments],
  ]
    .map(
      ([label, value]) =>
        `<div class="kpi"><div class="label">${label}</div><div class="value">${formatNumber(value)}</div></div>`
    )
    .join("");
}

function renderCampaignRows(rows) {
  campaignBody.innerHTML = rows
    .map(
      (row, index) => `<tr>
      <td>${index + 1}</td>
      <td>${row.campaign_name}</td>
      <td>${formatNumber(row.posts)}</td>
      <td>${formatNumber(row.views)}</td>
      <td>${formatNumber(row.likes)}</td>
      <td>${formatNumber(row.comments)}</td>
      <td><a href="/campaign/${row.campaign_id}" target="_blank" rel="noreferrer">Open</a></td>
    </tr>`
    )
    .join("");
}

function renderPostRows(rows) {
  postBody.innerHTML = rows
    .map(
      (row, index) => {
        const statusText = row.source_status || "pending";
        const statusClass = String(statusText).split(":")[0];
        return `<tr>
      <td>${index + 1}</td>
      <td>${row.campaign_name}</td>
      <td>${row.creator_name || row.creator_handle}</td>
      <td><a href="${row.post_url}" target="_blank" rel="noreferrer">Open Post</a></td>
      <td>${row.followers_text || "-"}</td>
      <td>${formatMetric(row.views)}</td>
      <td>${formatMetric(row.likes)}</td>
      <td>${formatMetric(row.comments)}</td>
      <td class="status ${statusClass}">${statusText}${
        row.source_error ? `: ${row.source_error}` : ""
      }</td>
      <td>${formatDate(row.last_snapshot_at)}</td>
      <td>
        <button
          type="button"
          class="danger-btn"
          data-delete-creator-id="${row.creator_id}"
          data-delete-creator-handle="${row.creator_handle}"
        >
          Delete Creator
        </button>
      </td>
    </tr>`;
      }
    )
    .join("");
}

function getSortedPosts(rows) {
  const mode = viewsSort?.value || "desc";
  return [...rows].sort((a, b) => {
    const av = Number(a.views ?? -1);
    const bv = Number(b.views ?? -1);
    return mode === "asc" ? av - bv : bv - av;
  });
}

function rerenderPosts() {
  renderPostRows(getSortedPosts(currentPosts));
}

async function refreshDashboard() {
  const res = await fetch("/api/dashboard");
  const data = await res.json();
  renderTotals(data.totals);
  renderCampaignRows(data.campaigns);
  currentPosts = Array.isArray(data.posts) ? data.posts : [];
  rerenderPosts();
  lastUpdated.textContent = `Last dashboard refresh: ${formatDate(data.generated_at)}`;
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const formData = new FormData(form);
  const payload = Object.fromEntries(formData.entries());

  try {
    const res = await fetch("/api/posts", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!res.ok) {
      const err = await res.json();
      throw new Error(err.detail || "Could not add post");
    }

    setMessage("Post added. Metrics will refresh at next 9:00 AM run.");
    form.reset();
    await refreshDashboard();
  } catch (error) {
    setMessage(error.message || "Request failed", true);
  }
});

bulkForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const formData = new FormData(bulkForm);
  const payload = Object.fromEntries(formData.entries());

  try {
    const res = await fetch("/api/posts/bulk", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    if (!res.ok) {
      throw new Error(data.detail || "Bulk add failed");
    }

    if (Array.isArray(data.errors) && data.errors.length) {
      setBulkMessage(`Inserted ${data.inserted}. Skipped ${data.errors.length} row(s).`, true);
    } else {
      setBulkMessage(
        `Inserted ${data.inserted} post(s). Initial fetch done for ${data.initial_polled || 0} post(s); next scheduled update is 9:00 AM.`
      );
    }
    await refreshDashboard();
  } catch (error) {
    setBulkMessage(error.message || "Bulk add failed", true);
  }
});

deleteAllBtn.addEventListener("click", async () => {
  const ok = window.confirm(
    "Delete ALL campaigns, creators, posts, and snapshots? This cannot be undone."
  );
  if (!ok) return;

  deleteAllBtn.disabled = true;
  try {
    const res = await fetch("/api/all-data", { method: "DELETE" });
    const data = await res.json();
    if (!res.ok) {
      throw new Error(data.detail || "Delete all failed");
    }
    setMessage(
      `Deleted all data: ${data.deleted_campaigns} campaigns, ${data.deleted_creators} creators, ${data.deleted_posts} posts, ${data.deleted_snapshots} snapshots.`
    );
    await refreshDashboard();
  } catch (error) {
    setMessage(error.message || "Delete all failed", true);
  } finally {
    deleteAllBtn.disabled = false;
  }
});

updateNowBtn.addEventListener("click", async () => {
  updateNowBtn.disabled = true;
  try {
    const res = await fetch("/api/poll-now", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    const data = await res.json();
    if (!res.ok) {
      throw new Error(data.detail || "Update failed");
    }
    setMessage(`Update started for ${data.polled || 0} posts (processed in batches).`);
    await refreshDashboard();
  } catch (error) {
    setMessage(error.message || "Update failed", true);
  } finally {
    updateNowBtn.disabled = false;
  }
});

if (viewsSort) {
  viewsSort.addEventListener("change", () => {
    rerenderPosts();
  });
}

postBody.addEventListener("click", async (event) => {
  const target = event.target.closest("button[data-delete-creator-id]");
  if (!target) return;

  const creatorId = Number(target.dataset.deleteCreatorId);
  const creatorHandle = target.dataset.deleteCreatorHandle || "";
  const ok = window.confirm(
    `Delete @${creatorHandle} and all their posts/snapshots from every campaign?`
  );
  if (!ok) return;

  target.disabled = true;
  try {
    const res = await fetch(`/api/creators/${creatorId}`, { method: "DELETE" });
    if (!res.ok) {
      const err = await res.json();
      throw new Error(err.detail || "Delete failed");
    }

    const data = await res.json();
    setMessage(
      `Deleted @${data.creator_handle}: ${data.deleted_posts} posts and ${data.deleted_snapshots} snapshots.`
    );
    await refreshDashboard();
  } catch (error) {
    setMessage(error.message || "Delete failed", true);
  } finally {
    target.disabled = false;
  }
});

refreshDashboard().catch((error) => setMessage(error.message || "Failed to load dashboard", true));
