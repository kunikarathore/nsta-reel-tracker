const totalsEl = document.getElementById("totals");
const postBody = document.getElementById("post-body");
const titleEl = document.getElementById("campaign-title");
const viewsSort = document.getElementById("views-sort");

const numberFmt = new Intl.NumberFormat("en-US");
const compactFmt = new Intl.NumberFormat("en-US", {
  notation: "compact",
  maximumFractionDigits: 1,
});
let currentPosts = [];

function formatNumber(value) {
  return numberFmt.format(value || 0);
}

function formatCompact(value) {
  return compactFmt.format(value || 0);
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

function getCampaignId() {
  const parts = window.location.pathname.split("/").filter(Boolean);
  return parts[parts.length - 1];
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
        `<div class=\"kpi\"><div class=\"label\">${label}</div><div class=\"value\">${formatCompact(value)}</div></div>`
    )
    .join("");
}

function renderPosts(rows) {
  postBody.innerHTML = rows
    .map(
      (row, index) => `<tr>
      <td>${index + 1}</td>
      <td>${row.creator_name || row.creator_handle}</td>
      <td><a href="${row.post_url}" target="_blank" rel="noreferrer">Open Post</a></td>
      <td>${row.followers_text || "-"}</td>
      <td>${formatMetric(row.views)}</td>
      <td>${formatMetric(row.likes)}</td>
      <td>${formatMetric(row.comments)}</td>
    </tr>`
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
  renderPosts(getSortedPosts(currentPosts));
}

async function loadCampaign() {
  const id = getCampaignId();
  const res = await fetch(`/api/campaigns/${id}/dashboard`);
  const data = await res.json();
  if (!res.ok) {
    throw new Error(data.detail || "Failed to load campaign");
  }

  titleEl.textContent = `${data.campaign.campaign_name} - Campaign Dashboard`;
  renderTotals(data.totals);
  currentPosts = Array.isArray(data.posts) ? data.posts : [];
  rerenderPosts();
}

loadCampaign().catch((error) => {
  titleEl.textContent = error.message || "Failed to load campaign";
});

if (viewsSort) {
  viewsSort.addEventListener("change", () => {
    rerenderPosts();
  });
}
