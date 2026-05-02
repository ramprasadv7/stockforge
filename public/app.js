// ─── Universal SSE Stream Reader with Timeout ─────────────────────
// Wraps fetch SSE streams with a 90s timeout so spinners never hang forever
async function readSSE(url, options, onMessage) {
  const controller = new AbortController();
  const hardTimeout = setTimeout(() => controller.abort(), 90000);
  let lastDataTime = Date.now();
  const watchdog = setInterval(() => {
    if (Date.now() - lastDataTime > 60000) controller.abort();
  }, 5000);

  try {
    const res = await fetch(url, { ...options, signal: controller.signal });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      lastDataTime = Date.now();
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop();
      for (const line of lines) {
        if (!line.startsWith('data:')) continue;
        const jsonStr = line.slice(line.indexOf(':') + 1).trim();
        if (!jsonStr) continue;
        try { onMessage(JSON.parse(jsonStr)); } catch {}
      }
    }
  } catch (e) {
    if (e.name === 'AbortError') {
      onMessage({ type: 'error', text: 'Timed out — AI took too long. Is your AI provider running? Check AI Settings.' });
    } else { throw e; }
  } finally {
    clearTimeout(hardTimeout);
    clearInterval(watchdog);
  }
}

// ─── Patched ReadableStream — add timeout to ALL existing SSE readers ──
// This wraps the native ReadableStreamDefaultReader.read() so ALL
// existing res.body.getReader() calls automatically get a 90s timeout
const _origGetReader = ReadableStream.prototype.getReader;
ReadableStream.prototype.getReader = function(...args) {
  const reader = _origGetReader.apply(this, args);
  const _origRead = reader.read.bind(reader);
  let lastRead = Date.now();
  const watchdog = setInterval(() => {
    if (Date.now() - lastRead > 90000) {
      reader.cancel('SSE timeout — AI provider took too long');
      clearInterval(watchdog);
    }
  }, 5000);
  reader.read = function() {
    lastRead = Date.now();
    return _origRead().finally(() => {
      // Clear watchdog when stream finishes
    });
  };
  // Clear watchdog when stream is cancelled/released
  const _origCancel = reader.cancel?.bind(reader);
  if (_origCancel) reader.cancel = function(...a) { clearInterval(watchdog); return _origCancel(...a); };
  const _origRelease = reader.releaseLock?.bind(reader);
  if (_origRelease) reader.releaseLock = function() { clearInterval(watchdog); return _origRelease(); };
  return reader;
};

// ─── Theme ────────────────────────────────────────────────────────
const THEMES = ['light', 'dark', 'cosmos', 'glass'];
const THEME_LABELS = { light: '️', dark: '', cosmos: '', glass: '' };
const THEME_NAMES  = { light: 'Light', dark: 'Dark', cosmos: 'Cosmos', glass: 'Glass' };

(function initTheme() {
  const saved = localStorage.getItem('sf-theme') || 'light';
  document.documentElement.setAttribute('data-theme', saved);
})();

function cycleTheme() {
  const current = document.documentElement.getAttribute('data-theme') || 'light';
  const idx = THEMES.indexOf(current);
  const next = THEMES[(idx + 1) % THEMES.length];
  document.documentElement.setAttribute('data-theme', next);
  localStorage.setItem('sf-theme', next);
  updateThemeBtn(next);
}

function updateThemeBtn(theme) {
  const btn = document.getElementById('themeToggleBtn');
  if (!btn) return;
  btn.title = `Theme: ${THEME_NAMES[theme]} — click to switch`;
  // Update icon visibility
  const darkIcon = document.getElementById('themeIconDark');
  const lightIcon = document.getElementById('themeIconLight');
  if (darkIcon) darkIcon.style.display = (theme === 'dark' || theme === 'cosmos') ? 'none' : 'block';
  if (lightIcon) lightIcon.style.display = (theme === 'dark' || theme === 'cosmos') ? 'block' : 'none';
  // Show theme label
  let label = btn.querySelector('.theme-label');
  if (!label) { label = document.createElement('span'); label.className = 'theme-label'; btn.appendChild(label); }
  label.textContent = THEME_NAMES[theme];
}

document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('themeToggleBtn')?.addEventListener('click', cycleTheme);
  const theme = document.documentElement.getAttribute('data-theme') || 'light';
  updateThemeBtn(theme);
});

// ─── State ────────────────────────────────────────────────────────
let appData = null;
let prices = {};
let cryptoPrices = {};
let ratings = {};
let selectedContract = null;
let closeContractId = null;
let editContractId = null; // null = add mode, string = edit mode
let priceRefreshInterval = null;

// ─── Data & Status State ──────────────────────────────────────────
const dsState = {
  lastRefreshed: null,
  totalRefreshes: 0,
  fetchDurations: [],
  lastFetchDuration: null,
  tickerStatus: {},
  countdownSec: 60,
  countdownInterval: null
};

// ─── Init ─────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', async () => {
  setupTabs();
  setupModals();
  setupDayTrading();
  setupLongTerm();
  setupCrypto();
  await loadData();
  // Load cached ratings immediately
  await loadCachedRatings();
  // Await prices so UI renders with real data immediately
  await trackedRefresh();
  // Background: rate portfolio items + load paper account
  setTimeout(() => {
    rateAllPortfolio();
    paperLoadAccount(); // load paper trades so they persist across restarts
  }, 1500);
  // Auto-refresh every 60 seconds
  priceRefreshInterval = setInterval(async () => {
    await trackedRefresh();
    dsResetCountdown();
  }, 60000);
  // Re-rate portfolio every 6 hours while app is open
  setInterval(() => { rateAllPortfolio(true); }, 6 * 60 * 60 * 1000);
  dsStartCountdown();
  // Run first smart alert scan 10s after load (not 5s — give prices time to settle)
  setTimeout(() => { if (typeof runSmartAlertScan === 'function') runSmartAlertScan(); }, 10000);
  setupDataStatus();
});

// ─── Tabs ─────────────────────────────────────────────────────────
function switchToTab(tabId) {
  // Deactivate all
  // Hub sub-tab mapping — which hub does this tab live in?
  const HUB_MAP = {
    daytrading: 'trading', paper: 'trading',
    longterm: 'portfolio', crypto: 'portfolio',
    tools: 'toolshub', journal: 'toolshub', alertlog: 'toolshub',
    backtest: 'research', edgar: 'research'
  };

  const hubId = HUB_MAP[tabId];

  // Deactivate all top-level tabs
  document.querySelectorAll('.tab-btn, .tab-group-btn, .tab-group-item').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));

  if (hubId) {
    // Activate the hub tab-content
    const hubContent = document.getElementById(`tab-${hubId}`);
    if (hubContent) hubContent.classList.add('active');
    // Activate the hub nav button
    document.querySelectorAll('.tab-btn').forEach(b => {
      if (b.dataset.tab === hubId) b.classList.add('active');
    });
    // Switch to the correct sub-panel inside the hub
    switchHubPanel(hubId, tabId);
  } else {
    // Normal standalone tab
    const content = document.getElementById(`tab-${tabId}`);
    if (content) content.classList.add('active');
    document.querySelectorAll('.tab-btn').forEach(b => {
      if (b.dataset.tab === tabId) b.classList.add('active');
    });
  }
}

function switchHubPanel(hubId, panelId) {
  const hubEl = document.getElementById(`tab-${hubId}`);
  if (!hubEl) return;
  // Deactivate all panels in this hub
  hubEl.querySelectorAll('.hub-panel').forEach(p => p.classList.add('hidden'));
  hubEl.querySelectorAll('.hub-subtab').forEach(b => b.classList.remove('active'));
  // Activate the target panel
  const panel = document.getElementById(`hubpanel-${panelId}`);
  if (panel) panel.classList.remove('hidden');
  // Activate the matching sub-tab button
  hubEl.querySelectorAll('.hub-subtab').forEach(b => {
    if (b.dataset.hubtab === panelId) b.classList.add('active');
  });
}

let openDropdown = null;

function closeAllDropdowns() {
  document.querySelectorAll('.tab-group-dropdown.open').forEach(d => d.classList.remove('open'));
  openDropdown = null;
}

function setupTabs() {
  // Standalone tab buttons
  document.querySelectorAll('.tab-btn:not(.tab-group-btn)').forEach(btn => {
    btn.addEventListener('click', () => {
      closeAllDropdowns();
      switchToTab(btn.dataset.tab);
    });
  });

  // Group buttons — toggle dropdown on click
  document.querySelectorAll('.tab-group-btn').forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      const group = btn.closest('.tab-group');
      const dropdown = group?.querySelector('.tab-group-dropdown');
      if (!dropdown) return;

      if (dropdown.classList.contains('open')) {
        // Already open — close it and switch to default tab
        closeAllDropdowns();
        switchToTab(btn.dataset.tab);
      } else {
        // Close any other open dropdown
        closeAllDropdowns();
        // Position dropdown below the button using fixed coords
        const rect = btn.getBoundingClientRect();
        dropdown.style.top = (rect.bottom + 4) + 'px';
        dropdown.style.left = rect.left + 'px';
        dropdown.classList.add('open');
        openDropdown = dropdown;
      }
    });
  });

  // Group dropdown items
  document.querySelectorAll('.tab-group-item').forEach(item => {
    item.addEventListener('click', (e) => {
      e.stopPropagation();
      closeAllDropdowns();
      switchToTab(item.dataset.tab);
    });
  });

  // Close dropdown when clicking anywhere else
  document.addEventListener('click', () => closeAllDropdowns());

  // Hub sub-tab buttons
  document.querySelectorAll('.hub-subtab').forEach(btn => {
    btn.addEventListener('click', () => {
      switchToTab(btn.dataset.hubtab);
    });
  });
}

// Make switchToTab globally accessible for onclick handlers
window.switchToTab = switchToTab;

// ─── Modals ───────────────────────────────────────────────────────
function setupModals() {
  document.querySelectorAll('.modal-close, [data-modal]').forEach(el => {
    el.addEventListener('click', () => {
      const modalId = el.dataset.modal || el.closest('.modal')?.id;
      if (modalId) {
        document.getElementById(modalId)?.classList.add('hidden');
        // Reset contract modal if closed without saving
        if (modalId === 'modalAddContract') resetContractModal();
      }
    });
  });
  document.querySelectorAll('.modal').forEach(modal => {
    modal.addEventListener('click', e => {
      if (e.target === modal) {
        modal.classList.add('hidden');
        if (modal.id === 'modalAddContract') resetContractModal();
      }
    });
  });
}

function openModal(id) { document.getElementById(id)?.classList.remove('hidden'); }
function closeModal(id) { document.getElementById(id)?.classList.add('hidden'); }

// ─── Ratings ──────────────────────────────────────────────────────
async function loadCachedRatings() {
  try {
    const res = await fetch('/api/ratings');
    ratings = await res.json();
  } catch {}
}

async function fetchRatingForTicker(ticker, price, isCrypto = false) {
  try {
    const res = await fetch('/api/ai/quick-rate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ticker, price, isCrypto })
    });
    const data = await res.json();
    ratings[ticker] = data;
    renderLtPortfolio();
    renderLtCryptoPortfolio();
  } catch {}
}

function getRatingBadge(ticker) {
  const r = ratings[ticker];
  if (!r) return `<span class="rating-badge loading" onclick="fetchRatingForTicker('${ticker}', ${prices[ticker]?.price || cryptoPrices[ticker]?.price || 0})">⟳ Rate</span>`;
  const dot = `<span class="rating-dot ${r.color}"></span>`;
  return `<span class="rating-badge ${r.color}" title="Click to re-rate" onclick="fetchRatingForTicker('${ticker}', ${prices[ticker]?.price || cryptoPrices[ticker]?.price || 0}, ${!!cryptoPrices[ticker]})">${dot} ${r.rating}</span>`;
}

// Background rate all portfolio items — uses batch API for speed
async function rateAllPortfolio(force = false) {
  if (!appData) return;
  const SIX_HOURS = 6 * 60 * 60 * 1000;
  const toRate = [];

  for (const s of (appData.longterm.portfolio || [])) {
    const p = prices[s.ticker]?.price || 0;
    if (p <= 0) continue;
    const existing = ratings[s.ticker];
    const isStale = existing && (Date.now() - new Date(existing.updatedAt).getTime() > SIX_HOURS);
    if (force || !existing || isStale) toRate.push({
      ticker: s.ticker, price: p, isCrypto: false,
      avgCost: s.avgCost || null, shares: s.shares || null
    });
  }
  for (const c of (appData.longterm.cryptoPortfolio || [])) {
    const p = cryptoPrices[c.ticker]?.price || 0;
    if (p <= 0) continue;
    const existing = ratings[c.ticker];
    const isStale = existing && (Date.now() - new Date(existing.updatedAt).getTime() > SIX_HOURS);
    if (force || !existing || isStale) toRate.push({
      ticker: c.ticker, price: p, isCrypto: true,
      avgCost: c.avgCost || null, coins: c.coins || null
    });
  }

  if (!toRate.length) return;

  try {
    // Batch all ratings in ONE AI call instead of sequential calls
    const res = await fetch('/api/ai/batch-rate', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ tickers: toRate })
    });
    const data = await res.json();
    Object.entries(data).forEach(([ticker, r]) => { ratings[ticker] = { ...r, updatedAt: r.updatedAt || new Date().toISOString() }; });
    renderLtPortfolio();
    renderLtCryptoPortfolio();
  } catch {
    // Fallback to individual calls if batch fails
    for (const t of toRate) {
      await fetchRatingForTicker(t.ticker, t.price, t.isCrypto);
      await new Promise(r => setTimeout(r, 300));
    }
  }
}

async function addPickToWatchlist(ticker, type) {
  if (type === 'crypto') {
    await fetch('/api/longterm/crypto/watchlist/add', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ticker }) });
    await fetch('/api/daytrading/crypto/add', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ticker }) });
  } else {
    await fetch('/api/longterm/watchlist/add', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ticker }) });
    await fetch('/api/daytrading/watchlist/add', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ticker }) });
  }
  await loadData();
  await trackedRefresh();
  // Show confirmation
  const btn = event.target;
  btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Added';
  btn.disabled = true;
}

// ─── Data ─────────────────────────────────────────────────────────
async function loadData() {
  const res = await fetch('/api/data');
  appData = await res.json();
  renderDayTrading();
  renderLongTerm();
}

async function saveData() {
  await fetch('/api/data', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(appData) });
}

// ─── Prices ───────────────────────────────────────────────────────
// refreshAllPrices removed — use trackedRefresh() instead

function updateMarketStatus() {
  const now = new Date();
  const etNow = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const hour = etNow.getHours();
  const min = etNow.getMinutes();
  const day = etNow.getDay();
  const isWeekday = day >= 1 && day <= 5;
  const isMarketHours = (hour > 9 || (hour === 9 && min >= 30)) && hour < 16;
  const isPreMarket = isWeekday && (hour >= 4 && (hour < 9 || (hour === 9 && min < 30)));
  const isAfterHours = isWeekday && (hour >= 16 && hour < 20);

  const statusEl = document.getElementById('marketStatus');
  const sourceEl = document.getElementById('priceSourceLabel');
  const banner = document.getElementById('marketBanner');
  const bannerDot = document.getElementById('marketBannerDot');
  const bannerLabel = document.getElementById('marketBannerLabel');
  const bannerDetail = document.getElementById('marketBannerDetail');
  const bannerTime = document.getElementById('marketBannerTime');

  const etTimeStr = etNow.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit' }) + ' ET';

  // Helper: next open time string
  function nextOpenStr() {
    const nextDay = day === 5 ? 'Monday' : day === 6 ? 'Monday' : 'Today';
    return `Opens ${nextDay} 9:30 AM ET`;
  }

  if (isWeekday && isMarketHours) {
    statusEl.textContent = '● Open';
    statusEl.className = 'market-status open';
    sourceEl.textContent = '● Live prices';
    sourceEl.className = 'price-source-label live';
    banner.className = 'market-banner open';
    bannerDot.className = 'market-banner-dot open';
    bannerLabel.className = 'market-banner-label open';
    bannerLabel.textContent = 'Market Open';
    bannerDetail.textContent = 'Live prices — 15-min delayed · Auto-refreshing every 60s';
    bannerTime.textContent = etTimeStr;
  } else if (isPreMarket) {
    statusEl.textContent = '● Pre-Market';
    statusEl.className = 'market-status';
    sourceEl.textContent = 'Pre-Market';
    sourceEl.className = 'price-source-label';
    banner.className = 'market-banner premarket';
    bannerDot.className = 'market-banner-dot premarket';
    bannerLabel.className = 'market-banner-label premarket';
    bannerLabel.textContent = 'Pre-Market';
    bannerDetail.textContent = 'Showing last closing prices · Market opens 9:30 AM ET';
    bannerTime.textContent = etTimeStr;
  } else if (isAfterHours) {
    statusEl.textContent = '● After Hours';
    statusEl.className = 'market-status';
    sourceEl.textContent = 'After Hours';
    sourceEl.className = 'price-source-label';
    banner.className = 'market-banner afterhours';
    bannerDot.className = 'market-banner-dot afterhours';
    bannerLabel.className = 'market-banner-label afterhours';
    bannerLabel.textContent = 'After Hours';
    bannerDetail.textContent = 'Showing last closing prices · Regular session ended 4:00 PM ET';
    bannerTime.textContent = etTimeStr;
  } else {
    const dayName = day === 0 ? 'Sunday' : 'Saturday';
    statusEl.textContent = '● Closed';
    statusEl.className = 'market-status';
    sourceEl.textContent = 'Market Closed';
    sourceEl.className = 'price-source-label';
    banner.className = 'market-banner closed';
    bannerDot.className = 'market-banner-dot closed';
    bannerLabel.className = 'market-banner-label closed';
    bannerLabel.textContent = isWeekday ? 'Market Closed' : `Market Closed — ${dayName}`;
    bannerDetail.textContent = `Showing last closing prices · ${nextOpenStr()}`;
    bannerTime.textContent = etTimeStr;
  }
}

// Update banner clock every second
setInterval(updateMarketStatus, 1000);

// Format number with commas and decimals: 108318.67 → "108,318.67"
function fmt(n, decimals = 2) {
  const v = Number(n || 0);
  return v.toLocaleString('en-US', { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
}

// Format as dollar amount with commas: 108318.67 → "$108,318.67"
function fmtMoney(n) {
  const v = Math.abs(Number(n || 0));
  return '$' + v.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

// Format P&L with sign and commas: 12007.55 → "+$12,007.55"
function fmtPnl(n) {
  const v = Number(n || 0);
  const abs = Math.abs(v).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  return (v >= 0 ? '+$' : '-$') + abs;
}

function pnlClass(n) { return Number(n) >= 0 ? 'pnl-positive' : 'pnl-negative'; }

// ─── DAY TRADING ──────────────────────────────────────────────────
function setupDayTrading() {
  // Add stock to watchlist — validation handled by ticker search (see initAllTickerSearches)
  document.getElementById('dtAddStock').addEventListener('click', () => openModal('modalDtAddStock'));

  // Add crypto from unified watchlist button
  document.getElementById('dtAddCryptoFromWatchlist')?.addEventListener('click', () => openModal('modalDtAddCrypto'));

  // Refresh prices
  document.getElementById('dtRefreshPrices')?.addEventListener('click', () => trackedRefresh());

  // Add contract
  document.getElementById('dtAddContract').addEventListener('click', () => {
    resetContractModal();
    openModal('modalAddContract');
  });

  // Contract calc preview
  ['ctPricePaid', 'ctContracts'].forEach(id => {
    document.getElementById(id).addEventListener('input', updateContractCalc);
  });

  async function saveContractFromModal(alsoPaperTrade = false) {
    const ticker = document.getElementById('ctTicker').value.trim().toUpperCase();
    const type = document.getElementById('ctType').value;
    const strike = document.getElementById('ctStrike').value;
    const expiry = document.getElementById('ctExpiry').value;
    const pricePaid = document.getElementById('ctPricePaid').value;
    const contracts = document.getElementById('ctContracts').value || 1;
    if (!ticker || !strike || !expiry || !pricePaid) return alert('Please fill in all required fields.');

    const payload = {
      ticker, type, strike: parseFloat(strike), expiry,
      pricePaid: parseFloat(pricePaid), contracts: parseInt(contracts),
      dateBought: document.getElementById('ctDateBought').value,
      timeBought: document.getElementById('ctTimeBought').value,
      notes: document.getElementById('ctNotes').value
    };

    if (editContractId) {
      await fetch(`/api/daytrading/contracts/${editContractId}`, {
        method: 'PUT', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      editContractId = null;
    } else {
      await fetch('/api/daytrading/contracts/add', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
    }

    // Also paper trade if requested
    if (alsoPaperTrade && !editContractId) {
      const stockPrice = prices[ticker]?.price || 0;
      await fetch('/api/paper/trade', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ticker, type, strike: parseFloat(strike), expiry,
          premium: parseFloat(pricePaid), contracts: parseInt(contracts),
          stockPriceAtEntry: stockPrice, signal: { source: 'manual' }
        })
      });
      if (paperAccount) paperAccount = await (await fetch('/api/paper/account')).json();
    }

    closeModal('modalAddContract');
    document.getElementById('modalAddContract').querySelector('h3').textContent = 'Log New Trade';
    document.getElementById('ctSaveContract').innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg> Save Trade';
    ['ctTicker','ctStrike','ctExpiry','ctPricePaid','ctNotes'].forEach(id => document.getElementById(id).value = '');
    document.getElementById('ctContracts').value = '1';
    document.getElementById('ctType').value = 'CALL';
    document.getElementById('ctCalcBox').style.display = 'none';
    await loadData();
    trackedRefresh();
  }

  document.getElementById('ctSaveContract').addEventListener('click', () => saveContractFromModal(false));
  document.getElementById('ctSaveAndPaper')?.addEventListener('click', () => saveContractFromModal(true));

  // Generate signal — crew signal button wired in DOMContentLoaded below

  // Close contract modal
  document.getElementById('closeSoldPrice').addEventListener('input', updateClosePnlPreview);
  document.getElementById('closeContractConfirm').addEventListener('click', closeContract);
}

function updateContractCalc() {
  const price = parseFloat(document.getElementById('ctPricePaid').value) || 0;
  const contracts = parseInt(document.getElementById('ctContracts').value) || 1;
  const total = price * 100 * contracts;
  const box = document.getElementById('ctCalcBox');
  if (total > 0) {
    box.style.display = 'block';
    document.getElementById('ctTotalCost').textContent = `$${total.toFixed(2)}`;
    document.getElementById('ctFormula').textContent = `$${price.toFixed(2)} premium × 100 shares × ${contracts} contract${contracts > 1 ? 's' : ''}`;
    document.getElementById('ctMaxLoss').textContent = `$${total.toFixed(2)} (if expires worthless)`;
  } else {
    box.style.display = 'none';
  }
  // Keep old calc text too
  document.getElementById('ctCalc').textContent = '';
}

function renderDayTrading() {
  if (!appData) return;
  renderDtWatchlist();
  renderDtContracts();
  renderDtPerformance();
  updateDtSignalSelect();
}

function tickerColor(p) {
  if (!p) return 'wl-ticker-neutral';
  if (p.changePct > 0) return 'wl-ticker-up';
  if (p.changePct < 0) return 'wl-ticker-down';
  return 'wl-ticker-neutral';
}

function renderDtWatchlist() {
  const el = document.getElementById('dtWatchlist');
  const list = appData.daytrading.watchlist;
  if (!list.length) { el.innerHTML = '<div class="empty-state">No stocks. Click + Add Stock.</div>'; return; }
  el.innerHTML = list.map(s => {
    const p = prices[s.ticker];
    const price = p ? `$${fmt(p.price)}` : '—';
    const chg = p ? `${p.changePct >= 0 ? '+' : ''}${fmt(p.changePct, 2)}%` : '—';
    const cls = p ? (p.changePct > 0 ? 'green' : p.changePct < 0 ? 'red' : 'flat') : 'flat';
    const tc = tickerColor(p);
    return `<div class="watchlist-row" onclick="selectDtStock('${s.ticker}')">
      <span class="wl-ticker ${tc}">${s.ticker}</span>
      <span class="wl-price">${price}</span>
      <span class="wl-change ${cls}">${chg}</span>
      <button class="wl-remove" onclick="removeDtStock(event,'${s.ticker}')"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg></button>
    </div>`;
  }).join('');
}

function selectDtStock(ticker) {
  document.getElementById('dtSignalTicker').value = ticker;
}

async function removeDtStock(e, ticker) {
  e.stopPropagation();
  await fetch('/api/daytrading/watchlist/remove', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ticker }) });
  await loadData();
}

function updateDtSignalSelect() {
  const sel = document.getElementById('dtSignalTicker');
  const current = sel.value;
  sel.innerHTML = '<option value="">Select a stock...</option>' +
    appData.daytrading.watchlist.map(s => `<option value="${s.ticker}" ${s.ticker === current ? 'selected' : ''}>${s.ticker}${prices[s.ticker] ? ' — $' + fmt(prices[s.ticker].price) : ''}</option>`).join('');
}

function renderDtContracts() {
  const el = document.getElementById('dtContracts');
  const list = appData.daytrading.contracts;
  if (!list.length) { el.innerHTML = '<div class="empty-state">No open contracts. Generate a signal and log your first trade.</div>'; return; }
  el.innerHTML = `<div class="contracts-table-wrap"><table class="contracts-table">
    <thead><tr>
      <th>Stock</th><th>Type</th><th>Strike</th><th>Expiry</th>
      <th>Paid</th><th>P&L</th><th>Action</th>
    </tr></thead>
    <tbody>${list.map(c => {
      const stockPrice = prices[c.ticker]?.price || 0;
      const hasPrice = stockPrice > 0;
      const contractValue = hasPrice ? stockPrice * 100 * c.contracts : null;
      const costBasis = c.pricePaid * 100 * c.contracts;
      const pnl = contractValue !== null ? contractValue - costBasis : null;
      const pnlPct = pnl !== null ? ((pnl / costBasis) * 100).toFixed(1) : null;
      const pnlStr = pnl !== null
        ? `<span class="${pnlClass(pnl)}">${fmtPnl(pnl)}<br><span style="font-size:10px">(${pnlPct}%)</span></span>`
        : `<span style="color:var(--text3);font-size:10px">loading...</span>`;
      const expiry = new Date(c.expiry);
      const daysLeft = Math.ceil((expiry - new Date()) / (1000 * 60 * 60 * 24));
      const expiryStr = daysLeft <= 3 ? `<span class="red">${c.expiry} (${daysLeft}d)</span>` : `${c.expiry}`;
      return `<tr onclick="selectContract('${c.id}')" id="contract-row-${c.id}" ${selectedContract?.id === c.id ? 'class="selected"' : ''}>
        <td><strong>${c.ticker}</strong></td>
        <td><span class="badge badge-${c.type.toLowerCase()}">${c.type}</span></td>
        <td>$${fmt(c.strike)}</td>
        <td>${expiryStr}</td>
        <td><strong>$${fmt(costBasis, 0)}</strong><br><span style="font-size:10px;color:var(--text3)">$${fmt(c.pricePaid)} premium</span></td>
        <td>${pnlStr}</td>
        <td style="display:flex;gap:4px">
          <button class="btn-icon" title="Edit trade" onclick="openEditContract(event,'${c.id}')"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>️</button>
          <button class="btn-secondary btn-sm" onclick="openCloseModal(event,'${c.id}')">Close</button>
        </td>
      </tr>`;
    }).join('')}</tbody>
  </table></div>`;
}

function selectContract(id) {
  selectedContract = appData.daytrading.contracts.find(c => c.id === id);
  document.querySelectorAll('.contracts-table tbody tr').forEach(r => r.classList.remove('selected'));
  document.getElementById(`contract-row-${id}`)?.classList.add('selected');
  analyzeContract(selectedContract);
}

function openEditContract(e, id) {
  e.stopPropagation();
  const c = appData.daytrading.contracts.find(c => c.id === id);
  if (!c) return;

  // Switch modal to edit mode
  editContractId = id;
  document.getElementById('modalAddContract').querySelector('h3').textContent = 'Edit Trade';
  document.getElementById('ctSaveContract').textContent = 'Update Trade';

  // Pre-fill all fields with existing values
  document.getElementById('ctTicker').value = c.ticker || '';
  document.getElementById('ctType').value = c.type || 'CALL';
  document.getElementById('ctStrike').value = c.strike || '';
  document.getElementById('ctExpiry').value = c.expiry || '';
  document.getElementById('ctPricePaid').value = c.pricePaid || '';
  document.getElementById('ctContracts').value = c.contracts || 1;
  document.getElementById('ctDateBought').value = c.dateBought || '';
  document.getElementById('ctTimeBought').value = c.timeBought || '';
  document.getElementById('ctNotes').value = c.notes || '';

  // Trigger calc preview
  updateContractCalc();
  openModal('modalAddContract');
}

function openCloseModal(e, id) {
  e.stopPropagation();
  closeContractId = id;
  const c = appData.daytrading.contracts.find(c => c.id === id);
  if (!c) return;
  document.getElementById('closeContractInfo').innerHTML = `
    <strong>${c.ticker} ${c.type}</strong> — Strike $${fmt(c.strike)} | Expiry ${c.expiry}<br>
    Total paid: $${fmt(c.pricePaid * 100 * (c.contracts || 1), 0)} ($${fmt(c.pricePaid)} premium × 100 × ${c.contracts || 1} contract${(c.contracts || 1) > 1 ? 's' : ''})<br>
    Max loss: $${fmt(c.pricePaid * 100 * (c.contracts || 1), 0)} if expires worthless
  `;
  document.getElementById('closeSoldDate').value = new Date().toISOString().split('T')[0];
  document.getElementById('closeSoldPrice').value = '';
  document.getElementById('closePnlPreview').innerHTML = '';
  openModal('modalCloseContract');
}

function updateClosePnlPreview() {
  const soldPrice = parseFloat(document.getElementById('closeSoldPrice').value) || 0;
  if (!closeContractId || !soldPrice) return;
  const c = appData.daytrading.contracts.find(c => c.id === closeContractId);
  if (!c) return;
  const pnl = (soldPrice - c.pricePaid) * 100 * (c.contracts || 1);
  const pnlPct = ((soldPrice - c.pricePaid) / c.pricePaid * 100).toFixed(1);
  const el = document.getElementById('closePnlPreview');
  el.style.background = pnl >= 0 ? 'rgba(0,212,170,0.1)' : 'rgba(255,77,109,0.1)';
  el.style.color = pnl >= 0 ? 'var(--green)' : 'var(--red)';
  el.textContent = `P&L: ${fmtPnl(pnl)} (${pnlPct}%)`;
}

async function closeContract() {
  const soldPrice = parseFloat(document.getElementById('closeSoldPrice').value);
  if (!soldPrice) return alert('Please enter the sold price.');
  await fetch('/api/daytrading/contracts/close', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      id: closeContractId,
      soldPrice,
      soldDate: document.getElementById('closeSoldDate').value,
      soldTime: document.getElementById('closeSoldTime').value
    })
  });
  closeModal('modalCloseContract');
  closeContractId = null;
  selectedContract = null;
  document.getElementById('dtMonitorOutput').innerHTML = '<div class="empty-state">Select a contract to analyze.</div>';
  await loadData();
}

function renderDtPerformance() {
  const closed = appData.daytrading.closedTrades || [];
  const wins = closed.filter(t => t.pnl > 0).length;
  const winRate = closed.length ? Math.round(wins / closed.length * 100) : 0;
  const netPnl = closed.reduce((sum, t) => sum + (t.pnl || 0), 0);
  const best = closed.reduce((b, t) => t.pnl > (b?.pnl || -Infinity) ? t : b, null);

  document.getElementById('dtWinRate').textContent = closed.length ? `${winRate}%` : '—';
  document.getElementById('dtWinRate').className = `perf-value ${winRate >= 50 ? 'green' : 'red'}`;
  document.getElementById('dtNetPnl').textContent = closed.length ? fmtPnl(netPnl) : '—';
  document.getElementById('dtNetPnl').className = `perf-value ${netPnl >= 0 ? 'green' : 'red'}`;
  document.getElementById('dtTotalTrades').textContent = closed.length || '—';
  document.getElementById('dtBestTrade').textContent = best ? `${best.ticker} ${fmtPnl(best.pnl)}` : '—';

  const listEl = document.getElementById('dtClosedTrades');
  if (!closed.length) { listEl.innerHTML = ''; return; }
  listEl.innerHTML = closed.slice(0, 5).map(t => `
    <div class="closed-trade-row">
      <span class="closed-trade-info">${t.ticker} ${t.type} $${fmt(t.strike)} ${t.expiry}</span>
      <span class="closed-trade-pnl ${pnlClass(t.pnl)}">${fmtPnl(t.pnl)}</span>
    </div>
  `).join('');
}

// ─── AI Signal ────────────────────────────────────────────────────
async function generateSignal() {
  const ticker = document.getElementById('dtSignalTicker').value;
  if (!ticker) return alert('Please select a stock first.');

  const outputEl = document.getElementById('dtSignalOutput');
  const loadingEl = document.getElementById('dtSignalLoading');
  const cardEl = document.getElementById('dtSignalCard');
  const btn = document.getElementById('dtCrewSignalBtn');

  outputEl.classList.remove('hidden');
  loadingEl.classList.remove('hidden');
  cardEl.classList.add('hidden');
  btn.disabled = true;

  try {
    // Always fetch fresh price for signal
    let priceData = prices[ticker] || {};
    if (!priceData.price) {
      try {
        const pr = await fetch(`/api/price/${ticker}`);
        priceData = await pr.json();
        if (priceData.price > 0) prices[ticker] = priceData;
      } catch {}
    }

    let news = [];
    try {
      const newsRes = await fetch(`/api/news/${ticker}`);
      const newsData = await newsRes.json();
      news = Array.isArray(newsData) ? newsData : [];
    } catch { news = []; }

    await readSSE('/api/ai/signal', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ticker, price: priceData.price, change: priceData.change, changePct: priceData.changePct, news })
    }, (msg) => {
      if (msg.type === 'status') {
        document.getElementById('dtSignalStatus').textContent = msg.text;
      } else if (msg.type === 'signal') {
        loadingEl.classList.add('hidden');
        renderSignalCard(cardEl, msg.data, ticker, priceData.price);
        cardEl.classList.remove('hidden');
      } else if (msg.type === 'error') {
        loadingEl.classList.add('hidden');
        cardEl.innerHTML = `<div class="empty-state red">⚠ ${msg.text}</div>`;
        cardEl.classList.remove('hidden');
      }
    });
  } catch (e) {
    loadingEl.classList.add('hidden');
    document.getElementById('dtSignalCard').innerHTML = `<div class="empty-state red">Failed: ${e.message}</div>`;
    document.getElementById('dtSignalCard').classList.remove('hidden');
  } finally {
    btn.disabled = false;
  }
}

function renderSignalCard(el, signal, ticker, currentPrice) {
  const isCall = signal.action.includes('CALL');
  const isPut  = signal.action.includes('PUT');
  const isWait = signal.action === 'WAIT';
  const actionClass = isCall ? 'call' : isPut ? 'put' : 'wait';
  const confPct = signal.confidence || 0;
  const dirColor = signal.direction === 'Bullish' ? 'var(--green)' : signal.direction === 'Bearish' ? 'var(--red)' : 'var(--yellow)';

  el.innerHTML = `
    <!-- Header -->
    <div class="signal-card-header">
      <div style="flex:1">
        <div class="signal-action ${actionClass}">${signal.action} — ${ticker}</div>
        <div style="display:flex;align-items:center;gap:10px;margin-top:5px">
          <span style="font-size:11px;color:var(--text2)">Stock: <b>$${fmt(currentPrice)}</b></span>
          <span style="font-size:11px;font-weight:700;color:${dirColor}">● ${signal.direction || ''}</span>
          <span style="font-size:10px;color:${signal.riskLevel==='Low'?'var(--green)':signal.riskLevel==='High'?'var(--red)':'var(--yellow)'}">${signal.riskLevel} Risk</span>
        </div>
      </div>
      <div class="signal-confidence">
        <div style="font-size:10px;color:var(--text3);text-transform:uppercase;letter-spacing:0.4px">Confidence</div>
        <div style="font-size:22px;font-weight:900;color:${confPct>=70?'var(--green)':confPct>=50?'var(--yellow)':'var(--red)'}">${confPct}%</div>
        <div class="confidence-bar"><div class="confidence-fill" style="width:${confPct}%"></div></div>
      </div>
    </div>

    ${isWait ? `<div class="signal-wait-msg"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/></svg> No clear options signal right now. Wait for a better setup.</div>` : `

    <!-- Contract Details Box -->
    <div class="signal-contract-box signal-contract-${actionClass}">
      <div class="signal-contract-title">${isCall ? '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 7 13.5 15.5 8.5 10.5 2 17"/><polyline points="16 7 22 7 22 13"/></svg>' : '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 17 13.5 8.5 8.5 13.5 2 7"/><polyline points="16 17 22 17 22 11"/></svg>'} ${signal.action} Contract Details</div>
      <div class="signal-contract-grid">
        <div class="signal-contract-item">
          <div class="signal-contract-label">Strike Price</div>
          <div class="signal-contract-value">${signal.strikeLabel || '$' + signal.strike}</div>
        </div>
        <div class="signal-contract-item">
          <div class="signal-contract-label">Expiry Date</div>
          <div class="signal-contract-value">${signal.expiryLabel || signal.expiry}</div>
        </div>
        <div class="signal-contract-item">
          <div class="signal-contract-label">Contracts</div>
          <div class="signal-contract-value">${signal.contracts || 1} contract</div>
        </div>
        <div class="signal-contract-item">
          <div class="signal-contract-label">Total Cost</div>
          <div class="signal-contract-value green">${signal.costEstimate}</div>
        </div>
      </div>
    </div>

    <!-- P&L Metrics -->
    <div class="signal-pnl-row">
      <div class="signal-pnl-item signal-pnl-red">
        <div class="signal-pnl-label">Max Loss</div>
        <div class="signal-pnl-value">${signal.maxLossLabel || '$' + (signal.maxLoss || signal.totalCost || '—')}</div>
        <div class="signal-pnl-sub">Premium paid</div>
      </div>
      <div class="signal-pnl-item signal-pnl-blue">
        <div class="signal-pnl-label">Break-Even</div>
        <div class="signal-pnl-value">${signal.breakEvenLabel || '$' + signal.breakEven}</div>
        <div class="signal-pnl-sub">Stock must reach</div>
      </div>
      <div class="signal-pnl-item signal-pnl-green">
        <div class="signal-pnl-label">Target Return</div>
        <div class="signal-pnl-value">${signal.targetReturn || '—'}</div>
        <div class="signal-pnl-sub">If target hit</div>
      </div>
    </div>

    <!-- Why this strike + expiry -->
    <div class="signal-why-grid">
      <div class="signal-why-item">
        <div class="signal-why-label"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="6"/><circle cx="12" cy="12" r="2"/></svg> Why $${signal.strike} Strike</div>
        <div class="signal-why-text">${signal.whyThisStrike || '—'}</div>
      </div>
      <div class="signal-why-item">
        <div class="signal-why-label">Why ${signal.expiryLabel || signal.expiry}</div>
        <div class="signal-why-text">${signal.whyThisExpiry || '—'}</div>
      </div>
    </div>`}

    <!-- Pre-Trade Intelligence Panel -->
    <div class="pti-panel" id="pti-${ticker}">
      <div class="pti-header">
        <span class="pti-title"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg> Pre-Trade Intelligence</span>
        <button class="pti-load-btn" onclick="loadPreTradeIntel('${ticker}', this)">Load Intel</button>
      </div>
      <div class="pti-body" id="pti-body-${ticker}">
        <div class="pti-hint">Click Load Intel to check IV Rank, SEC filings, and macro events before trading.</div>
      </div>
    </div>

    <!-- Analysis -->
    <div class="signal-reasons">
      <h4>Signal Analysis</h4>
      ${(signal.technical || []).map(r => `<div class="signal-reason-item"><span><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 7 13.5 15.5 8.5 10.5 2 17"/><polyline points="16 7 22 7 22 13"/></svg></span><span>${r}</span></div>`).join('')}
      ${(signal.fundamental || []).map(r => `<div class="signal-reason-item"><span><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg></span><span>${r}</span></div>`).join('')}
      ${(signal.news || []).map(r => `<div class="signal-reason-item"><span><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 22h16a2 2 0 0 0 2-2V4a2 2 0 0 0-2-2H8a2 2 0 0 0-2 2v16a2 2 0 0 0-2 2zm0 0a2 2 0 0 1-2-2v-9c0-1.1.9-2 2-2h2"/></svg></span><span>${r}</span></div>`).join('')}
      ${(signal.risk || []).map(r => `<div class="signal-reason-item"><span><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>️</span><span>${r}</span></div>`).join('')}
    </div>

    ${!isWait && signal.exitPlan ? `<div class="signal-exit-plan">
      <span style="font-weight:700;color:var(--text)"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/><path d="M9 18H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h4"/></svg> Exit Plan:</span> ${signal.exitPlan}
    </div>` : ''}

    <!-- How to place on Robinhood -->
    ${signal.steps?.length ? `<div class="signal-steps">
      <h4><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="5" y="2" width="14" height="20" rx="2"/><line x1="12" y1="18" x2="12.01" y2="18"/></svg> How to Place on Robinhood</h4>
      ${signal.steps.map((s, i) => `<div class="signal-step"><span class="step-num">${i + 1}</span><span>${s}</span></div>`).join('')}
    </div>` : ''}

    ${signal.warning ? `<div class="signal-warning"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="9" y1="18" x2="15" y2="18"/><line x1="10" y1="22" x2="14" y2="22"/><path d="M15.09 14c.18-.98.65-1.74 1.41-2.5A4.65 4.65 0 0 0 18 8 6 6 0 0 0 6 8c0 1 .23 2.23 1.5 3.5A4.61 4.61 0 0 1 8.91 14"/></svg> ${signal.warning}</div>` : ''}

    <!-- Action buttons — always shown for ALL signal types -->
    <div class="signal-action-row">
      <button class="signal-log-btn ${isWait ? 'signal-log-btn-manual' : ''}"
        id="signalLogBtn-${ticker}"
        onclick="signalLogTrade('${ticker}','${isCall ? 'CALL' : isPut ? 'PUT' : 'CALL'}',${signal.strike || 0},'${signal.expiry || ''}',${signal.estimatedPremium || 3.5},this)">
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg> Log Trade
      </button>
      <div class="signal-secondary-btns">
        <button class="btn-paper-trade"
          onclick="paperTradeFromSignal('${ticker}','${isCall ? 'CALL' : isPut ? 'PUT' : 'CALL'}',${signal.strike || 0},'${signal.expiry || ''}',${signal.estimatedPremium || (signal.totalCost ? signal.totalCost/100 : 3.5)},${currentPrice})"
          title="Paper trade — no real money">
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="6" width="20" height="12" rx="2"/><path d="M6 12h4m-2-2v4"/><circle cx="16" cy="11" r="1" fill="currentColor"/><circle cx="18" cy="13" r="1" fill="currentColor"/></svg> Paper
        </button>
        <button class="btn-tastytrade"
          onclick="ttPlaceFromSignal('${ticker}','${isCall ? 'CALL' : isPut ? 'PUT' : 'CALL'}',${signal.strike || 0},'${signal.expiry || ''}',${signal.estimatedPremium || 3.5})"
          title="Place on Tastytrade">
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4.5 16.5c-1.5 1.26-2 5-2 5s3.74-.5 5-2c.71-.84.7-2.13-.09-2.91a2.18 2.18 0 0 0-2.91-.09z"/><path d="m12 15-3-3a22 22 0 0 1 2-3.95A12.88 12.88 0 0 1 22 2c0 2.72-.78 7.5-6 11a22.35 22.35 0 0 1-4 2z"/></svg> Tastytrade
        </button>
        <button class="btn-icon"
          onclick="prefillContractFromSignal('${ticker}','${isCall ? 'CALL' : isPut ? 'PUT' : 'CALL'}','${signal.strike || 0}','${signal.expiry || ''}','${signal.costEstimate || ''}')"
          title="Edit details before logging"
          style="font-size:14px;padding:6px 10px;border:1px solid var(--border);border-radius:var(--radius-sm);background:var(--bg3);cursor:pointer">
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>️
        </button>
      </div>
    </div>
  `;
}

// Signal Log Trade — one click, auto-adds, shows inline confirmation
async function signalLogTrade(ticker, type, strike, expiry, premium, btn) {
  if (!btn) return;
  const origText = btn.textContent;
  btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/></svg> Logging...';
  btn.disabled = true;
  btn.classList.add('signal-log-btn-loading');

  const today = new Date().toISOString().split('T')[0];
  const time = new Date().toTimeString().slice(0, 5);
  const totalCost = (parseFloat(premium) * 100).toFixed(0);

  try {
    await fetch('/api/daytrading/contracts/add', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        ticker: ticker.toUpperCase(),
        type,
        strike: parseFloat(strike),
        expiry,
        pricePaid: parseFloat(premium),
        contracts: 1,
        dateBought: today,
        timeBought: time,
        notes: 'Logged from AI signal'
      })
    });
    await loadData();
    renderDayTrading();

    // Show success state on button
    btn.innerHTML = `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Logged! ${ticker} ${type} $${strike}`;
    btn.classList.remove('signal-log-btn-loading');
    btn.classList.add('signal-log-btn-success');
    btn.disabled = false;

    // Show inline confirmation below button
    const row = btn.closest('.signal-action-row');
    if (row) {
      const existing = row.querySelector('.signal-log-confirm');
      if (existing) existing.remove();
      const confirm = document.createElement('div');
      confirm.className = 'signal-log-confirm';
      confirm.innerHTML = `
        <span class="signal-log-confirm-icon"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg></span>
        <span><b>${ticker} ${type} $${strike}</b> logged · Premium: $${parseFloat(premium).toFixed(2)}/share · Total cost: $${totalCost} · Expiry: ${expiry}</span>
        <span class="signal-log-confirm-hint">View in Contract Monitor below ↓</span>`;
      row.after(confirm);
    }
  } catch (e) {
    btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> Failed — try again';
    btn.classList.remove('signal-log-btn-loading');
    btn.disabled = false;
    setTimeout(() => { btn.textContent = origText; }, 3000);
  }
}
window.signalLogTrade = signalLogTrade;

// One-click add from signal — no modal, saves instantly
async function quickAddContract(ticker, type, strike, expiry, premium, btn) {
  if (btn) { btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/></svg> Adding...'; btn.disabled = true; }
  const today = new Date().toISOString().split('T')[0];
  const payload = {
    ticker: ticker.toUpperCase(),
    type,
    strike: parseFloat(strike),
    expiry,
    pricePaid: parseFloat(premium),
    contracts: 1,
    dateBought: today,
    notes: 'Added from AI signal'
  };
  try {
    await fetch('/api/daytrading/contracts/add', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    await loadData();
    renderDayTrading();
    if (btn) {
      btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Added!';
      btn.style.background = 'var(--green)';
      btn.style.color = '#fff';
      setTimeout(() => {
        btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg> Add to My Trades';
        btn.style.background = '';
        btn.style.color = '';
        btn.disabled = false;
      }, 3000);
    }
  } catch (e) {
    if (btn) { btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> Failed'; btn.disabled = false; }
  }
}
window.quickAddContract = quickAddContract;

function prefillContractFromSignal(ticker, type, strike, expiry, costEst) {
  // Parse values
  const strikeNum = String(strike).replace(/[^0-9.]/g, '');
  const costNum = String(costEst).replace(/[^0-9.]/g, '');
  const premium = costNum ? (parseFloat(costNum) / 100).toFixed(2) : '';

  // Fill essential fields
  document.getElementById('ctTicker').value = ticker;
  document.getElementById('ctType').value = type;
  document.getElementById('ctStrike').value = strikeNum;
  document.getElementById('ctExpiry').value = expiry;
  if (premium) document.getElementById('ctPricePaid').value = premium;
  document.getElementById('ctContracts').value = '1';

  // Fill hidden fields
  document.getElementById('ctDateBought').value = new Date().toISOString().split('T')[0];
  document.getElementById('ctTimeBought').value = new Date().toTimeString().slice(0,5);
  document.getElementById('ctNotes').value = 'From AI signal';

  // Show signal summary card
  const summary = document.getElementById('ctSignalSummary');
  if (summary) {
    summary.classList.remove('hidden');
    document.getElementById('ctSigTicker').textContent = ticker;
    document.getElementById('ctSigTicker').className = `ct-signal-item ct-sig-${type.toLowerCase()}`;
    document.getElementById('ctSigType').textContent = type;
    document.getElementById('ctSigType').className = `ct-signal-item ct-sig-${type.toLowerCase()}`;
    document.getElementById('ctSigStrike').textContent = `$${strikeNum}`;
    document.getElementById('ctSigExpiry').textContent = expiry;
    document.getElementById('ctSigPremium').textContent = premium ? `$${premium}/share` : '—';
  }

  // Update modal title
  document.getElementById('ctModalTitle').textContent = `Log Trade — ${ticker} ${type}`;

  updateContractCalc();
  openModal('modalAddContract');
}

// Reset modal to blank state (for manual entry)
function resetContractModal() {
  document.getElementById('ctModalTitle').textContent = 'Log Trade';
  document.getElementById('ctSignalSummary')?.classList.add('hidden');
  ['ctTicker','ctStrike','ctExpiry','ctPricePaid','ctNotes'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.value = '';
  });
  document.getElementById('ctContracts').value = '1';
  document.getElementById('ctType').value = 'CALL';
  document.getElementById('ctDateBought').value = new Date().toISOString().split('T')[0];
  document.getElementById('ctCalcBox').style.display = 'none';
}

// ─── Contract Monitor ─────────────────────────────────────────────
async function analyzeContract(contract) {
  if (!contract) return;
  const el = document.getElementById('dtMonitorOutput');
  el.innerHTML = `<div class="signal-loading"><div class="spinner"></div><span>Analyzing your ${contract.ticker} ${contract.type}...</span></div>`;

  try {
    const currentPrice = prices[contract.ticker]?.price || 0;
    const res = await fetch('/api/ai/monitor', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ contract, currentPrice })
    });

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop();
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const msg = JSON.parse(line.slice(6));
          if (msg.type === 'analysis') renderMonitorCard(el, msg.data, contract);
          if (msg.type === 'error') el.innerHTML = `<div class="empty-state red">Error: ${msg.text}</div>`;
        } catch {}
      }
    }
  } catch (e) {
    el.innerHTML = `<div class="empty-state red">Failed: ${e.message}</div>`;
  }
}

function renderMonitorCard(el, data, contract) {
  const recClass = data.recommendation.includes('SELL') ? 'sell' : data.recommendation === 'HOLD' ? 'hold' : data.recommendation === 'ROLL' ? 'roll' : 'partial';
  el.innerHTML = `<div class="monitor-card">
    <div class="monitor-header">
      <div>
        <div class="monitor-title">${contract.ticker} ${contract.type} $${fmt(contract.strike)}</div>
        <div class="monitor-subtitle">Expiry: ${contract.expiry} · ${data.daysLeft} days left · Break even: $${data.breakEven}</div>
      </div>
    </div>
    <div class="monitor-stats">
      <div class="monitor-stat">
        <div class="monitor-stat-label">You Paid</div>
        <div class="monitor-stat-value">$${fmt(contract.pricePaid * 100 * (contract.contracts || 1), 0)}</div>
      </div>
      <div class="monitor-stat">
        <div class="monitor-stat-label">P&L</div>
        <div class="monitor-stat-value ${Number(data.pnl) >= 0 ? 'green' : 'red'}">${fmtPnl(data.pnl)}</div>
      </div>
      <div class="monitor-stat">
        <div class="monitor-stat-label">Return</div>
        <div class="monitor-stat-value ${Number(data.pnlPct) >= 0 ? 'green' : 'red'}">${data.pnlPct}%</div>
      </div>
    </div>
    <div class="monitor-rec">
      <div class="monitor-rec-label">AI Recommendation</div>
      <div class="monitor-rec-value ${recClass}">${data.recommendation}</div>
      <div style="font-size:12px;color:var(--text2);margin-top:6px">${data.summary}</div>
    </div>
    <div class="monitor-reasons">
      ${(data.reasons || []).map(r => `<div class="monitor-reason"><span>•</span><span>${r}</span></div>`).join('')}
    </div>
    <div style="font-size:11px;color:var(--text3);margin-bottom:10px">
      Target exit: <strong>${data.targetExit}</strong> · Stop loss: <strong>${data.stopLoss}</strong>
    </div>
    <div class="monitor-actions">
      <button class="btn-secondary btn-sm" onclick="openCloseModal(event,'${contract.id}')"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Close Trade</button>
      <button class="btn-secondary btn-sm" onclick="analyzeContract(selectedContract)"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></svg> Re-analyze</button>
    </div>
  </div>`;
}

// ─── LONG TERM ────────────────────────────────────────────────────
function setupLongTerm() {
  // Re-rate all button — force refresh all ratings immediately
  document.getElementById('ltRerateBtn')?.addEventListener('click', async () => {
    const btn = document.getElementById('ltRerateBtn');
    btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/></svg>';
    btn.disabled = true;
    // Clear all existing ratings so everything gets re-rated
    Object.keys(ratings).forEach(k => delete ratings[k]);
    await rateAllPortfolio(true);
    btn.textContent = '⭐';
    btn.disabled = false;
  });
  // Add stock + watchlist — validation handled by ticker search (see initAllTickerSearches)
  document.getElementById('ltAddStock').addEventListener('click', () => openModal('modalLtAddStock'));
  document.getElementById('ltAddWatchlist')?.addEventListener('click', () => openModal('modalLtAddWatchlist'));

  document.getElementById('ltAnalyzeBtn')?.addEventListener('click', analyzeStock);
  document.getElementById('ltWeeklySummary')?.addEventListener('click', generateWeeklySummary);
  document.getElementById('ltAiSuggest')?.addEventListener('click', aiSuggestStocks);
}

function renderLongTerm() {
  if (!appData) return;
  renderLtPortfolio();
  renderLtWatchlist();
  updateLtAnalyzeSelect();
}

function renderLtPortfolio() {
  const el = document.getElementById('ltPortfolioTable');
  const list = appData.longterm.portfolio;
  if (!list.length) { el.innerHTML = '<div class="empty-state">No stocks yet. Click + Add Stock I Own.</div>'; document.getElementById('ltTotals').innerHTML = ''; return; }

  let totalInvested = 0, totalValue = 0;
  const rows = list.map(s => {
    const priceObj = prices[s.ticker];
    const p = priceObj?.price || 0;
    const hasPrice = p > 0;
    const value = p * s.shares;
    const invested = s.avgCost * s.shares;
    const pnl = value - invested;
    const pnlPct = invested ? ((pnl / invested) * 100).toFixed(1) : 0;
    totalInvested += invested;
    if (hasPrice) totalValue += value;
    const priceCell = hasPrice ? `$${fmt(p)}` : `<span style="color:var(--text3);font-size:10px">loading...</span>`;
    const valueCell = hasPrice ? `$${fmt(value, 2)}` : `<span style="color:var(--text3)">—</span>`;
    const pnlCell = hasPrice
      ? `<span class="${pnlClass(pnl)}">${fmtPnl(pnl)}<br><span style="font-size:10px">(${pnlPct}%)</span></span>`
      : `<span style="color:var(--text3)">—</span>`;
    return `<tr onclick="selectLtStock('${s.ticker}')">
      <td><strong>${s.ticker}</strong>${s.notes ? `<div style="font-size:10px;color:var(--text3)">${s.notes}</div>` : ''}</td>
      <td>${s.shares}</td>
      <td>$${fmt(s.avgCost)}</td>
      <td>${priceCell}</td>
      <td>${valueCell}</td>
      <td>${pnlCell}</td>
      <td>${getRatingBadge(s.ticker)}</td>
      <td style="display:flex;gap:4px">
        <button class="btn-icon" title="Edit" onclick="openEditStock(event,'${s.ticker}')"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>️</button>
        <button class="btn-secondary btn-sm" onclick="removeLtStock(event,'${s.ticker}')"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg></button>
      </td>
    </tr>`;
  });

  el.innerHTML = `<div class="portfolio-table-wrap"><table class="portfolio-table">
    <thead><tr>
      <th>Stock</th><th>Shares</th><th>Avg Cost</th><th>Price</th><th>Value</th><th>P&L</th><th>AI Rating</th><th></th>
    </tr></thead>
    <tbody>${rows.join('')}</tbody>
  </table></div>`;

  const totalPnl = totalValue - totalInvested;
  const totalPnlPct = totalInvested ? ((totalPnl / totalInvested) * 100).toFixed(2) : 0;
  document.getElementById('ltTotals').innerHTML = `
    <div class="total-item"><div class="total-label">Invested</div><div class="total-value">$${fmt(totalInvested, 2)}</div></div>
    <div class="total-item"><div class="total-label">Value Today</div><div class="total-value">$${fmt(totalValue, 2)}</div></div>
    <div class="total-item"><div class="total-label">Total P&L</div><div class="total-value ${pnlClass(totalPnl)}">${fmtPnl(totalPnl)} (${totalPnlPct}%)</div></div>
  `;
}

function selectLtStock(ticker) {
  document.getElementById('ltAnalyzeTicker').value = ticker;
  analyzeStock();
}

async function removeLtStock(e, ticker) {
  e.stopPropagation();
  if (!confirm(`Remove ${ticker} from portfolio?`)) return;
  await fetch('/api/longterm/portfolio/remove', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ticker }) });
  await loadData();
}

function renderLtWatchlist() {
  const list = appData.longterm.watchlist;
  const html = !list.length
    ? '<div class="empty-state">No stocks on watchlist. Click + Add Stock.</div>'
    : list.map(s => {
        const p = prices[s.ticker];
        const price = p ? `$${fmt(p.price)}` : '—';
        const chg = p ? `${p.changePct >= 0 ? '+' : ''}${fmt(p.changePct, 2)}%` : '—';
        const cls = p ? (p.changePct > 0 ? 'green' : p.changePct < 0 ? 'red' : 'flat') : 'flat';
        const tc = tickerColor(p);
        return `<div class="watchlist-row" onclick="selectLtWatchStock('${s.ticker}')">
          <span class="wl-ticker ${tc}">${s.ticker}</span>
          <span class="wl-price">${price}</span>
          <span class="wl-change ${cls}">${chg}</span>
          <button class="wl-remove" onclick="removeLtWatchStock(event,'${s.ticker}')"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg></button>
        </div>`;
      }).join('');
  // Render in Watchlist tab (primary)
  const el = document.getElementById('ltWatchlist');
  if (el) el.innerHTML = html;
  // Also populate watchlist analyze select
  const sel = document.getElementById('wlAnalyzeTicker');
  if (sel) {
    const cur = sel.value;
    sel.innerHTML = '<option value="">Select a stock...</option>' +
      list.map(s => `<option value="${s.ticker}" ${s.ticker === cur ? 'selected' : ''}>${s.ticker}${prices[s.ticker] ? ' — $' + fmt(prices[s.ticker].price) : ''}</option>`).join('');
  }
}

function selectLtWatchStock(ticker) {
  // Select in both analyze dropdowns
  const sel1 = document.getElementById('ltAnalyzeTicker');
  const sel2 = document.getElementById('wlAnalyzeTicker');
  if (sel1) sel1.value = ticker;
  if (sel2) sel2.value = ticker;
}

async function removeLtWatchStock(e, ticker) {
  e.stopPropagation();
  await fetch('/api/longterm/watchlist/remove', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ticker }) });
  await loadData();
}

function updateLtAnalyzeSelect() {
  const sel = document.getElementById('ltAnalyzeTicker');
  const current = sel.value;
  const allStocks = [
    ...appData.longterm.portfolio.map(s => ({ ticker: s.ticker, label: `${s.ticker} (Portfolio)` })),
    ...appData.longterm.watchlist.map(s => ({ ticker: s.ticker, label: `${s.ticker} (Watchlist)` }))
  ];
  sel.innerHTML = '<option value="">Select a stock...</option>' +
    allStocks.map(s => {
      const p = prices[s.ticker];
      const priceStr = p && p.price > 0 ? ` — $${fmt(p.price)}` : '';
      return `<option value="${s.ticker}" ${s.ticker === current ? 'selected' : ''}>${s.label}${priceStr}</option>`;
    }).join('');
}

async function analyzeStock() {
  const ticker = document.getElementById('ltAnalyzeTicker').value;
  if (!ticker) return alert('Please select a stock first.');

  const el = document.getElementById('ltAnalysisOutput');
  el.innerHTML = `<div class="signal-loading"><div class="spinner"></div><span>Analyzing ${ticker}...</span></div>`;

  try {
    const stock = appData.longterm.portfolio.find(s => s.ticker === ticker);

    // Always fetch a fresh live price directly — never use $0 cache
    el.innerHTML = `<div class="signal-loading"><div class="spinner"></div><span>Fetching live price for ${ticker}...</span></div>`;
    let currentPrice = 0;
    try {
      const pr = await fetch(`/api/price/${ticker}`);
      const pd = await pr.json();
      currentPrice = parseFloat(pd.price) || 0;
      if (currentPrice > 0) {
        prices[ticker] = pd;
        renderDayTrading();
        renderLongTerm();
      }
    } catch {}

    el.innerHTML = `<div class="signal-loading"><div class="spinner"></div><span>Analyzing ${ticker} @ $${fmt(currentPrice)}...</span></div>`;

    let news = [];
    try {
      const r = await fetch(`/api/news/${ticker}`);
      const newsData = await r.json();
      news = Array.isArray(newsData) ? newsData : [];
    } catch { news = []; }

    const res = await fetch('/api/ai/analyze-stock', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ticker, currentPrice, shares: stock?.shares, avgCost: stock?.avgCost, news })
    });

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop();
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const msg = JSON.parse(line.slice(6));
          if (msg.type === 'analysis') renderAnalysisCard(el, msg.data, ticker, currentPrice, stock);
          if (msg.type === 'error') el.innerHTML = `<div class="empty-state red">Error: ${msg.text}</div>`;
        } catch {}
      }
    }
  } catch (e) {
    el.innerHTML = `<div class="empty-state red">Failed: ${e.message}</div>`;
  }
}

function renderAnalysisCard(el, data, ticker, currentPrice, stock) {
  const recStr = data.recommendation || 'HOLD';
  const recClass = recStr.includes('BUY') ? 'buy' : recStr.includes('SELL') ? 'sell' : recStr === 'HOLD' ? 'hold' : 'wait';

  el.innerHTML = `<div class="analysis-card">
    <div class="analysis-header">
      <div>
        <div style="font-size:12px;color:var(--text3);margin-bottom:4px">${ticker} Analysis</div>
        <div class="analysis-rec ${recClass}">${recStr}</div>
      </div>
      <div style="text-align:right">
        <div style="font-size:11px;color:var(--text3)">12-Month Target</div>
        <div style="font-size:18px;font-weight:700;color:var(--green)">${data.priceTarget}</div>
        <div style="font-size:11px;color:var(--text2)">${data.upside} upside</div>
      </div>
    </div>
    <div class="analysis-prices">
      <div class="analysis-price-item">
        <div class="analysis-price-label">Current</div>
        <div class="analysis-price-value">$${fmt(currentPrice)}</div>
      </div>
      <div class="analysis-price-item">
        <div class="analysis-price-label">Fair Value</div>
        <div class="analysis-price-value green">${data.fairValue}</div>
      </div>
      ${stock ? `<div class="analysis-price-item">
        <div class="analysis-price-label">Your Cost</div>
        <div class="analysis-price-value">$${fmt(stock.avgCost)}</div>
      </div>` : `<div class="analysis-price-item">
        <div class="analysis-price-label">Buy More At</div>
        <div class="analysis-price-value blue">${data.buyMoreAt}</div>
      </div>`}
    </div>
    ${stock ? `<div class="analysis-prices" style="margin-top:-8px">
      <div class="analysis-price-item">
        <div class="analysis-price-label">Your P&L</div>
        <div class="analysis-price-value ${pnlClass(data.pnl)}">${fmtPnl(data.pnl)}</div>
      </div>
      <div class="analysis-price-item">
        <div class="analysis-price-label">Return</div>
        <div class="analysis-price-value ${pnlClass(data.pnlPct)}">${data.pnlPct}%</div>
      </div>
      <div class="analysis-price-item">
        <div class="analysis-price-label">Buy More At</div>
        <div class="analysis-price-value blue">${data.buyMoreAt}</div>
      </div>
    </div>` : ''}
    <div class="analysis-section">
      <h4><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Bull Case</h4>
      ${(data.bullCase || []).map(r => `<div class="analysis-bullet"><span><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg></span><span>${r}</span></div>`).join('')}
    </div>
    <div class="analysis-section">
      <h4><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>️ Bear Case / Risks</h4>
      ${(data.bearCase || []).map(r => `<div class="analysis-bullet"><span><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>️</span><span>${r}</span></div>`).join('')}
    </div>
    <div class="analysis-section">
      <h4><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg> Sell If</h4>
      ${(data.sellTriggers || []).map(r => `<div class="analysis-bullet"><span><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg></span><span>${r}</span></div>`).join('')}
    </div>
    <div class="analysis-summary">${data.summary}</div>
  </div>`;
}

async function generateWeeklySummary() {
  const el = document.getElementById('ltSummaryOutput');
  el.innerHTML = `<div class="signal-loading"><div class="spinner"></div><span>Generating weekly summary...</span></div>`;

  try {
    const priceMap = {};
    appData.longterm.portfolio.forEach(s => { priceMap[s.ticker] = prices[s.ticker]?.price || 0; });

    const res = await fetch('/api/ai/weekly-summary', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ portfolio: appData.longterm.portfolio, prices: priceMap })
    });

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop();
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const msg = JSON.parse(line.slice(6));
          if (msg.type === 'summary') renderWeeklySummary(el, msg.data);
          if (msg.type === 'error') el.innerHTML = `<div class="empty-state red">Error: ${msg.text}</div>`;
        } catch {}
      }
    }
  } catch (e) {
    el.innerHTML = `<div class="empty-state red">Failed: ${e.message}</div>`;
  }
}

function renderWeeklySummary(el, data) {
  const actionColor = a => a.includes('ADDING') ? 'green' : a.includes('TRIMMING') ? 'red' : a === 'WATCH' ? 'yellow' : 'var(--text2)';
  el.innerHTML = `<div class="summary-card">
    <div class="summary-headline">${data.headline}</div>
    <div style="font-size:12px;color:var(--text2);margin-bottom:12px">${data.weeklyChange}</div>
    ${(data.stocks || []).map(s => `
      <div class="summary-stock-row">
        <strong>${s.ticker}</strong>
        <span style="color:${actionColor(s.action)};font-size:11px;font-weight:600">${s.action}</span>
        <span style="color:var(--text2);font-size:11px;max-width:200px;text-align:right">${s.note}</span>
      </div>
    `).join('')}
    <div class="summary-advice">${data.advice}</div>
  </div>`;
}

async function aiSuggestStocks() {
  openModal('modalAiSuggest');
  const el = document.getElementById('aiSuggestContent');
  el.innerHTML = `<div class="loading-state"><div class="spinner"></div><span>Claude is scanning the market...</span></div>`;

  try {
    // Build complete list of ALL tickers already tracked — across every list
    const existing = [...new Set([
      ...(appData.longterm?.portfolio   || []).map(s => s.ticker),
      ...(appData.longterm?.watchlist   || []).map(s => s.ticker),
      ...(appData.daytrading?.watchlist || []).map(s => s.ticker),
      ...(appData.daytrading?.cryptoWatchlist || []).map(s => s.ticker),
      ...(appData.longterm?.cryptoWatchlist   || []).map(s => s.ticker),
      ...(appData.longterm?.cryptoPortfolio   || []).map(s => s.ticker),
    ])];

    const res = await fetch('/api/ai/suggest-stocks', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ existing })
    });

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop();
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const msg = JSON.parse(line.slice(6));
          if (msg.type === 'suggestions') {
            // Safety net — filter out anything already in any watchlist
            const allExisting = new Set([
              ...(appData.longterm?.portfolio   || []).map(s => s.ticker),
              ...(appData.longterm?.watchlist   || []).map(s => s.ticker),
              ...(appData.daytrading?.watchlist || []).map(s => s.ticker),
              ...(appData.daytrading?.cryptoWatchlist || []).map(s => s.ticker),
              ...(appData.longterm?.cryptoWatchlist   || []).map(s => s.ticker),
            ]);
            const fresh = msg.data.filter(s => !allExisting.has(s.ticker));
            if (fresh.length) renderSuggestions(el, fresh);
            else el.innerHTML = '<div class="empty-state">All suggestions are already in your watchlist — great coverage! Check back later for new ideas.</div>';
          }
          if (msg.type === 'error') el.innerHTML = `<div class="empty-state red">Error: ${msg.text}</div>`;
        } catch {}
      }
    }
  } catch (e) {
    el.innerHTML = `<div class="empty-state red">Failed: ${e.message}</div>`;
  }
}

function renderSuggestions(el, suggestions) {
  el.innerHTML = suggestions.map(s => `
    <div class="suggestion-card">
      <div class="suggestion-header">
        <div>
          <div class="suggestion-ticker">${s.ticker}</div>
          <div class="suggestion-name">${s.name}</div>
        </div>
        <button class="btn-primary btn-sm" onclick="addSuggestionToWatchlist('${s.ticker}')">+ Watch</button>
      </div>
      <div class="suggestion-reason">${s.reason}</div>
      <div class="suggestion-meta">
        <span class="suggestion-tag"> ${s.sector}</span>
        <span class="suggestion-tag"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>️ ${s.riskLevel} Risk</span>
        <span class="suggestion-tag">⏱ ${s.timeHorizon}</span>
      </div>
    </div>
  `).join('');
}

async function addSuggestionToWatchlist(ticker) {
  await fetch('/api/longterm/watchlist/add', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ticker }) });
  await loadData();
  trackedRefresh();
  closeModal('modalAiSuggest');
}

// ═══════════════════════════════════════════════════════════════════
// CRYPTO
// ═══════════════════════════════════════════════════════════════════

// refreshAllCryptoPrices removed — use trackedRefresh() instead

function fmtCrypto(n) {
  const v = parseFloat(n) || 0;
  if (v >= 1000) return '$' + v.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  if (v >= 1) return '$' + v.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  return '$' + v.toFixed(6); // small coins like DOGE, SHIB
}

// ─── Setup Crypto ─────────────────────────────────────────────────
function setupCrypto() {
  // DT crypto watchlist
  document.getElementById('dtAddCrypto').addEventListener('click', () => openModal('modalDtAddCrypto'));
  document.getElementById('dtCryptoRefresh').addEventListener('click', () => trackedRefresh());
  document.getElementById('dtAddCryptoConfirm').addEventListener('click', async () => {
    const ticker = document.getElementById('dtAddCryptoTicker').value.trim().toUpperCase();
    if (!ticker) return;
    await fetch('/api/daytrading/crypto/add', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ticker }) });
    document.getElementById('dtAddCryptoTicker').value = '';
    closeModal('modalDtAddCrypto');
    await loadData();
    trackedRefresh();
  });

  // DT crypto signal
  document.getElementById('dtGenerateCryptoSignal').addEventListener('click', generateCryptoSignal);

  // LT crypto portfolio
  document.getElementById('ltAddCryptoPortfolio').addEventListener('click', () => openModal('modalLtAddCryptoPortfolio'));
  document.getElementById('ltAddCryptoPortfolioConfirm').addEventListener('click', async () => {
    const ticker = document.getElementById('ltCryptoTicker').value.trim().toUpperCase();
    const coins = parseFloat(document.getElementById('ltCryptoCoins').value);
    const avgCost = parseFloat(document.getElementById('ltCryptoAvgCost').value);
    if (!ticker || !coins || !avgCost) return alert('Please fill in coin, amount, and buy price.');
    await fetch('/api/longterm/crypto/portfolio/add', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ticker, coins, avgCost, dateBought: document.getElementById('ltCryptoDateBought').value, exchange: document.getElementById('ltCryptoExchange').value, notes: document.getElementById('ltCryptoNotes').value })
    });
    closeModal('modalLtAddCryptoPortfolio');
    ['ltCryptoTicker','ltCryptoCoins','ltCryptoAvgCost','ltCryptoDateBought','ltCryptoExchange','ltCryptoNotes'].forEach(id => document.getElementById(id).value = '');
    await loadData();
    trackedRefresh();
  });

  // LT crypto watchlist (button may not exist if card was removed)
  document.getElementById('ltAddCryptoWatchlist')?.addEventListener('click', () => openModal('modalLtAddCryptoWatchlist'));
  document.getElementById('ltAddCryptoWatchlistConfirm')?.addEventListener('click', async () => {
    const ticker = document.getElementById('ltCryptoWatchlistTicker').value.trim().toUpperCase();
    if (!ticker) return;
    await fetch('/api/longterm/crypto/watchlist/add', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ticker }) });
    document.getElementById('ltCryptoWatchlistTicker').value = '';
    closeModal('modalLtAddCryptoWatchlist');
    await loadData();
    trackedRefresh();
  });

  // LT crypto analyze
  document.getElementById('ltCryptoAnalyzeBtn').addEventListener('click', analyzeCrypto);
}

// ─── Render DT Crypto Watchlist ───────────────────────────────────
function renderDtCrypto() {
  if (!appData) return;
  const list = appData.daytrading.cryptoWatchlist || [];
  const html = !list.length
    ? '<div class="empty-state" style="font-size:11px">No coins. Click + Crypto.</div>'
    : list.map(s => {
        const p = cryptoPrices[s.ticker];
        const price = p ? fmtCrypto(p.price) : '—';
        const chg = p ? `${p.changePct >= 0 ? '+' : ''}${fmt(p.changePct, 2)}%` : '—';
        const cls = p ? (p.changePct > 0 ? 'green' : p.changePct < 0 ? 'red' : 'flat') : 'flat';
        const tc = tickerColor(p);
        return `<div class="watchlist-row" onclick="selectDtCrypto('${s.ticker}')">
          <span class="wl-ticker wl-ticker-crypto ${tc}">₿ ${s.ticker}</span>
          <span class="wl-price">${price}</span>
          <span class="wl-change ${cls}">${chg}</span>
          <button class="wl-remove" onclick="removeDtCrypto(event,'${s.ticker}')"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg></button>
        </div>`;
      }).join('');

  // Render in Crypto tab
  const el = document.getElementById('dtCryptoWatchlist');
  if (el) el.innerHTML = html;
  // Also render in Day Trading unified watchlist
  const elInline = document.getElementById('dtCryptoWatchlistInline');
  if (elInline) elInline.innerHTML = html;

  // Update crypto signal select
  const sel = document.getElementById('dtCryptoSignalTicker');
  const cur = sel.value;
  sel.innerHTML = '<option value="">Select a coin...</option>' +
    list.map(s => {
      const p = cryptoPrices[s.ticker];
      return `<option value="${s.ticker}" ${s.ticker === cur ? 'selected' : ''}>₿ ${s.ticker}${p && p.price > 0 ? ' — ' + fmtCrypto(p.price) : ''}</option>`;
    }).join('');
}

function selectDtCrypto(ticker) {
  document.getElementById('dtCryptoSignalTicker').value = ticker;
}

async function removeDtCrypto(e, ticker) {
  e.stopPropagation();
  await fetch('/api/daytrading/crypto/remove', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ticker }) });
  await loadData();
  renderDtCrypto();
}

// ─── Render LT Crypto ─────────────────────────────────────────────
function renderLtCrypto() {
  if (!appData) return;
  renderLtCryptoPortfolio();
  renderLtCryptoWatchlist();
  updateLtCryptoAnalyzeSelect();
}

function renderLtCryptoPortfolio() {
  const el = document.getElementById('ltCryptoPortfolioTable');
  const list = appData.longterm.cryptoPortfolio || [];
  if (!list.length) { el.innerHTML = '<div class="empty-state">No crypto yet. Click + Add Coin I Own.</div>'; document.getElementById('ltCryptoTotals').innerHTML = ''; return; }

  let totalInvested = 0, totalValue = 0;
  const rows = list.map(s => {
    const priceObj = cryptoPrices[s.ticker];
    const p = priceObj?.price || 0;
    const hasPrice = p > 0;
    const value = p * s.coins;
    const invested = s.avgCost * s.coins;
    const pnl = value - invested;
    const pnlPct = invested ? ((pnl / invested) * 100).toFixed(1) : 0;
    totalInvested += invested;
    if (hasPrice) totalValue += value;
    const priceCell = hasPrice ? fmtCrypto(p) : `<span style="color:var(--text3);font-size:10px">loading...</span>`;
    const valueCell = hasPrice ? `$${fmt(value, 2)}` : `<span style="color:var(--text3)">—</span>`;
    const pnlCell = hasPrice
      ? `<span class="${pnlClass(pnl)}">${fmtPnl(pnl)}<br><span style="font-size:10px">(${pnlPct}%)</span></span>`
      : `<span style="color:var(--text3)">—</span>`;
    return `<tr onclick="selectLtCrypto('${s.ticker}')">
      <td><strong style="color:var(--yellow)">₿ ${s.ticker}</strong></td>
      <td>${s.coins}</td>
      <td>${fmtCrypto(s.avgCost)}</td>
      <td>${priceCell}</td>
      <td>${valueCell}</td>
      <td>${pnlCell}</td>
      <td>${getRatingBadge(s.ticker)}</td>
      <td style="display:flex;gap:4px">
        <button class="btn-icon" title="Edit" onclick="openEditCrypto(event,'${s.ticker}')"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>️</button>
        <button class="btn-secondary btn-sm" onclick="removeLtCrypto(event,'${s.ticker}')"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg></button>
      </td>
    </tr>`;
  });

  el.innerHTML = `<div class="portfolio-table-wrap"><table class="portfolio-table">
    <thead><tr><th>Coin</th><th>Quantity</th><th>Avg Cost</th><th>Price</th><th>Value</th><th>P&L</th><th>AI Rating</th><th></th></tr></thead>
    <tbody>${rows.join('')}</tbody>
  </table></div>`;

  const totalPnl = totalValue - totalInvested;
  const totalPnlPct = totalInvested ? ((totalPnl / totalInvested) * 100).toFixed(2) : 0;
  document.getElementById('ltCryptoTotals').innerHTML = `
    <div class="total-item"><div class="total-label">Invested</div><div class="total-value">$${fmt(totalInvested, 2)}</div></div>
    <div class="total-item"><div class="total-label">Value Today</div><div class="total-value">$${fmt(totalValue, 2)}</div></div>
    <div class="total-item"><div class="total-label">Total P&L</div><div class="total-value ${pnlClass(totalPnl)}">${fmtPnl(totalPnl)} (${totalPnlPct}%)</div></div>
  `;
}

function selectLtCrypto(ticker) {
  document.getElementById('ltCryptoAnalyzeTicker').value = ticker;
}

async function removeLtCrypto(e, ticker) {
  e.stopPropagation();
  if (!confirm(`Remove ${ticker} from crypto portfolio?`)) return;
  await fetch('/api/longterm/crypto/portfolio/remove', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ticker }) });
  await loadData();
  renderLtCrypto();
}

function renderLtCryptoWatchlist() {
  const list = appData.longterm.cryptoWatchlist || [];
  const html = !list.length
    ? '<div class="empty-state">No coins on watchlist.</div>'
    : list.map(s => {
        const p = cryptoPrices[s.ticker];
        const price = p ? fmtCrypto(p.price) : '—';
        const chg = p ? `${p.changePct >= 0 ? '+' : ''}${fmt(p.changePct, 2)}%` : '—';
        const cls = p ? (p.changePct > 0 ? 'green' : p.changePct < 0 ? 'red' : 'flat') : 'flat';
        const tc = tickerColor(p);
        return `<div class="watchlist-row" onclick="selectLtCrypto('${s.ticker}')">
          <span class="wl-ticker wl-ticker-crypto ${tc}">₿ ${s.ticker}</span>
          <span class="wl-price">${price}</span>
          <span class="wl-change ${cls}">${chg}</span>
          <button class="wl-remove" onclick="removeLtCryptoWatch(event,'${s.ticker}')"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg></button>
        </div>`;
      }).join('');
  // Render in Crypto tab (original location)
  const el = document.getElementById('ltCryptoWatchlist');
  if (el) el.innerHTML = html;
  // Render in Watchlist tab
  const elWl = document.getElementById('ltCryptoWatchlistEl');
  if (elWl) elWl.innerHTML = html;
}

async function removeLtCryptoWatch(e, ticker) {
  e.stopPropagation();
  await fetch('/api/longterm/crypto/watchlist/remove', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ticker }) });
  await loadData();
  renderLtCrypto();
}

function updateLtCryptoAnalyzeSelect() {
  const sel = document.getElementById('ltCryptoAnalyzeTicker');
  const cur = sel.value;
  const allCoins = [
    ...(appData.longterm.cryptoPortfolio || []).map(s => ({ ticker: s.ticker, label: `₿ ${s.ticker} (Portfolio)` })),
    ...(appData.longterm.cryptoWatchlist || []).map(s => ({ ticker: s.ticker, label: `₿ ${s.ticker} (Watchlist)` }))
  ];
  sel.innerHTML = '<option value="">Select a coin...</option>' +
    allCoins.map(s => {
      const p = cryptoPrices[s.ticker];
      return `<option value="${s.ticker}" ${s.ticker === cur ? 'selected' : ''}>${s.label}${p && p.price > 0 ? ' — ' + fmtCrypto(p.price) : ''}</option>`;
    }).join('');
}

// ─── Crypto AI Signal ─────────────────────────────────────────────
async function generateCryptoSignal() {
  const ticker = document.getElementById('dtCryptoSignalTicker').value;
  if (!ticker) return alert('Please select a coin first.');

  const outputEl = document.getElementById('dtCryptoSignalOutput');
  const loadingEl = document.getElementById('dtCryptoSignalLoading');
  const cardEl = document.getElementById('dtCryptoSignalCard');
  const btn = document.getElementById('dtGenerateCryptoSignal');

  outputEl.classList.remove('hidden');
  loadingEl.classList.remove('hidden');
  cardEl.classList.add('hidden');
  btn.disabled = true;

  try {
    // Fetch fresh price
    let priceData = cryptoPrices[ticker] || {};
    if (!priceData.price) {
      try {
        const pr = await fetch(`/api/crypto/price/${ticker}`);
        priceData = await pr.json();
        if (priceData.price > 0) cryptoPrices[ticker] = priceData;
      } catch {}
    }

    document.getElementById('dtCryptoSignalStatus').innerHTML = `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="10" rx="2"/><circle cx="12" cy="5" r="2"/><path d="M12 7v4"/></svg> Claude analyzing ${ticker}...`;

    const res = await fetch('/api/ai/crypto-signal', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ticker, price: priceData.price, change: priceData.change, changePct: priceData.changePct })
    });

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop();
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const msg = JSON.parse(line.slice(6));
          if (msg.type === 'status') document.getElementById('dtCryptoSignalStatus').textContent = msg.text;
          if (msg.type === 'signal') {
            loadingEl.classList.add('hidden');
            renderCryptoSignalCard(cardEl, msg.data, ticker, priceData.price);
            cardEl.classList.remove('hidden');
          }
          if (msg.type === 'error') {
            loadingEl.classList.add('hidden');
            cardEl.innerHTML = `<div class="empty-state red">Error: ${msg.text}</div>`;
            cardEl.classList.remove('hidden');
          }
        } catch {}
      }
    }
  } catch (e) {
    loadingEl.classList.add('hidden');
    document.getElementById('dtCryptoSignalCard').innerHTML = `<div class="empty-state red">Failed: ${e.message}</div>`;
    document.getElementById('dtCryptoSignalCard').classList.remove('hidden');
  } finally {
    btn.disabled = false;
  }
}

function renderCryptoSignalCard(el, signal, ticker, currentPrice) {
  const actionClass = signal.action === 'BUY' ? 'call' : signal.action === 'SELL' ? 'put' : 'wait';
  const confPct = signal.confidence || 0;
  el.innerHTML = `
    <div class="signal-card-header">
      <div>
        <div class="signal-action ${actionClass}">₿ ${signal.action} — ${ticker}</div>
        <div style="font-size:12px;color:var(--text2);margin-top:4px">Current: ${fmtCrypto(currentPrice)}</div>
      </div>
      <div class="signal-confidence">
        <div style="font-size:11px;color:var(--text3)">Confidence</div>
        <div style="font-size:18px;font-weight:800;color:${confPct >= 70 ? 'var(--green)' : confPct >= 55 ? 'var(--yellow)' : 'var(--red)'}">${confPct}%</div>
        <div class="confidence-bar"><div class="confidence-fill" style="width:${confPct}%"></div></div>
      </div>
    </div>
    <div class="signal-details">
      <div class="signal-detail-item"><div class="signal-detail-label">Entry</div><div class="signal-detail-value">${signal.targets?.entry || '—'}</div></div>
      <div class="signal-detail-item"><div class="signal-detail-label">Target</div><div class="signal-detail-value green">${signal.targets?.target || '—'}</div></div>
      <div class="signal-detail-item"><div class="signal-detail-label">Stop Loss</div><div class="signal-detail-value red">${signal.targets?.stopLoss || '—'}</div></div>
      <div class="signal-detail-item"><div class="signal-detail-label">Risk</div><div class="signal-detail-value ${signal.riskLevel === 'Low' ? 'green' : signal.riskLevel === 'High' ? 'red' : 'yellow'}">${signal.riskLevel}</div></div>
    </div>
    <div class="signal-reasons">
      <h4>Technical</h4>
      ${(signal.technical || []).map(r => `<div class="signal-reason-item"><span><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 7 13.5 15.5 8.5 10.5 2 17"/><polyline points="16 7 22 7 22 13"/></svg></span><span>${r}</span></div>`).join('')}
      <h4 style="margin-top:10px">Sentiment</h4>
      ${(signal.sentiment || []).map(r => `<div class="signal-reason-item"><span><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 22h16a2 2 0 0 0 2-2V4a2 2 0 0 0-2-2H8a2 2 0 0 0-2 2v16a2 2 0 0 0-2 2zm0 0a2 2 0 0 1-2-2v-9c0-1.1.9-2 2-2h2"/></svg></span><span>${r}</span></div>`).join('')}
    </div>
    ${signal.warning ? `<div class="signal-warning"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="9" y1="18" x2="15" y2="18"/><line x1="10" y1="22" x2="14" y2="22"/><path d="M15.09 14c.18-.98.65-1.74 1.41-2.5A4.65 4.65 0 0 0 18 8 6 6 0 0 0 6 8c0 1 .23 2.23 1.5 3.5A4.61 4.61 0 0 1 8.91 14"/></svg> ${signal.warning}</div>` : ''}
    <div style="padding:12px 16px;font-size:12px;color:var(--text2);border-top:1px solid var(--border)">${signal.summary}</div>
  `;
}

// ─── Crypto AI Analysis ───────────────────────────────────────────
async function analyzeCrypto() {
  const ticker = document.getElementById('ltCryptoAnalyzeTicker').value;
  if (!ticker) return alert('Please select a coin first.');

  const el = document.getElementById('ltCryptoAnalysisOutput');
  el.innerHTML = `<div class="signal-loading"><div class="spinner"></div><span>Fetching live price for ${ticker}...</span></div>`;

  try {
    // Always fetch fresh price
    let price = 0;
    try {
      const pr = await fetch(`/api/crypto/price/${ticker}`);
      const pd = await pr.json();
      price = parseFloat(pd.price) || 0;
      if (price > 0) { cryptoPrices[ticker] = pd; renderLtCrypto(); }
    } catch {}

    el.innerHTML = `<div class="signal-loading"><div class="spinner"></div><span>Analyzing ${ticker} @ ${fmtCrypto(price)}...</span></div>`;

    const owned = (appData.longterm.cryptoPortfolio || []).find(s => s.ticker === ticker);

    const res = await fetch('/api/ai/crypto-analyze', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ticker, price, coins: owned?.coins, avgCost: owned?.avgCost })
    });

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop();
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const msg = JSON.parse(line.slice(6));
          if (msg.type === 'analysis') renderCryptoAnalysisCard(el, msg.data, ticker, price, owned);
          if (msg.type === 'error') el.innerHTML = `<div class="empty-state red">Error: ${msg.text}</div>`;
        } catch {}
      }
    }
  } catch (e) {
    el.innerHTML = `<div class="empty-state red">Failed: ${e.message}</div>`;
  }
}

function renderCryptoAnalysisCard(el, data, ticker, price, owned) {
  const recStr = data.recommendation || 'HOLD';
  const recClass = recStr.includes('BUY') ? 'buy' : recStr.includes('SELL') ? 'sell' : 'hold';
  el.innerHTML = `<div class="analysis-card">
    <div class="analysis-header">
      <div>
        <div style="font-size:12px;color:var(--text3);margin-bottom:4px">₿ ${ticker} Analysis</div>
        <div class="analysis-rec ${recClass}">${recStr}</div>
      </div>
      <div style="text-align:right">
        <div style="font-size:11px;color:var(--text3)">12-Month Target</div>
        <div style="font-size:18px;font-weight:700;color:var(--green)">${data.priceTarget}</div>
        <div style="font-size:11px;color:var(--text2)">${data.upside} upside</div>
      </div>
    </div>
    <div class="analysis-prices">
      <div class="analysis-price-item"><div class="analysis-price-label">Current</div><div class="analysis-price-value">${fmtCrypto(price)}</div></div>
      ${owned ? `<div class="analysis-price-item"><div class="analysis-price-label">Your Cost</div><div class="analysis-price-value">${fmtCrypto(owned.avgCost)}</div></div>
      <div class="analysis-price-item"><div class="analysis-price-label">P&L</div><div class="analysis-price-value ${pnlClass(data.pnl)}">${fmtPnl(data.pnl)} (${data.pnlPct}%)</div></div>` :
      `<div class="analysis-price-item"><div class="analysis-price-label">Buy More At</div><div class="analysis-price-value blue">${data.buyMoreAt}</div></div>
      <div class="analysis-price-item"><div class="analysis-price-label">Stop Loss</div><div class="analysis-price-value red">—</div></div>`}
    </div>
    <div class="analysis-section"><h4><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Bull Case</h4>${(data.bullCase || []).map(r => `<div class="analysis-bullet"><span><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg></span><span>${r}</span></div>`).join('')}</div>
    <div class="analysis-section"><h4><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>️ Bear Case</h4>${(data.bearCase || []).map(r => `<div class="analysis-bullet"><span><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>️</span><span>${r}</span></div>`).join('')}</div>
    <div class="analysis-section"><h4><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg> Sell If</h4>${(data.sellTriggers || []).map(r => `<div class="analysis-bullet"><span><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg></span><span>${r}</span></div>`).join('')}</div>
    <div class="analysis-summary">${data.summary}</div>
  </div>`;
}

// ─── Data & Status ────────────────────────────────────────────────
async function trackedRefresh() {
  const start = Date.now();
  await Promise.all([
    refreshAllPricesTracked(),
    refreshAllCryptoPricesTracked()
  ]);
  const duration = Date.now() - start;
  dsState.lastFetchDuration = duration;
  dsState.fetchDurations.push(duration);
  if (dsState.fetchDurations.length > 20) dsState.fetchDurations.shift();
  dsState.lastRefreshed = new Date();
  dsState.totalRefreshes++;
  dsUpdateUI();
  // Run smart alert scan after every refresh
  if (typeof runSmartAlertScan === 'function') runSmartAlertScan();
}

async function refreshAllPricesTracked() {
  if (!appData) return;
  const allTickers = [
    ...appData.daytrading.watchlist.map(s => s.ticker),
    ...(appData.daytrading.contracts || []).map(c => c.ticker),
    ...appData.longterm.portfolio.map(s => s.ticker),
    ...appData.longterm.watchlist.map(s => s.ticker)
  ];
  const unique = [...new Set(allTickers)].filter(Boolean);
  if (!unique.length) return;
  try {
    const res = await fetch('/api/prices/multi', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ tickers: unique })
    });
    const data = await res.json();
    data.forEach(d => {
      if (d.price > 0) prices[d.ticker] = d;
      dsState.tickerStatus[d.ticker] = {
        type: 'stock',
        price: d.price,
        change: d.changePct,
        fetchedAt: new Date(),
        fetchMs: d.fetchMs || 0,
        ok: d.valid,
        confidence: d.confidence,
        divergencePct: d.divergencePct,
        sources: d.sources,
        marketState: d.marketState
      };
    });
  } catch (e) {
    console.error('Multi-source stock fetch failed:', e);
  }
  renderDayTrading();
  renderLongTerm();
}

async function refreshAllCryptoPricesTracked() {
  if (!appData) return;
  const allCoins = [
    ...(appData.daytrading.cryptoWatchlist || []).map(c => c.ticker),
    ...(appData.longterm.cryptoPortfolio || []).map(c => c.ticker),
    ...(appData.longterm.cryptoWatchlist || []).map(c => c.ticker)
  ];
  const unique = [...new Set(allCoins)].filter(Boolean);
  if (!unique.length) return;
  try {
    const res = await fetch('/api/crypto/prices/multi', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ coins: unique })
    });
    const data = await res.json();
    data.forEach(d => {
      if (d.price > 0) cryptoPrices[d.ticker] = d;
      dsState.tickerStatus[d.ticker] = {
        type: 'crypto',
        price: d.price,
        change: d.changePct,
        fetchedAt: new Date(),
        fetchMs: d.fetchMs || 0,
        ok: d.valid,
        confidence: d.confidence,
        divergencePct: d.divergencePct,
        sources: d.sources,
        marketState: d.marketState
      };
    });
  } catch (e) {
    console.error('Multi-source crypto fetch failed:', e);
  }
  renderDtCrypto();
  renderLtCryptoPortfolio();
  renderLtCryptoWatchlist();
}

function dsStartCountdown() {
  dsState.countdownSec = 60;
  if (dsState.countdownInterval) clearInterval(dsState.countdownInterval);
  dsState.countdownInterval = setInterval(() => {
    dsState.countdownSec = Math.max(0, dsState.countdownSec - 1);
    dsUpdateCountdown();
  }, 1000);
}

function dsResetCountdown() {
  dsState.countdownSec = 60;
  dsUpdateCountdown();
}

function dsUpdateCountdown() {
  const num = document.getElementById('dsCountdownNum');
  const ring = document.getElementById('dsCountdownRing');
  const next = document.getElementById('dsNextRefresh');
  if (!num) return;
  const sec = dsState.countdownSec;
  num.textContent = sec;
  const circumference = 213.6;
  const offset = circumference * (1 - sec / 60);
  if (ring) ring.style.strokeDashoffset = offset;
  if (next) next.textContent = sec > 0 ? `${sec}s` : 'Now...';
  // Update status bar countdown
  const sbCountdown = document.getElementById('sbCountdown');
  if (sbCountdown) sbCountdown.textContent = sec > 0 ? `Next refresh in ${sec}s` : 'Refreshing...';
}

function dsUpdateUI() {
  const lastEl = document.getElementById('dsLastRefreshed');
  const totalEl = document.getElementById('dsTotalRefreshes');
  const avgEl = document.getElementById('dsAvgFetchTime');
  const lastDurEl = document.getElementById('dsLastFetchDuration');
  const countEl = document.getElementById('dsTickerCount');

  if (lastEl && dsState.lastRefreshed) {
    lastEl.textContent = dsState.lastRefreshed.toLocaleTimeString();
  }
  if (totalEl) totalEl.textContent = dsState.totalRefreshes;
  if (avgEl && dsState.fetchDurations.length) {
    const avg = Math.round(dsState.fetchDurations.reduce((a, b) => a + b, 0) / dsState.fetchDurations.length);
    avgEl.textContent = `${avg}ms`;
  }
  if (lastDurEl && dsState.lastFetchDuration !== null) {
    lastDurEl.textContent = `${dsState.lastFetchDuration}ms`;
  }
  const tickerCount = Object.keys(dsState.tickerStatus).length;
  if (countEl) countEl.textContent = `${tickerCount} ticker${tickerCount !== 1 ? 's' : ''} tracked`;

  dsRenderTickerTable();
  updateStatusBar();
}

function dsRenderTickerTable() {
  const tbody = document.getElementById('dsTickerTableBody');
  if (!tbody) return;
  const entries = Object.entries(dsState.tickerStatus);
  if (!entries.length) {
    tbody.innerHTML = `<tr><td colspan="9" class="loading-row">No tickers fetched yet.</td></tr>`;
    return;
  }
  tbody.innerHTML = entries.map(([ticker, s]) => {
    const changeStr = s.change >= 0
      ? `<span class="green">+${s.change.toFixed(2)}%</span>`
      : `<span class="red">${s.change.toFixed(2)}%</span>`;
    const priceStr = s.type === 'crypto'
      ? (s.price >= 1 ? `$${s.price.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : `$${s.price.toFixed(6)}`)
      : `$${s.price.toFixed(2)}`;
    const fetchedStr = s.fetchedAt ? s.fetchedAt.toLocaleTimeString() : '—';
    const typeBadge = s.type === 'crypto'
      ? `<span class="ds-type-badge ds-type-crypto">Crypto</span>`
      : `<span class="ds-type-badge ds-type-stock">Stock</span>`;

    // Confidence badge
    const confMap = {
      high:   { cls: 'ds-conf-high',   label: '<svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg> High',   title: 'Both sources agree (< 0.5% diff)' },
      medium: { cls: 'ds-conf-medium', label: '~ Medium',  title: 'Sources differ slightly (0.5–2%)' },
      low:    { cls: 'ds-conf-low',    label: '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg> Low',     title: 'Sources diverge significantly (> 2%)' },
      single: { cls: 'ds-conf-single', label: '◎ Single',  title: 'Only one source returned data' }
    };
    const conf = confMap[s.confidence] || confMap.single;
    const confBadge = `<span class="ds-conf-badge ${conf.cls}" title="${conf.title}">${conf.label}</span>`;

    // Source prices
    const fh = s.sources?.finnhub;
    const yh = s.sources?.yahoo;
    const fhStr = fh?.ok ? `$${s.type === 'crypto' && fh.price < 1 ? fh.price.toFixed(6) : fh.price.toFixed(2)}` : '—';
    const yhStr = yh?.ok ? `$${s.type === 'crypto' && yh.price < 1 ? yh.price.toFixed(6) : yh.price.toFixed(2)}` : '—';
    const sourcesStr = `
      <div class="ds-sources-cell">
        <span class="ds-src ${fh?.ok ? 'ds-src-ok' : 'ds-src-err'}">FH: ${fhStr}</span>
        <span class="ds-src ${yh?.ok ? 'ds-src-ok' : 'ds-src-err'}">YH: ${yhStr}</span>
      </div>`;

    const divStr = s.divergencePct !== null && s.divergencePct !== undefined
      ? `<span style="color:${s.divergencePct < 0.5 ? 'var(--green)' : s.divergencePct < 2 ? 'var(--yellow)' : 'var(--red)'}">${s.divergencePct.toFixed(2)}%</span>`
      : `<span style="color:var(--text3)">—</span>`;

    return `<tr>
      <td style="font-weight:700">${ticker}</td>
      <td>${typeBadge}</td>
      <td style="font-weight:600">${s.price > 0 ? priceStr : '—'}</td>
      <td>${s.price > 0 ? changeStr : '—'}</td>
      <td>${sourcesStr}</td>
      <td>${divStr}</td>
      <td>${confBadge}</td>
      <td style="color:var(--text3);font-size:11px">${fetchedStr}</td>
      <td style="color:var(--text2)">${s.fetchMs}ms</td>
    </tr>`;
  }).join('');

  // Update summary stats
  dsUpdateSourceSummary();
}

function dsUpdateSourceSummary() {
  const entries = Object.values(dsState.tickerStatus);
  const total = entries.length;
  if (!total) return;
  const highConf = entries.filter(s => s.confidence === 'high').length;
  const medConf  = entries.filter(s => s.confidence === 'medium').length;
  const lowConf  = entries.filter(s => s.confidence === 'low').length;
  const single   = entries.filter(s => s.confidence === 'single').length;
  const fhOk     = entries.filter(s => s.sources?.finnhub?.ok).length;
  const yhOk     = entries.filter(s => s.sources?.yahoo?.ok).length;

  const el = document.getElementById('dsSourceSummary');
  if (!el) return;
  el.innerHTML = `
    <div class="ds-summary-item"><span class="ds-conf-badge ds-conf-high"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg> High</span><span>${highConf} tickers</span></div>
    <div class="ds-summary-item"><span class="ds-conf-badge ds-conf-medium">~ Medium</span><span>${medConf} tickers</span></div>
    <div class="ds-summary-item"><span class="ds-conf-badge ds-conf-low"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg> Low</span><span>${lowConf} tickers</span></div>
    <div class="ds-summary-item"><span class="ds-conf-badge ds-conf-single">◎ Single</span><span>${single} tickers</span></div>
    <div class="ds-summary-divider"></div>
    <div class="ds-summary-item"><span style="color:var(--text2)">Finnhub</span><span class="${fhOk === total ? 'green' : 'yellow'}">${fhOk}/${total} OK</span></div>
    <div class="ds-summary-item"><span style="color:var(--text2)">Yahoo Finance</span><span class="${yhOk === total ? 'green' : 'yellow'}">${yhOk}/${total} OK</span></div>
  `;
}

function setupDataStatus() {
  // Legacy Data tab force refresh (still works if tab somehow shown)
  const forceBtn = document.getElementById('dsForceRefresh');
  if (forceBtn) {
    forceBtn.addEventListener('click', async () => {
      forceBtn.textContent = '↻ Refreshing...';
      forceBtn.disabled = true;
      await trackedRefresh();
      dsResetCountdown();
      forceBtn.textContent = '↻ Refresh Now';
      forceBtn.disabled = false;
    });
  }
  // Status bar refresh button
  const sbBtn = document.getElementById('sbRefreshBtn');
  if (sbBtn) {
    sbBtn.addEventListener('click', async () => {
      sbBtn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/></svg>';
      sbBtn.disabled = true;
      await trackedRefresh();
      dsResetCountdown();
      sbBtn.textContent = '↻';
      sbBtn.disabled = false;
    });
  }
}

// Update status bar — called from dsUpdateUI
function updateStatusBar() {
  const lastRefresh = dsState.lastRefreshed;
  const avgMs = dsState.fetchDurations.length
    ? Math.round(dsState.fetchDurations.reduce((a, b) => a + b, 0) / dsState.fetchDurations.length)
    : 0;

  const sbLastRefresh = document.getElementById('sbLastRefresh');
  const sbRefreshCount = document.getElementById('sbRefreshCount');
  const sbAvgFetch = document.getElementById('sbAvgFetch');
  const sbConfidence = document.getElementById('sbConfidence');

  if (sbLastRefresh) sbLastRefresh.textContent = lastRefresh ? `⏱ ${lastRefresh.toLocaleTimeString()}` : '⏱ —';
  if (sbRefreshCount) sbRefreshCount.textContent = `${dsState.totalRefreshes} refresh${dsState.totalRefreshes !== 1 ? 'es' : ''}`;
  if (sbAvgFetch) sbAvgFetch.textContent = avgMs ? `${avgMs}ms avg` : '—ms avg';

  // Confidence from ticker statuses
  if (sbConfidence) {
    const statuses = Object.values(dsState.tickerStatus || {});
    const highs = statuses.filter(s => s.confidence === 'high').length;
    const total = statuses.length;
    if (total === 0) {
      sbConfidence.textContent = '● Prices: —';
      sbConfidence.className = 'status-item';
    } else if (highs === total) {
      sbConfidence.innerHTML = `<svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg> All ${total} prices high confidence`;
      sbConfidence.className = 'status-item status-conf-high';
    } else {
      sbConfidence.textContent = `~ ${highs}/${total} high confidence`;
      sbConfidence.className = 'status-item status-conf-med';
    }
  }
}

// ─── AI Settings ──────────────────────────────────────────────────
const PROVIDERS = [
  { id: 'ollama',   name: 'Ollama (Local)',    sub: 'Free · Private · No API key needed',       icon: 'OL', cls: 'ais-icon-ollama'   },
  { id: 'opencode', name: 'OpenCode',          sub: 'Any model via OpenCode app (Claude, GPT…)', icon: 'OC', cls: 'ais-icon-opencode' },
  { id: 'groq',     name: 'Groq (Fast)',       sub: 'Free · Llama 3.1 70B · ~500 tok/sec',      icon: 'GQ', cls: 'ais-icon-groq'    },
  { id: 'openai',   name: 'OpenAI',           sub: 'GPT-4o, GPT-4 Turbo',                      icon: 'AI', cls: 'ais-icon-openai'   },
  { id: 'anthropic',name: 'Anthropic (Claude)',sub: 'claude-sonnet, opus',                      icon: 'AN', cls: 'ais-icon-anthropic' }
];

let aisCurrentProvider = 'ollama';

async function setupAISettings() {
  aisBindEvents();          // bind first so no duplicate listeners
  await aisLoadSettings();  // then load saved values
  aisRenderProviderList();
  aisSwitchProvider(aisCurrentProvider); // show correct panel
  aisRefreshStatus();
}

async function aisLoadSettings() {
  try {
    const res = await fetch('/api/ai/settings');
    const s = await res.json();
    aisCurrentProvider = s.provider || 'ollama';
    if (s.openai?.apiKey)     { const el = document.getElementById('aisOpenaiKey');     if (el) el.value = s.openai.apiKey; }
    if (s.openai?.model)      { const el = document.getElementById('aisOpenaiModel');    if (el) el.value = s.openai.model; }
    if (s.anthropic?.apiKey)  { const el = document.getElementById('aisAnthropicKey');  if (el) el.value = s.anthropic.apiKey; }
    if (s.anthropic?.model)   { const el = document.getElementById('aisAnthropicModel');if (el) el.value = s.anthropic.model; }
    if (s.finnhubKey)       { const el = document.getElementById('aisFinnhubKey');  if (el) el.value = s.finnhubKey; }
    if (s.groq?.apiKey)    { const el = document.getElementById('aisGroqKey');     if (el) el.value = s.groq.apiKey; }
    if (s.groq?.model)     { const el = document.getElementById('aisGroqModel');   if (el) el.value = s.groq.model; }
    if (s.ollama?.url)        { const el = document.getElementById('aisOllamaUrl');      if (el) el.value = s.ollama.url; }
    if (s.ollama?.model) {
      const sel = document.getElementById('aisOllamaModel');
      if (sel) {
        // If model isn't in the list yet, add it
        const exists = Array.from(sel.options).some(o => o.value === s.ollama.model);
        if (!exists) sel.innerHTML += `<option value="${s.ollama.model}">${s.ollama.model}</option>`;
        sel.value = s.ollama.model;
      }
    }
  } catch (e) { console.error('aisLoadSettings:', e); }
}

function aisRenderProviderList() {
  const list = document.getElementById('aisProviderList');
  if (!list) return;
  list.innerHTML = PROVIDERS.map(p => `
    <div class="ais-provider-item ${p.id === aisCurrentProvider ? 'active' : ''}" data-provider="${p.id}">
      <span class="ais-provider-icon ${p.cls}">${p.icon}</span>
      <div class="ais-provider-info">
        <div class="ais-provider-name">${p.name}</div>
        <div class="ais-provider-sub">${p.sub}</div>
      </div>
      ${p.id === aisCurrentProvider ? '<span class="ais-active-badge">● Active</span>' : ''}
    </div>
  `).join('');

  list.querySelectorAll('.ais-provider-item').forEach(el => {
    el.addEventListener('click', () => aisSwitchProvider(el.dataset.provider));
  });
}

function aisSwitchProvider(providerId) {
  aisCurrentProvider = providerId;
  aisRenderProviderList();
  // Show correct panel
  document.querySelectorAll('.ais-panel').forEach(p => p.classList.add('hidden'));
  const panel = document.getElementById(`aisPanel-${providerId}`);
  if (panel) panel.classList.remove('hidden');
}

function aisBindEvents() {
  // Save buttons — ALL providers
  PROVIDERS.forEach(p => {
    const btn = document.getElementById(`aisSave-${p.id}`);
    if (btn) btn.addEventListener('click', () => aisSave(p.id));
  });

  // Test buttons
  PROVIDERS.forEach(p => {
    const btn = document.getElementById(`aisBtnTest-${p.id}`);
    if (btn) btn.addEventListener('click', () => aisTest(p.id));
  });

  // Eye toggle buttons
  document.querySelectorAll('.ais-eye-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const input = document.getElementById(btn.dataset.target);
      if (!input) return;
      input.type = input.type === 'password' ? 'text' : 'password';
       btn.innerHTML = input.type === 'password' ? '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>' : '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19m-6.72-1.07a3 3 0 1 1-4.24-4.24"/><line x1="1" y1="1" x2="23" y2="23"/></svg>';
    });
  });

  // Refresh status
  const refreshBtn = document.getElementById('aisRefreshStatus');
  if (refreshBtn) refreshBtn.addEventListener('click', aisRefreshStatus);

  // Ollama: Detect Models button
  document.getElementById('aisOllamaRefreshModels')?.addEventListener('click', async () => {
    const btn = document.getElementById('aisOllamaRefreshModels');
    btn.textContent = '...'; btn.disabled = true;
    try {
      const url = document.getElementById('aisOllamaUrl')?.value.trim() || 'http://localhost:11434';
      const data = await fetch('/api/ollama/models').then(r => r.json());
      const sel = document.getElementById('aisOllamaModel');
      if (!sel) return;
      if (data.running && data.models?.length > 0) {
        const current = sel.value;
        sel.innerHTML = data.models.map(m => `<option value="${m.name}">${m.name}</option>`).join('');
        if (current && Array.from(sel.options).some(o => o.value === current)) sel.value = current;
        else sel.selectedIndex = 0;
        showToast(`Found ${data.models.length} Ollama model(s)`, 'success');
      } else if (data.running) {
        showToast('Ollama running but no models — run: ollama pull llama3.2', 'warning');
      } else {
        showToast('Ollama not running — start with: ollama serve', 'warning');
      }
    } catch (e) {
      showToast('Could not reach Ollama: ' + e.message, 'error');
    } finally {
      btn.textContent = '↻ Detect'; btn.disabled = false;
    }
  });

  // Finnhub key save
  document.getElementById('aisFinnhubSave')?.addEventListener('click', async () => {
    const key = document.getElementById('aisFinnhubKey')?.value.trim();
    try {
      const current = await fetch('/api/ai/settings').then(r => r.json());
      await fetch('/api/ai/settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...current, finnhubKey: key })
      });
      showToast(key ? 'Finnhub key saved' : 'Finnhub key cleared — using shared key', 'success');
    } catch (e) { showToast('Failed to save: ' + e.message, 'error'); }
  });

  // Show active panel on load
  aisSwitchProvider(aisCurrentProvider);
}

async function aisSave(provider) {
  const btn = document.getElementById(`aisSave-${provider}`);
  const origText = btn?.textContent || 'Save & Set Active';
  if (btn) { btn.textContent = 'Saving...'; btn.disabled = true; }

  const body = { provider };

  // Collect form values for each provider
  switch (provider) {
    case 'openai':
      body.openai = {
        apiKey: document.getElementById('aisOpenaiKey')?.value.trim() || '',
        model:  document.getElementById('aisOpenaiModel')?.value || 'gpt-4o'
      };
      if (!body.openai.apiKey) {
        showToast('Please enter your OpenAI API key', 'warning');
        if (btn) { btn.textContent = origText; btn.disabled = false; }
        return;
      }
      break;
    case 'anthropic':
      body.anthropic = {
        apiKey: document.getElementById('aisAnthropicKey')?.value.trim() || '',
        model:  document.getElementById('aisAnthropicModel')?.value || 'claude-sonnet-4-5'
      };
      if (!body.anthropic.apiKey) {
        showToast('Please enter your Anthropic API key', 'warning');
        if (btn) { btn.textContent = origText; btn.disabled = false; }
        return;
      }
      break;
    case 'groq':
      body.groq = {
        apiKey: document.getElementById('aisGroqKey')?.value.trim() || '',
        model:  document.getElementById('aisGroqModel')?.value || 'llama-3.1-70b-versatile'
      };
      if (!body.groq.apiKey) {
        showToast('Please enter your Groq API key (free at console.groq.com)', 'warning');
        if (btn) { btn.textContent = origText; btn.disabled = false; }
        return;
      }
      break;
    case 'ollama':
      body.ollama = {
        url:   document.getElementById('aisOllamaUrl')?.value.trim()   || 'http://localhost:11434',
        model: document.getElementById('aisOllamaModel')?.value?.trim() || 'llama3.2'
      };
      break;
    case 'opencode':
      body.opencode = { agent: 'general' };
      break;
  }

  try {
    const saveRes = await fetch('/api/ai/settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    if (!saveRes.ok) throw new Error('Save failed');

    // Update UI immediately — don't wait for async status refresh
    aisCurrentProvider = provider;
    aisRenderProviderList();

    // Update Connection Status active badge immediately using local state
    const statusRows = document.querySelectorAll('.ais-status-row');
    statusRows.forEach(row => {
      const nameEl = row.querySelector('.ais-status-name');
      const badge  = row.querySelector('.ais-active-badge');
      const provMap = { 'Ollama (Local)':'ollama', 'OpenCode':'opencode', 'Groq (Fast)':'groq', 'OpenAI':'openai', 'Anthropic':'anthropic' };
      const rowProvider = provMap[nameEl?.textContent?.trim()] || '';
      if (rowProvider === provider) {
        row.classList.add('ais-status-active');
        if (!badge) {
          const b = document.createElement('span');
          b.className = 'ais-active-badge';
          b.textContent = 'Active';
          row.querySelector('.ais-status-left')?.appendChild(b);
        }
      } else {
        row.classList.remove('ais-status-active');
        badge?.remove();
      }
    });

    if (btn) { btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Active!'; setTimeout(() => { btn.textContent = origText; btn.disabled = false; }, 2000); }

    // Also refresh full status in background
    aisRefreshStatus();
  } catch (e) {
    if (btn) { btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> Error'; setTimeout(() => { btn.textContent = origText; btn.disabled = false; }, 2000); }
  }
}

async function aisTest(provider) {
  const btn = document.getElementById(`aisBtnTest-${provider}`);
  const resultEl = document.getElementById(`aisTestResult-${provider}`);
  if (!resultEl) return;

  if (btn) { btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/></svg> Testing...'; btn.disabled = true; }
  resultEl.className = 'ais-test-result ais-test-loading';
  resultEl.textContent = 'Sending test prompt...';

  try {
    // Build the test payload with current form values
    // This way Test Connection works even before clicking Save
    const payload = { provider };

    if (provider === 'openai') {
      payload.openai = {
        apiKey: document.getElementById('aisOpenaiKey')?.value.trim() || '',
        model:  document.getElementById('aisOpenaiModel')?.value || 'gpt-4o'
      };
    } else if (provider === 'anthropic') {
      payload.anthropic = {
        apiKey: document.getElementById('aisAnthropicKey')?.value.trim() || '',
        model:  document.getElementById('aisAnthropicModel')?.value || 'claude-sonnet-4-5'
      };
    } else if (provider === 'groq') {
      payload.groq = {
        apiKey: document.getElementById('aisGroqKey')?.value.trim() || '',
        model:  document.getElementById('aisGroqModel')?.value || 'llama-3.1-70b-versatile'
      };
    } else if (provider === 'ollama') {
      payload.ollama = {
        url:   document.getElementById('aisOllamaUrl')?.value.trim()   || 'http://localhost:11434',
        model: document.getElementById('aisOllamaModel')?.value.trim() || 'llama3.2'
      };
    }
    // opencode has no credentials to pass — it auto-detects

    // 30s timeout — Ollama/OpenCode can be slow on first call
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 30000);
    const res = await fetch('/api/ai/settings/test', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      signal: controller.signal
    });
    clearTimeout(timeout);
    const data = await res.json();
    if (data.ok) {
      resultEl.className = 'ais-test-result ais-test-ok';
      resultEl.innerHTML = `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Connected! Response: ${data.response?.slice(0, 100) || 'OK'}`;
    } else {
      resultEl.className = 'ais-test-result ais-test-err';
      resultEl.innerHTML = `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> Failed: ${data.error}`;
    }
  } catch (e) {
    resultEl.className = 'ais-test-result ais-test-err';
    const msg = e.name === 'AbortError' ? 'Timed out after 30s — provider not responding' : e.message;
    resultEl.innerHTML = `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> ${msg}`;
  } finally {
    if (btn) { btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg> Test Connection'; btn.disabled = false; }
  }
}

async function aisRefreshStatus() {
  const body = document.getElementById('aisStatusBody');
  if (!body) return;
  body.innerHTML = '<div class="loading-row">Checking...</div>';
  try {
    const res = await fetch('/api/ai/status');
    const s = await res.json();
    const rows = [
      { id: 'ollama',    name: 'Ollama (Local)',
        ok: s.ollama?.running && (s.ollama?.availableModels?.length > 0),
        detail: !s.ollama?.running
          ? 'Not running — start with: ollama serve'
          : s.ollama?.availableModels?.length === 0
            ? '⚠ Running but no models — run: ollama pull llama3.2'
            : `${s.ollama.model} · ${s.ollama.availableModels?.length} model(s) ready`
      },
      { id: 'opencode',  name: 'OpenCode',       ok: s.opencode?.running,     detail: s.opencode?.running ? `Running on port ${s.opencode.port} · agent: ${s.opencode.agent}` : 'Not detected — open the OpenCode app' },
      { id: 'openai',    name: 'OpenAI',         ok: s.openai?.configured,    detail: s.openai?.configured ? `Model: ${s.openai.model}` : 'No API key set' },
      { id: 'groq',      name: 'Groq (Fast)',    ok: s.groq?.configured,      detail: s.groq?.configured ? `Model: ${s.groq.model}` : 'No API key — free at console.groq.com' },
      { id: 'anthropic', name: 'Anthropic',      ok: s.anthropic?.configured, detail: s.anthropic?.configured ? `Model: ${s.anthropic.model}` : 'No API key set' }
    ];
    // Update OpenCode status badge in panel
    const ocStatusEl = document.getElementById('aisOcStatus');
    if (ocStatusEl) {
      ocStatusEl.textContent = s.opencode?.running
        ? `✓ OpenCode detected on port ${s.opencode.port} — ready to use`
        : '✗ OpenCode not detected — open the OpenCode app first';
      ocStatusEl.style.background = s.opencode?.running ? 'var(--green-bg)' : 'var(--red-bg)';
      ocStatusEl.style.color = s.opencode?.running ? 'var(--green)' : 'var(--red)';
    }
    body.innerHTML = rows.map(r => `
      <div class="ais-status-row ${r.id === s.provider ? 'ais-status-active' : ''}">
        <div class="ais-status-left">
          <span class="ais-status-dot ${r.ok ? 'ok' : 'off'}"></span>
          <span class="ais-status-name">${r.name}</span>
          ${r.id === s.provider ? '<span class="ais-active-badge">Active</span>' : ''}
        </div>
        <span class="ais-status-detail">${r.detail}</span>
      </div>
    `).join('');
  } catch {
    body.innerHTML = '<div class="empty-state">Could not fetch status.</div>';
  }
}

// AI Settings — modal triggered from titlebar ️ AI button
let _aisModalInit = false;
document.addEventListener('DOMContentLoaded', () => {
  const btn = document.getElementById('aiSettingsBtn');
  const overlay = document.getElementById('aiSettingsOverlay');
  const closeBtn = document.getElementById('aiSettingsClose');

  btn?.addEventListener('click', async () => {
    overlay?.classList.remove('hidden');
    if (!_aisModalInit) {
      await setupAISettings();
      _aisModalInit = true;
    } else {
      // Always refresh status on open
      aisRefreshStatus();
    }
  });

  closeBtn?.addEventListener('click', () => overlay?.classList.add('hidden'));
  overlay?.addEventListener('click', e => { if (e.target === overlay) overlay.classList.add('hidden'); });
});

// ─── News & Intelligence ──────────────────────────────────────────
let newsActiveTicker = null;
let newsArticles = [];
let newsSentiments = {};

function setupNewsTab() {
  document.getElementById('newsLoadBtn').addEventListener('click', () => {
    const val = document.getElementById('newsTickerInput').value.trim().toUpperCase();
    if (val) newsLoadTicker(val);
  });
  document.getElementById('newsTickerInput').addEventListener('keydown', e => {
    if (e.key === 'Enter') {
      const val = e.target.value.trim().toUpperCase();
      if (val) newsLoadTicker(val);
    }
  });
  document.getElementById('newsMarketRefresh').addEventListener('click', newsLoadMarket);
  document.getElementById('newsRefreshFeed').addEventListener('click', () => {
    if (newsActiveTicker) newsLoadTicker(newsActiveTicker);
  });
  document.getElementById('newsAnalyzeSentiment').addEventListener('click', newsRunSentiment);
  document.getElementById('newsIntelBtn').addEventListener('click', newsRunIntelligence);

  newsPopulateQuickTickers();
  newsLoadMarket();
}

function newsPopulateQuickTickers() {
  const wrap = document.getElementById('newsQuickTickers');
  if (!wrap || !appData) return;
  const stocks = [
    ...appData.daytrading.watchlist.map(s => ({ t: s.ticker, type: 'stock' })),
    ...appData.longterm.portfolio.map(s => ({ t: s.ticker, type: 'stock' })),
    ...(appData.daytrading.cryptoWatchlist || []).map(c => ({ t: c.ticker, type: 'crypto' }))
  ];
  const unique = [...new Map(stocks.map(s => [s.t, s])).values()].slice(0, 12);
  wrap.innerHTML = unique.map(s => `
    <button class="news-quick-btn ${s.type === 'crypto' ? 'crypto' : ''}" onclick="newsLoadTicker('${s.t}')">${s.t}</button>
  `).join('');
}

async function newsLoadTicker(ticker) {
  newsActiveTicker = ticker;
  newsSentiments = {};
  newsArticles = [];

  // Update UI
  document.getElementById('newsActiveTicker').textContent = ticker;
  document.getElementById('newsActiveTicker').classList.remove('hidden');
  document.getElementById('newsRefreshFeed').classList.remove('hidden');
  document.getElementById('newsAnalyzeSentiment').classList.remove('hidden');
  document.getElementById('newsIntelBtn').disabled = false;
  document.getElementById('newsTickerInput').value = ticker;

  const feed = document.getElementById('newsFeed');
  feed.innerHTML = `<div class="news-loading"><div class="spinner"></div><span>Fetching news from Finnhub + Yahoo Finance...</span></div>`;

  try {
    const res = await fetch(`/api/news/merged/${ticker}`);
    const data = await res.json();
    newsArticles = data.articles || [];
    newsRenderFeed(newsArticles, {});
  } catch (e) {
    feed.innerHTML = `<div class="empty-state red">Failed to load news: ${e.message}</div>`;
  }
}

function newsRenderFeed(articles, sentiments) {
  const feed = document.getElementById('newsFeed');
  if (!articles.length) {
    feed.innerHTML = `<div class="empty-state">No news found for ${newsActiveTicker}.</div>`;
    return;
  }
  feed.innerHTML = articles.map((a, i) => {
    const s = sentiments[i];
    const timeAgo = newsTimeAgo(a.datetime);
    const sourceBadge = (a.sources || [a.source]).map(src =>
      `<span class="news-src-badge news-src-${src.toLowerCase()}">${src}</span>`
    ).join('');
    const sentBadge = s
      ? `<span class="news-sent-badge news-sent-${s.sentiment}">${s.sentiment === 'bullish' ? '●' : s.sentiment === 'bearish' ? '●' : '●'} ${s.sentiment}</span>`
      : '';
    const scoreBadge = s ? `<span class="news-score ${s.score >= 0 ? 'green' : 'red'}">${s.score >= 0 ? '+' : ''}${s.score}</span>` : '';
    const catBadge = s ? `<span class="news-cat-badge">${s.category}</span>` : '';
    const insight = s ? `<div class="news-insight"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="9" y1="18" x2="15" y2="18"/><line x1="10" y1="22" x2="14" y2="22"/><path d="M15.09 14c.18-.98.65-1.74 1.41-2.5A4.65 4.65 0 0 0 18 8 6 6 0 0 0 6 8c0 1 .23 2.23 1.5 3.5A4.61 4.61 0 0 1 8.91 14"/></svg> ${s.insight}</div>` : '';
    return `
      <div class="news-article" data-index="${i}">
        ${a.image ? `<img class="news-thumb" src="${a.image}" onerror="this.style.display='none'" loading="lazy"/>` : ''}
        <div class="news-article-body">
          <div class="news-article-meta">
            ${sourceBadge}
            ${sentBadge}
            ${catBadge}
            ${scoreBadge}
            <span class="news-time">${timeAgo}</span>
            <span class="news-publisher">${a.publisher}</span>
          </div>
          <div class="news-headline" onclick="window.open('${a.url}','_blank')">${a.headline}</div>
          ${a.summary ? `<div class="news-summary">${a.summary.slice(0, 200)}${a.summary.length > 200 ? '...' : ''}</div>` : ''}
          ${insight}
        </div>
      </div>`;
  }).join('');
}

async function newsRunSentiment() {
  if (!newsArticles.length) return;
  const btn = document.getElementById('newsAnalyzeSentiment');
  btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/></svg> Analyzing...';
  btn.disabled = true;

  const feed = document.getElementById('newsFeed');
  const statusDiv = document.createElement('div');
  statusDiv.className = 'news-sentiment-status';
  statusDiv.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="10" rx="2"/><circle cx="12" cy="5" r="2"/><path d="M12 7v4"/></svg> AI analyzing sentiment for all articles...';
  feed.prepend(statusDiv);

  try {
    const es = new EventSource(`/api/news/sentiment`);
    const res = await fetch('/api/news/sentiment', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ articles: newsArticles, ticker: newsActiveTicker })
    });
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop();
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const msg = JSON.parse(line.slice(6));
          if (msg.type === 'sentiments') {
            msg.data.forEach(s => { newsSentiments[s.index - 1] = s; });
            statusDiv.remove();
            newsRenderFeed(newsArticles, newsSentiments);
          }
        } catch {}
      }
    }
  } catch (e) {
    statusDiv.innerHTML = `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> Sentiment analysis failed: ${e.message}`;
  } finally {
    btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="10" rx="2"/><circle cx="12" cy="5" r="2"/><path d="M12 7v4"/></svg> Analyze Sentiment';
    btn.disabled = false;
  }
}

async function newsRunIntelligence() {
  if (!newsActiveTicker) return;
  const btn = document.getElementById('newsIntelBtn');
  const output = document.getElementById('newsIntelOutput');
  btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/></svg> Analyzing...';
  btn.disabled = true;
  output.innerHTML = `<div class="news-loading"><div class="spinner"></div><span><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9.5 2A2.5 2.5 0 0 1 12 4.5v15a2.5 2.5 0 0 1-4.96-.46 2.5 2.5 0 0 1-1.07-4.73A3 3 0 0 1 4.46 9.5a2.5 2.5 0 0 1 5.04-5z"/><path d="M14.5 2A2.5 2.5 0 0 0 12 4.5v15a2.5 2.5 0 0 0 4.96-.46 2.5 2.5 0 0 0 1.07-4.73A3 3 0 0 0 19.54 9.5a2.5 2.5 0 0 0-5.04-5z"/></svg> AI analyzing ${newsActiveTicker} strategy, earnings & analyst sentiment...</span></div>`;

  const price = prices[newsActiveTicker]?.price || cryptoPrices[newsActiveTicker]?.price || 0;

  try {
    const res = await fetch(`/api/intelligence/${newsActiveTicker}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ price, articles: newsArticles })
    });
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop();
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const msg = JSON.parse(line.slice(6));
          if (msg.type === 'intelligence') newsRenderIntelligence(msg.data);
          if (msg.type === 'error') output.innerHTML = `<div class="empty-state red"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> ${msg.text}</div>`;
        } catch {}
      }
    }
  } catch (e) {
    output.innerHTML = `<div class="empty-state red"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> ${e.message}</div>`;
  } finally {
    btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg> Run Intelligence';
    btn.disabled = false;
  }
}

function newsRenderIntelligence(d) {
  const output = document.getElementById('newsIntelOutput');
  const verdictClass = d.verdictColor || 'yellow';
  output.innerHTML = `
    <div class="intel-card">
      <div class="intel-header">
        <div>
          <div class="intel-ticker">${newsActiveTicker}</div>
          <div class="intel-summary">${d.summary}</div>
        </div>
        <div class="intel-verdict intel-verdict-${verdictClass}">${d.verdict}</div>
      </div>
      <div class="intel-grid">
        <div class="intel-section">
          <div class="intel-section-title"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 5H7a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V7a2 2 0 0 0-2-2h-2"/><rect x="9" y="3" width="6" height="4" rx="1"/></svg> Strategy Moves</div>
          ${(d.strategy || []).map(s => `<div class="intel-bullet"><span>→</span><span>${s}</span></div>`).join('')}
        </div>
        <div class="intel-section">
          <div class="intel-section-title"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg> Earnings & Guidance</div>
          <div class="intel-text">${d.earnings}</div>
        </div>
        <div class="intel-section">
          <div class="intel-section-title"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg> Analyst Sentiment</div>
          <div class="intel-text">${d.analystSentiment}</div>
        </div>
        <div class="intel-section">
          <div class="intel-section-title"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/></svg> Macro Impact</div>
          <div class="intel-text">${d.macroImpact}</div>
        </div>
        <div class="intel-section">
          <div class="intel-section-title"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Opportunities</div>
          ${(d.opportunities || []).map(s => `<div class="intel-bullet green"><span><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg></span><span>${s}</span></div>`).join('')}
        </div>
        <div class="intel-section">
          <div class="intel-section-title"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>️ Risks</div>
          ${(d.risks || []).map(s => `<div class="intel-bullet red"><span>!</span><span>${s}</span></div>`).join('')}
        </div>
      </div>
      <div class="intel-watch">
        <span class="intel-watch-label"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg> Watch For:</span>
        <span>${d.watchFor}</span>
      </div>
    </div>`;
}

async function newsLoadMarket() {
  const feed = document.getElementById('newsMarketFeed');
  feed.innerHTML = `<div class="loading-row">Loading...</div>`;
  try {
    const res = await fetch('/api/news/market');
    const data = await res.json();
    const articles = data.articles || [];
    if (!articles.length) { feed.innerHTML = `<div class="empty-state">No market news available.</div>`; return; }
    feed.innerHTML = articles.map(a => {
      const timeAgo = newsTimeAgo(a.datetime);
      const srcBadge = (a.sources || [a.source]).map(s =>
        `<span class="news-src-badge news-src-${s.toLowerCase()}">${s}</span>`
      ).join('');
      return `
        <div class="news-market-item" onclick="window.open('${a.url}','_blank')">
          <div class="news-market-meta">${srcBadge}<span class="news-time">${timeAgo}</span></div>
          <div class="news-market-headline">${a.headline}</div>
          <div class="news-market-pub">${a.publisher}</div>
        </div>`;
    }).join('');
  } catch {
    feed.innerHTML = `<div class="empty-state">Could not load market news.</div>`;
  }
}

function newsTimeAgo(ts) {
  if (!ts) return '';
  const diff = Math.floor((Date.now() / 1000) - ts);
  if (diff < 60) return `${diff}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

// Init news tab on first click
document.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('.tab-btn').forEach(btn => {
    if (btn.dataset.tab === 'news') {
      btn.addEventListener('click', () => {
        if (!btn._newsInit) {
          setupNewsTab();
          btn._newsInit = true;
        }
      });
    }
  });
});

// ─── TOOLS TAB ────────────────────────────────────────────────────
let toolsActiveTicker = null;
let toolsActiveRange = '1mo';
let chartData = null;

function setupToolsTab() {
  document.getElementById('toolsLoadBtn').addEventListener('click', () => {
    const t = document.getElementById('toolsTicker').value.trim().toUpperCase();
    if (t) toolsLoadTicker(t);
  });
  document.getElementById('toolsTicker').addEventListener('keydown', e => {
    if (e.key === 'Enter') { const t = e.target.value.trim().toUpperCase(); if (t) toolsLoadTicker(t); }
  });
  document.getElementById('optionsLoadBtn').addEventListener('click', toolsLoadOptions);
  document.getElementById('alertAddBtn').addEventListener('click', () => {
    document.getElementById('alertForm').classList.remove('hidden');
    if (toolsActiveTicker) document.getElementById('alertTicker').value = toolsActiveTicker;
  });
  document.getElementById('alertCancelBtn').addEventListener('click', () => document.getElementById('alertForm').classList.add('hidden'));
  document.getElementById('alertSaveBtn').addEventListener('click', toolsSaveAlert);
  document.querySelectorAll('.chart-range-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.chart-range-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      toolsActiveRange = btn.dataset.range;
      if (toolsActiveTicker) toolsLoadChart(toolsActiveTicker, toolsActiveRange);
    });
  });
  toolsPopulateQuick();
  toolsLoadAlerts();
  toolsLoadEarningsCalendar();
}

function toolsPopulateQuick() {
  const wrap = document.getElementById('toolsQuickTickers');
  if (!wrap || !appData) return;
  const tickers = [...new Set([
    ...appData.daytrading.watchlist.map(s => s.ticker),
    ...appData.longterm.portfolio.map(s => s.ticker)
  ])].slice(0, 8);
  wrap.innerHTML = tickers.map(t => `<button class="news-quick-btn" onclick="toolsLoadTicker('${t}')">${t}</button>`).join('');
}

async function toolsLoadTicker(ticker) {
  toolsActiveTicker = ticker;
  document.getElementById('toolsTicker').value = ticker;
  document.getElementById('optionsLoadBtn').disabled = false;
  document.getElementById('insiderTickerLabel').textContent = ticker;
  await Promise.all([
    toolsLoadChart(ticker, toolsActiveRange),
    toolsLoadInsider(ticker)
  ]);
}

// ── Chart ──────────────────────────────────────────────────────────
async function toolsLoadChart(ticker, range) {
  const container = document.getElementById('chartContainer');
  container.innerHTML = `<div class="chart-loading"><div class="spinner"></div><span>Loading ${ticker} chart...</span></div>`;
  try {
    const res = await fetch(`/api/chart/${ticker}?range=${range}&interval=${range === '1mo' ? '1d' : range === '3mo' ? '1d' : '1wk'}`);
    const data = await res.json();
    if (!data.candles?.length) { container.innerHTML = `<div class="empty-state">No chart data available.</div>`; return; }
    chartData = data;
    renderChart(container, data);
  } catch (e) {
    container.innerHTML = `<div class="empty-state red">Chart failed: ${e.message}</div>`;
  }
}

function renderChart(container, data) {
  const candles = data.candles;
  const closes = candles.map(c => c.c);
  const highs = candles.map(c => c.h);
  const lows = candles.map(c => c.l);
  const minP = Math.min(...lows) * 0.998;
  const maxP = Math.max(...highs) * 1.002;
  const range = maxP - minP;
  const W = 800, H = 220, PAD = { t: 16, r: 16, b: 32, l: 56 };
  const cW = W - PAD.l - PAD.r;
  const cH = H - PAD.t - PAD.b;
  const n = candles.length;
  const barW = Math.max(2, Math.floor(cW / n) - 1);
  const xStep = cW / n;
  const toX = i => PAD.l + i * xStep + xStep / 2;
  const toY = p => PAD.t + cH - ((p - minP) / range) * cH;

  // Line path
  const linePts = candles.map((c, i) => `${toX(i)},${toY(c.c)}`).join(' ');
  const firstClose = closes[0];
  const lastClose = closes[closes.length - 1];
  const isUp = lastClose >= firstClose;
  const lineColor = isUp ? 'var(--green)' : 'var(--red)';

  // Area fill
  const areaPath = `M${toX(0)},${toY(candles[0].c)} ` +
    candles.map((c, i) => `L${toX(i)},${toY(c.c)}`).join(' ') +
    ` L${toX(n - 1)},${PAD.t + cH} L${toX(0)},${PAD.t + cH} Z`;

  // Y axis labels
  const yLabels = [0, 0.25, 0.5, 0.75, 1].map(f => {
    const p = minP + f * range;
    const y = toY(p);
    return `<text x="${PAD.l - 4}" y="${y + 4}" text-anchor="end" fill="var(--text3)" font-size="10">${p >= 1000 ? p.toFixed(0) : p.toFixed(2)}</text>
            <line x1="${PAD.l}" y1="${y}" x2="${W - PAD.r}" y2="${y}" stroke="var(--border)" stroke-width="0.5"/>`;
  }).join('');

  // X axis labels (show ~5)
  const xStep2 = Math.floor(n / 5);
  const xLabels = candles.filter((_, i) => i % xStep2 === 0 || i === n - 1).map((c, _, arr) => {
    const i = candles.indexOf(c);
    return `<text x="${toX(i)}" y="${H - 6}" text-anchor="middle" fill="var(--text3)" font-size="10">${c.date}</text>`;
  }).join('');

  // Candle bars
  const candleBars = candles.map((c, i) => {
    const x = toX(i);
    const isGreen = c.c >= c.o;
    const color = isGreen ? 'var(--green)' : 'var(--red)';
    const bodyTop = toY(Math.max(c.o, c.c));
    const bodyH = Math.max(1, Math.abs(toY(c.o) - toY(c.c)));
    return `<line x1="${x}" y1="${toY(c.h)}" x2="${x}" y2="${toY(c.l)}" stroke="${color}" stroke-width="1"/>
            <rect x="${x - barW / 2}" y="${bodyTop}" width="${barW}" height="${bodyH}" fill="${color}" opacity="0.85"/>`;
  }).join('');

  const pctChange = ((lastClose - firstClose) / firstClose * 100).toFixed(2);
  const meta = data.meta || {};

  container.innerHTML = `
    <div class="chart-header">
      <div class="chart-ticker-info">
        <span class="chart-ticker">${data.ticker}</span>
        <span class="chart-price">$${lastClose.toFixed(2)}</span>
        <span class="chart-change ${isUp ? 'green' : 'red'}">${isUp ? '+' : ''}${pctChange}% (${data.range})</span>
      </div>
      <div class="chart-meta">
        <span>52W H: <b>$${meta.fiftyTwoWeekHigh?.toFixed(2) || '—'}</b></span>
        <span>52W L: <b>$${meta.fiftyTwoWeekLow?.toFixed(2) || '—'}</b></span>
      </div>
    </div>
    <div class="chart-wrap">
      <svg viewBox="0 0 ${W} ${H}" width="100%" style="display:block">
        <defs>
          <linearGradient id="areaGrad" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stop-color="${lineColor}" stop-opacity="0.2"/>
            <stop offset="100%" stop-color="${lineColor}" stop-opacity="0"/>
          </linearGradient>
        </defs>
        ${yLabels}
        ${xLabels}
        <path d="${areaPath}" fill="url(#areaGrad)"/>
        ${candleBars}
      </svg>
    </div>`;
}

// ── Insider ────────────────────────────────────────────────────────
async function toolsLoadInsider(ticker) {
  const container = document.getElementById('insiderContainer');
  container.innerHTML = `<div class="loading-row">Loading insider data...</div>`;
  try {
    const res = await fetch(`/api/insider/${ticker}`);
    const data = await res.json();
    if (!data.length) { container.innerHTML = `<div class="empty-state">No recent insider transactions.</div>`; return; }
    container.innerHTML = `<div class="insider-table-wrap"><table class="insider-table">
      <thead><tr><th>Name</th><th>Action</th><th>Shares</th><th>Price</th><th>Value</th><th>Date</th></tr></thead>
      <tbody>${data.map(t => {
        const isBuy = t.change > 0;
        const val = Math.abs(t.change * (t.transactionPrice || 0));
        return `<tr>
          <td style="font-weight:600">${t.name || '—'}</td>
          <td><span class="insider-badge ${isBuy ? 'buy' : 'sell'}">${isBuy ? '▲ BUY' : '▼ SELL'}</span></td>
          <td>${Math.abs(t.change).toLocaleString()}</td>
          <td>$${(t.transactionPrice || 0).toFixed(2)}</td>
          <td class="${isBuy ? 'green' : 'red'}">$${val >= 1000000 ? (val / 1000000).toFixed(1) + 'M' : val >= 1000 ? (val / 1000).toFixed(0) + 'K' : val.toFixed(0)}</td>
          <td style="color:var(--text3)">${t.transactionDate || '—'}</td>
        </tr>`;
      }).join('')}</tbody>
    </table></div>`;
  } catch (e) {
    container.innerHTML = `<div class="empty-state red">Failed: ${e.message}</div>`;
  }
}

// ── Options ────────────────────────────────────────────────────────
async function toolsLoadOptions() {
  if (!toolsActiveTicker) return;
  const container = document.getElementById('optionsContainer');
  const btn = document.getElementById('optionsLoadBtn');
  btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/></svg> Generating...';
  btn.disabled = true;
  container.innerHTML = `<div class="news-loading"><div class="spinner"></div><span>AI generating estimated options chain for ${toolsActiveTicker}...</span></div>`;
  const price = prices[toolsActiveTicker]?.price || cryptoPrices[toolsActiveTicker]?.price || 0;
  try {
    const res = await fetch('/api/options/estimated', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ticker: toolsActiveTicker, price })
    });
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n'); buffer = lines.pop();
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const msg = JSON.parse(line.slice(6));
          if (msg.type === 'chain') renderOptionsChain(container, msg.data);
          if (msg.type === 'error') container.innerHTML = `<div class="empty-state red"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> ${msg.text}</div>`;
        } catch {}
      }
    }
  } catch (e) {
    container.innerHTML = `<div class="empty-state red"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> ${e.message}</div>`;
  } finally {
    btn.textContent = 'Generate Chain';
    btn.disabled = false;
  }
}

function renderOptionsChain(container, chain) {
  let activeExp = 0;
  const renderExp = (idx) => {
    const exp = chain.expirations[idx];
    if (!exp) return '';
    const renderRow = (o, type) => {
      const itmClass = o.itm ? 'options-itm' : '';
      return `<tr class="${itmClass}">
        <td>${o.itm ? '●' : ''}</td>
        <td>$${o.bid?.toFixed(2)}</td><td>$${o.ask?.toFixed(2)}</td>
        <td>$${o.last?.toFixed(2)}</td><td>${o.iv?.toFixed(0)}%</td>
        <td>${o.delta?.toFixed(2)}</td><td>${o.theta?.toFixed(3)}</td>
        <td>${(o.volume || 0).toLocaleString()}</td><td>${(o.oi || 0).toLocaleString()}</td>
        <td style="font-weight:700">$${o.strike}</td>
      </tr>`;
    };
    return `
      <div class="options-disclaimer"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>️ ${chain.disclaimer}</div>
      <div class="options-meta">Stock: $${chain.stockPrice?.toFixed(2)} · IV: ${chain.iv}% · Expiry: ${exp.label} (${exp.daysToExpiry}d)</div>
      <div class="options-cols">
        <div class="options-side">
          <div class="options-side-title green">CALLS</div>
          <table class="options-table"><thead><tr><th>ITM</th><th>Bid</th><th>Ask</th><th>Last</th><th>IV</th><th>Δ</th><th>Θ</th><th>Vol</th><th>OI</th><th>Strike</th></tr></thead>
          <tbody>${exp.calls.map(o => renderRow(o, 'call')).join('')}</tbody></table>
        </div>
        <div class="options-side">
          <div class="options-side-title red">PUTS</div>
          <table class="options-table"><thead><tr><th>ITM</th><th>Bid</th><th>Ask</th><th>Last</th><th>IV</th><th>Δ</th><th>Θ</th><th>Vol</th><th>OI</th><th>Strike</th></tr></thead>
          <tbody>${exp.puts.map(o => renderRow(o, 'put')).join('')}</tbody></table>
        </div>
      </div>`;
  };
  container.innerHTML = `
    <div class="options-exp-tabs">${chain.expirations.map((e, i) =>
      `<button class="options-exp-btn ${i === 0 ? 'active' : ''}" onclick="optionsSwitchExp(this,${i})">${e.label}</button>`
    ).join('')}</div>
    <div id="optionsExpContent">${renderExp(0)}</div>`;
}

window.optionsSwitchExp = (btn, idx) => {
  document.querySelectorAll('.options-exp-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  const chain = window._lastChain;
  if (!chain) return;
  // re-render — store chain globally
};

// ── Alerts ─────────────────────────────────────────────────────────
async function toolsLoadAlerts() {
  const list = document.getElementById('alertsList');
  try {
    const res = await fetch('/api/alerts');
    const alerts = await res.json();
    if (!alerts.length) { list.innerHTML = `<div class="empty-state">No alerts set.</div>`; return; }
    list.innerHTML = `<div class="alerts-list">${alerts.map(a => `
      <div class="alert-row ${a.triggered ? 'triggered' : ''}">
        <div class="alert-info">
          <span class="alert-ticker">${a.ticker}</span>
          <span class="alert-cond">${a.condition === 'above' ? '↑ Above' : '↓ Below'} $${a.price.toFixed(2)}</span>
          ${a.note ? `<span class="alert-note">${a.note}</span>` : ''}
        </div>
        <div class="alert-right">
          ${a.triggered ? '<span class="alert-triggered-badge"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg> Triggered</span>' : ''}
          <button class="wl-remove" onclick="toolsDeleteAlert('${a.id}')"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg></button>
        </div>
      </div>`).join('')}</div>`;
  } catch { list.innerHTML = `<div class="empty-state">Could not load alerts.</div>`; }
}

async function toolsSaveAlert() {
  const ticker = document.getElementById('alertTicker').value.trim().toUpperCase();
  const condition = document.getElementById('alertCondition').value;
  const price = parseFloat(document.getElementById('alertPrice').value);
  const note = document.getElementById('alertNote').value.trim();
  if (!ticker || !price) return;
  await fetch('/api/alerts', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ticker, condition, price, note }) });
  document.getElementById('alertForm').classList.add('hidden');
  document.getElementById('alertTicker').value = '';
  document.getElementById('alertPrice').value = '';
  document.getElementById('alertNote').value = '';
  toolsLoadAlerts();
}

async function toolsDeleteAlert(id) {
  await fetch(`/api/alerts/${id}`, { method: 'DELETE' });
  toolsLoadAlerts();
}

// ── Earnings Calendar ──────────────────────────────────────────────
async function toolsLoadEarningsCalendar() {
  const container = document.getElementById('earningsCalendar');
  try {
    const res = await fetch('/api/earnings/calendar');
    const data = await res.json();
    const myTickers = new Set([
      ...appData?.daytrading?.watchlist?.map(s => s.ticker) || [],
      ...appData?.longterm?.portfolio?.map(s => s.ticker) || [],
      ...appData?.longterm?.watchlist?.map(s => s.ticker) || []
    ]);
    const grouped = {};
    data.forEach(e => {
      if (!grouped[e.date]) grouped[e.date] = [];
      grouped[e.date].push(e);
    });
    const dates = Object.keys(grouped).sort().slice(0, 14);
    if (!dates.length) { container.innerHTML = `<div class="empty-state">No upcoming earnings.</div>`; return; }
    container.innerHTML = dates.map(date => {
      const items = grouped[date];
      const myItems = items.filter(e => myTickers.has(e.symbol));
      const others = items.filter(e => !myTickers.has(e.symbol)).slice(0, 3);
      const shown = [...myItems, ...others];
      const d = new Date(date + 'T12:00:00');
      const label = d.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' });
      return `<div class="earnings-day">
        <div class="earnings-date">${label}</div>
        ${shown.map(e => `
          <div class="earnings-row ${myTickers.has(e.symbol) ? 'my-ticker' : ''}">
            <span class="earnings-symbol">${e.symbol}</span>
            <span class="earnings-hour">${e.hour === 'bmo' ? ' BMO' : e.hour === 'amc' ? ' AMC' : '—'}</span>
            ${e.epsEstimate ? `<span class="earnings-eps">EPS est: $${e.epsEstimate.toFixed(2)}</span>` : ''}
          </div>`).join('')}
        ${items.length > shown.length ? `<div class="earnings-more">+${items.length - shown.length} more</div>` : ''}
      </div>`;
    }).join('');
  } catch (e) {
    container.innerHTML = `<div class="empty-state">Could not load calendar.</div>`;
  }
}

// ── Alert Checker (runs every 60s with price refresh) ──────────────
async function checkAlerts() {
  const allPrices = {};
  Object.entries(prices).forEach(([t, d]) => { if (d.price) allPrices[t] = d.price; });
  Object.entries(cryptoPrices).forEach(([t, d]) => { if (d.price) allPrices[t] = d.price; });
  if (!Object.keys(allPrices).length) return;
  try {
    const res = await fetch('/api/alerts/check', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ prices: allPrices }) });
    const data = await res.json();
    (data.triggered || []).forEach(a => {
      window.electronAPI?.notify?.(`<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg> ${a.ticker} Alert`, `${a.ticker} is ${a.condition} $${a.price} — now $${a.currentPrice?.toFixed(2)}`);
      toolsLoadAlerts();
    });
  } catch {}
}

// Init tools tab
document.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('.tab-btn').forEach(btn => {
    if (btn.dataset.tab === 'tools') {
      btn.addEventListener('click', () => { if (!btn._toolsInit) { setupToolsTab(); btn._toolsInit = true; } });
    }
  });
  // Hook alert check into price refresh
  const origTracked = window.trackedRefresh;
});

// ─── TRADE JOURNAL TAB ────────────────────────────────────────────
function setupJournalTab() {
  journalLoadTrades();
  document.getElementById('briefOpenBtn')?.addEventListener('click', () => openMorningBrief(false));
  document.getElementById('briefRefreshBtn')?.addEventListener('click', () => openMorningBrief(true));
}

function journalLoadTrades() {
  const list = document.getElementById('journalTradesList');
  if (!appData?.daytrading?.closedTrades?.length) {
    list.innerHTML = `<div class="empty-state">No closed trades yet. Close a trade from Day Trading to analyze it here.</div>`;
    return;
  }
  list.innerHTML = appData.daytrading.closedTrades.map((t, i) => {
    const pnlClass = t.pnl >= 0 ? 'green' : 'red';
    return `<div class="journal-trade-row" onclick="journalAnalyzeTrade(${i})">
      <div class="journal-trade-left">
        <span class="journal-trade-ticker">${t.ticker}</span>
        <span class="badge ${t.type === 'CALL' ? 'badge-call' : 'badge-put'}">${t.type}</span>
        <span style="font-size:11px;color:var(--text3)">$${t.strike} · ${t.expiry}</span>
      </div>
      <div class="journal-trade-right">
        <span class="${pnlClass}" style="font-weight:700">${t.pnl >= 0 ? '+' : ''}$${t.pnl?.toFixed(0)}</span>
        <span style="font-size:10px;color:var(--text3)">${t.soldDate || ''}</span>
      </div>
    </div>`;
  }).join('');
}

async function journalAnalyzeTrade(idx) {
  const trade = appData.daytrading.closedTrades[idx];
  if (!trade) return;
  document.getElementById('journalTradeLabel').textContent = `${trade.ticker} ${trade.type} — analyzing...`;
  const output = document.getElementById('journalAnalysisOutput');
  output.innerHTML = `<div class="news-loading"><div class="spinner"></div><span><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="10" rx="2"/><circle cx="12" cy="5" r="2"/><path d="M12 7v4"/></svg> AI analyzing your ${trade.ticker} trade...</span></div>`;
  try {
    const res = await fetch('/api/journal/analyze', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ trade })
    });
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n'); buffer = lines.pop();
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const msg = JSON.parse(line.slice(6));
          if (msg.type === 'analysis') renderJournalAnalysis(output, msg.data, trade);
          if (msg.type === 'error') output.innerHTML = `<div class="empty-state red"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> ${msg.text}</div>`;
        } catch {}
      }
    }
  } catch (e) {
    output.innerHTML = `<div class="empty-state red"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> ${e.message}</div>`;
  }
}

function renderJournalAnalysis(output, d, trade) {
  document.getElementById('journalTradeLabel').textContent = `${trade.ticker} ${trade.type} · Grade: ${d.grade}`;
  output.innerHTML = `
    <div class="journal-analysis">
      <div class="journal-grade-row">
        <div class="journal-grade journal-grade-${d.gradeColor}">${d.grade}</div>
        <div class="journal-verdict">${d.verdict}</div>
      </div>
      <div class="journal-grid">
        <div class="intel-section">
          <div class="intel-section-title"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> What Went Right</div>
          ${(d.whatWentRight || []).map(s => `<div class="intel-bullet green"><span><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg></span><span>${s}</span></div>`).join('')}
        </div>
        <div class="intel-section">
          <div class="intel-section-title"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> What Went Wrong</div>
          ${(d.whatWentWrong || []).map(s => `<div class="intel-bullet red"><span><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg></span><span>${s}</span></div>`).join('')}
        </div>
        <div class="intel-section">
          <div class="intel-section-title"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg> Entry Analysis</div>
          <div class="intel-text">${d.entryAnalysis}</div>
        </div>
        <div class="intel-section">
          <div class="intel-section-title"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/><path d="M9 18H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h4"/></svg> Exit Analysis</div>
          <div class="intel-text">${d.exitAnalysis}</div>
        </div>
        <div class="intel-section">
          <div class="intel-section-title"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m16 16 3-8 3 8c-.87.65-1.92 1-3 1s-2.13-.35-3-1z"/><path d="m2 16 3-8 3 8c-.87.65-1.92 1-3 1s-2.13-.35-3-1z"/><path d="M7 21h10"/><path d="M12 3v18"/><path d="M3 7h2c2 0 5-1 7-2 2 1 5 2 7 2h2"/></svg>️ Risk Management</div>
          <div class="intel-text">${d.riskManagement}</div>
        </div>
        <div class="intel-section">
          <div class="intel-section-title"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg> Pattern Detected</div>
          <div class="intel-text">${d.patternDetected}</div>
        </div>
      </div>
      <div class="journal-lesson">
        <span class="journal-lesson-label"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="9" y1="18" x2="15" y2="18"/><line x1="10" y1="22" x2="14" y2="22"/><path d="M15.09 14c.18-.98.65-1.74 1.41-2.5A4.65 4.65 0 0 0 18 8 6 6 0 0 0 6 8c0 1 .23 2.23 1.5 3.5A4.61 4.61 0 0 1 8.91 14"/></svg> Key Lesson:</span>
        <span>${d.keyLesson}</span>
      </div>
      <div class="intel-section" style="margin-top:10px">
        <div class="intel-section-title"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="6"/><circle cx="12" cy="12" r="2"/></svg> Do Next Time</div>
        ${(d.doNextTime || []).map((s, i) => `<div class="intel-bullet"><span class="step-num">${i + 1}</span><span>${s}</span></div>`).join('')}
      </div>
    </div>`;
}

// ─── MORNING BRIEF ────────────────────────────────────────────────
async function openMorningBrief(force = false) {
  const overlay = document.getElementById('briefOverlay');
  const content = document.getElementById('briefContent');
  overlay.classList.remove('hidden');
  content.innerHTML = `<div class="loading-state"><div class="spinner"></div><span>️ Generating your morning brief...</span></div>`;
  try {
    const portfolio = appData?.longterm?.portfolio || [];
    const watchlist = [...(appData?.daytrading?.watchlist || []), ...(appData?.longterm?.watchlist || [])];
    const res = await fetch(`/api/ai/morning-brief${force ? '?force=true' : ''}`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ portfolio, watchlist, prices, cryptoPrices })
    });
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n'); buffer = lines.pop();
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const msg = JSON.parse(line.slice(6));
          if (msg.type === 'brief') renderMorningBrief(content, msg.data);
          if (msg.type === 'error') content.innerHTML = `<div class="empty-state red"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> ${msg.text}</div>`;
        } catch {}
      }
    }
  } catch (e) {
    content.innerHTML = `<div class="empty-state red"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> ${e.message}</div>`;
  }
}

function renderMorningBrief(container, d) {
  const moodColor = d.marketMoodColor || 'yellow';
  const time = d.generatedAt ? new Date(d.generatedAt).toLocaleTimeString() : '';

  // Watchlist news section
  const watchlistNewsHtml = (d.watchlistNewsRaw || []).map(r => `
    <div class="brief-news-ticker">
      <span class="brief-news-ticker-label ${tickerColor(null)}">${r.ticker}</span>
      ${r.news.map(n => `
        <div class="brief-news-item" onclick="window.open('${n.url}','_blank')">
          <span class="brief-news-src brief-news-src-${(n.source||'').toLowerCase()}">${n.source}</span>
          <span class="brief-news-headline">${n.headline}</span>
          <span class="brief-news-time">${newsTimeAgo ? newsTimeAgo(n.datetime) : ''}</span>
        </div>`).join('')}
    </div>`).join('') || '<div class="brief-text" style="color:var(--text3)">No recent news for your stocks.</div>';

  // Market news section
  const marketNewsHtml = (d.marketNewsRaw || []).slice(0, 5).map(n => `
    <div class="brief-news-item" onclick="window.open('${n.url}','_blank')">
      <span class="brief-news-src brief-news-src-${(n.sources?.[0]||n.source||'').toLowerCase()}">${n.sources?.[0] || n.source}</span>
      <span class="brief-news-headline">${n.headline}</span>
      <span class="brief-news-time">${newsTimeAgo ? newsTimeAgo(n.datetime) : ''}</span>
    </div>`).join('') || '<div class="brief-text" style="color:var(--text3)">No market news available.</div>';

  // Stocks to watch section
  const stocksToWatchHtml = (d.stocksToWatch || []).map(s => `
    <div class="brief-watch-stock">
      <div class="brief-watch-left">
        <span class="brief-watch-ticker">${s.ticker}</span>
        <span class="brief-watch-name">${s.name}</span>
      </div>
      <div class="brief-watch-right">
        <span class="brief-watch-action">${s.action}</span>
        <div class="brief-watch-reason">${s.reason}</div>
      </div>
      <button class="btn-primary btn-sm" onclick="briefAddToWatchlist('${s.ticker}', this)">+ Watch</button>
    </div>`).join('');

  container.innerHTML = `
    <div class="brief-card">
      <div class="brief-mood brief-mood-${moodColor}">
        <div class="brief-mood-left">
          <span class="brief-mood-label">${d.marketMood}</span>
          <span class="brief-time">Generated ${time}</span>
        </div>
        <button class="brief-refresh-btn" onclick="openMorningBrief(true)">↻ Refresh</button>
      </div>
      <div class="brief-greeting">${d.greeting}</div>
      <div class="brief-headline"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 22h16a2 2 0 0 0 2-2V4a2 2 0 0 0-2-2H8a2 2 0 0 0-2 2v16a2 2 0 0 0-2 2zm0 0a2 2 0 0 1-2-2v-9c0-1.1.9-2 2-2h2"/></svg> ${d.headline}</div>

      <div class="brief-section">
        <div class="brief-section-title"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="7" width="20" height="14" rx="2"/><path d="M16 7V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v2"/></svg> Portfolio Snapshot</div>
        <div class="brief-text">${d.portfolioSnapshot}</div>
      </div>

      ${d.topMover ? `<div class="brief-section">
        <div class="brief-section-title"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M8.5 14.5A2.5 2.5 0 0 0 11 12c0-1.38-.5-2-1-3-1.072-2.143-.224-4.054 2-6 .5 2.5 2 4.9 4 6.5 2 1.6 3 3.5 3 5.5a7 7 0 1 1-14 0c0-1.153.433-2.294 1-3a2.5 2.5 0 0 0 2.5 2.5z"/></svg> Top Mover</div>
        <div class="brief-text"><b>${d.topMover.ticker}</b> is ${d.topMover.direction === 'up' ? '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 7 13.5 15.5 8.5 10.5 2 17"/><polyline points="16 7 22 7 22 13"/></svg>' : '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 17 13.5 8.5 8.5 13.5 2 7"/><polyline points="16 17 22 17 22 11"/></svg>'} — ${d.topMover.note}</div>
      </div>` : ''}

      <div class="brief-section">
        <div class="brief-section-title"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Today's Focus</div>
        ${(d.todaysFocus || []).map((f, i) => `<div class="intel-bullet"><span class="step-num">${i + 1}</span><span>${f}</span></div>`).join('')}
      </div>

      ${d.watchlistAlert ? `<div class="brief-alert"> ${d.watchlistAlert}</div>` : ''}
      ${d.riskAlert ? `<div class="brief-risk"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>️ ${d.riskAlert}</div>` : ''}

      <div class="brief-section">
        <div class="brief-section-title"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 22h16a2 2 0 0 0 2-2V4a2 2 0 0 0-2-2H8a2 2 0 0 0-2 2v16a2 2 0 0 0-2 2zm0 0a2 2 0 0 1-2-2v-9c0-1.1.9-2 2-2h2"/></svg> Your Stocks — Latest News</div>
        <div class="brief-news-feed">${watchlistNewsHtml}</div>
      </div>

      <div class="brief-section">
        <div class="brief-section-title"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/></svg> General Market News</div>
        <div class="brief-news-feed">${marketNewsHtml}</div>
      </div>

      ${stocksToWatchHtml ? `<div class="brief-section">
        <div class="brief-section-title"> Stocks Worth Watching Today</div>
        <div class="brief-stocks-watch">${stocksToWatchHtml}</div>
      </div>` : ''}

      <div class="brief-closing">${d.closingThought}</div>
    </div>`;

  // Update preview in journal tab
  const preview = document.getElementById('briefPreview');
  if (preview) {
    preview.innerHTML = `<div style="padding:12px">
      <div class="brief-mood brief-mood-${moodColor}" style="margin-bottom:8px">
        <span class="brief-mood-label">${d.marketMood}</span>
        <span class="brief-time">${time}</span>
      </div>
      <div style="font-size:12px;font-weight:600;margin-bottom:4px">${d.greeting}</div>
      <div style="font-size:11px;color:var(--text2);margin-bottom:8px">${d.headline}</div>
      <button class="btn-secondary btn-sm" onclick="openMorningBrief(false)" style="width:100%">Open Full Brief</button>
    </div>`;
  }
}

async function briefAddToWatchlist(ticker, btn) {
  // Capture button immediately — event is lost after first await
  const button = btn || event?.target;
  if (button) { button.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/></svg>'; button.disabled = true; }
  try {
    await Promise.all([
      fetch('/api/longterm/watchlist/add', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ticker }) }),
      fetch('/api/daytrading/watchlist/add', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ticker }) })
    ]);
    await loadData();
    trackedRefresh();
    if (button) { button.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Added'; button.disabled = true; }
  } catch {
    if (button) { button.textContent = '+ Watch'; button.disabled = false; }
  }
}

// Init journal tab + wire brief buttons globally on DOM ready
document.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('.tab-btn').forEach(btn => {
    if (btn.dataset.tab === 'journal') {
      btn.addEventListener('click', () => { if (!btn._journalInit) { setupJournalTab(); btn._journalInit = true; } });
    }
  });

  // Brief close + overlay click — always wired regardless of which tab is active
  document.getElementById('briefCloseBtn')?.addEventListener('click', () => {
    document.getElementById('briefOverlay')?.classList.add('hidden');
  });
  document.getElementById('briefClose')?.addEventListener('click', () => {
    document.getElementById('briefOverlay')?.classList.add('hidden');
  });
  document.getElementById('briefRefreshBtnOverlay')?.addEventListener('click', () => openMorningBrief(true));
  document.getElementById('briefOverlay')?.addEventListener('click', (e) => {
    if (e.target === document.getElementById('briefOverlay')) {
      document.getElementById('briefOverlay').classList.add('hidden');
    }
  });

  // Titlebar brief button — always available
  document.getElementById('titlebarBriefBtn')?.addEventListener('click', () => openMorningBrief(false));
});

// ─── SUGGESTIONS TAB ──────────────────────────────────────────────
function setupSuggestionsTab() {
  document.getElementById('suggLtGenBtn').addEventListener('click', suggGenerateLongTerm);
  document.getElementById('suggOptGenBtn').addEventListener('click', suggGenerateOptions);
  suggUpdatePortfolioHeader();
}

function suggGetPortfolioContext() {
  const portfolio = appData?.longterm?.portfolio || [];
  const watchlist = appData?.longterm?.watchlist || [];
  let totalValue = 0, totalInvested = 0;
  const enriched = portfolio.map(s => {
    const p = prices[s.ticker] || {};
    const currentPrice = p.price || s.avgCost;
    const value = currentPrice * s.shares;
    const cost = s.avgCost * s.shares;
    totalValue += value;
    totalInvested += cost;
    return { ...s, currentPrice, value, cost };
  });
  return { portfolio: enriched, watchlist, totalValue, totalInvested, priceMap: prices };
}

function suggUpdatePortfolioHeader() {
  const { portfolio, totalValue, totalInvested } = suggGetPortfolioContext();
  const pnl = totalValue - totalInvested;
  const pnlPct = totalInvested > 0 ? ((pnl / totalInvested) * 100).toFixed(1) : 0;
  const isUp = pnl >= 0;

  document.getElementById('suggPortfolioValue').textContent = totalValue.toLocaleString('en-US', { maximumFractionDigits: 0 });

  const stats = document.getElementById('suggHealthStats');
  if (stats) {
    stats.innerHTML = `
      <div class="sugg-stat">
        <div class="sugg-stat-label">Total Value</div>
        <div class="sugg-stat-value">$${totalValue.toLocaleString('en-US', { maximumFractionDigits: 0 })}</div>
      </div>
      <div class="sugg-stat">
        <div class="sugg-stat-label">Invested</div>
        <div class="sugg-stat-value">$${totalInvested.toLocaleString('en-US', { maximumFractionDigits: 0 })}</div>
      </div>
      <div class="sugg-stat">
        <div class="sugg-stat-label">Total P&L</div>
        <div class="sugg-stat-value ${isUp ? 'green' : 'red'}">${isUp ? '+' : ''}$${Math.abs(pnl).toLocaleString('en-US', { maximumFractionDigits: 0 })} (${isUp ? '+' : ''}${pnlPct}%)</div>
      </div>
      <div class="sugg-stat">
        <div class="sugg-stat-label">Positions</div>
        <div class="sugg-stat-value">${portfolio.length}</div>
      </div>`;
  }
}

async function suggGenerateLongTerm() {
  const btn = document.getElementById('suggLtGenBtn');
  const output = document.getElementById('suggLtOutput');
  btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/></svg> Analyzing...';
  btn.disabled = true;
  output.innerHTML = `<div class="sugg-loading"><div class="spinner"></div><span><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="10" rx="2"/><circle cx="12" cy="5" r="2"/><path d="M12 7v4"/></svg> AI analyzing your portfolio and generating personalized suggestions...</span></div>`;

  const { portfolio, watchlist, totalValue, totalInvested } = suggGetPortfolioContext();

  try {
    const res = await fetch('/api/suggestions/longterm', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ portfolio, watchlist, prices, totalValue, totalInvested })
    });
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n'); buffer = lines.pop();
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const msg = JSON.parse(line.slice(6));
          if (msg.type === 'status') output.innerHTML = `<div class="sugg-loading"><div class="spinner"></div><span>${msg.text}</span></div>`;
          if (msg.type === 'suggestions') {
            // Client-side safety filter
            const allTracked = new Set([
              ...(appData.longterm?.portfolio   || []).map(s => s.ticker),
              ...(appData.longterm?.watchlist   || []).map(s => s.ticker),
              ...(appData.daytrading?.watchlist || []).map(s => s.ticker),
              ...(appData.daytrading?.cryptoWatchlist || []).map(s => s.ticker),
            ]);
            const data = { ...msg.data, suggestions: (msg.data.suggestions || []).filter(s => !allTracked.has(s.ticker)) };
            if (data.suggestions?.length) suggRenderLongTerm(output, data);
            else output.innerHTML = '<div class="sugg-empty">All suggestions are already in your watchlist. Check back later for fresh ideas.</div>';
          }
          if (msg.type === 'error') output.innerHTML = `<div class="sugg-empty red"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> ${msg.text}</div>`;
        } catch {}
      }
    }
  } catch (e) {
    output.innerHTML = `<div class="sugg-empty red"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> ${e.message}</div>`;
  } finally {
    btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg> Generate';
    btn.disabled = false;
  }
}

function suggRenderLongTerm(output, data) {
  // Update health bar
  const healthEl = document.getElementById('suggHealthValue');
  const divEl = document.getElementById('suggDivScore');
  const summaryEl = document.getElementById('suggSummaryText');
  if (healthEl) { healthEl.textContent = data.portfolioHealth; healthEl.className = `sugg-health-value sugg-health-${data.portfolioHealthColor}`; }
  if (divEl) { divEl.textContent = `${data.diversificationScore}/10`; divEl.className = `sugg-div-score ${data.diversificationScore >= 7 ? 'green' : data.diversificationScore >= 5 ? 'yellow' : 'red'}`; }
  if (summaryEl) { summaryEl.textContent = data.portfolioSummary; }

  const typeConfig = {
    ADD:       { icon: '', color: 'green',  label: 'Add More',    bg: 'sugg-card-green'  },
    NEW:       { icon: '🆕', color: 'blue',   label: 'New Position', bg: 'sugg-card-blue'  },
    HOLD:      { icon: '', color: 'yellow', label: 'Hold',         bg: 'sugg-card-yellow' },
    TRIM:      { icon: '️', color: 'orange', label: 'Trim',         bg: 'sugg-card-orange' },
    EXIT:      { icon: '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M13 4h3a2 2 0 0 1 2 2v14"/><path d="M2 20h3"/><path d="M13 20h9"/><path d="M10 12v.01"/><path d="M13 4.562v16.157a1 1 0 0 1-1.242.97L5 20V5.562a2 2 0 0 1 1.515-1.94l4-1A2 2 0 0 1 13 4.561z"/></svg>', color: 'red',    label: 'Consider Exit', bg: 'sugg-card-red'   },
    REBALANCE: { icon: '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m16 16 3-8 3 8c-.87.65-1.92 1-3 1s-2.13-.35-3-1z"/><path d="m2 16 3-8 3 8c-.87.65-1.92 1-3 1s-2.13-.35-3-1z"/><path d="M7 21h10"/><path d="M12 3v18"/><path d="M3 7h2c2 0 5-1 7-2 2 1 5 2 7 2h2"/></svg>️', color: 'purple', label: 'Rebalance',    bg: 'sugg-card-purple' }
  };

  output.innerHTML = `
    <div class="sugg-rebalance-note"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m16 16 3-8 3 8c-.87.65-1.92 1-3 1s-2.13-.35-3-1z"/><path d="m2 16 3-8 3 8c-.87.65-1.92 1-3 1s-2.13-.35-3-1z"/><path d="M7 21h10"/><path d="M12 3v18"/><path d="M3 7h2c2 0 5-1 7-2 2 1 5 2 7 2h2"/></svg>️ ${data.rebalanceNote}</div>
    ${(data.suggestions || []).map((s, i) => {
      const cfg = typeConfig[s.type] || typeConfig.HOLD;
      const isTopPriority = s.ticker === data.topPriority;
      const confColor = s.confidence >= 75 ? 'green' : s.confidence >= 50 ? 'yellow' : 'red';
      return `
        <div class="sugg-card ${cfg.bg} ${isTopPriority ? 'sugg-card-priority' : ''}">
          ${isTopPriority ? '<div class="sugg-priority-badge"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M8.5 14.5A2.5 2.5 0 0 0 11 12c0-1.38-.5-2-1-3-1.072-2.143-.224-4.054 2-6 .5 2.5 2 4.9 4 6.5 2 1.6 3 3.5 3 5.5a7 7 0 1 1-14 0c0-1.153.433-2.294 1-3a2.5 2.5 0 0 0 2.5 2.5z"/></svg> Top Priority</div>' : ''}
          <div class="sugg-card-header">
            <div class="sugg-card-left">
              <div class="sugg-type-icon">${cfg.icon}</div>
              <div>
                <div class="sugg-card-ticker">${s.ticker}</div>
                <div class="sugg-card-action">${s.action}</div>
              </div>
            </div>
            <div class="sugg-card-right">
              <div class="sugg-dollar-amount">$${(s.dollarAmount || 0).toLocaleString('en-US', { maximumFractionDigits: 0 })}</div>
              <div class="sugg-dollar-label">${s.dollarAmountLabel}</div>
            </div>
          </div>
          <div class="sugg-card-body">
            <div class="sugg-reasoning">${s.reasoning}</div>
            <div class="sugg-card-meta">
              <div class="sugg-meta-row">
                <span class="sugg-meta-item"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg> ${s.shares}</span>
                <span class="sugg-meta-item">Before: <b>${s.allocationBefore}</b></span>
                <span class="sugg-meta-item">After: <b>${s.allocationAfter}</b></span>
              </div>
              <div class="sugg-meta-row">
                <span class="sugg-badge sugg-badge-${s.urgency?.toLowerCase()}">⏰ ${s.urgency} Urgency</span>
                <span class="sugg-badge sugg-badge-risk-${s.riskLevel?.toLowerCase()}"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>️ ${s.riskLevel} Risk</span>
                <span class="sugg-conf ${confColor}">${s.confidence}% confidence</span>
              </div>
            </div>
          </div>
        </div>`;
    }).join('')}`;
}

async function suggGenerateOptions() {
  const btn = document.getElementById('suggOptGenBtn');
  const output = document.getElementById('suggOptOutput');
  btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/></svg> Analyzing...';
  btn.disabled = true;
  output.innerHTML = `<div class="sugg-loading"><div class="spinner"></div><span><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg> Building your personalized options playbook...</span></div>`;

  const { portfolio, watchlist, totalValue, totalInvested } = suggGetPortfolioContext();

  try {
    const res = await fetch('/api/suggestions/options', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ portfolio, watchlist, prices, totalValue, totalInvested })
    });
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n'); buffer = lines.pop();
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const msg = JSON.parse(line.slice(6));
          if (msg.type === 'status') output.innerHTML = `<div class="sugg-loading"><div class="spinner"></div><span>${msg.text}</span></div>`;
          if (msg.type === 'suggestions') suggRenderOptions(output, msg.data);
          if (msg.type === 'error') output.innerHTML = `<div class="sugg-empty red"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> ${msg.text}</div>`;
        } catch {}
      }
    }
  } catch (e) {
    output.innerHTML = `<div class="sugg-empty red"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> ${e.message}</div>`;
  } finally {
    btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg> Generate';
    btn.disabled = false;
  }
}

function suggRenderOptions(output, data) {
  const riskColor = { Low: 'green', Medium: 'yellow', High: 'red' };
  const typeColor = { CALL: 'green', PUT: 'red' };

  output.innerHTML = `
    <div class="sugg-options-budget">
      <div class="sugg-budget-icon"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg></div>
      <div>
        <div class="sugg-budget-amount">$${(data.optionsBudget || 0).toLocaleString()}</div>
        <div class="sugg-budget-note">${data.optionsBudgetNote}</div>
      </div>
    </div>
    <div class="sugg-beginner-note"> ${data.beginnerNote}</div>

    ${(data.suggestions || []).map((s, i) => {
      const rc = riskColor[s.riskLevel] || 'yellow';
      const tc = typeColor[s.optionType] || 'green';
      const confColor = s.confidence >= 75 ? 'green' : s.confidence >= 50 ? 'yellow' : 'red';
      return `
        <div class="sugg-opt-card">
          <div class="sugg-opt-header">
            <div class="sugg-opt-rank">#${s.rank}</div>
            <div class="sugg-opt-title">
              <span class="sugg-opt-ticker">${s.ticker}</span>
              <span class="sugg-opt-type sugg-opt-type-${tc}">${s.optionType}</span>
              <span class="sugg-opt-strike">$${s.strike} Strike</span>
              <span class="sugg-opt-expiry">${s.expiryLabel}</span>
              ${(() => { const cp = prices[s.ticker]?.price || cryptoPrices[s.ticker]?.price; return cp ? `<span class="sugg-opt-current-price">Stock: $${cp.toFixed(2)}</span>` : ''; })()}
            </div>
            <div class="sugg-opt-cost">
              <div class="sugg-dollar-amount">${s.totalCostLabel}</div>
              <div class="sugg-opt-contracts">${s.contracts} contract${s.contracts > 1 ? 's' : ''}</div>
            </div>
          </div>

          <div class="sugg-opt-metrics">
            <div class="sugg-opt-metric sugg-opt-metric-red">
              <div class="sugg-opt-metric-label">Max Loss</div>
              <div class="sugg-opt-metric-value">${s.maxLossLabel}</div>
            </div>
            <div class="sugg-opt-metric sugg-opt-metric-blue">
              <div class="sugg-opt-metric-label">Break-Even</div>
              <div class="sugg-opt-metric-value">${s.breakEvenLabel}</div>
            </div>
            <div class="sugg-opt-metric sugg-opt-metric-green">
              <div class="sugg-opt-metric-label">Target Return</div>
              <div class="sugg-opt-metric-value">${s.targetReturn}</div>
            </div>
          </div>

          <div class="sugg-opt-body">
            <div class="sugg-opt-why">${s.whyThisStock}</div>
            <div class="sugg-reasoning" style="margin-top:6px">${s.reasoning}</div>
            <div class="sugg-opt-plan">
              <div class="sugg-opt-plan-item"><span class="sugg-opt-plan-label"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg> Entry:</span> ${s.entryTip}</div>
              <div class="sugg-opt-plan-item"><span class="sugg-opt-plan-label"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/><path d="M9 18H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h4"/></svg> Exit:</span> ${s.exitPlan}</div>
            </div>
            <div class="sugg-card-meta" style="margin-top:8px">
              <div class="sugg-meta-row">
                <span class="sugg-badge sugg-badge-risk-${s.riskLevel?.toLowerCase()}"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>️ ${s.riskLevel} Risk</span>
                <span class="sugg-conf ${confColor}">${s.confidence}% confidence</span>
              </div>
            </div>
          </div>
        </div>`;
    }).join('')}

    <div class="sugg-tips-box">
      <div class="sugg-tips-title"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="9" y1="18" x2="15" y2="18"/><line x1="10" y1="22" x2="14" y2="22"/><path d="M15.09 14c.18-.98.65-1.74 1.41-2.5A4.65 4.65 0 0 0 18 8 6 6 0 0 0 6 8c0 1 .23 2.23 1.5 3.5A4.61 4.61 0 0 1 8.91 14"/></svg> Beginner Tips</div>
      ${(data.generalTips || []).map(t => `<div class="sugg-tip">• ${t}</div>`).join('')}
    </div>`;
}

// Init suggestions tab
document.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('.tab-btn').forEach(btn => {
    if (btn.dataset.tab === 'suggestions') {
      btn.addEventListener('click', () => {
        if (!btn._suggInit) { setupSuggestionsTab(); btn._suggInit = true; }
        else { suggUpdatePortfolioHeader(); }
      });
    }
  });
});

// ─── TICKER SEARCH & VALIDATE ─────────────────────────────────────
let searchTimers = {};
let resolvedTickers = {}; // inputId → { ticker, name }

function setupTickerSearch(inputId, dropdownId, msgId) {
  const input = document.getElementById(inputId);
  const dropdown = document.getElementById(dropdownId);
  const msg = document.getElementById(msgId);
  if (!input || !dropdown) return;

  input.addEventListener('input', () => {
    const val = input.value.trim();
    resolvedTickers[inputId] = null;
    if (msg) { msg.textContent = ''; msg.className = 'ticker-validate-msg'; }
    clearTimeout(searchTimers[inputId]);
    if (val.length < 1) { dropdown.classList.add('hidden'); dropdown.innerHTML = ''; return; }
    searchTimers[inputId] = setTimeout(() => tickerSearch(val, inputId, dropdownId, msgId), 350);
  });

  input.addEventListener('keydown', e => {
    if (e.key === 'Escape') { dropdown.classList.add('hidden'); }
  });

  // Close dropdown on outside click
  document.addEventListener('click', e => {
    if (!input.contains(e.target) && !dropdown.contains(e.target)) {
      dropdown.classList.add('hidden');
    }
  });
}

async function tickerSearch(query, inputId, dropdownId, msgId) {
  const dropdown = document.getElementById(dropdownId);
  if (!dropdown) return;
  try {
    const res = await fetch(`/api/search/${encodeURIComponent(query)}`);
    const results = await res.json();
    if (!results.length) { dropdown.classList.add('hidden'); return; }
    dropdown.innerHTML = results.map(r => `
      <div class="ticker-drop-item" onclick="tickerSelect('${inputId}','${dropdownId}','${msgId}','${r.symbol}','${r.name.replace(/'/g,"\\'")}')">
        <span class="ticker-drop-symbol">${r.symbol}</span>
        <span class="ticker-drop-name">${r.name}</span>
      </div>`).join('');
    dropdown.classList.remove('hidden');
  } catch { dropdown.classList.add('hidden'); }
}

function tickerSelect(inputId, dropdownId, msgId, symbol, name) {
  const input = document.getElementById(inputId);
  const dropdown = document.getElementById(dropdownId);
  const msg = document.getElementById(msgId);
  if (input) input.value = symbol;
  if (dropdown) { dropdown.classList.add('hidden'); dropdown.innerHTML = ''; }
  resolvedTickers[inputId] = { ticker: symbol, name };
  if (msg) {
    msg.innerHTML = `<svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg> ${name} (${symbol})`;
    msg.className = 'ticker-validate-msg msg-ok';
  }
}

async function validateAndGetTicker(inputId, msgId) {
  const input = document.getElementById(inputId);
  const msg = document.getElementById(msgId);
  const val = input?.value?.trim().toUpperCase();
  if (!val) return null;

  // Already resolved via dropdown
  if (resolvedTickers[inputId]?.ticker === val) return resolvedTickers[inputId].ticker;

  // Show validating
  if (msg) { msg.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/></svg> Validating...'; msg.className = 'ticker-validate-msg msg-loading'; }

  try {
    const res = await fetch(`/api/validate/${encodeURIComponent(val)}`);
    const data = await res.json();
    if (data.valid) {
      // Update input if ticker was resolved from full name
      if (input && data.ticker !== val) input.value = data.ticker;
      resolvedTickers[inputId] = { ticker: data.ticker, name: data.name };
      if (msg) {
        msg.innerHTML = `<svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg> ${data.name} (${data.ticker}) — $${data.price?.toFixed(2)}`;
        msg.className = 'ticker-validate-msg msg-ok';
      }
      return data.ticker;
    } else {
      if (msg) {
        msg.innerHTML = `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> ${data.error || `"${val}" not found. Try the full company name or check the ticker.`}`;
        msg.className = 'ticker-validate-msg msg-err';
      }
      return null;
    }
  } catch (e) {
    if (msg) { msg.innerHTML = `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> Validation failed: ${e.message}`; msg.className = 'ticker-validate-msg msg-err'; }
    return null;
  }
}

// Init all search fields when modals open
function initAllTickerSearches() {
  setupTickerSearch('dtAddStockTicker', 'dtAddStockDropdown', 'dtAddStockMsg');
  setupTickerSearch('ltTicker', 'ltTickerDropdown', 'ltTickerMsg');
  setupTickerSearch('ltWatchlistTicker', 'ltWatchlistDropdown', 'ltWatchlistMsg');
}

document.addEventListener('DOMContentLoaded', () => {
  setTimeout(initAllTickerSearches, 300);
});

// ─── EDIT PORTFOLIO STOCK ─────────────────────────────────────────
let editingTicker = null;

function openEditStock(e, ticker) {
  e.stopPropagation();
  editingTicker = ticker;
  const stock = appData?.longterm?.portfolio?.find(s => s.ticker === ticker);
  if (!stock) return;

  document.getElementById('editStockTickerLabel').textContent = ticker;
  document.getElementById('editShares').value = stock.shares;
  document.getElementById('editAvgCost').value = stock.avgCost;
  document.getElementById('editDateBought').value = stock.dateBought || '';
  document.getElementById('editBroker').value = stock.broker || '';
  document.getElementById('editNotes').value = stock.notes || '';

  updateEditPreview(stock);
  openModal('modalEditStock');
}

function updateEditPreview(stock) {
  const preview = document.getElementById('editStockPreview');
  if (!preview) return;
  const p = prices[stock.ticker]?.price || 0;
  const shares = parseFloat(document.getElementById('editShares')?.value) || stock.shares;
  const avgCost = parseFloat(document.getElementById('editAvgCost')?.value) || stock.avgCost;
  const invested = shares * avgCost;
  const value = p > 0 ? shares * p : 0;
  const pnl = value - invested;
  const pnlPct = invested > 0 ? ((pnl / invested) * 100).toFixed(1) : 0;
  preview.innerHTML = `
    <div class="edit-preview-grid">
      <div class="edit-preview-item">
        <div class="edit-preview-label">Total Invested</div>
        <div class="edit-preview-value">$${invested.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</div>
      </div>
      ${p > 0 ? `
      <div class="edit-preview-item">
        <div class="edit-preview-label">Current Value</div>
        <div class="edit-preview-value">$${value.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</div>
      </div>
      <div class="edit-preview-item">
        <div class="edit-preview-label">P&L</div>
        <div class="edit-preview-value ${pnl >= 0 ? 'green' : 'red'}">${pnl >= 0 ? '+' : ''}$${Math.abs(pnl).toFixed(2)} (${pnlPct}%)</div>
      </div>` : ''}
      <div class="edit-preview-item">
        <div class="edit-preview-label">Current Price</div>
        <div class="edit-preview-value">${p > 0 ? '$' + p.toFixed(2) : '—'}</div>
      </div>
    </div>`;
}

document.addEventListener('DOMContentLoaded', () => {
  // Live preview update as user edits
  ['editShares', 'editAvgCost'].forEach(id => {
    document.getElementById(id)?.addEventListener('input', () => {
      if (editingTicker) {
        const stock = appData?.longterm?.portfolio?.find(s => s.ticker === editingTicker);
        if (stock) updateEditPreview(stock);
      }
    });
  });

  document.getElementById('editStockConfirm')?.addEventListener('click', async () => {
    if (!editingTicker) return;
    const shares = parseFloat(document.getElementById('editShares').value);
    const avgCost = parseFloat(document.getElementById('editAvgCost').value);
    if (!shares || !avgCost) return alert('Shares and Average Cost are required.');

    const btn = document.getElementById('editStockConfirm');
    btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg> Saving...';
    btn.disabled = true;

    await fetch('/api/longterm/portfolio/add', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        ticker: editingTicker,
        shares,
        avgCost,
        dateBought: document.getElementById('editDateBought').value,
        broker: document.getElementById('editBroker').value,
        notes: document.getElementById('editNotes').value
      })
    });

    closeModal('modalEditStock');
    btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg> Save Changes';
    btn.disabled = false;
    editingTicker = null;
    await loadData();
    await trackedRefresh();
  });
});

// Override DT add stock confirm with validation
document.addEventListener('DOMContentLoaded', () => {
  setTimeout(() => {
    // DT Watchlist
    const dtConfirm = document.getElementById('dtAddStockConfirm');
    if (dtConfirm) {
      dtConfirm.replaceWith(dtConfirm.cloneNode(true)); // remove old listener
      document.getElementById('dtAddStockConfirm').addEventListener('click', async () => {
        const ticker = await validateAndGetTicker('dtAddStockTicker', 'dtAddStockMsg');
        if (!ticker) return;
        await fetch('/api/daytrading/watchlist/add', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ticker }) });
        document.getElementById('dtAddStockTicker').value = '';
        resolvedTickers['dtAddStockTicker'] = null;
        closeModal('modalDtAddStock');
        await loadData();
        trackedRefresh();
      });
    }

    // LT Portfolio
    const ltConfirm = document.getElementById('ltAddStockConfirm');
    if (ltConfirm) {
      ltConfirm.replaceWith(ltConfirm.cloneNode(true));
      document.getElementById('ltAddStockConfirm').addEventListener('click', async () => {
        const ticker = await validateAndGetTicker('ltTicker', 'ltTickerMsg');
        if (!ticker) return;
        const shares = parseFloat(document.getElementById('ltShares').value);
        const avgCost = parseFloat(document.getElementById('ltAvgCost').value);
        if (!shares || !avgCost) { alert('Please fill in shares and average cost.'); return; }
        await fetch('/api/longterm/portfolio/add', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ ticker, shares, avgCost, dateBought: document.getElementById('ltDateBought').value, broker: document.getElementById('ltBroker').value, notes: document.getElementById('ltNotes').value })
        });
        closeModal('modalLtAddStock');
        ['ltTicker','ltShares','ltAvgCost','ltDateBought','ltBroker','ltNotes'].forEach(id => { const el = document.getElementById(id); if (el) el.value = ''; });
        resolvedTickers['ltTicker'] = null;
        await loadData();
        await trackedRefresh();
        // Trigger AI rating for the newly added stock
        const newPrice = prices[ticker]?.price || avgCost;
        if (!ratings[ticker]) fetchRatingForTicker(ticker, newPrice, false);
      });
    }

    // LT Watchlist
    const ltWlConfirm = document.getElementById('ltAddWatchlistConfirm');
    if (ltWlConfirm) {
      ltWlConfirm.replaceWith(ltWlConfirm.cloneNode(true));
      document.getElementById('ltAddWatchlistConfirm').addEventListener('click', async () => {
        const ticker = await validateAndGetTicker('ltWatchlistTicker', 'ltWatchlistMsg');
        if (!ticker) return;
        await fetch('/api/longterm/watchlist/add', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ticker }) });
        document.getElementById('ltWatchlistTicker').value = '';
        resolvedTickers['ltWatchlistTicker'] = null;
        closeModal('modalLtAddWatchlist');
        await loadData();
        trackedRefresh();
      });
    }
  }, 400);
});

// ─── INVEST TAB ───────────────────────────────────────────────────
let invType = null;   // 'options' | 'longterm'
let invRisk = null;   // 'safe' | 'medium' | 'high'

function invSetAmount(val) {
  document.getElementById('invAmount').value = val;
  // highlight active quick btn
  document.querySelectorAll('.inv-quick-btn').forEach(b => {
    b.classList.toggle('active', parseInt(b.textContent.replace(/[^0-9]/g,'')) * (b.textContent.includes('K') ? 1000 : 1) === val);
  });
  invCheckReady();
}

function invSelectType(type) {
  invType = type;
  // Update type cards
  document.getElementById('invTypeOptions').classList.toggle('selected', type === 'options');
  document.getElementById('invTypeLongterm').classList.toggle('selected', type === 'longterm');
  document.getElementById('invCheckOptions').classList.toggle('hidden', type !== 'options');
  document.getElementById('invCheckLongterm').classList.toggle('hidden', type !== 'longterm');
  // Show/hide risk section
  const riskSection = document.getElementById('invRiskSection');
  if (type === 'options') {
    riskSection.classList.remove('hidden');
  } else {
    riskSection.classList.add('hidden');
    invRisk = null;
  }
  invCheckReady();
}

function invSelectRisk(risk) {
  invRisk = risk;
  ['safe','medium','high'].forEach(r => {
    document.getElementById(`invRisk${r.charAt(0).toUpperCase()+r.slice(1)}`).classList.toggle('selected', r === risk);
    document.getElementById(`invCheck${r.charAt(0).toUpperCase()+r.slice(1)}`).classList.toggle('hidden', r !== risk);
  });
  invCheckReady();
}

function invCheckReady() {
  const amount = parseFloat(document.getElementById('invAmount')?.value);
  const btn = document.getElementById('invGenerateBtn');
  if (!btn) return;
  const ready = amount >= 100 && invType && (invType === 'longterm' || invRisk);
  btn.disabled = !ready;
}

function invLoadContext() {
  const body = document.getElementById('invContextBody');
  if (!body || !appData) return;
  const portfolio = appData.longterm.portfolio || [];
  const totalValue = portfolio.reduce((sum, s) => {
    const p = prices[s.ticker] || {};
    return sum + (p.price || s.avgCost) * s.shares;
  }, 0);
  const totalInvested = portfolio.reduce((sum, s) => sum + s.avgCost * s.shares, 0);
  const pnl = totalValue - totalInvested;
  body.innerHTML = `
    <div class="inv-context-grid">
      <div class="inv-ctx-item">
        <div class="inv-ctx-label">Portfolio Value</div>
        <div class="inv-ctx-value">$${totalValue.toLocaleString('en-US',{maximumFractionDigits:0})}</div>
      </div>
      <div class="inv-ctx-item">
        <div class="inv-ctx-label">Total P&L</div>
        <div class="inv-ctx-value ${pnl>=0?'green':'red'}">${pnl>=0?'+':''}$${Math.abs(pnl).toLocaleString('en-US',{maximumFractionDigits:0})}</div>
      </div>
      <div class="inv-ctx-item">
        <div class="inv-ctx-label">Positions</div>
        <div class="inv-ctx-value">${portfolio.length}</div>
      </div>
    </div>
    <div class="inv-ctx-holdings">
      ${portfolio.map(s => {
        const p = prices[s.ticker] || {};
        const val = (p.price || s.avgCost) * s.shares;
        const allocPct = totalValue > 0 ? ((val/totalValue)*100).toFixed(0) : 0;
        const pnlS = p.price ? ((p.price - s.avgCost)/s.avgCost*100).toFixed(1) : null;
        const tc = p.price ? (p.changePct > 0 ? 'wl-ticker-up' : p.changePct < 0 ? 'wl-ticker-down' : 'wl-ticker-neutral') : 'wl-ticker-neutral';
        return `<div class="inv-ctx-holding">
          <span class="inv-ctx-ticker ${tc}">${s.ticker}</span>
          <div class="inv-ctx-bar-wrap"><div class="inv-ctx-bar" style="width:${Math.min(allocPct,100)}%"></div></div>
          <span class="inv-ctx-alloc">${allocPct}%</span>
          ${pnlS ? `<span class="${parseFloat(pnlS)>=0?'green':'red'}" style="font-size:10px;min-width:40px;text-align:right">${parseFloat(pnlS)>=0?'+':''}${pnlS}%</span>` : ''}
        </div>`;
      }).join('')}
    </div>`;
}

async function invGenerate() {
  const amount = parseFloat(document.getElementById('invAmount').value);
  if (!amount || !invType) return;
  const btn = document.getElementById('invGenerateBtn');
  const output = document.getElementById('invPlanOutput');
  const title = document.getElementById('invPlanTitle');
  const hint = document.getElementById('invPlanHint');
  btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/></svg> Generating...';
  btn.disabled = true;
  const typeLabel = invType === 'options' ? `Options (${invRisk})` : 'Long Term';
  title.innerHTML = `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 5H7a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V7a2 2 0 0 0-2-2h-2"/><rect x="9" y="3" width="6" height="4" rx="1"/></svg> ${typeLabel} Plan — $${amount.toLocaleString()}`;
  hint.textContent = 'AI is building your plan...';
  output.innerHTML = `<div class="sugg-loading"><div class="spinner"></div><span><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="10" rx="2"/><circle cx="12" cy="5" r="2"/><path d="M12 7v4"/></svg> AI is building your personalized investment plan...</span></div>`;
  const portfolio = appData?.longterm?.portfolio || [];
  const watchlist = [...(appData?.longterm?.watchlist||[]), ...(appData?.daytrading?.watchlist||[])];
  const endpoint = invType === 'options' ? '/api/invest/options-plan' : '/api/invest/longterm-plan';
  const body = invType === 'options'
    ? { amount, riskLevel: invRisk, portfolio, watchlist, prices }
    : { amount, portfolio, watchlist, prices };
  try {
    const res = await fetch(endpoint, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n'); buffer = lines.pop();
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const msg = JSON.parse(line.slice(6));
          if (msg.type === 'status') output.innerHTML = `<div class="sugg-loading"><div class="spinner"></div><span>${msg.text}</span></div>`;
          if (msg.type === 'plan') {
            if (invType === 'options') invRenderOptionsPlan(output, msg.data);
            else invRenderLongtermPlan(output, msg.data);
            hint.textContent = `Generated ${new Date().toLocaleTimeString()}`;
          }
          if (msg.type === 'error') output.innerHTML = `<div class="sugg-empty red"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> ${msg.text}</div>`;
        } catch {}
      }
    }
  } catch (e) {
    output.innerHTML = `<div class="sugg-empty red"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> ${e.message}</div>`;
  } finally {
    btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg> Build My Investment Plan';
    btn.disabled = false;
  }
}

function invRenderOptionsPlan(output, d) {
  const riskColors = { Safe: 'green', 'Medium Risk': 'yellow', 'Medium': 'yellow', High: 'red', 'High Risk': 'red' };
  const actionColors = { 'BUY CALL': 'green', 'BUY PUT': 'red', 'SELL COVERED CALL': 'blue', 'BUY CALL SPREAD': 'green', 'BUY PUT SPREAD': 'red' };

  output.innerHTML = `
    <div class="inv-plan">

      <!-- Summary bar -->
      <div class="inv-plan-summary">
        <div class="inv-plan-sum-item">
          <div class="inv-sum-label">Budget</div>
          <div class="inv-sum-value">$${(d.totalBudget||0).toLocaleString()}</div>
        </div>
        <div class="inv-plan-sum-item">
          <div class="inv-sum-label">Allocated</div>
          <div class="inv-sum-value green">$${(d.totalAllocated||0).toLocaleString()}</div>
        </div>
        <div class="inv-plan-sum-item">
          <div class="inv-sum-label">Cash Left</div>
          <div class="inv-sum-value ${(d.remainingCash||0)>0?'yellow':'green'}">$${(d.remainingCash||0).toLocaleString()}</div>
        </div>
        <div class="inv-plan-sum-item">
          <div class="inv-sum-label">Trades</div>
          <div class="inv-sum-value">${(d.trades||[]).length}</div>
        </div>
        <div class="inv-plan-sum-item">
          <div class="inv-sum-label">Risk Level</div>
          <div class="inv-sum-value ${riskColors[d.riskLevel]||'yellow'}">${d.riskLevel}</div>
        </div>
      </div>

      ${d.marketContext ? `<div class="inv-market-context"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/></svg> ${d.marketContext}</div>` : ''}
      <div class="inv-plan-strategy">${d.strategy}</div>

      <!-- Allocation donut-style bar -->
      <div class="inv-alloc-bar-wrap">
        <div class="inv-alloc-bar-label">Budget Allocation</div>
        <div class="inv-alloc-bar">
          ${(d.allocationBreakdown||[]).map((a,i) => {
            const colors = ['#4d9fff','#00d4aa','#a78bfa','#ffd166','#ff4d6d'];
            return `<div class="inv-alloc-seg" style="width:${a.pct}%;background:${colors[i%colors.length]}" title="${a.label}: $${a.amount} (${a.pct}%)"></div>`;
          }).join('')}
        </div>
        <div class="inv-alloc-legend">
          ${(d.allocationBreakdown||[]).map((a,i) => {
            const colors = ['#4d9fff','#00d4aa','#a78bfa','#ffd166','#ff4d6d'];
            return `<span class="inv-alloc-leg-item"><span style="background:${colors[i%colors.length]};width:8px;height:8px;border-radius:50%;display:inline-block;margin-right:4px"></span>${a.label} $${a.amount}</span>`;
          }).join('')}
        </div>
      </div>

      <!-- Trades -->
      ${(d.trades||[]).map(t => {
        const ac = actionColors[t.action] || 'blue';
        return `
        <div class="inv-trade-card">
          <div class="inv-trade-header">
            <div class="inv-trade-rank">#${t.rank}</div>
            <div class="inv-trade-title">
              <span class="inv-trade-ticker">${t.ticker}</span>
              ${t.companyName ? `<span class="inv-lt-name">${t.companyName}</span>` : ''}
              <span class="inv-trade-action inv-trade-action-${ac}">${t.action}</span>
              <span class="inv-trade-strike">$${t.strike} Strike</span>
              <span class="inv-trade-expiry">${t.expiryLabel}</span>
              ${t.currentStockPrice ? `<span class="sugg-opt-current-price">Stock: $${t.currentStockPrice}</span>` : ''}
              ${t.isExistingHolding ? `<span class="inv-lt-owned">You own this</span>` : ''}
            </div>
            <div class="inv-trade-cost">
              <div class="inv-trade-cost-amount">${t.totalCostLabel}</div>
              <div class="inv-trade-cost-pct">${t.percentOfBudget}% of budget</div>
            </div>
          </div>
          ${t.whyThisStock ? `<div class="inv-why-stock"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="9" y1="18" x2="15" y2="18"/><line x1="10" y1="22" x2="14" y2="22"/><path d="M15.09 14c.18-.98.65-1.74 1.41-2.5A4.65 4.65 0 0 0 18 8 6 6 0 0 0 6 8c0 1 .23 2.23 1.5 3.5A4.61 4.61 0 0 1 8.91 14"/></svg> ${t.whyThisStock}</div>` : ''}
          <div class="inv-trade-metrics">
            <div class="inv-metric inv-metric-red">
              <div class="inv-metric-label">Max Loss</div>
              <div class="inv-metric-value">$${(t.maxLoss||0).toLocaleString()}</div>
            </div>
            <div class="inv-metric inv-metric-blue">
              <div class="inv-metric-label">Break-Even</div>
              <div class="inv-metric-value">$${t.breakEven}</div>
            </div>
            <div class="inv-metric inv-metric-green">
              <div class="inv-metric-label">Max Gain</div>
              <div class="inv-metric-value">${t.maxGain}</div>
            </div>
            <div class="inv-metric inv-metric-purple">
              <div class="inv-metric-label">Target</div>
              <div class="inv-metric-value">${t.targetReturn}</div>
            </div>
          </div>
          <div class="inv-trade-body">
            <div class="inv-trade-reasoning">${t.reasoning}</div>
            <div class="inv-trade-plan"><span class="inv-plan-label"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg> Entry:</span> ${t.entryNote}</div>
            <div class="inv-trade-plan"><span class="inv-plan-label"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/><path d="M9 18H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h4"/></svg> Exit:</span> ${t.exitPlan}</div>
          </div>
        </div>`;
      }).join('')}

      <!-- Risks + Tips -->
      <div class="inv-bottom-grid">
        <div class="inv-risks-box">
          <div class="inv-box-title"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>️ Key Risks</div>
          ${(d.keyRisks||[]).map(r => `<div class="inv-box-item">• ${r}</div>`).join('')}
        </div>
        <div class="inv-tips-box">
          <div class="inv-box-title"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="9" y1="18" x2="15" y2="18"/><line x1="10" y1="22" x2="14" y2="22"/><path d="M15.09 14c.18-.98.65-1.74 1.41-2.5A4.65 4.65 0 0 0 18 8 6 6 0 0 0 6 8c0 1 .23 2.23 1.5 3.5A4.61 4.61 0 0 1 8.91 14"/></svg> Strategy Tips</div>
          ${(d.tips||[]).map(t => `<div class="inv-box-item">• ${t}</div>`).join('')}
        </div>
      </div>
    </div>`;
}

function invRenderLongtermPlan(output, d) {
  const riskColors = { Low: 'green', Medium: 'yellow', High: 'red' };
  const actionIcons = { ADD: '', NEW: '🆕', WATCHLIST: '' };

  output.innerHTML = `
    <div class="inv-plan">

      <!-- Summary bar -->
      <div class="inv-plan-summary">
        <div class="inv-plan-sum-item">
          <div class="inv-sum-label">Budget</div>
          <div class="inv-sum-value">$${(d.totalBudget||0).toLocaleString()}</div>
        </div>
        <div class="inv-plan-sum-item">
          <div class="inv-sum-label">Allocated</div>
          <div class="inv-sum-value green">$${(d.totalAllocated||0).toLocaleString()}</div>
        </div>
        <div class="inv-plan-sum-item">
          <div class="inv-sum-label">Time Horizon</div>
          <div class="inv-sum-value blue">${d.timeHorizon}</div>
        </div>
        <div class="inv-plan-sum-item">
          <div class="inv-sum-label">Stocks</div>
          <div class="inv-sum-value">${(d.picks||[]).length}</div>
        </div>
        <div class="inv-plan-sum-item">
          <div class="inv-sum-label">Review</div>
          <div class="inv-sum-value" style="font-size:10px;color:var(--text3)">${d.nextReviewDate}</div>
        </div>
      </div>

      ${d.marketContext ? `<div class="inv-market-context"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/></svg> ${d.marketContext}</div>` : ''}
      <div class="inv-plan-strategy">${d.strategy}</div>
      <div class="inv-portfolio-impact"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg> ${d.portfolioImpact}</div>

      <!-- Stock picks -->
      ${(d.picks||[]).map(p => `
        <div class="inv-lt-card">
          <div class="inv-lt-header">
            <div class="inv-lt-rank">${actionIcons[p.action]||'<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 7 13.5 15.5 8.5 10.5 2 17"/><polyline points="16 7 22 7 22 13"/></svg>'}</div>
            <div class="inv-lt-title">
              <span class="inv-lt-ticker">${p.ticker}</span>
              <span class="inv-lt-name">${p.companyName}</span>
              <span class="inv-lt-sector">${p.sector}</span>
              ${p.industry ? `<span class="inv-lt-industry">${p.industry}</span>` : ''}
              ${p.existingPosition ? '<span class="inv-lt-owned">You own this</span>' : ''}
              ${p.alternativeTo && p.alternativeTo !== 'null' ? `<span class="inv-lt-alt">Alt to ${p.alternativeTo}</span>` : ''}
            </div>
            <div class="inv-lt-amount">
              <div class="inv-lt-dollar">$${(p.dollarAmount||0).toLocaleString()}</div>
              <div class="inv-lt-shares">${p.shares}</div>
            </div>
          </div>
          <div class="inv-lt-metrics">
            <div class="inv-metric inv-metric-blue">
              <div class="inv-metric-label">Current Price</div>
              <div class="inv-metric-value">$${p.currentPrice}</div>
            </div>
            <div class="inv-metric inv-metric-green">
              <div class="inv-metric-label">Target Price</div>
              <div class="inv-metric-value">${p.targetPrice}</div>
            </div>
            <div class="inv-metric inv-metric-green">
              <div class="inv-metric-label">Upside</div>
              <div class="inv-metric-value green">${p.upside}</div>
            </div>
            <div class="inv-metric inv-metric-purple">
              <div class="inv-metric-label">Portfolio After</div>
              <div class="inv-metric-value">${p.portfolioAllocationAfter}</div>
            </div>
          </div>
          <div class="inv-trade-body">
            <div class="inv-trade-reasoning">${p.whyNow}</div>
            <div class="inv-trade-plan"><span class="inv-plan-label"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4.5 16.5c-1.5 1.26-2 5-2 5s3.74-.5 5-2c.71-.84.7-2.13-.09-2.91a2.18 2.18 0 0 0-2.91-.09z"/><path d="m12 15-3-3a22 22 0 0 1 2-3.95A12.88 12.88 0 0 1 22 2c0 2.72-.78 7.5-6 11a22.35 22.35 0 0 1-4 2z"/></svg> Catalyst:</span> ${p.growthCatalyst}</div>
            <div class="inv-trade-plan"><span class="inv-plan-label"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>️ Risk:</span> <span class="${riskColors[p.riskLevel]||'yellow'}">${p.riskLevel}</span></div>
          </div>
        </div>`).join('')}

      <!-- Don't buy + Risks -->
      <div class="inv-bottom-grid">
        ${d.dontBuy?.length ? `<div class="inv-risks-box">
          <div class="inv-box-title"> Avoid Right Now</div>
          ${d.dontBuy.map(x => `<div class="inv-box-item"><b>${x.ticker}</b> — ${x.reason}</div>`).join('')}
        </div>` : ''}
        <div class="inv-risks-box">
          <div class="inv-box-title"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>️ Key Risks</div>
          ${(d.keyRisks||[]).map(r => `<div class="inv-box-item">• ${r}</div>`).join('')}
        </div>
        <div class="inv-tips-box">
          <div class="inv-box-title"> Diversification</div>
          <div class="inv-box-item">${d.diversificationNote}</div>
        </div>
      </div>
    </div>`;
}

// Init Ideas & Invest combined tab — sub-tab switching + invest init
document.addEventListener('DOMContentLoaded', () => {
  // Sub-tab switching inside Ideas & Invest tab
  document.querySelectorAll('.ideas-subtab').forEach(btn => {
    btn.addEventListener('click', () => {
      const target = btn.dataset.subtab;
      // Toggle active subtab button
      document.querySelectorAll('.ideas-subtab').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      // Toggle panels
      document.querySelectorAll('.ideas-panel').forEach(p => p.classList.add('hidden'));
      document.getElementById(`ideasPanel-${target}`)?.classList.remove('hidden');
      // Load invest context when switching to invest sub-tab
      if (target === 'invest') {
        invLoadContext();
        if (!btn._invInit) {
          document.getElementById('invGenerateBtn')?.addEventListener('click', invGenerate);
          document.getElementById('invAmount')?.addEventListener('input', invCheckReady);
          btn._invInit = true;
        }
      }
    });
  });
});

window.invSetAmount = invSetAmount;
window.invSelectType = invSelectType;
window.invSelectRisk = invSelectRisk;

// ─── SMART ALERT SYSTEM ───────────────────────────────────────────
const smartAlerts = {
  all: [],
  unseen: 0,
  seenIds: new Set(JSON.parse(localStorage.getItem('sf_seen_alert_ids') || '[]'))
};

function persistSeenIds() {
  const arr = [...smartAlerts.seenIds].slice(-200);
  localStorage.setItem('sf_seen_alert_ids', JSON.stringify(arr));
}

const SEVERITY_CONFIG = {
  critical: { color: 'var(--red)',    bg: 'rgba(255,77,109,0.12)',  border: 'rgba(255,77,109,0.35)',  icon: '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>' },
  warning:  { color: 'var(--yellow)', bg: 'rgba(255,209,102,0.1)',  border: 'rgba(255,209,102,0.3)',  icon: '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>️' },
  info:     { color: 'var(--blue)',   bg: 'rgba(77,159,255,0.08)',  border: 'rgba(77,159,255,0.25)',  icon: '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="9" y1="18" x2="15" y2="18"/><line x1="10" y1="22" x2="14" y2="22"/><path d="M15.09 14c.18-.98.65-1.74 1.41-2.5A4.65 4.65 0 0 0 18 8 6 6 0 0 0 6 8c0 1 .23 2.23 1.5 3.5A4.61 4.61 0 0 1 8.91 14"/></svg>' }
};

const TYPE_ICONS = {
  // Pre-spike signals (new)
  catalyst_news:       '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M8.5 14.5A2.5 2.5 0 0 0 11 12c0-1.38-.5-2-1-3-1.072-2.143-.224-4.054 2-6 .5 2.5 2 4.9 4 6.5 2 1.6 3 3.5 3 5.5a7 7 0 1 1-14 0c0-1.153.433-2.294 1-3a2.5 2.5 0 0 0 2.5 2.5z"/></svg>',
  sec_filing:          '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 5H7a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V7a2 2 0 0 0-2-2h-2"/><rect x="9" y="3" width="6" height="4" rx="1"/></svg>',
  trending:            '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>',
  volume_accumulation: '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg>',
  // Contract alerts (kept)
  expiry:        '⏰',
  otm_warning:   '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>️',
  take_profit:   '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg>',
  deep_otm:      '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>',
  big_move:      '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg>',
  contract_watch:'<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="10" rx="2"/><circle cx="12" cy="5" r="2"/><path d="M12 7v4"/></svg>',
  earnings:      '',
  // Manual scan only (not background)
  market_mover:  '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4.5 16.5c-1.5 1.26-2 5-2 5s3.74-.5 5-2c.71-.84.7-2.13-.09-2.91a2.18 2.18 0 0 0-2.91-.09z"/><path d="m12 15-3-3a22 22 0 0 1 2-3.95A12.88 12.88 0 0 1 22 2c0 2.72-.78 7.5-6 11a22.35 22.35 0 0 1-4 2z"/></svg>',
  news:          '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 22h16a2 2 0 0 0 2-2V4a2 2 0 0 0-2-2H8a2 2 0 0 0-2 2v16a2 2 0 0 0-2 2zm0 0a2 2 0 0 1-2-2v-9c0-1.1.9-2 2-2h2"/></svg>',
  // Legacy (kept for log display)
  price_drop:    '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 17 13.5 8.5 8.5 13.5 2 7"/><polyline points="16 17 22 17 22 11"/></svg>',
  price_spike:   '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 7 13.5 15.5 8.5 10.5 2 17"/><polyline points="16 7 22 7 22 13"/></svg>',
  stop_loss:     '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>',
  watchlist_move:'',
};

// Track which contracts have already triggered AI watch advice this session
const contractWatchCache = new Map();

// Listen for background alerts from main process (fires even when window was closed)
window.electronAPI?.onBackgroundAlert?.((alert) => {
  if (!alert?.id) return;
  if (smartAlerts.seenIds.has(alert.id)) return;
  smartAlerts.all.unshift(alert);
  smartAlerts.unseen++;
  smartAlerts.seenIds.add(alert.id);
  updateAlertBadge();
  showAlertToast(alert);
  persistAlert(alert); // save to log
  if (!document.getElementById('alertPanel')?.classList.contains('hidden')) renderAlertPanel();
});

// Run smart alert scan — called after every price refresh
async function runSmartAlertScan() {
  if (!appData) return;
  try {
    const res = await fetch('/api/smartalerts/scan', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        portfolio: appData.longterm.portfolio || [],
        contracts: appData.daytrading.contracts || [],
        watchlist: [...(appData.daytrading.watchlist || []), ...(appData.longterm.watchlist || [])],
        prices,
        seenIds: [...smartAlerts.seenIds] // tell server which alerts we already know about
      })
    });
    const data = await res.json();
    // Server already filtered out seenIds — these are all new
    const newAlerts = data.alerts || [];

    if (newAlerts.length > 0) {
      smartAlerts.all = [...newAlerts, ...smartAlerts.all].slice(0, 30);
      smartAlerts.unseen += newAlerts.length;
      newAlerts.forEach(a => smartAlerts.seenIds.add(a.id));
      persistSeenIds(); // save to localStorage so they survive restarts

      updateAlertBadge();

      newAlerts.forEach(a => {
        // macOS notification: critical only
        if (a.severity === 'critical') {
          window.electronAPI?.notify?.(a.title, a.body);
        }
        // In-app toast: critical + warning only (not info — those just go to bell panel)
        if (a.severity === 'critical' || a.severity === 'warning') {
          showAlertToast(a);
        }
      });

      if (!document.getElementById('alertPanel')?.classList.contains('hidden')) {
        renderAlertPanel();
      }
    }

    // Update scan info
    const scanEl = document.getElementById('alertScanInfo');
    if (scanEl) {
      const t = new Date(data.scannedAt).toLocaleTimeString();
      scanEl.textContent = `Last scanned: ${t} · ${(appData.longterm.portfolio||[]).length} positions monitored`;
    }

  } catch (e) {
    console.error('Smart alert scan failed:', e.message);
  }
}

// AI-powered contract watch — fires when a threshold alert is triggered
async function runContractWatch(newAlerts) {
  if (!appData?.daytrading?.contracts?.length) return;

  const actionableTypes = new Set(['expiry', 'otm_warning', 'take_profit', 'deep_otm']);
  const triggeredContracts = new Map(); // contractId → trigger type

  // Find which contracts have actionable alerts this scan
  for (const alert of newAlerts) {
    if (!actionableTypes.has(alert.type)) continue;
    const contract = appData.daytrading.contracts.find(c =>
      c.ticker === alert.ticker && (!c.id || alert.id.includes(c.id) || alert.id.includes(c.ticker))
    );
    if (!contract) continue;
    const cid = contract.id || contract.ticker;

    // Only call AI once per contract per 2 hours
    const cached = contractWatchCache.get(cid);
    if (cached && (Date.now() - cached.triggeredAt) < 2 * 60 * 60 * 1000) continue;

    triggeredContracts.set(cid, { contract, trigger: alert.type });
  }

  if (!triggeredContracts.size) return;

  // Fire AI watch calls for triggered contracts (max 2 at a time to avoid spam)
  const toWatch = [...triggeredContracts.entries()].slice(0, 2);

  for (const [cid, { contract, trigger }] of toWatch) {
    const stockPrice = prices[contract.ticker]?.price || 0;
    if (!stockPrice) continue;

    try {
      const res = await fetch('/api/contracts/watch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ contract, currentPrice: stockPrice, trigger })
      });
      if (!res.ok) continue;
      const advice = await res.json();

      // Cache it
      contractWatchCache.set(cid, { triggeredAt: Date.now(), advice });

      // Build alert from AI advice
      const urgencyColor = advice.urgency === 'Immediate' ? 'critical' : advice.urgency === 'Today' ? 'warning' : 'info';
      const alertId = `watch-${cid}-${new Date().toISOString().split('T')[0]}-${trigger}`;

      if (!smartAlerts.seenIds.has(alertId)) {
        const watchAlert = {
          id: alertId,
          type: 'contract_watch',
          severity: urgencyColor,
          ticker: contract.ticker,
          title: `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="10" rx="2"/><circle cx="12" cy="5" r="2"/><path d="M12 7v4"/></svg> ${contract.ticker} ${contract.type}: ${advice.action}`,
          body: advice.reasoning || `${advice.urgency} action needed on your $${contract.strike} ${contract.type}`,
          steps: advice.steps || [],
          exitTarget: advice.exitTarget,
          stopLoss: advice.stopLoss,
          action: advice.action,
          timestamp: new Date().toISOString()
        };

        smartAlerts.all.unshift(watchAlert);
        smartAlerts.unseen++;
        smartAlerts.seenIds.add(alertId);
        updateAlertBadge();

        // macOS notification
        window.electronAPI?.notify?.(
          watchAlert.title,
          `${advice.reasoning} | Steps: ${(advice.steps || []).slice(0, 1).join('')}`
        );

        // In-app toast
        showAlertToast(watchAlert);
        // Persist to log
        persistAlert(watchAlert);

        // Refresh alert panel if open
        if (!document.getElementById('alertPanel')?.classList.contains('hidden')) {
          renderAlertPanel();
        }
      }
    } catch (e) {
      console.error('Contract watch AI call failed:', e.message);
    }
  }
}

function updateAlertBadge() {
  const badge = document.getElementById('alertBellBadge');
  const btn = document.getElementById('alertBellBtn');
  if (!badge) return;
  if (smartAlerts.unseen > 0) {
    badge.textContent = smartAlerts.unseen > 9 ? '9+' : smartAlerts.unseen;
    badge.classList.remove('hidden');
    btn?.classList.add('has-alerts');
  } else {
    badge.classList.add('hidden');
    btn?.classList.remove('has-alerts');
  }
}

function showAlertToast(alert) {
  const container = document.getElementById('toastContainer');
  if (!container) return;
  const cfg = SEVERITY_CONFIG[alert.severity] || SEVERITY_CONFIG.info;
  const icon = TYPE_ICONS[alert.type] || '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>';
  const toast = document.createElement('div');
  toast.className = `alert-toast alert-toast-${alert.severity}`;
  toast.style.cssText = `background:${cfg.bg};border-color:${cfg.border}`;
  toast.innerHTML = `
    <div class="alert-toast-header">
      <span class="alert-toast-icon">${icon}</span>
      <span class="alert-toast-title">${alert.title}</span>
      <button class="alert-toast-close" onclick="this.closest('.alert-toast').remove()"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg></button>
    </div>
    <div class="alert-toast-body">${alert.body}</div>
    ${alert.action ? `<button class="alert-toast-action" onclick="openAlertPanel();this.closest('.alert-toast').remove()">${alert.action} →</button>` : ''}`;
  container.appendChild(toast);
  // Animate in
  requestAnimationFrame(() => toast.classList.add('visible'));
  // Auto-dismiss after 8s (critical stays 12s)
  const duration = alert.severity === 'critical' ? 12000 : 8000;
  setTimeout(() => {
    toast.classList.remove('visible');
    setTimeout(() => toast.remove(), 400);
  }, duration);
}

function renderAlertPanel() {
  const body = document.getElementById('alertPanelBody');
  if (!body) return;
  if (!smartAlerts.all.length) {
    body.innerHTML = `<div class="alert-empty"><div style="font-size:24px;margin-bottom:8px"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg></div><div>No alerts yet. Alerts appear here when significant events are detected.</div></div>`;
    return;
  }
  body.innerHTML = smartAlerts.all.map(a => {
    const cfg = SEVERITY_CONFIG[a.severity] || SEVERITY_CONFIG.info;
    const icon = TYPE_ICONS[a.type] || '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>';
    const time = new Date(a.timestamp).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });

    // AI contract watch alerts get expanded step-by-step display
    const stepsHtml = (a.type === 'contract_watch' && a.steps?.length)
      ? `<div class="alert-steps">
          <div class="alert-steps-label">Action Steps:</div>
          ${a.steps.map((s, i) => `<div class="alert-step"><span class="alert-step-num">${i + 1}</span>${s}</div>`).join('')}
          ${a.exitTarget ? `<div class="alert-step-meta"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="6"/><circle cx="12" cy="12" r="2"/></svg> Exit target: ${a.exitTarget}</div>` : ''}
          ${a.stopLoss ? `<div class="alert-step-meta">■ Stop loss: ${a.stopLoss}</div>` : ''}
        </div>`
      : '';

    return `
      <div class="alert-item alert-item-${a.severity}" style="border-left-color:${cfg.color}">
        <div class="alert-item-header">
          <span class="alert-item-icon">${icon}</span>
          <span class="alert-item-title" style="color:${cfg.color}">${a.title}</span>
          <span class="alert-item-time">${time}</span>
        </div>
        <div class="alert-item-body">${a.body}</div>
        ${stepsHtml}
        ${a.ticker ? `<div class="alert-item-footer">
          <span class="alert-item-ticker">${a.ticker}</span>
          <span class="alert-item-type">${a.type.replace(/_/g,' ')}</span>
        </div>` : ''}
      </div>`;
  }).join('');
}

function openAlertPanel() {
  const panel = document.getElementById('alertPanel');
  panel.classList.remove('hidden');
  smartAlerts.unseen = 0;
  updateAlertBadge();
  renderAlertPanel();
}

function closeAlertPanel() {
  document.getElementById('alertPanel')?.classList.add('hidden');
}

// Wire up bell button and panel controls
document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('alertBellBtn')?.addEventListener('click', () => {
    const panel = document.getElementById('alertPanel');
    if (panel.classList.contains('hidden')) openAlertPanel();
    else closeAlertPanel();
  });
  document.getElementById('alertPanelClose')?.addEventListener('click', closeAlertPanel);

  // Market movers scan button
  document.getElementById('scanMoversBtn')?.addEventListener('click', async () => {
    const btn = document.getElementById('scanMoversBtn');
    const threshold = document.getElementById('moversThreshold')?.value || 10;
    btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/></svg> Scanning...';
    btn.disabled = true;
    try {
      const res = await fetch(`/api/market/movers?threshold=${threshold}`);
      const data = await res.json();
      const movers = data.movers || [];
      const unusualVol = data.unusualVolume || [];

      if (!movers.length && !unusualVol.length) {
        // No toast — just update the panel silently
      } else {
        const ts = new Date().toISOString();
        const slot = ts.slice(0, 16);

        // Add movers to alert panel
        for (const m of movers) {
          const alertId = `mover-manual-${m.ticker}-${slot}`;
          if (smartAlerts.seenIds.has(alertId)) continue;
          const dir = m.changePct > 0 ? '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4.5 16.5c-1.5 1.26-2 5-2 5s3.74-.5 5-2c.71-.84.7-2.13-.09-2.91a2.18 2.18 0 0 0-2.91-.09z"/><path d="m12 15-3-3a22 22 0 0 1 2-3.95A12.88 12.88 0 0 1 22 2c0 2.72-.78 7.5-6 11a22.35 22.35 0 0 1-4 2z"/></svg>' : '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 17 13.5 8.5 8.5 13.5 2 7"/><polyline points="16 17 22 17 22 11"/></svg>';
          const absPct = Math.abs(m.changePct);
          const severity = absPct >= 50 ? 'critical' : absPct >= 20 ? 'warning' : 'info';
          const alert = {
            id: alertId, type: 'market_mover', severity, ticker: m.ticker,
            title: `${dir} ${m.ticker} ${m.changePct > 0 ? '+' : ''}${m.changePct.toFixed(1)}%`,
            body: `${m.name || m.ticker} — $${m.price.toFixed(2)} (was $${m.prevClose.toFixed(2)}) · Vol: ${m.volRatio}x avg`,
            changePct: m.changePct, price: m.price, timestamp: ts
          };
          smartAlerts.all.unshift(alert);
          smartAlerts.unseen++;
          smartAlerts.seenIds.add(alertId);
          persistAlert(alert);
        }

        // Add unusual volume alerts
        for (const m of unusualVol) {
          const alertId = `vol-manual-${m.ticker}-${slot}`;
          if (smartAlerts.seenIds.has(alertId)) continue;
          const alert = {
            id: alertId, type: 'big_move', severity: 'info', ticker: m.ticker,
            title: `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg> ${m.ticker} unusual volume — ${m.volRatio}x avg`,
            body: `${m.name || m.ticker} trading ${m.volRatio}x normal volume with ${m.changePct > 0 ? '+' : ''}${m.changePct.toFixed(1)}% move — worth watching`,
            changePct: m.changePct, timestamp: ts
          };
          smartAlerts.all.unshift(alert);
          smartAlerts.unseen++;
          smartAlerts.seenIds.add(alertId);
          persistAlert(alert);
        }

        updateAlertBadge();
        renderAlertPanel();
        // No toast — results visible in alert panel
      }
    } catch (e) {
      console.error('Market scan failed:', e.message);
    } finally {
      btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4.5 16.5c-1.5 1.26-2 5-2 5s3.74-.5 5-2c.71-.84.7-2.13-.09-2.91a2.18 2.18 0 0 0-2.91-.09z"/><path d="m12 15-3-3a22 22 0 0 1 2-3.95A12.88 12.88 0 0 1 22 2c0 2.72-.78 7.5-6 11a22.35 22.35 0 0 1-4 2z"/></svg> Scan Market Movers Now';
      btn.disabled = false;
    }
  });

  document.getElementById('alertClearAll')?.addEventListener('click', () => {
    smartAlerts.all = [];
    smartAlerts.unseen = 0;
    // Also clear seen IDs so fresh alerts can fire again
    smartAlerts.seenIds.clear();
    localStorage.removeItem('sf_seen_alert_ids');
    updateAlertBadge();
    renderAlertPanel();
  });
  // Close panel on outside click
  document.addEventListener('click', e => {
    const panel = document.getElementById('alertPanel');
    const bell = document.getElementById('alertBellBtn');
    if (!panel?.contains(e.target) && !bell?.contains(e.target)) {
      panel?.classList.add('hidden');
    }
  });
});

// Smart alert scan is now hooked directly inside trackedRefresh()

// ─── EDIT CRYPTO HOLDING ──────────────────────────────────────────
let editingCryptoTicker = null;

function openEditCrypto(e, ticker) {
  e.stopPropagation();
  editingCryptoTicker = ticker;
  const holding = appData?.longterm?.cryptoPortfolio?.find(s => s.ticker === ticker);
  if (!holding) return;
  document.getElementById('editCryptoTickerLabel').textContent = `₿ ${ticker}`;
  document.getElementById('editCryptoCoins').value = holding.coins;
  document.getElementById('editCryptoAvgCost').value = holding.avgCost;
  document.getElementById('editCryptoDateBought').value = holding.dateBought || '';
  document.getElementById('editCryptoExchange').value = holding.exchange || '';
  document.getElementById('editCryptoNotes').value = holding.notes || '';
  updateEditCryptoPreview(holding);
  openModal('modalEditCrypto');
}

function updateEditCryptoPreview(holding) {
  const preview = document.getElementById('editCryptoPreview');
  if (!preview) return;
  const p = cryptoPrices[holding.ticker]?.price || 0;
  const coins = parseFloat(document.getElementById('editCryptoCoins')?.value) || holding.coins;
  const avgCost = parseFloat(document.getElementById('editCryptoAvgCost')?.value) || holding.avgCost;
  const invested = coins * avgCost;
  const value = p > 0 ? coins * p : 0;
  const pnl = value - invested;
  const pnlPct = invested > 0 ? ((pnl / invested) * 100).toFixed(1) : 0;
  preview.innerHTML = `
    <div class="edit-preview-grid">
      <div class="edit-preview-item">
        <div class="edit-preview-label">Total Invested</div>
        <div class="edit-preview-value">$${invested.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</div>
      </div>
      ${p > 0 ? `
      <div class="edit-preview-item">
        <div class="edit-preview-label">Current Value</div>
        <div class="edit-preview-value">$${value.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</div>
      </div>
      <div class="edit-preview-item">
        <div class="edit-preview-label">P&L</div>
        <div class="edit-preview-value ${pnl >= 0 ? 'green' : 'red'}">${pnl >= 0 ? '+' : ''}$${Math.abs(pnl).toFixed(2)} (${pnlPct}%)</div>
      </div>` : ''}
      <div class="edit-preview-item">
        <div class="edit-preview-label">Current Price</div>
        <div class="edit-preview-value">${p > 0 ? fmtCrypto(p) : '—'}</div>
      </div>
    </div>`;
}

document.addEventListener('DOMContentLoaded', () => {
  ['editCryptoCoins', 'editCryptoAvgCost'].forEach(id => {
    document.getElementById(id)?.addEventListener('input', () => {
      if (editingCryptoTicker) {
        const h = appData?.longterm?.cryptoPortfolio?.find(s => s.ticker === editingCryptoTicker);
        if (h) updateEditCryptoPreview(h);
      }
    });
  });

  document.getElementById('editCryptoConfirm')?.addEventListener('click', async () => {
    if (!editingCryptoTicker) return;
    const coins = parseFloat(document.getElementById('editCryptoCoins').value);
    const avgCost = parseFloat(document.getElementById('editCryptoAvgCost').value);
    if (!coins || !avgCost) { alert('Quantity and Avg Buy Price are required.'); return; }
    const btn = document.getElementById('editCryptoConfirm');
    btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg> Saving...'; btn.disabled = true;
    await fetch('/api/longterm/crypto/portfolio/add', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        ticker: editingCryptoTicker, coins, avgCost,
        dateBought: document.getElementById('editCryptoDateBought').value,
        exchange: document.getElementById('editCryptoExchange').value,
        notes: document.getElementById('editCryptoNotes').value
      })
    });
    closeModal('modalEditCrypto');
    btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg> Save Changes'; btn.disabled = false;
    editingCryptoTicker = null;
    await loadData();
    await trackedRefresh();
  });
});

window.openEditCrypto = openEditCrypto;

// ═══════════════════════════════════════════════════════════════════
// CREW SIGNAL — Multi-Agent Signal Pipeline
// Calls /api/ai/signal/crew instead of /api/ai/signal
// Shows agent breakdown before final signal
// ═══════════════════════════════════════════════════════════════════

async function generateCrewSignal() {
  const ticker = document.getElementById('dtSignalTicker').value;
  if (!ticker) return alert('Please select a stock first.');

  const outputEl = document.getElementById('dtSignalOutput');
  const loadingEl = document.getElementById('dtSignalLoading');
  const cardEl = document.getElementById('dtSignalCard');
  const btn = document.getElementById('dtCrewSignalBtn');

  outputEl.classList.remove('hidden');
  loadingEl.classList.remove('hidden');
  cardEl.classList.add('hidden');
  btn.disabled = true;

  try {
    let priceData = prices[ticker] || {};
    if (!priceData.price) {
      try {
        const pr = await fetch(`/api/price/${ticker}`);
        priceData = await pr.json();
        if (priceData.price > 0) prices[ticker] = priceData;
      } catch {}
    }

    let news = [];
    try {
      const newsRes = await fetch(`/api/news/${ticker}`);
      const newsData = await newsRes.json();
      news = Array.isArray(newsData) ? newsData : [];
    } catch {}

    // Show agent breakdown container
    cardEl.innerHTML = `<div id="crewAgentBreakdown" class="crew-breakdown"></div>`;
    cardEl.classList.remove('hidden');

    await readSSE('/api/ai/signal/crew', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ticker, price: priceData.price, change: priceData.change, changePct: priceData.changePct, news })
    }, (msg) => {
          if (msg.type === 'status') {
            document.getElementById('dtSignalStatus').textContent = msg.text;
          } else if (msg.type === 'agent') {
            renderCrewAgentCard(msg.agent, msg.data);
          } else if (msg.type === 'signal') {
            // Validate signal before rendering — guard against empty/partial responses
            const s = msg.data;
            if (!s.action || !s.strike || s.confidence === undefined) {
              loadingEl.classList.add('hidden');
              cardEl.innerHTML = `<div class="empty-state red">⚠ AI returned incomplete signal. Try again or check your AI provider in Settings.</div>`;
              return;
            }
            loadingEl.classList.add('hidden');
            const breakdown = document.getElementById('crewAgentBreakdown');
            const signalDiv = document.createElement('div');
            signalDiv.innerHTML = `<div class="crew-badge"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="10" rx="2"/><circle cx="12" cy="5" r="2"/><path d="M12 7v4"/></svg> CREW SIGNAL — 3-Agent Consensus</div>`;
            if (s.agentConsensus) signalDiv.innerHTML += `<div class="crew-consensus">${s.agentConsensus}</div>`;
            if (s.validationWarnings?.length) signalDiv.innerHTML += `<div class="crew-warnings">Validation notes: ${s.validationWarnings.join(', ')}</div>`;
            cardEl.innerHTML = '';
            cardEl.appendChild(signalDiv);
            renderSignalCard(cardEl, s, ticker, priceData.price);
            saveSignalToHistory(ticker, s, priceData.price);
          } else if (msg.type === 'error') {
            loadingEl.classList.add('hidden');
            cardEl.innerHTML = `<div class="empty-state red">⚠ ${msg.text}</div>`;
          }
    });
  } catch (e) {
    loadingEl.classList.add('hidden');
    cardEl.innerHTML = `<div class="empty-state red">Failed: ${e.message}</div>`;
  } finally {
    btn.disabled = false;
  }
}
        } catch {}
      }
    }
  } catch (e) {
    loadingEl.classList.add('hidden');
    cardEl.innerHTML = `<div class="empty-state red">Failed: ${e.message}</div>`;
  } finally {
    btn.disabled = false;
  }
}

function renderCrewAgentCard(agentName, data) {
  const breakdown = document.getElementById('crewAgentBreakdown');
  if (!breakdown) return;
  const icons = { Technical: '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg>', Fundamental: '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 22h16a2 2 0 0 0 2-2V4a2 2 0 0 0-2-2H8a2 2 0 0 0-2 2v16a2 2 0 0 0-2 2zm0 0a2 2 0 0 1-2-2v-9c0-1.1.9-2 2-2h2"/></svg>', Risk: '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>️' };
  const dirColor = (d) => d === 'Bullish' || d === 'Positive' || d === 'PROCEED' ? 'green' : d === 'Bearish' || d === 'Negative' || d === 'SKIP' ? 'red' : 'yellow';
  const dir = data.direction || data.sentiment || data.recommendation || '—';
  const conf = data.confidence || 0;
  const card = document.createElement('div');
  card.className = 'crew-agent-card';
  card.innerHTML = `
    <div class="crew-agent-header">
      <span class="crew-agent-icon">${icons[agentName] || '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="10" rx="2"/><circle cx="12" cy="5" r="2"/><path d="M12 7v4"/></svg>'}</span>
      <span class="crew-agent-name">${agentName} Agent</span>
      <span class="crew-agent-dir ${dirColor(dir)}">${dir}</span>
      <span class="crew-agent-conf">${conf}%</span>
    </div>
    <div class="crew-agent-signals">
      ${([...(data.signals || []), ...(data.catalysts || []), ...(data.warnings || [])]).slice(0, 2).map(s => `<div class="crew-agent-signal">• ${s}</div>`).join('')}
    </div>`;
  breakdown.appendChild(card);
}

// Save signal to history after generation
async function saveSignalToHistory(ticker, signal, stockPrice) {
  try {
    await fetch('/api/signals/save', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        ticker,
        action: signal.action,
        strike: signal.strike,
        expiry: signal.expiry,
        stockPriceAtSignal: stockPrice,
        confidence: signal.confidence,
        direction: signal.direction,
        isCrypto: false
      })
    });
  } catch {}
}

// ═══════════════════════════════════════════════════════════════════
// SIGNAL CONVERGENCE SCANNER — Scans watchlist for multi-signal setups
// ═══════════════════════════════════════════════════════════════════

async function runConvergenceScan() {
  const container = document.getElementById('convergenceScanBody');
  if (!container) return;
  const btn = document.getElementById('convergenceScanBtn');
  if (btn) { btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/></svg> Scanning...'; btn.disabled = true; }
  container.innerHTML = `<div class="loading-row"><div class="spinner"></div><span>Scanning watchlist for signal convergence...</span></div>`;

  try {
    const tickers = [
      ...(appData?.daytrading?.watchlist || []).map(s => s.ticker),
      ...(appData?.longterm?.portfolio || []).map(s => s.ticker)
    ];
    const unique = [...new Set(tickers)].slice(0, 10); // limit to 10 to avoid rate limits

    if (!unique.length) {
      container.innerHTML = `<div class="empty-state">Add stocks to your watchlist first.</div>`;
      return;
    }

    const results = await Promise.all(unique.map(ticker =>
      fetch(`/api/signals/convergence/${ticker}`).then(r => r.json()).catch(() => null)
    ));

    const valid = results.filter(Boolean).sort((a, b) => b.score - a.score);

    container.innerHTML = valid.map(r => `
      <div class="convergence-row">
        <div class="convergence-ticker">${r.ticker}</div>
        <div class="convergence-score-wrap">
          <div class="convergence-bar-bg">
            <div class="convergence-bar" style="width:${r.score}%;background:${r.convergenceColor === 'green' ? 'var(--green)' : r.convergenceColor === 'yellow' ? 'var(--yellow)' : 'var(--red)'}"></div>
          </div>
          <div class="convergence-score ${r.convergenceColor}">${r.score}</div>
          <div class="convergence-level ${r.convergenceColor}">${r.convergenceLevel}</div>
        </div>
        <div class="convergence-signals">
          ${r.signals.map(s => `<span class="convergence-signal-tag convergence-signal-${s.type}">${s.label}</span>`).join('')}
        </div>
        ${r.score >= 35 ? `<button class="btn-secondary btn-sm" onclick="selectDtStock('${r.ticker}');switchToTab('daytrading')"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg> Signal</button>` : ''}
      </div>
    `).join('') || `<div class="empty-state">No signals found across watchlist.</div>`;

  } catch (e) {
    container.innerHTML = `<div class="empty-state red">Scan failed: ${e.message}</div>`;
  } finally {
    if (btn) { btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg> Scan Now'; btn.disabled = false; }
  }
}
window.runConvergenceScan = runConvergenceScan;

// ═══════════════════════════════════════════════════════════════════
// SIGNAL SCORECARD — Shows accuracy stats in Journal tab
// ═══════════════════════════════════════════════════════════════════

async function loadSignalScorecard() {
  const container = document.getElementById('signalScorecardBody');
  if (!container) return;
  container.innerHTML = `<div class="loading-row">Loading scorecard...</div>`;

  try {
    // Score any pending signals first
    await fetch('/api/signals/score', { method: 'POST' });

    const [scorecardRes, historyRes] = await Promise.all([
      fetch('/api/signals/scorecard'),
      fetch('/api/signals/history')
    ]);
    const scorecard = await scorecardRes.json();
    const history = await historyRes.json();

    if (!scorecard.total) {
      container.innerHTML = `<div class="empty-state">No signals tracked yet. Generate signals to start tracking accuracy.</div>`;
      return;
    }

    const winColor = scorecard.winRate >= 60 ? 'green' : scorecard.winRate >= 40 ? 'yellow' : 'red';

    container.innerHTML = `
      <!-- Stats Row -->
      <div class="scorecard-stats">
        <div class="scorecard-stat">
          <div class="scorecard-stat-label">Win Rate</div>
          <div class="scorecard-stat-value ${winColor}">${scorecard.winRate}%</div>
        </div>
        <div class="scorecard-stat">
          <div class="scorecard-stat-label">Correct</div>
          <div class="scorecard-stat-value green">${scorecard.correct}</div>
        </div>
        <div class="scorecard-stat">
          <div class="scorecard-stat-label">Wrong</div>
          <div class="scorecard-stat-value red">${scorecard.wrong}</div>
        </div>
        <div class="scorecard-stat">
          <div class="scorecard-stat-label">Neutral</div>
          <div class="scorecard-stat-value yellow">${scorecard.neutral}</div>
        </div>
        <div class="scorecard-stat">
          <div class="scorecard-stat-label">Pending</div>
          <div class="scorecard-stat-value">${scorecard.unscored}</div>
        </div>
        <div class="scorecard-stat">
          <div class="scorecard-stat-label">Total</div>
          <div class="scorecard-stat-value">${scorecard.total}</div>
        </div>
      </div>

      <!-- Per-Ticker Breakdown -->
      ${Object.keys(scorecard.byTicker).length ? `
      <div class="scorecard-section-title">Per-Ticker Accuracy</div>
      <div class="scorecard-ticker-grid">
        ${Object.entries(scorecard.byTicker).map(([t, s]) => {
          const wr = Math.round((s.correct / s.total) * 100);
          const wc = wr >= 60 ? 'green' : wr >= 40 ? 'yellow' : 'red';
          return `<div class="scorecard-ticker-item">
            <span class="scorecard-ticker-name">${t}</span>
            <span class="scorecard-ticker-wr ${wc}">${wr}%</span>
            <span class="scorecard-ticker-detail">${s.correct}W/${s.wrong}L/${s.neutral}N</span>
          </div>`;
        }).join('')}
      </div>` : ''}

      <!-- Recent Signal History -->
      <div class="scorecard-section-title">Recent Signals</div>
      <div class="scorecard-history">
        ${history.slice(0, 10).map(s => {
          const outcomeIcon = s.scored
            ? (s.outcome === 'correct' ? '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>' : s.outcome === 'wrong' ? '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>' : '')
            : '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/></svg>';
          const outcomeColor = s.scored
            ? (s.outcome === 'correct' ? 'green' : s.outcome === 'wrong' ? 'red' : 'yellow')
            : '';
          const date = s.generatedAt ? new Date(s.generatedAt).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) : '—';
          const action = s.action || 'SIGNAL';
          const actionColor = action.includes('CALL') ? 'green' : action.includes('PUT') ? 'red' : 'yellow';
          const entryPrice = s.stockPriceAtSignal ? `$${Number(s.stockPriceAtSignal).toFixed(0)}` : '—';
          const exitPrice = s.stockPriceAtScore ? `$${Number(s.stockPriceAtScore).toFixed(0)}` : '—';
          const diffPct = s.priceDiffPct != null ? `${s.priceDiffPct > 0 ? '+' : ''}${s.priceDiffPct}%` : '';
          return `<div class="scorecard-history-row">
            <span class="scorecard-h-icon">${outcomeIcon}</span>
            <span class="scorecard-h-ticker">${s.ticker || '—'}</span>
            <span class="scorecard-h-action ${actionColor}">${action}</span>
            <span class="scorecard-h-price">@ ${entryPrice}</span>
            ${s.scored
              ? `<span class="scorecard-h-result ${outcomeColor}">→ ${exitPrice} (${diffPct})</span>`
              : '<span class="scorecard-h-pending">Pending score</span>'}
            <span class="scorecard-h-date">${date}</span>
            <button class="wl-remove" onclick="deleteSignalHistory('${s.id}')"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg></button>
          </div>`;
        }).join('')}
      </div>`;
  } catch (e) {
    container.innerHTML = `<div class="empty-state red">Failed to load scorecard: ${e.message}</div>`;
  }
}

async function deleteSignalHistory(id) {
  await fetch(`/api/signals/history/${id}`, { method: 'DELETE' });
  loadSignalScorecard();
}

// Wire scorecard load when journal tab opens
document.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('.tab-btn').forEach(btn => {
    if (btn.dataset.tab === 'journal') {
      btn.addEventListener('click', () => {
        setTimeout(loadSignalScorecard, 300);
      });
    }
  });

  // Wire crew signal button if it exists
  document.getElementById('dtCrewSignalBtn')?.addEventListener('click', generateCrewSignal);
});

window.deleteSignalHistory = deleteSignalHistory;
window.generateCrewSignal = generateCrewSignal;
window.openEditContract = openEditContract;

// ═══════════════════════════════════════════════════════════════════
// ALERT LOG TAB
// ═══════════════════════════════════════════════════════════════════

let alertLogData = [];
let alertLogInit = false;

async function loadAlertLog() {
  const list = document.getElementById('alertLogList');
  const statsEl = document.getElementById('alStats');
  if (!list) return;

  const type = document.getElementById('alFilterType')?.value || '';
  const severity = document.getElementById('alFilterSeverity')?.value || '';
  const ticker = document.getElementById('alFilterTicker')?.value?.trim().toUpperCase() || '';

  try {
    const params = new URLSearchParams();
    if (type) params.set('type', type);
    if (severity) params.set('severity', severity);
    if (ticker) params.set('ticker', ticker);
    params.set('limit', '200');

    const res = await fetch(`/api/alerts/log?${params}`);
    alertLogData = await res.json();
    renderAlertLog();

    // Stats
    const total = alertLogData.length;
    const critical = alertLogData.filter(a => a.severity === 'critical').length;
    const warning = alertLogData.filter(a => a.severity === 'warning').length;
    if (statsEl) statsEl.innerHTML = `
      <span class="al-stat">${total} total</span>
      <span class="al-stat red">${critical} critical</span>
      <span class="al-stat yellow">${warning} warnings</span>
    `;
  } catch (e) {
    list.innerHTML = `<div class="empty-state red">Failed to load: ${e.message}</div>`;
  }
}

function renderAlertLog() {
  const list = document.getElementById('alertLogList');
  if (!list) return;

  if (!alertLogData.length) {
    list.innerHTML = `<div class="empty-state">
      <div style="font-size:28px;margin-bottom:8px"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg></div>
      <div>No alerts logged yet.</div>
      <div style="font-size:11px;color:var(--text3);margin-top:6px">Alerts are saved automatically when detected — from price scans, contract monitoring, and market movers.</div>
    </div>`;
    return;
  }

  // Group by date
  const groups = {};
  for (const a of alertLogData) {
    const date = new Date(a.loggedAt || a.timestamp).toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' });
    if (!groups[date]) groups[date] = [];
    groups[date].push(a);
  }

  list.innerHTML = Object.entries(groups).map(([date, alerts]) => `
    <div class="al-date-group">
      <div class="al-date-header">${date} <span class="al-date-count">${alerts.length}</span></div>
      ${alerts.map(a => {
        const icon = TYPE_ICONS[a.type] || '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>';
        const cfg = SEVERITY_CONFIG[a.severity] || SEVERITY_CONFIG.info;
        const time = new Date(a.loggedAt || a.timestamp).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
        const stepsHtml = (a.steps?.length)
          ? `<div class="al-steps">${a.steps.map(s => `<div class="al-step">• ${s}</div>`).join('')}</div>`
          : '';
        return `
          <div class="al-item al-item-${a.severity}" style="border-left-color:${cfg.color}">
            <div class="al-item-header">
              <span class="al-item-icon">${icon}</span>
              <span class="al-item-title" style="color:${cfg.color}">${a.title}</span>
              <span class="al-item-time">${time}</span>
              <button class="al-item-delete" onclick="deleteAlertLogEntry('${a.id}')" title="Remove"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg></button>
            </div>
            <div class="al-item-body">${a.body}</div>
            ${stepsHtml}
            <div class="al-item-footer">
              ${a.ticker ? `<span class="al-item-ticker">${a.ticker}</span>` : ''}
              <span class="al-item-type">${(a.type || '').replace(/_/g,' ')}</span>
              <span class="al-item-sev al-sev-${a.severity}">${a.severity}</span>
            </div>
          </div>`;
      }).join('')}
    </div>
  `).join('');
}

async function deleteAlertLogEntry(id) {
  await fetch(`/api/alerts/log/${encodeURIComponent(id)}`, { method: 'DELETE' });
  alertLogData = alertLogData.filter(a => a.id !== id);
  renderAlertLog();
}

async function clearAlertLog() {
  if (!confirm('Clear all alert log entries?')) return;
  await fetch('/api/alerts/log', { method: 'DELETE' });
  alertLogData = [];
  renderAlertLog();
  document.getElementById('alStats').innerHTML = '';
}

// Save alert to server log whenever one fires in-app
async function persistAlert(alert) {
  try {
    await fetch('/api/alerts/log', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(alert)
    });
  } catch {}
}

window.deleteAlertLogEntry = deleteAlertLogEntry;

// Wire Alert Log tab
document.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('.tab-btn').forEach(btn => {
    if (btn.dataset.tab === 'alertlog') {
      btn.addEventListener('click', () => {
        if (!alertLogInit) {
          document.getElementById('alRefreshBtn')?.addEventListener('click', loadAlertLog);
          document.getElementById('alClearBtn')?.addEventListener('click', clearAlertLog);
          ['alFilterType','alFilterSeverity','alFilterTicker'].forEach(id => {
            document.getElementById(id)?.addEventListener('change', loadAlertLog);
            document.getElementById(id)?.addEventListener('input', loadAlertLog);
          });
          alertLogInit = true;
        }
        loadAlertLog();
      });
    }
  });
});

// ─── PAPER TRADING ────────────────────────────────────────────────
let paperAccount = null;

// Estimate current option premium based on stock move + time decay
function estimatePaperPremium(p, stockPrice) {
  if (!stockPrice || stockPrice <= 0) return p.premium;
  const isCall = p.type === 'CALL';
  // Intrinsic value now
  const intrinsic = isCall
    ? Math.max(0, stockPrice - p.strike)
    : Math.max(0, p.strike - stockPrice);
  // Intrinsic at entry
  const entryPrice = p.stockPriceAtEntry || stockPrice;
  const intrinsicAtEntry = isCall
    ? Math.max(0, entryPrice - p.strike)
    : Math.max(0, p.strike - entryPrice);
  // Time value at entry
  const timeValueAtEntry = Math.max(0, p.premium - intrinsicAtEntry);
  // Days elapsed and remaining
  const daysElapsed = p.openedAt
    ? Math.max(0, (Date.now() - new Date(p.openedAt).getTime()) / 86400000)
    : 0;
  const daysToExpiry = p.expiry
    ? Math.max(0, (new Date(p.expiry) - Date.now()) / 86400000)
    : 30;
  const totalDays = daysElapsed + daysToExpiry;
  // Time value decays linearly (simplified)
  const timeValueNow = totalDays > 0
    ? timeValueAtEntry * (daysToExpiry / totalDays)
    : 0;
  return Math.max(0.01, intrinsic + timeValueNow);
}
let selectedPaperPosition = null;
let closingPaperPositionId = null;

async function setupPaperTrading() {
  await paperLoadAccount();
  paperPopulateSignalTicker();
  paperBindEvents();
}

async function paperLoadAccount() {
  try {
    const res = await fetch('/api/paper/account');
    paperAccount = await res.json();
    paperRenderAll();
  } catch (e) {
    console.error('Paper account load failed:', e);
  }
}

function paperPopulateSignalTicker() {
  const sel = document.getElementById('paperSignalTicker');
  if (!sel || !appData) return;
  const tickers = [...new Set([
    ...appData.daytrading.watchlist.map(s => s.ticker),
    ...appData.longterm.portfolio.map(s => s.ticker)
  ])];
  sel.innerHTML = '<option value="">Select stock...</option>' +
    tickers.map(t => `<option value="${t}">${t}</option>`).join('');
}

function paperBindEvents() {
  // Refresh AI advice button
  document.getElementById('paperAdviceRefreshBtn')?.addEventListener('click', () => {
    const positions = paperAccount?.positions || [];
    if (positions.length > 0) paperAutoAdvice(positions[0]);
  });

  // Reset account
  document.getElementById('paperResetBtn')?.addEventListener('click', async () => {
    const amount = prompt('Starting virtual cash amount:', '10000');
    if (!amount) return;
    const res = await fetch('/api/paper/reset', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ startingCash: parseFloat(amount) })
    });
    const data = await res.json();
    paperAccount = data.account;
    paperRenderAll();
  });

  // Manual trade button
  document.getElementById('paperManualTradeBtn')?.addEventListener('click', () => {
    openModal('modalPaperTrade');
  });

  // Manual trade calc
  ['ptPremium', 'ptContracts'].forEach(id => {
    document.getElementById(id)?.addEventListener('input', paperUpdateManualCalc);
  });

  // Manual trade confirm
  document.getElementById('ptConfirm')?.addEventListener('click', paperPlaceManualTrade);

  // Quick signal
  document.getElementById('paperGenerateSignal')?.addEventListener('click', paperGenerateQuickSignal);

  // Close position calc
  document.getElementById('paperClosePremium')?.addEventListener('input', paperUpdateCloseCalc);
  document.getElementById('paperCloseConfirm')?.addEventListener('click', paperClosePosition);
  document.getElementById('paperExpireBtn')?.addEventListener('click', paperExpirePosition);
}

function paperUpdateManualCalc() {
  const premium = parseFloat(document.getElementById('ptPremium')?.value) || 0;
  const contracts = parseInt(document.getElementById('ptContracts')?.value) || 1;
  const total = premium * 100 * contracts;
  const box = document.getElementById('ptCalcBox');
  if (total > 0 && box) {
    box.style.display = 'block';
    document.getElementById('ptTotalCost').textContent = `$${total.toFixed(2)}`;
    document.getElementById('ptFormula').textContent = `$${premium.toFixed(2)} × 100 × ${contracts} contract${contracts > 1 ? 's' : ''}`;
    document.getElementById('ptCashAfter').textContent = `$${((paperAccount?.cash || 0) - total).toFixed(2)}`;
    document.getElementById('ptMaxLoss').textContent = `$${total.toFixed(2)}`;
    const cashAfterEl = document.getElementById('ptCashAfter');
    if (cashAfterEl) cashAfterEl.className = `contract-calc-value ${(paperAccount?.cash || 0) - total < 0 ? 'red' : 'green'}`;
  } else if (box) {
    box.style.display = 'none';
  }
}

async function paperPlaceManualTrade() {
  const ticker = document.getElementById('ptTicker').value.trim().toUpperCase();
  const type = document.getElementById('ptType').value;
  const strike = parseFloat(document.getElementById('ptStrike').value);
  const expiry = document.getElementById('ptExpiry').value;
  const premium = parseFloat(document.getElementById('ptPremium').value);
  const contracts = parseInt(document.getElementById('ptContracts').value) || 1;
  if (!ticker || !strike || !expiry || !premium) { alert('Please fill all required fields.'); return; }

  const stockPrice = prices[ticker]?.price || 0;
  const btn = document.getElementById('ptConfirm');
  btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/></svg> Placing...'; btn.disabled = true;

  try {
    const res = await fetch('/api/paper/trade', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ticker, type, strike, expiry, premium, contracts, stockPriceAtEntry: stockPrice })
    });
    const data = await res.json();
    if (data.error) { alert(data.error); return; }
    paperAccount = await (await fetch('/api/paper/account')).json();
    closeModal('modalPaperTrade');
    ['ptTicker','ptStrike','ptExpiry','ptPremium'].forEach(id => { const el = document.getElementById(id); if (el) el.value = ''; });
    document.getElementById('ptContracts').value = '1';
    document.getElementById('ptCalcBox').style.display = 'none';
    paperRenderAll();
  } catch (e) {
    alert('Trade failed: ' + e.message);
  } finally {
    btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="6" width="20" height="12" rx="2"/><path d="M6 12h4m-2-2v4"/><circle cx="16" cy="11" r="1" fill="currentColor"/><circle cx="18" cy="13" r="1" fill="currentColor"/></svg> Place Paper Trade'; btn.disabled = false;
  }
}

async function paperTradeFromSignal(ticker, type, strike, expiry, premium, stockPrice) {
  // Always load fresh account data
  await paperLoadAccount();

  if (!paperAccount) {
    alert('Could not load paper trading account. Please open the Paper Trade tab first.');
    return;
  }

  // Validate inputs
  const cleanStrike = parseFloat(strike) || 0;
  const cleanPremium = parseFloat(premium) || 3.5;
  const cleanExpiry = expiry || '';

  if (!cleanStrike || !cleanExpiry) {
    alert('Signal is missing strike price or expiry date. Try generating a new signal.');
    return;
  }

  const totalCost = cleanPremium * 100;
  if (totalCost > paperAccount.cash) {
    alert(`Insufficient virtual cash. Need $${totalCost.toFixed(0)}, have $${paperAccount.cash.toFixed(0)}`);
    return;
  }

  const confirmed = window.confirm(
    `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="6" width="20" height="12" rx="2"/><path d="M6 12h4m-2-2v4"/><circle cx="16" cy="11" r="1" fill="currentColor"/><circle cx="18" cy="13" r="1" fill="currentColor"/></svg> Paper Trade:\n\n${ticker} ${type} $${cleanStrike} — expires ${cleanExpiry}\nPremium: $${cleanPremium.toFixed(2)}/share\nTotal cost: $${totalCost.toFixed(0)}\n\nPlace this paper trade?`
  );
  if (!confirmed) return;

  try {
    const res = await fetch('/api/paper/trade', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        ticker, type,
        strike: cleanStrike,
        expiry: cleanExpiry,
        premium: cleanPremium,
        contracts: 1,
        stockPriceAtEntry: parseFloat(stockPrice) || 0,
        signal: { ticker, type, strike: cleanStrike, expiry: cleanExpiry }
      })
    });
    const data = await res.json();
    if (data.error) { alert(data.error); return; }

    // Reload account and render
    await paperLoadAccount();
    paperRenderAll();
    showAlertToast({ severity: 'info', title: `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="6" width="20" height="12" rx="2"/><path d="M6 12h4m-2-2v4"/><circle cx="16" cy="11" r="1" fill="currentColor"/><circle cx="18" cy="13" r="1" fill="currentColor"/></svg> Paper Trade Placed!`, body: `${ticker} ${type} $${cleanStrike} — $${totalCost.toFixed(0)} deducted. Cash: $${paperAccount.cash.toFixed(0)}` });

    // Switch to paper tab
    switchToTab('paper');
  } catch (e) {
    alert('Paper trade failed: ' + e.message);
  }
}

window.paperTradeFromSignal = paperTradeFromSignal;

async function paperGenerateQuickSignal() {
  const tickerEl = document.getElementById('paperSignalTicker');
  const ticker = tickerEl?.value;
  if (!ticker) { alert('Select a stock first'); return; }
  const btn = document.getElementById('paperGenerateSignal');
  const output = document.getElementById('paperSignalOutput');
  btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/></svg>'; btn.disabled = true;
  output.innerHTML = `<div class="sugg-loading" style="padding:12px"><div class="spinner"></div><span>Generating signal for ${ticker}...</span></div>`;

  try {
    const priceData = prices[ticker] || {};
    const newsRes = await fetch(`/api/news/${ticker}`);
    const newsData = await newsRes.json().catch(() => []);
    const res = await fetch('/api/ai/signal', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ticker, price: priceData.price || 0, change: priceData.change || 0, changePct: priceData.changePct || 0, news: Array.isArray(newsData) ? newsData.slice(0, 3) : [], currentPrice: priceData.price || 0 })
    });
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n'); buffer = lines.pop();
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const msg = JSON.parse(line.slice(6));
          if (msg.type === 'signal') {
            const cardEl = document.createElement('div');
            cardEl.className = 'signal-card';
            output.innerHTML = '';
            output.appendChild(cardEl);
            renderSignalCard(cardEl, msg.data, ticker, priceData.price || 0);
          }
        } catch {}
      }
    }
  } catch (e) {
    output.innerHTML = `<div class="empty-state red"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> ${e.message}</div>`;
  } finally {
    btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg> Signal'; btn.disabled = false;
  }
}

function paperRenderAll() {
  if (!paperAccount) return;
  paperRenderAccountBar();
  paperRenderPositions();
  paperRenderClosedTrades();
}

async function paperAutoAdvice(pos) {
  if (!pos) return;
  const adviceEl = document.getElementById('paperAdviceOutput');
  const labelEl = document.getElementById('paperAdviceLabel');
  if (!adviceEl) return;
  if (labelEl) labelEl.textContent = `${pos.ticker} ${pos.type} $${pos.strike}`;
  adviceEl.innerHTML = `<div class="sugg-loading" style="padding:16px"><div class="spinner"></div><span><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="10" rx="2"/><circle cx="12" cy="5" r="2"/><path d="M12 7v4"/></svg> Analyzing ${pos.ticker} ${pos.type}...</span></div>`;
  const stockPrice = prices[pos.ticker]?.price || 0;
  const estPremium = estimatePaperPremium(pos, stockPrice);
  try {
    const res = await fetch(`/api/paper/analyze/${pos.id}`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ currentStockPrice: stockPrice, currentPremium: estPremium })
    });
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n'); buffer = lines.pop();
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const msg = JSON.parse(line.slice(6));
          if (msg.type === 'advice') paperRenderAdvice(adviceEl, msg.data, pos, stockPrice, estPremium);
          if (msg.type === 'error') adviceEl.innerHTML = `<div class="empty-state red"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> ${msg.text}</div>`;
        } catch {}
      }
    }
  } catch (e) {
    adviceEl.innerHTML = `<div class="empty-state red">Failed: ${e.message}</div>`;
  }
}
window.paperAutoAdvice = paperAutoAdvice;

function paperRenderAccountBar() {
  const { cash, startingCash, positions, closedTrades } = paperAccount;

  // Estimate open positions value using current prices
  let posValue = 0;
  (positions || []).forEach(p => {
    const stockPrice = prices[p.ticker]?.price || 0;
    if (stockPrice > 0) {
      const estPremium = estimatePaperPremium(p, stockPrice);
      posValue += estPremium * 100 * p.contracts;
    } else {
      posValue += p.totalCost; // fallback to cost
    }
  });

  const totalValue = cash + posValue;
  const allTimePnl = totalValue - startingCash;
  const wins = (closedTrades || []).filter(t => t.pnl > 0).length;
  const total = (closedTrades || []).length;
  const winRate = total > 0 ? `${wins}W/${total - wins}L (${Math.round(wins/total*100)}%)` : '—';
  const pnlColor = allTimePnl >= 0 ? 'green' : 'red';

  document.getElementById('paperCash').textContent = `$${cash.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  document.getElementById('paperPositionsValue').textContent = `$${posValue.toFixed(2)}`;
  document.getElementById('paperTotalValue').textContent = `$${totalValue.toFixed(2)}`;
  document.getElementById('paperAllTimePnl').textContent = `${allTimePnl >= 0 ? '+' : ''}$${allTimePnl.toFixed(2)}`;
  document.getElementById('paperAllTimePnl').className = `paper-account-value ${pnlColor}`;
  document.getElementById('paperWinRate').textContent = winRate;
  document.getElementById('paperTradeCount').textContent = total;
}

function paperRenderPositions() {
  const el = document.getElementById('paperPositions');
  const positions = paperAccount?.positions || [];

  if (!positions.length) {
    el.innerHTML = `<div class="paper-empty">
      <div class="paper-empty-icon"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="6" width="20" height="12" rx="2"/><path d="M6 12h4m-2-2v4"/><circle cx="16" cy="11" r="1" fill="currentColor"/><circle cx="18" cy="13" r="1" fill="currentColor"/></svg></div>
      <div>No open positions.</div>
      <div style="margin-top:6px;font-size:11px;color:var(--text3)">Generate a signal → click <b><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="6" width="20" height="12" rx="2"/><path d="M6 12h4m-2-2v4"/><circle cx="16" cy="11" r="1" fill="currentColor"/><circle cx="18" cy="13" r="1" fill="currentColor"/></svg> Paper Trade This</b></div>
    </div>`;
    return;
  }

  el.innerHTML = positions.map(p => {
    const stockPrice = prices[p.ticker]?.price || 0;
    const estPremium = estimatePaperPremium(p, stockPrice);
    const estValue = estPremium * 100 * p.contracts;
    const pnl = estValue - p.totalCost;
    const pnlPct = ((pnl / p.totalCost) * 100).toFixed(1);
    const pnlColor = pnl >= 0 ? 'green' : 'red';
    const daysLeft = Math.ceil((new Date(p.expiry) - new Date()) / (1000 * 60 * 60 * 24));
    const expiryColor = daysLeft <= 2 ? 'red' : daysLeft <= 5 ? 'yellow' : 'var(--text2)';
    const intrinsic = p.type === 'CALL'
      ? Math.max(0, stockPrice - p.strike)
      : Math.max(0, p.strike - stockPrice);
    const itm = intrinsic > 0;

    return `<div class="paper-position ${selectedPaperPosition?.id === p.id ? 'selected' : ''}" onclick="paperSelectPosition('${p.id}')">
      <div class="paper-pos-header">
        <div class="paper-pos-left">
          <span class="paper-pos-ticker">${p.ticker}</span>
          <span class="badge badge-${p.type.toLowerCase()}">${p.type}</span>
          <span class="paper-pos-strike">$${p.strike}</span>
          <span class="paper-pos-expiry" style="color:${expiryColor}">${p.expiry} (${daysLeft}d)</span>
          ${itm ? '<span class="paper-itm-badge">ITM</span>' : '<span class="paper-otm-badge">OTM</span>'}
        </div>
        <div class="paper-pos-right">
          <div class="paper-pos-pnl ${pnlColor}">${pnl >= 0 ? '+' : ''}$${pnl.toFixed(0)} (${pnlPct}%)</div>
          <div class="paper-pos-cost">Paid $${p.totalCost.toFixed(0)}</div>
        </div>
      </div>
      <div class="paper-pos-metrics">
        <div class="paper-pos-metric">
          <span class="paper-pos-metric-label">Entry Premium</span>
          <span class="paper-pos-metric-value">$${p.premium.toFixed(2)}</span>
        </div>
        <div class="paper-pos-metric">
          <span class="paper-pos-metric-label">Est. Now</span>
          <span class="paper-pos-metric-value ${pnlColor}">$${estPremium.toFixed(2)}</span>
        </div>
        <div class="paper-pos-metric">
          <span class="paper-pos-metric-label">Stock Price</span>
          <span class="paper-pos-metric-value">${stockPrice > 0 ? '$' + stockPrice.toFixed(2) : '—'}</span>
        </div>
        <div class="paper-pos-metric">
          <span class="paper-pos-metric-label">Contracts</span>
          <span class="paper-pos-metric-value">${p.contracts}</span>
        </div>
      </div>
      <div class="paper-pos-actions">
        <button class="btn-primary btn-sm" onclick="paperOpenCloseModal(event,'${p.id}')"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Close</button>
        <button class="btn-secondary btn-sm" onclick="paperGetAdvice(event,'${p.id}')"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="10" rx="2"/><circle cx="12" cy="5" r="2"/><path d="M12 7v4"/><line x1="8" y1="16" x2="8" y2="16"/><line x1="16" y1="16" x2="16" y2="16"/></svg> AI Advice</button>
      </div>
    </div>`;
  }).join('');
}

function paperRenderClosedTrades() {
  const el = document.getElementById('paperClosedTrades');
  const trades = paperAccount?.closedTrades || [];
  document.getElementById('paperClosedCount').textContent = `${trades.length} trade${trades.length !== 1 ? 's' : ''}`;

  if (!trades.length) { el.innerHTML = '<div class="empty-state">No closed trades yet.</div>'; return; }

  el.innerHTML = trades.slice(0, 20).map(t => {
    const pnlColor = t.pnl >= 0 ? 'green' : 'red';
    const statusIcon = t.status === 'expired' ? '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="9" cy="12" r="1"/><circle cx="15" cy="12" r="1"/><path d="M8 20v2h8v-2"/><path d="M16 20a2 2 0 0 0 1.56-3.25 8 8 0 1 0-11.12 0A2 2 0 0 0 8 20"/></svg>' : t.pnl >= 0 ? '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>' : '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>';
    const date = new Date(t.closedAt).toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
    return `<div class="paper-closed-row">
      <div class="paper-closed-left">
        <span class="paper-closed-icon">${statusIcon}</span>
        <div>
          <div style="font-weight:700;font-size:12px">${t.ticker} ${t.type} $${t.strike}</div>
          <div style="font-size:10px;color:var(--text3)">${date} · ${t.expiry}</div>
        </div>
      </div>
      <div class="paper-closed-right">
        <div class="${pnlColor}" style="font-weight:800;font-size:13px">${t.pnl >= 0 ? '+' : ''}$${t.pnl.toFixed(0)}</div>
        <div style="font-size:10px;color:var(--text3)">${t.pnlPct >= 0 ? '+' : ''}${t.pnlPct.toFixed(1)}%</div>
      </div>
    </div>`;
  }).join('');
}

function paperSelectPosition(id) {
  selectedPaperPosition = paperAccount?.positions?.find(p => p.id === id);
  paperRenderPositions();
}

function paperOpenCloseModal(e, id) {
  e.stopPropagation();
  closingPaperPositionId = id;
  const pos = paperAccount?.positions?.find(p => p.id === id);
  if (!pos) return;
  document.getElementById('paperCloseLabel').textContent = `${pos.ticker} ${pos.type} $${pos.strike}`;
  const stockPrice = prices[pos.ticker]?.price || 0;
  document.getElementById('paperCloseInfo').innerHTML = `
    <div class="paper-close-info-grid">
      <div><span class="paper-close-info-label">Paid</span><span class="paper-close-info-value">$${pos.premium.toFixed(2)}/share ($${pos.totalCost.toFixed(0)} total)</span></div>
      <div><span class="paper-close-info-label">Stock Now</span><span class="paper-close-info-value">${stockPrice > 0 ? '$' + stockPrice.toFixed(2) : '—'}</span></div>
      <div><span class="paper-close-info-label">Strike</span><span class="paper-close-info-value">$${pos.strike} ${pos.type}</span></div>
      <div><span class="paper-close-info-label">Expires</span><span class="paper-close-info-value">${pos.expiry}</span></div>
    </div>`;
  document.getElementById('paperClosePremium').value = '';
  document.getElementById('paperCloseCalcBox').style.display = 'none';
  openModal('modalPaperClose');
}

function paperUpdateCloseCalc() {
  const pos = paperAccount?.positions?.find(p => p.id === closingPaperPositionId);
  if (!pos) return;
  const closePremium = parseFloat(document.getElementById('paperClosePremium').value) || 0;
  const proceeds = closePremium * 100 * pos.contracts;
  const pnl = proceeds - pos.totalCost;
  const pnlPct = ((pnl / pos.totalCost) * 100).toFixed(1);
  const box = document.getElementById('paperCloseCalcBox');
  if (closePremium > 0) {
    box.style.display = 'block';
    document.getElementById('paperCloseProceeds').textContent = `$${proceeds.toFixed(2)}`;
    document.getElementById('paperClosePnl').textContent = `${pnl >= 0 ? '+' : ''}$${pnl.toFixed(2)}`;
    document.getElementById('paperClosePnl').className = `contract-calc-value ${pnl >= 0 ? 'green' : 'red'}`;
    document.getElementById('paperCloseReturn').textContent = `${pnlPct >= 0 ? '+' : ''}${pnlPct}%`;
    document.getElementById('paperCloseReturn').className = `contract-calc-value ${pnl >= 0 ? 'green' : 'red'}`;
  } else {
    box.style.display = 'none';
  }
}

async function paperClosePosition() {
  const closePremium = parseFloat(document.getElementById('paperClosePremium').value);
  if (!closePremium) { alert('Enter the current premium price'); return; }
  if (!closingPaperPositionId) { alert('No position selected. Please try again.'); return; }

  const btn = document.getElementById('paperCloseConfirm');
  if (btn) { btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/></svg> Closing...'; btn.disabled = true; }

  try {
    const pos = paperAccount?.positions?.find(p => p.id === closingPaperPositionId);
    const stockPrice = prices[pos?.ticker]?.price || 0;
    const res = await fetch(`/api/paper/close/${closingPaperPositionId}`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ closePremium, currentStockPrice: stockPrice })
    });
    const data = await res.json();
    if (data.error) { alert(data.error); return; }
    closingPaperPositionId = null;
    selectedPaperPosition = null;
    closeModal('modalPaperClose');
    // Clear advice panel
    document.getElementById('paperAdviceOutput').innerHTML = '<div class="paper-empty"><div class="paper-empty-icon"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="10" rx="2"/><circle cx="12" cy="5" r="2"/><path d="M12 7v4"/></svg></div><div>Click any open position to get AI advice — hold, close, or roll.</div></div>';
    document.getElementById('paperAdviceLabel').textContent = 'Click a position to get advice';
    paperAccount = await (await fetch('/api/paper/account')).json();
    paperRenderAll();
  } catch (e) {
    alert('Failed to close position: ' + e.message);
  } finally {
    if (btn) { btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Close Position'; btn.disabled = false; }
  }
}

async function paperExpirePosition() {
  if (!confirm('Mark this position as expired worthless? You will lose the full premium paid.')) return;
  if (!closingPaperPositionId) { alert('No position selected. Please try again.'); return; }

  const btn = document.getElementById('paperExpireBtn');
  if (btn) { btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/></svg>...'; btn.disabled = true; }

  try {
    const res = await fetch(`/api/paper/expire/${closingPaperPositionId}`, { method: 'POST' });
    const data = await res.json();
    if (data.error) { alert(data.error); return; }
    closingPaperPositionId = null;
    selectedPaperPosition = null;
    closeModal('modalPaperClose');
    document.getElementById('paperAdviceOutput').innerHTML = '<div class="paper-empty"><div class="paper-empty-icon"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="10" rx="2"/><circle cx="12" cy="5" r="2"/><path d="M12 7v4"/></svg></div><div>Click any open position to get AI advice — hold, close, or roll.</div></div>';
    document.getElementById('paperAdviceLabel').textContent = 'Click a position to get advice';
    paperAccount = await (await fetch('/api/paper/account')).json();
    paperRenderAll();
  } catch (e) {
    alert('Failed to expire position: ' + e.message);
  } finally {
    if (btn) { btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="9" cy="12" r="1"/><circle cx="15" cy="12" r="1"/><path d="M8 20v2h8v-2"/><path d="M16 20a2 2 0 0 0 1.56-3.25 8 8 0 1 0-11.12 0A2 2 0 0 0 8 20"/></svg> Expire Worthless'; btn.disabled = false; }
  }
}

async function paperGetAdvice(e, id) {
  e.stopPropagation();
  const pos = paperAccount?.positions?.find(p => p.id === id);
  if (!pos) return;
  selectedPaperPosition = pos;
  const adviceEl = document.getElementById('paperAdviceOutput');
  const labelEl = document.getElementById('paperAdviceLabel');
  labelEl.textContent = `${pos.ticker} ${pos.type} $${pos.strike}`;
  adviceEl.innerHTML = `<div class="sugg-loading" style="padding:16px"><div class="spinner"></div><span><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="10" rx="2"/><circle cx="12" cy="5" r="2"/><path d="M12 7v4"/></svg> AI analyzing your position...</span></div>`;

  const stockPrice = prices[pos.ticker]?.price || 0;
  // Use the shared helper — no inline calculation that can have scope issues
  const estPremium = estimatePaperPremium(pos, stockPrice);

  try {
    const res = await fetch(`/api/paper/analyze/${id}`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ currentStockPrice: stockPrice, currentPremium: estPremium })
    });
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n'); buffer = lines.pop();
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const msg = JSON.parse(line.slice(6));
          if (msg.type === 'advice') paperRenderAdvice(adviceEl, msg.data, pos, stockPrice, estPremium);
          if (msg.type === 'error') adviceEl.innerHTML = `<div class="empty-state red"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> ${msg.text}</div>`;
        } catch {}
      }
    }
  } catch (e) {
    adviceEl.innerHTML = `<div class="empty-state red"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> ${e.message}</div>`;
  }
}

function paperRenderAdvice(el, advice, pos, stockPrice, estPremium) {
  const actionColors = { 'HOLD': 'yellow', 'CLOSE NOW': 'red', 'CLOSE PARTIAL': 'orange', 'LET EXPIRE': 'red', 'ROLL': 'blue' };
  const color = actionColors[advice.action] || 'yellow';
  const pnl = (estPremium - pos.premium) * 100 * pos.contracts;
  const pnlPct = ((pnl / pos.totalCost) * 100).toFixed(1);

  el.innerHTML = `
    <div class="paper-advice-card">
      <div class="paper-advice-action paper-advice-${color}">${advice.action}</div>
      <div class="paper-advice-reasoning">${advice.reasoning}</div>
      <div class="paper-advice-grid">
        <div class="paper-advice-item">
          <div class="paper-advice-label">Current Est. P&L</div>
          <div class="paper-advice-value ${pnl >= 0 ? 'green' : 'red'}">${pnl >= 0 ? '+' : ''}$${pnl.toFixed(0)} (${pnlPct}%)</div>
        </div>
        <div class="paper-advice-item">
          <div class="paper-advice-label">Stock Price</div>
          <div class="paper-advice-value">$${stockPrice.toFixed(2)}</div>
        </div>
        <div class="paper-advice-item">
          <div class="paper-advice-label">Urgency</div>
          <div class="paper-advice-value ${advice.urgency === 'High' ? 'red' : advice.urgency === 'Medium' ? 'yellow' : 'green'}">${advice.urgency}</div>
        </div>
      </div>
      <div class="paper-advice-target"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="6"/><circle cx="12" cy="12" r="2"/></svg> ${advice.targetExit}</div>
      <div class="paper-advice-risk"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>️ ${advice.riskNote}</div>
      <div style="display:flex;gap:8px;margin-top:12px">
        <button class="btn-primary btn-sm" style="flex:1" onclick="paperOpenCloseModal(event,'${pos.id}')"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Close Position</button>
        <button class="btn-secondary btn-sm" style="flex:1" onclick="paperExpirePosition()"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="9" cy="12" r="1"/><circle cx="15" cy="12" r="1"/><path d="M8 20v2h8v-2"/><path d="M16 20a2 2 0 0 0 1.56-3.25 8 8 0 1 0-11.12 0A2 2 0 0 0 8 20"/></svg> Expire Worthless</button>
      </div>
    </div>`;
}

// Init paper trading tab — triggered when paper hub-subtab is clicked
document.addEventListener('DOMContentLoaded', () => {
  document.querySelector('.hub-subtab[data-hubtab="paper"]')
    ?.addEventListener('click', async () => {
      if (!window._paperInit) { await setupPaperTrading(); window._paperInit = true; }
      else { await paperLoadAccount(); paperRenderAll(); }
    });
});

window.paperSelectPosition = paperSelectPosition;
window.paperOpenCloseModal = paperOpenCloseModal;
window.paperGetAdvice = paperGetAdvice;

// ─── TASTYTRADE INTEGRATION ───────────────────────────────────────
let ttState = { connected: false, mode: 'live', accountNumber: null, accounts: [] };
let ttPendingOrder = null; // order being placed

async function setupBrokerageTab() {
  await ttLoadConfig();
  ttBindEvents();
  if (ttState.connected) {
    ttShowConnected();
    ttRefreshAll();
  }
}

async function ttLoadConfig() {
  try {
    const res = await fetch('/api/tastytrade/config');
    const cfg = await res.json();
    ttState = { ...ttState, ...cfg };
    document.getElementById('ttUsername').value = cfg.username || '';
    ttState.mode = 'live'; // always live
    if (cfg.accounts?.length) ttPopulateAccounts(cfg.accounts, cfg.accountNumber);
  } catch {}
}

function ttSetMode(mode) {
  ttState.mode = 'live'; // always live — sandbox removed
}
window.ttSetMode = ttSetMode;

function ttBindEvents() {
  document.getElementById('ttConnectBtn')?.addEventListener('click', ttConnect);
  document.getElementById('ttDisconnectBtn')?.addEventListener('click', ttDisconnect);
  document.getElementById('ttVerifyBtn')?.addEventListener('click', ttVerifyDevice);
  document.getElementById('ttDeviceCode')?.addEventListener('keydown', e => {
    if (e.key === 'Enter') ttVerifyDevice();
  });
  document.getElementById('ttSecurityAnswerBtn')?.addEventListener('click', ttAnswerSecurity);
  document.getElementById('ttSecurityAnswer')?.addEventListener('keydown', e => {
    if (e.key === 'Enter') ttAnswerSecurity();
  });
  document.getElementById('ttRefreshPositions')?.addEventListener('click', ttLoadPositions);
  document.getElementById('ttRefreshOrders')?.addEventListener('click', ttLoadOrders);
  document.getElementById('ttLoadChain')?.addEventListener('click', () => {
    const ticker = document.getElementById('ttChainTicker').value.trim().toUpperCase();
    if (ticker) ttLoadOptionsChain(ticker);
  });
  document.getElementById('ttChainTicker')?.addEventListener('keydown', e => {
    if (e.key === 'Enter') {
      const ticker = e.target.value.trim().toUpperCase();
      if (ticker) ttLoadOptionsChain(ticker);
    }
  });
  document.getElementById('ttAccountNumber')?.addEventListener('change', async (e) => {
    ttState.accountNumber = e.target.value;
    await fetch('/api/tastytrade/config', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ accountNumber: e.target.value }) });
    ttRefreshAll();
  });
  // Order modal
  document.getElementById('ttDryRunBtn')?.addEventListener('click', ttDryRun);
  document.getElementById('ttPlaceOrderBtn')?.addEventListener('click', ttPlaceOrder);
  ['ttOrderPrice','ttOrderQty'].forEach(id => {
    document.getElementById(id)?.addEventListener('input', ttUpdateOrderCalc);
  });
  // Eye toggle handled by setupAISettings global handler — no duplicate needed
}

// Store credentials temporarily for device auth flow
let ttPendingCreds = null;

async function ttConnect() {
  const username = document.getElementById('ttUsername').value.trim();
  const password = document.getElementById('ttPassword').value;
  const mode = ttState.mode;
  if (!username || !password) { ttShowMsg('Enter username and password', 'error'); return; }

  const btn = document.getElementById('ttConnectBtn');
  btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/></svg> Connecting...'; btn.disabled = true;
  ttShowMsg('Authenticating with Tastytrade...', 'loading');

  try {
    const res = await fetch('/api/tastytrade/connect', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password, mode })
    });
    const data = await res.json();

    // Security question — must answer before device auth
    if (data.requiresSecurityQuestion) {
      ttPendingCreds = { username, password, mode };
      document.getElementById('ttSecurityAuth').classList.remove('hidden');
      document.getElementById('ttSecurityQuestion').innerHTML = data.message || '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="11" rx="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg> Tastytrade requires your security question answer.';
      document.getElementById('ttSecurityAnswer').value = '';
      document.getElementById('ttSecurityAnswer').focus();
      ttShowMsg('<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="11" rx="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg> Answer your security question to continue', 'loading');
      return;
    }

    // Device auth challenge — Tastytrade sent a code to email/phone
    if (data.requiresDeviceAuth) {
      ttPendingCreds = { username, password, mode };
      document.getElementById('ttDeviceAuth').classList.remove('hidden');
      const msgEl = document.getElementById('ttDeviceAuthMsg');
      msgEl.innerHTML = `
        <div style="margin-bottom:6px">${data.message || 'Tastytrade requires device verification.'}</div>
        ${data.rawMessage ? `<div style="font-size:10px;color:var(--text3);margin-top:4px">Tastytrade says: "${data.rawMessage}"</div>` : ''}
        ${data.challengeType && data.challengeType !== 'unknown' ? `<div style="font-size:10px;color:var(--text3)">Challenge type: ${data.challengeType}</div>` : ''}
        <div style="font-size:10px;color:var(--yellow);margin-top:6px"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>️ Also check your spam folder and SMS messages</div>
        ${data._debug ? `<details style="margin-top:8px"><summary style="font-size:10px;color:var(--text3);cursor:pointer"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg> Debug info</summary><pre style="font-size:9px;color:var(--text3);white-space:pre-wrap;margin-top:4px">${JSON.stringify(data._debug, null, 2)}</pre></details>` : ''}`;
      document.getElementById('ttDeviceCode').focus();
      ttShowMsg('<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/></svg> Check your email and phone for a verification code', 'loading');
      return;
    }

    if (!data.ok || data.error) {
      ttShowMsg(`<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> ${data.error}`, 'error');
      if (data.hint) {
        setTimeout(() => {
          const existing = document.getElementById('ttApiHint');
          if (existing) existing.remove();
          const hintEl = document.createElement('div');
          hintEl.id = 'ttApiHint';
          hintEl.className = 'tt-api-hint';
          hintEl.innerHTML = `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="9" y1="18" x2="15" y2="18"/><line x1="10" y1="22" x2="14" y2="22"/><path d="M15.09 14c.18-.98.65-1.74 1.41-2.5A4.65 4.65 0 0 0 18 8 6 6 0 0 0 6 8c0 1 .23 2.23 1.5 3.5A4.61 4.61 0 0 1 8.91 14"/></svg> <b>Fix:</b> ${data.hint}`;
          document.getElementById('ttConnectMsg')?.after(hintEl);
          setTimeout(() => hintEl.remove(), 20000);
        }, 100);
      }
      return;
    }

    ttHandleConnected(data);
  } catch (e) {
    ttShowMsg(`<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> ${e.message}`, 'error');
  } finally {
    btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22v-5"/><path d="M9 8V2"/><path d="M15 8V2"/><path d="M18 8H6a2 2 0 0 0-2 2v3a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-3a2 2 0 0 0-2-2z"/></svg> Connect'; btn.disabled = false;
  }
}

async function ttVerifyDevice() {
  const code = document.getElementById('ttDeviceCode').value.trim();
  if (!code) { ttShowMsg('Enter the verification code', 'error'); return; }
  if (!ttPendingCreds) { ttShowMsg('Session expired — please reconnect', 'error'); return; }

  const btn = document.getElementById('ttVerifyBtn');
  btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/></svg> Verifying...'; btn.disabled = true;
  ttShowMsg('Verifying code...', 'loading');

  try {
    const res = await fetch('/api/tastytrade/verify-device', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ...ttPendingCreds, code })
    });
    const data = await res.json();
    if (!data.ok || data.error) {
      const debugStr = data._debug ? `\n\nDebug: ${JSON.stringify(data._debug)}` : '';
      ttShowMsg(`<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> ${data.error}${debugStr}`, 'error');
      return;
    }

    document.getElementById('ttDeviceAuth').classList.add('hidden');
    document.getElementById('ttDeviceCode').value = '';
    ttPendingCreds = null;
    ttHandleConnected(data);
  } catch (e) {
    ttShowMsg(`<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> ${e.message}`, 'error');
  } finally {
    btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Verify & Connect'; btn.disabled = false;
  }
}

async function ttAnswerSecurity() {
  const answer = document.getElementById('ttSecurityAnswer').value.trim();
  if (!answer) { ttShowMsg('Enter your security answer', 'error'); return; }
  if (!ttPendingCreds) { ttShowMsg('Session expired — please reconnect', 'error'); return; }

  const btn = document.getElementById('ttSecurityAnswerBtn');
  btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/></svg> Submitting...'; btn.disabled = true;
  ttShowMsg('Submitting security answer...', 'loading');

  try {
    const res = await fetch('/api/tastytrade/answer-security', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ...ttPendingCreds, answer })
    });
    const data = await res.json();

    // Security answer accepted but now needs device auth
    if (data.requiresDeviceAuth) {
      document.getElementById('ttSecurityAuth').classList.add('hidden');
      document.getElementById('ttSecurityAnswer').value = '';
      document.getElementById('ttDeviceAuth').classList.remove('hidden');
      const msgEl = document.getElementById('ttDeviceAuthMsg');
      msgEl.innerHTML = `
        <div style="margin-bottom:6px">${data.message || 'Tastytrade requires device verification.'}</div>
        ${data.rawMessage ? `<div style="font-size:10px;color:var(--text3);margin-top:4px">Tastytrade says: "${data.rawMessage}"</div>` : ''}
        ${data.challengeType && data.challengeType !== 'unknown' ? `<div style="font-size:10px;color:var(--text3)">Challenge type: ${data.challengeType}</div>` : ''}
        <div style="font-size:10px;color:var(--yellow);margin-top:6px"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>️ Also check your spam folder and SMS messages</div>`;
      document.getElementById('ttDeviceCode').focus();
      ttShowMsg('<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/></svg> Check your email and phone for a verification code', 'loading');
      return;
    }

    if (!data.ok || data.error) { ttShowMsg(`<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> ${data.error}`, 'error'); return; }

    document.getElementById('ttSecurityAuth').classList.add('hidden');
    document.getElementById('ttSecurityAnswer').value = '';
    ttPendingCreds = null;
    ttHandleConnected(data);
  } catch (e) {
    ttShowMsg(`<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> ${e.message}`, 'error');
  } finally {
    btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="11" rx="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg> Submit Answer'; btn.disabled = false;
  }
}

function ttHandleConnected(data) {
  ttState.connected = true;
  ttState.accountNumber = data.accountNumber;
  ttState.accounts = data.accounts || [];
  ttPopulateAccounts(data.accounts, data.accountNumber);
  ttShowConnected();
  ttShowMsg(`<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Connected! Account: ${data.accountNumber}`, 'success');
  document.getElementById('ttPassword').value = '';
  ttRefreshAll();
}

async function ttDisconnect() {
  await fetch('/api/tastytrade/disconnect', { method: 'POST' });
  ttState.connected = false;
  ttShowDisconnected();
  ttShowMsg('Disconnected from Tastytrade', 'info');
}

function ttPopulateAccounts(accounts, selected) {
  const sel = document.getElementById('ttAccountNumber');
  if (!sel || !accounts?.length) return;
  sel.innerHTML = accounts.map(a =>
    `<option value="${a.accountNumber}" ${a.accountNumber === selected ? 'selected' : ''}>${a.accountNumber} — ${a.accountType} ${a.nickname ? '('+a.nickname+')' : ''}</option>`
  ).join('');
  document.getElementById('ttAccountSelect')?.classList.remove('hidden');
}

function ttShowConnected() {
  document.getElementById('ttStatusDot').className = 'tt-status-dot connected';
  document.getElementById('ttStatusLabel').innerHTML = `Connected · <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg> Live`;
  document.getElementById('ttConnectBtn').classList.add('hidden');
  document.getElementById('ttDisconnectBtn').classList.remove('hidden');
  document.getElementById('ttAccountCard')?.classList.remove('hidden');
  document.getElementById('ttSetupGuide')?.classList.add('hidden');
}

function ttShowDisconnected() {
  document.getElementById('ttStatusDot').className = 'tt-status-dot';
  document.getElementById('ttStatusLabel').textContent = 'Not Connected';
  document.getElementById('ttConnectBtn').classList.remove('hidden');
  document.getElementById('ttDisconnectBtn').classList.add('hidden');
  document.getElementById('ttAccountCard')?.classList.add('hidden');
  document.getElementById('ttSetupGuide')?.classList.remove('hidden');
  document.getElementById('ttPositions').innerHTML = '<div class="tt-not-connected">Connect to see live positions.</div>';
  document.getElementById('ttOrders').innerHTML = '<div class="tt-not-connected">Connect to see open orders.</div>';
}

function ttShowMsg(msg, type) {
  const el = document.getElementById('ttConnectMsg');
  if (!el) return;
  el.classList.remove('hidden');
  el.className = `tt-connect-msg tt-msg-${type}`;
  el.textContent = msg;
  if (type === 'success') setTimeout(() => el.classList.add('hidden'), 4000);
}

async function ttRefreshAll() {
  await Promise.all([ttLoadBalances(), ttLoadPositions(), ttLoadOrders()]);
}

async function ttLoadBalances() {
  try {
    const res = await fetch('/api/tastytrade/balances');
    if (!res.ok) return;
    const b = await res.json();
    const el = document.getElementById('ttAccountSummary');
    if (!el) return;
    el.innerHTML = `
      <div class="tt-balance-grid">
        <div class="tt-balance-item">
          <div class="tt-balance-label">Cash Balance</div>
          <div class="tt-balance-value">$${b.cashBalance.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})}</div>
        </div>
        <div class="tt-balance-item">
          <div class="tt-balance-label">Buying Power</div>
          <div class="tt-balance-value green">$${b.buyingPower.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})}</div>
        </div>
        <div class="tt-balance-item">
          <div class="tt-balance-label">Net Liquidating Value</div>
          <div class="tt-balance-value">$${b.netLiq.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})}</div>
        </div>
        <div class="tt-balance-item">
          <div class="tt-balance-label">Account</div>
          <div class="tt-balance-value" style="font-size:12px">${ttState.accountNumber}</div>
        </div>
      </div>`;
  } catch {}
}

async function ttLoadPositions() {
  const el = document.getElementById('ttPositions');
  if (!el) return;
  if (!ttState.connected) { el.innerHTML = '<div class="tt-not-connected">Connect to see positions.</div>'; return; }
  el.innerHTML = '<div class="loading-row">Loading positions...</div>';
  try {
    const res = await fetch('/api/tastytrade/positions');
    if (!res.ok) { el.innerHTML = '<div class="empty-state">Could not load positions.</div>'; return; }
    const positions = await res.json();
    if (!positions.length) { el.innerHTML = '<div class="empty-state">No open positions.</div>'; return; }
    el.innerHTML = `<div class="tt-positions-table-wrap"><table class="tt-table">
      <thead><tr><th>Symbol</th><th>Type</th><th>Qty</th><th>Avg Open</th><th>Close Price</th><th>Day P&L</th></tr></thead>
      <tbody>${positions.map(p => {
        const pnlColor = p.unrealizedDayGain >= 0 ? 'green' : 'red';
        return `<tr>
          <td style="font-weight:700;font-size:11px">${p.symbol}</td>
          <td><span class="tt-type-badge">${p.instrumentType}</span></td>
          <td>${p.quantity} ${p.quantityDirection}</td>
          <td>$${p.averageOpenPrice.toFixed(2)}</td>
          <td>$${p.closePrice.toFixed(2)}</td>
          <td class="${pnlColor}">${p.unrealizedDayGain >= 0 ? '+' : ''}$${p.unrealizedDayGain.toFixed(2)}</td>
        </tr>`;
      }).join('')}</tbody>
    </table></div>`;
  } catch (e) { el.innerHTML = `<div class="empty-state red">Error: ${e.message}</div>`; }
}

async function ttLoadOrders() {
  const el = document.getElementById('ttOrders');
  if (!el) return;
  if (!ttState.connected) { el.innerHTML = '<div class="tt-not-connected">Connect to see orders.</div>'; return; }
  el.innerHTML = '<div class="loading-row">Loading orders...</div>';
  try {
    const res = await fetch('/api/tastytrade/orders');
    if (!res.ok) { el.innerHTML = '<div class="empty-state">Could not load orders.</div>'; return; }
    const orders = await res.json();
    if (!orders.length) { el.innerHTML = '<div class="empty-state">No open orders.</div>'; return; }
    el.innerHTML = `<div class="tt-positions-table-wrap"><table class="tt-table">
      <thead><tr><th>Symbol</th><th>Action</th><th>Qty</th><th>Type</th><th>Price</th><th>Status</th><th></th></tr></thead>
      <tbody>${orders.map(o => {
        const leg = o.legs?.[0] || {};
        const statusColor = o.status === 'Filled' ? 'green' : o.status === 'Cancelled' ? 'red' : 'yellow';
        return `<tr>
          <td style="font-weight:700;font-size:11px">${leg.symbol || '—'}</td>
          <td>${leg.action || '—'}</td>
          <td>${leg.quantity || '—'}</td>
          <td>${o['order-type'] || '—'}</td>
          <td>${o.price ? '$'+parseFloat(o.price).toFixed(2) : 'Market'}</td>
          <td><span class="${statusColor}" style="font-size:11px;font-weight:700">${o.status}</span></td>
          <td>${o.status === 'Live' ? `<button class="btn-secondary btn-sm" onclick="ttCancelOrder('${o.id}')">Cancel</button>` : ''}</td>
        </tr>`;
      }).join('')}</tbody>
    </table></div>`;
  } catch (e) { el.innerHTML = `<div class="empty-state red">Error: ${e.message}</div>`; }
}

async function ttCancelOrder(orderId) {
  if (!confirm('Cancel this order?')) return;
  try {
    await fetch(`/api/tastytrade/order/${orderId}`, { method: 'DELETE' });
    ttLoadOrders();
  } catch (e) { alert('Cancel failed: ' + e.message); }
}
window.ttCancelOrder = ttCancelOrder;

async function ttLoadOptionsChain(ticker) {
  const el = document.getElementById('ttOptionsChain');
  if (!el) return;
  if (!ttState.connected) { el.innerHTML = '<div class="tt-not-connected">Connect first to load real options chain.</div>'; return; }
  el.innerHTML = `<div class="loading-row">Loading real options chain for ${ticker}...</div>`;
  try {
    const res = await fetch(`/api/tastytrade/options-chain/${ticker}`);
    if (!res.ok) { el.innerHTML = '<div class="empty-state">Could not load chain.</div>'; return; }
    const data = await res.json();
    if (!data.expirations?.length) { el.innerHTML = '<div class="empty-state">No options data available.</div>'; return; }

    let activeExp = 0;
    const renderExp = (idx) => {
      const exp = data.expirations[idx];
      if (!exp) return '';
      return `
        <div class="options-disclaimer" style="background:var(--green-bg);color:var(--green);border-color:rgba(0,212,170,0.3)"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> LIVE DATA from Tastytrade · ${exp.expirationDate} (${exp.daysToExpiration}d)</div>
        <div class="options-cols">
          <div class="options-side">
            <div class="options-side-title green">CALLS</div>
            <table class="options-table"><thead><tr><th>Strike</th><th>Bid</th><th>Ask</th><th>IV</th><th>Δ</th><th>Vol</th><th>OI</th><th></th></tr></thead>
            <tbody>${exp.strikes.filter(s => s.call).map(s => `<tr>
              <td style="font-weight:700">$${s.strikePrice}</td>
              <td>$${s.call.bid.toFixed(2)}</td><td>$${s.call.ask.toFixed(2)}</td>
              <td>${(s.call.iv*100).toFixed(0)}%</td><td>${s.call.delta.toFixed(2)}</td>
              <td>${s.call.volume.toLocaleString()}</td><td>${s.call.oi.toLocaleString()}</td>
              <td><button class="btn-primary btn-sm" onclick="ttOpenOrderModal('${ticker}','CALL',${s.strikePrice},'${exp.expirationDate}',${s.call.ask},'${s.call.symbol}')">Buy</button></td>
            </tr>`).join('')}</tbody></table>
          </div>
          <div class="options-side">
            <div class="options-side-title red">PUTS</div>
            <table class="options-table"><thead><tr><th>Strike</th><th>Bid</th><th>Ask</th><th>IV</th><th>Δ</th><th>Vol</th><th>OI</th><th></th></tr></thead>
            <tbody>${exp.strikes.filter(s => s.put).map(s => `<tr>
              <td style="font-weight:700">$${s.strikePrice}</td>
              <td>$${s.put.bid.toFixed(2)}</td><td>$${s.put.ask.toFixed(2)}</td>
              <td>${(s.put.iv*100).toFixed(0)}%</td><td>${s.put.delta.toFixed(2)}</td>
              <td>${s.put.volume.toLocaleString()}</td><td>${s.put.oi.toLocaleString()}</td>
              <td><button class="btn-primary btn-sm" style="background:var(--red)" onclick="ttOpenOrderModal('${ticker}','PUT',${s.strikePrice},'${exp.expirationDate}',${s.put.ask},'${s.put.symbol}')">Buy</button></td>
            </tr>`).join('')}</tbody></table>
          </div>
        </div>`;
    };

    el.innerHTML = `
      <div class="options-exp-tabs">${data.expirations.map((e,i) =>
        `<button class="options-exp-btn ${i===0?'active':''}" onclick="ttSwitchExp(this,${i},'${ticker}')">${e.expirationDate} (${e.daysToExpiration}d)</button>`
      ).join('')}</div>
      <div id="ttChainContent">${renderExp(0)}</div>`;

    window._ttChainData = data;
    window._ttChainTicker = ticker;
  } catch (e) { el.innerHTML = `<div class="empty-state red">Error: ${e.message}</div>`; }
}

window.ttSwitchExp = (btn, idx, ticker) => {
  document.querySelectorAll('.options-exp-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  const data = window._ttChainData;
  if (!data) return;
  const exp = data.expirations[idx];
  if (!exp) return;
  const content = document.getElementById('ttChainContent');
  if (content) {
    // Re-render using same logic
    ttLoadOptionsChain(ticker); // simplest approach
  }
};

// ── Place Order Flow ──────────────────────────────────────────────
function ttOpenOrderModal(ticker, type, strike, expiry, askPrice, symbol) {
  ttPendingOrder = { ticker, type, strike, expiry, symbol };
  const modeLabel = ttState.mode === 'live' ? '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg> LIVE — Real money!' : '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14.5 2v13.5L19 21H5l4.5-5.5V2"/><line x1="8.5" y1="2" x2="15.5" y2="2"/></svg> Sandbox — Fake money';
  const modeColor = ttState.mode === 'live' ? 'var(--red)' : 'var(--green)';

  document.getElementById('ttOrderSummary').innerHTML = `
    <div class="tt-order-info">
      <div class="tt-order-info-row">
        <span class="tt-order-ticker">${ticker}</span>
        <span class="badge badge-${type.toLowerCase()}">${type}</span>
        <span class="tt-order-detail">$${strike} Strike</span>
        <span class="tt-order-detail">${expiry}</span>
      </div>
      <div class="tt-order-mode" style="color:${modeColor}">${modeLabel}</div>
      ${symbol ? `<div style="font-size:10px;color:var(--text3);margin-top:4px">Symbol: ${symbol}</div>` : ''}
    </div>`;

  document.getElementById('ttOrderPrice').value = askPrice ? askPrice.toFixed(2) : '';
  document.getElementById('ttOrderQty').value = '1';
  document.getElementById('ttDryRunResult').classList.add('hidden');

  const warning = document.getElementById('ttModeWarning');
  if (ttState.mode === 'live') {
    warning.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>️ <b>LIVE MODE</b> — This will place a REAL order with REAL money on your Tastytrade account.';
    warning.className = 'tt-mode-warning tt-warning-live';
  } else {
    warning.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14.5 2v13.5L19 21H5l4.5-5.5V2"/><line x1="8.5" y1="2" x2="15.5" y2="2"/></svg> <b>Sandbox Mode</b> — This is a practice order. No real money involved.';
    warning.className = 'tt-mode-warning tt-warning-sandbox';
  }

  ttUpdateOrderCalc();
  openModal('modalTTOrder');
}
window.ttOpenOrderModal = ttOpenOrderModal;

function ttUpdateOrderCalc() {
  const price = parseFloat(document.getElementById('ttOrderPrice')?.value) || 0;
  const qty = parseInt(document.getElementById('ttOrderQty')?.value) || 1;
  const total = price * 100 * qty;
  const box = document.getElementById('ttOrderCalc');
  if (total > 0 && box) {
    box.style.display = 'block';
    document.getElementById('ttOrderCost').textContent = `$${total.toFixed(2)} (${qty} contract${qty>1?'s':''} × $${price.toFixed(2)} × 100)`;
    document.getElementById('ttOrderMaxLoss').textContent = `$${total.toFixed(2)}`;
    document.getElementById('ttOrderAccount').textContent = `${ttState.accountNumber} (${ttState.mode})`;
  } else if (box) { box.style.display = 'none'; }
}

async function ttDryRun() {
  if (!ttPendingOrder) return;
  const price = parseFloat(document.getElementById('ttOrderPrice').value);
  const qty = parseInt(document.getElementById('ttOrderQty').value) || 1;
  const btn = document.getElementById('ttDryRunBtn');
  btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/></svg> Checking...'; btn.disabled = true;
  try {
    const res = await fetch('/api/tastytrade/order/dry-run', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ symbol: ttPendingOrder.symbol, action: 'Buy to Open', quantity: qty, orderType: 'Limit', price })
    });
    const data = await res.json();
    const resultEl = document.getElementById('ttDryRunResult');
    resultEl.classList.remove('hidden');
    if (data.error) {
      resultEl.innerHTML = `<div class="tt-dry-run-error"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> ${data.error}</div>`;
    } else {
      const warnings = data.warnings || [];
      resultEl.innerHTML = `<div class="tt-dry-run-ok">
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Order validated — ready to place
        ${warnings.length ? `<div class="tt-dry-run-warnings">${warnings.map(w => `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>️ ${w.message || w}`).join('<br>')}</div>` : ''}
      </div>`;
    }
  } catch (e) {
    document.getElementById('ttDryRunResult').innerHTML = `<div class="tt-dry-run-error"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> ${e.message}</div>`;
    document.getElementById('ttDryRunResult').classList.remove('hidden');
  } finally { btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14.5 2v13.5L19 21H5l4.5-5.5V2"/><line x1="8.5" y1="2" x2="15.5" y2="2"/></svg> Dry Run'; btn.disabled = false; }
}

async function ttPlaceOrder() {
  if (!ttPendingOrder) return;
  const price = parseFloat(document.getElementById('ttOrderPrice').value);
  const qty = parseInt(document.getElementById('ttOrderQty').value) || 1;
  if (!price) { alert('Enter a limit price'); return; }

  const isLive = ttState.mode === 'live';
  if (isLive && !confirm(`<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>️ LIVE ORDER\n\nYou are about to place a REAL options order:\n${ttPendingOrder.ticker} ${ttPendingOrder.type} $${ttPendingOrder.strike} — ${qty} contract(s) at $${price}\nTotal cost: ~$${(price*100*qty).toFixed(0)}\n\nThis uses REAL money. Confirm?`)) return;

  const btn = document.getElementById('ttPlaceOrderBtn');
  btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/></svg> Placing...'; btn.disabled = true;

  try {
    const res = await fetch('/api/tastytrade/order/place', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ symbol: ttPendingOrder.symbol, action: 'Buy to Open', quantity: qty, orderType: 'Limit', price })
    });
    const data = await res.json();
    if (data.error) { alert(`Order failed: ${data.error}`); return; }

    closeModal('modalTTOrder');
    showAlertToast({ severity: 'info', title: `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4.5 16.5c-1.5 1.26-2 5-2 5s3.74-.5 5-2c.71-.84.7-2.13-.09-2.91a2.18 2.18 0 0 0-2.91-.09z"/><path d="m12 15-3-3a22 22 0 0 1 2-3.95A12.88 12.88 0 0 1 22 2c0 2.72-.78 7.5-6 11a22.35 22.35 0 0 1-4 2z"/></svg> Order Placed on Tastytrade`, body: `${ttPendingOrder.ticker} ${ttPendingOrder.type} $${ttPendingOrder.strike} — ${qty} contract(s) @ $${price}` });

    // Auto-log to StockForge contracts
    await fetch('/api/daytrading/contracts/add', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        ticker: ttPendingOrder.ticker, type: ttPendingOrder.type,
        strike: ttPendingOrder.strike, expiry: ttPendingOrder.expiry,
        pricePaid: price, contracts: qty,
        dateBought: new Date().toISOString().split('T')[0],
        notes: `Placed on Tastytrade (${ttState.mode}) · Order ID: ${data.orderId || '—'}`
      })
    });
    await loadData();
    renderDayTrading();
    ttLoadOrders();
  } catch (e) { alert(`Error: ${e.message}`); }
  finally { btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4.5 16.5c-1.5 1.26-2 5-2 5s3.74-.5 5-2c.71-.84.7-2.13-.09-2.91a2.18 2.18 0 0 0-2.91-.09z"/><path d="m12 15-3-3a22 22 0 0 1 2-3.95A12.88 12.88 0 0 1 22 2c0 2.72-.78 7.5-6 11a22.35 22.35 0 0 1-4 2z"/></svg> Place Order'; btn.disabled = false; }
}

// ── Wire Place on Tastytrade into signal card ─────────────────────
function ttPlaceFromSignal(ticker, type, strike, expiry, premium) {
  if (!ttState.connected) {
    alert('Connect to Tastytrade first in the  Brokerage tab.');
    switchToTab('brokerage');
    return;
  }
  ttOpenOrderModal(ticker, type, strike, expiry, premium, null);
}
window.ttPlaceFromSignal = ttPlaceFromSignal;

// Init brokerage tab
document.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('.tab-btn').forEach(btn => {
    if (btn.dataset.tab === 'brokerage') {
      btn.addEventListener('click', () => {
        if (!btn._ttInit) { setupBrokerageTab(); btn._ttInit = true; }
        else if (ttState.connected) ttRefreshAll();
      });
    }
  });
});

// ─── WATCHLIST TAB ────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  // + Add Stock button
  document.getElementById('wlAddStock')?.addEventListener('click', () => openModal('modalLtAddWatchlist'));
  // + Add Coin button
  document.getElementById('wlAddCrypto')?.addEventListener('click', () => openModal('modalLtAddCryptoWatchlist'));
  // Refresh button
  document.getElementById('wlRefreshPrices')?.addEventListener('click', () => trackedRefresh());
  // Analyze button — reuse ltAnalyzeBtn logic
  document.getElementById('wlAnalyzeBtn')?.addEventListener('click', () => {
    const ticker = document.getElementById('wlAnalyzeTicker')?.value;
    if (!ticker) return alert('Select a stock first');
    // Set the lt analyze ticker and trigger analysis
    const ltSel = document.getElementById('ltAnalyzeTicker');
    if (ltSel) ltSel.value = ticker;
    analyzeStock();
    // Show output in watchlist tab
    const out = document.getElementById('wlAnalysisOutput');
    const ltOut = document.getElementById('ltAnalysisOutput');
    if (out && ltOut) {
      // Mirror the lt analysis output into watchlist tab
      const observer = new MutationObserver(() => { out.innerHTML = ltOut.innerHTML; });
      observer.observe(ltOut, { childList: true, subtree: true });
      setTimeout(() => observer.disconnect(), 30000);
    }
  });
});

// ─── PRE-TRADE INTELLIGENCE ───────────────────────────────────────
async function loadPreTradeIntel(ticker, btn) {
  const body = document.getElementById(`pti-body-${ticker}`);
  if (!body) return;
  if (btn) { btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/></svg> Loading...'; btn.disabled = true; }
  body.innerHTML = `<div class="sugg-loading" style="padding:10px 0"><div class="spinner"></div><span>Fetching IV Rank, SEC filings, macro events...</span></div>`;

  try {
    const res = await fetch(`/api/intelligence/pretrade/${ticker}`);
    const data = await res.json();
    renderPreTradeIntel(body, data, ticker);
    if (btn) { btn.textContent = '↻ Refresh'; btn.disabled = false; }
  } catch (e) {
    body.innerHTML = `<div style="color:var(--red);font-size:11px;padding:8px"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> Failed: ${e.message}</div>`;
    if (btn) { btn.textContent = 'Retry'; btn.disabled = false; }
  }
}
window.loadPreTradeIntel = loadPreTradeIntel;

function renderPreTradeIntel(container, data, ticker) {
  const scoreColor = data.scoreColor || 'yellow';
  const iv = data.iv || {};
  const filings = data.recentFilings || [];
  const macro = data.upcomingMacro || [];
  const signals = data.signals || [];

  // IV Rank interpretation
  const ivRank = iv.ivRank;
  const ivLabel = ivRank === null ? '—' : ivRank < 30 ? 'Low (Cheap)' : ivRank > 70 ? 'High (Expensive)' : 'Medium';
  const ivColor = ivRank === null ? 'var(--text3)' : ivRank < 30 ? 'var(--green)' : ivRank > 70 ? 'var(--red)' : 'var(--yellow)';
  const ivAdvice = ivRank === null ? '—' : ivRank < 30 ? '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Good time to BUY options' : ivRank > 70 ? '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>️ Consider SELLING premium instead' : '️ Neutral — options fairly priced';
  const ivSource = iv.source === 'tastytrade' ? '(Tastytrade live)' : '(estimated — connect Tastytrade for precise data)';

  // Recent SEC filings
  const recentFilings = filings.filter(f => f.daysAgo !== null && f.daysAgo <= 14);
  const formColors = { '8-K': 'var(--red)', '4': 'var(--blue)', '10-Q': 'var(--yellow)', '10-K': 'var(--yellow)', 'DEF 14A': 'var(--text2)' };

  // Macro events
  const urgentMacro = macro.filter(e => e.isToday || e.isTomorrow || e.daysUntil <= 3);

  container.innerHTML = `
    <!-- Score bar -->
    <div class="pti-score-row">
      <div class="pti-score-label">Environment</div>
      <div class="pti-score pti-score-${scoreColor}">${data.scoreLabel || '—'}</div>
      <div class="pti-score-bar-wrap">
        <div class="pti-score-bar" style="width:${data.score || 50}%;background:${scoreColor === 'green' ? 'var(--green)' : scoreColor === 'red' ? 'var(--red)' : 'var(--yellow)'}"></div>
      </div>
      <div class="pti-score-num">${data.score || 50}/100</div>
    </div>

    <!-- Key signals -->
    ${signals.length ? `<div class="pti-signals">
      ${signals.map(s => `<div class="pti-signal pti-signal-${s.type}">
        ${s.type === 'bullish' ? '●' : s.type === 'warning' ? '●' : '○'} ${s.text}
      </div>`).join('')}
    </div>` : ''}

    <!-- Three columns -->
    <div class="pti-cols">

      <!-- IV Rank -->
      <div class="pti-col">
        <div class="pti-col-title"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg> IV Rank</div>
        <div class="pti-iv-rank" style="color:${ivColor}">${ivRank !== null ? ivRank : '—'}</div>
        <div class="pti-iv-label" style="color:${ivColor}">${ivLabel}</div>
        <div class="pti-iv-advice">${ivAdvice}</div>
        ${iv.historicalVolatility ? `<div class="pti-iv-detail">HV30: ${iv.historicalVolatility}%</div>` : ''}
        ${iv.impliedVolatility ? `<div class="pti-iv-detail">IV: ${(iv.impliedVolatility*100).toFixed(1)}%</div>` : ''}
        <div class="pti-iv-source">${ivSource}</div>
      </div>

      <!-- SEC Filings -->
      <div class="pti-col">
        <div class="pti-col-title"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 5H7a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V7a2 2 0 0 0-2-2h-2"/><rect x="9" y="3" width="6" height="4" rx="1"/></svg> SEC Filings</div>
        ${recentFilings.length ? recentFilings.slice(0,4).map(f => `
          <div class="pti-filing">
            <span class="pti-filing-form" style="color:${formColors[f.form] || 'var(--text2)'}">${f.form}</span>
            <span class="pti-filing-date">${f.daysAgo === 0 ? 'Today' : f.daysAgo === 1 ? 'Yesterday' : `${f.daysAgo}d ago`}</span>
            ${f.isMaterial ? '<span class="pti-filing-alert"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>️ Material</span>' : ''}
            ${f.isInsider ? '<span class="pti-filing-alert pti-filing-insider"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg> Insider</span>' : ''}
          </div>`).join('')
        : '<div class="pti-empty-col">No recent filings (14d)</div>'}
        ${filings.length > 4 ? `<div class="pti-filing-more">+${filings.length - 4} more</div>` : ''}
      </div>

      <!-- Macro Events -->
      <div class="pti-col">
        <div class="pti-col-title"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/></svg> Macro Events</div>
        ${urgentMacro.length ? urgentMacro.slice(0,4).map(e => `
          <div class="pti-macro">
            <span class="pti-macro-impact pti-macro-${e.impact}">${e.impact === 'high' ? '●' : '●'}</span>
            <div class="pti-macro-info">
              <div class="pti-macro-event">${e.event}</div>
              <div class="pti-macro-time">${e.isToday ? 'Today' : e.isTomorrow ? 'Tomorrow' : `In ${e.daysUntil}d`}${e.estimate ? ` · Est: ${e.estimate}${e.unit || ''}` : ''}</div>
            </div>
          </div>`).join('')
        : '<div class="pti-empty-col">No high-impact events this week <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg></div>'}
      </div>

    </div>

    <!-- Kalshi Prediction Markets -->
    ${(data.kalshi?.markets?.length) ? `
    <div class="pti-kalshi">
      <div class="pti-col-title"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="6"/><circle cx="12" cy="12" r="2"/></svg> Kalshi Prediction Markets <span style="font-size:9px;color:var(--text3);font-weight:400">(real money odds)</span></div>
      <div class="pti-kalshi-grid">
        ${data.kalshi.markets.slice(0,6).map(m => `
          <div class="pti-kalshi-item">
            <div class="pti-kalshi-title">${m.title?.length > 45 ? m.title.slice(0,45)+'…' : m.title}</div>
            <div class="pti-kalshi-prob" style="color:${m.probability > 60 ? 'var(--green)' : m.probability > 40 ? 'var(--yellow)' : 'var(--red)'}">
              ${m.probability}%
            </div>
          </div>`).join('')}
      </div>
    </div>` : ''}

    <div class="pti-footer">Updated ${new Date(data.fetchedAt).toLocaleTimeString()} · Kalshi data: CFTC-regulated prediction market</div>`;
}

// Also expose for standalone use in Tools tab
window.loadPreTradeIntelStandalone = async function(ticker) {
  const res = await fetch(`/api/intelligence/pretrade/${ticker}`);
  return res.json();
};

// ═══════════════════════════════════════════════════════════════════
// SIGNAL ENGINE — Phase 1 UI
// ═══════════════════════════════════════════════════════════════════

let signalCurrentData = null; // currently selected ticker score data

document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('signalScanBtn')?.addEventListener('click', runSignalScan);
  document.getElementById('signalManualBtn')?.addEventListener('click', () => {
    const ticker = document.getElementById('signalManualTicker')?.value.trim().toUpperCase();
    if (ticker) scoreManualTicker(ticker);
  });
  document.getElementById('signalManualTicker')?.addEventListener('keydown', e => {
    if (e.key === 'Enter') {
      const ticker = e.target.value.trim().toUpperCase();
      if (ticker) scoreManualTicker(ticker);
    }
  });
  document.getElementById('signalAiBriefBtn')?.addEventListener('click', generateSignalAiBrief);
});

async function runSignalScan() {
  const btn = document.getElementById('signalScanBtn');
  const statusEl = document.getElementById('signalScanStatus');
  const funnelCard = document.getElementById('signalFunnelCard');
  const resultsCard = document.getElementById('signalResultsCard');

  btn.disabled = true;
  btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg> Scanning...';
  statusEl.innerHTML = '<div class="loading-row"><div class="spinner"></div><span>Running universe funnel...</span></div>';
  funnelCard.classList.add('hidden');
  resultsCard.classList.add('hidden');

  const results = [];

  try {
    const res = await fetch('/api/signals/scan', { method: 'POST', headers: { 'Content-Type': 'application/json' } });
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n'); buffer = lines.pop();
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const msg = JSON.parse(line.slice(6));
          if (msg.type === 'status') {
            statusEl.innerHTML = `<div class="loading-row"><div class="spinner"></div><span>${msg.text}</span></div>`;
          } else if (msg.type === 'funnel') {
            funnelCard.classList.remove('hidden');
            document.getElementById('signalFunnelStats').innerHTML = `
              <div class="signals-funnel-stat"><div class="signals-funnel-stat-value">${msg.data.candidates}</div><div class="signals-funnel-stat-label">Candidates</div></div>
              <div class="signals-funnel-stat"><div class="signals-funnel-stat-value">${msg.data.earningsCount}</div><div class="signals-funnel-stat-label">Earnings 7d</div></div>
              <div class="signals-funnel-stat"><div class="signals-funnel-stat-value">${msg.data.insiderCount}</div><div class="signals-funnel-stat-label">Insider Buys</div></div>
            `;
          } else if (msg.type === 'ticker') {
            results.push(msg.data);
            renderSignalResultsList(results);
            resultsCard.classList.remove('hidden');
          } else if (msg.type === 'complete') {
            const actionable = msg.data.actionable?.length || 0;
            statusEl.innerHTML = `<div style="color:var(--green);font-weight:700">Scan complete — ${msg.data.results?.length || 0} scored, ${actionable} meet convergence rule</div>`;
            document.getElementById('signalResultsCount').textContent = `${msg.data.results?.length || 0} scored`;
          } else if (msg.type === 'error') {
            statusEl.innerHTML = `<div class="empty-state red">Error: ${msg.text}</div>`;
          }
        } catch {}
      }
    }
  } catch (e) {
    statusEl.innerHTML = `<div class="empty-state red">Scan failed: ${e.message}</div>`;
  } finally {
    btn.disabled = false;
    btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg> Run Scan';
  }
}

function renderSignalResultsList(results) {
  const el = document.getElementById('signalResultsList');
  const sorted = [...results].sort((a, b) => b.score - a.score);
  el.innerHTML = sorted.map(r => {
    const barColor = r.color === 'green' ? 'var(--green)' : r.color === 'yellow' ? 'var(--yellow)' : 'var(--red)';
    const badge = r.meetsConvergenceRule
      ? '<span class="signal-result-badge convergence">CONVERGENCE</span>'
      : r.hasAnchorSignal
        ? '<span class="signal-result-badge anchor">ANCHOR</span>'
        : '<span class="signal-result-badge watch">WATCH</span>';
    return `<div class="signal-result-row" onclick="selectSignalTicker('${r.ticker}', ${JSON.stringify(r).replace(/"/g, '&quot;')})">
      <div class="signal-result-ticker">${r.ticker}</div>
      <div class="signal-result-bar-wrap">
        <div class="signal-result-bar-bg"><div class="signal-result-bar" style="width:${r.score}%;background:${barColor}"></div></div>
        <div class="signal-result-score ${r.color}">${r.score}</div>
      </div>
      ${badge}
    </div>`;
  }).join('');
}

async function scoreManualTicker(ticker) {
  const resultEl = document.getElementById('signalManualResult');
  resultEl.innerHTML = '<div class="loading-row"><div class="spinner"></div><span>Scoring ' + ticker + '...</span></div>';
  try {
    const res = await fetch(`/api/signals/score/${ticker}`);
    const data = await res.json();
    if (data.error) { resultEl.innerHTML = `<div class="empty-state red">${data.error}</div>`; return; }
    resultEl.innerHTML = `<div class="signal-result-row" onclick="selectSignalTicker('${data.ticker}', ${JSON.stringify(data).replace(/"/g, '&quot;')})">
      <div class="signal-result-ticker">${data.ticker}</div>
      <div class="signal-result-bar-wrap">
        <div class="signal-result-bar-bg"><div class="signal-result-bar" style="width:${data.score}%;background:${data.color === 'green' ? 'var(--green)' : data.color === 'yellow' ? 'var(--yellow)' : 'var(--red)'}"></div></div>
        <div class="signal-result-score ${data.color}">${data.score}</div>
      </div>
      <span class="signal-result-badge ${data.meetsConvergenceRule ? 'convergence' : data.hasAnchorSignal ? 'anchor' : 'watch'}">${data.meetsConvergenceRule ? 'CONVERGENCE' : data.hasAnchorSignal ? 'ANCHOR' : 'WATCH'}</span>
    </div>`;
    selectSignalTicker(data.ticker, data);
  } catch (e) {
    resultEl.innerHTML = `<div class="empty-state red">Failed: ${e.message}</div>`;
  }
}

function selectSignalTicker(ticker, data) {
  signalCurrentData = data;
  document.getElementById('signalDetailTicker').textContent = ticker;
  document.getElementById('signalAiBriefBtn').classList.remove('hidden');
  document.getElementById('signalAiBrief').innerHTML = '<div class="empty-state">Click "Generate Brief" to get AI analysis.</div>';

  const bannerClass = data.meetsConvergenceRule ? 'met' : data.watchOnly ? 'watch' : 'weak';
  const bannerText = data.meetsConvergenceRule
    ? `Convergence rule MET — anchor signal present + ${data.firedCount} supporting signals`
    : data.watchOnly
      ? 'WATCH ONLY — no unusual options volume (anchor signal missing)'
      : `Weak convergence — ${data.firedCount} signals fired, need anchor + 2 more`;

  document.getElementById('signalBreakdown').innerHTML = `
    <div class="signal-convergence-banner ${bannerClass}">${bannerText}</div>
    ${(data.signals || []).map(s => `
      <div class="signal-breakdown-row">
        <div class="signal-breakdown-dot ${s.id === 1 && s.fired ? 'anchor-fired' : s.fired ? 'fired' : 'unfired'}"></div>
        <div style="flex:1">
          <div style="display:flex;justify-content:space-between;align-items:center">
            <span class="signal-breakdown-name">S${s.id}: ${s.name}</span>
            <span class="signal-breakdown-weight">${s.weight > 0 ? '+' + s.weight + ' pts' : 'filter only'}</span>
          </div>
          <div class="signal-breakdown-text">${s.text}</div>
          ${s.note ? `<div class="signal-breakdown-note">${s.note}</div>` : ''}
        </div>
      </div>
    `).join('')}
  `;
}
window.selectSignalTicker = selectSignalTicker;

async function generateSignalAiBrief() {
  if (!signalCurrentData) return;
  const btn = document.getElementById('signalAiBriefBtn');
  const briefEl = document.getElementById('signalAiBrief');
  btn.textContent = 'Generating...'; btn.disabled = true;
  briefEl.innerHTML = '<div class="loading-row"><div class="spinner"></div><span>AI analyzing signals...</span></div>';

  try {
    const res = await fetch('/api/signals/ai-brief', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ticker: signalCurrentData.ticker, scoreData: signalCurrentData })
    });
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n'); buffer = lines.pop();
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const msg = JSON.parse(line.slice(6));
          if (msg.type === 'brief') {
            const b = msg.data;
            const actionClass = b.action?.includes('CALL') ? 'buy' : b.action?.includes('PUT') ? 'sell' : 'watch';
            briefEl.innerHTML = `<div class="signal-ai-brief-card">
              <div class="signal-ai-action ${actionClass}">${b.action}</div>
              <div class="signal-ai-reasoning">${b.reasoning}</div>
              <div class="signal-ai-grid">
                <div class="signal-ai-item"><div class="signal-ai-item-label">Entry</div><div class="signal-ai-item-value">${b.entry || '—'}</div></div>
                <div class="signal-ai-item"><div class="signal-ai-item-label">Target</div><div class="signal-ai-item-value">${b.target || '—'}</div></div>
                <div class="signal-ai-item"><div class="signal-ai-item-label">Timeframe</div><div class="signal-ai-item-value">${b.timeframe || '2-4 weeks'}</div></div>
                <div class="signal-ai-item"><div class="signal-ai-item-label">Confidence</div><div class="signal-ai-item-value">${b.confidence || '—'}%</div></div>
              </div>
              ${b.invalidates ? `<div style="margin-top:8px;font-size:11px;color:var(--text3)"><b>Exit if:</b> ${b.invalidates}</div>` : ''}
            </div>`;
          } else if (msg.type === 'error') {
            briefEl.innerHTML = `<div class="empty-state red">${msg.text}</div>`;
          }
        } catch {}
      }
    }
  } catch (e) {
    briefEl.innerHTML = `<div class="empty-state red">Failed: ${e.message}</div>`;
  } finally {
    btn.textContent = 'Generate Brief'; btn.disabled = false;
  }
}

// ═══════════════════════════════════════════════════════════════════
// KALSHI BET PREDICTOR — Phase 2 UI
// ═══════════════════════════════════════════════════════════════════

let kalshiCurrentCategory = null;

document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('kalshiScanBtn')?.addEventListener('click', runKalshiScan);
  document.getElementById('kalshiAiBtn')?.addEventListener('click', getKalshiAiRecommendation);
});

async function runKalshiScan() {
  const btn = document.getElementById('kalshiScanBtn');
  const container = document.getElementById('kalshiCategories');
  btn.disabled = true;
  btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg> Scanning...';
  container.innerHTML = '<div class="loading-row"><div class="spinner"></div><span>Fetching external data and Kalshi markets...</span></div>';

  try {
    const res = await fetch('/api/kalshi/scan');
    const data = await res.json();
    if (data.error) { container.innerHTML = `<div class="empty-state red">${data.error}</div>`; return; }
    renderKalshiCategories(data.categories);
    // Show actionable count
    const actionable = data.actionable?.length || 0;
    if (actionable > 0) {
      showAlertToast({ severity: 'info', title: `${actionable} Kalshi opportunity${actionable > 1 ? 'ies' : 'y'} found`, body: 'Divergence threshold met — check Kalshi tab' });
    }
  } catch (e) {
    container.innerHTML = `<div class="empty-state red">Scan failed: ${e.message}</div>`;
  } finally {
    btn.disabled = false;
    btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg> Scan Markets';
  }
}

function renderKalshiCategories(categories) {
  const container = document.getElementById('kalshiCategories');
  if (!categories?.length) { container.innerHTML = '<div class="empty-state">No categories returned.</div>'; return; }

  container.innerHTML = categories.map((cat, i) => {
    const edgeClass = cat.edge === 'HIGH' ? 'HIGH' : cat.edge === 'MEDIUM' ? 'MEDIUM' : cat.edge === 'N/A' ? 'NA' : 'LOW';
    const isActionable = cat.meetsThreshold;
    const isOff = cat.active === false;
    const statusText = isActionable ? 'THRESHOLD MET — investigate' : cat.withinWindow ? 'In window — monitoring' : 'Outside time window';
    const statusClass = isActionable ? 'meets' : cat.withinWindow ? 'watch' : 'skip';
    const price = cat.kalshiPrice ?? cat.kalshiMarket?.probability ?? null;

    return `<div class="kalshi-cat-card ${isActionable ? 'actionable' : ''} ${isOff ? 'blocked' : ''}" onclick="selectKalshiCategory(${i})">
      <div class="kalshi-cat-header">
        <div class="kalshi-cat-name">${cat.category}</div>
        <div class="kalshi-cat-edge ${edgeClass}">${cat.edge}</div>
      </div>
      <div class="kalshi-cat-external">${cat.externalValue || 'Loading...'}</div>
      ${price !== null ? `
        <div class="kalshi-cat-price">
          <span class="kalshi-cat-price-label">Kalshi Price</span>
          <span class="kalshi-cat-price-value" style="color:${price > 60 ? 'var(--green)' : price < 40 ? 'var(--red)' : 'var(--yellow)'}">${price}%</span>
        </div>` : ''}
      ${cat.divergence > 0 ? `<div style="font-size:10px;color:var(--accent);margin-top:4px">Divergence: ${cat.divergence}pts</div>` : ''}
      <div class="kalshi-cat-status ${statusClass}">${isOff ? cat.note : statusText}</div>
    </div>`;
  }).join('');

  // Store for selection
  window._kalshiCategories = categories;
}

function selectKalshiCategory(index) {
  const cat = window._kalshiCategories?.[index];
  if (!cat || cat.active === false) return;
  kalshiCurrentCategory = cat;

  const detailCard = document.getElementById('kalshiDetailCard');
  const detailTitle = document.getElementById('kalshiDetailTitle');
  const detailBody = document.getElementById('kalshiDetailBody');
  const aiOutput = document.getElementById('kalshiAiOutput');

  detailCard.classList.remove('hidden');
  detailTitle.textContent = cat.category;
  aiOutput.classList.add('hidden');
  aiOutput.innerHTML = '';

  const price = cat.kalshiPrice ?? cat.kalshiMarket?.probability ?? null;
  const market = cat.kalshiMarket || cat.kalshiMarkets?.[0];

  detailBody.innerHTML = `
    <div class="kalshi-detail-grid">
      <div class="kalshi-detail-item">
        <div class="kalshi-detail-label">External Source</div>
        <div class="kalshi-detail-value">${cat.externalSource || '—'}</div>
      </div>
      <div class="kalshi-detail-item">
        <div class="kalshi-detail-label">External Data</div>
        <div class="kalshi-detail-value">${cat.externalValue || '—'}</div>
      </div>
      <div class="kalshi-detail-item">
        <div class="kalshi-detail-label">Kalshi Market</div>
        <div class="kalshi-detail-value" style="font-size:10px">${market?.title?.slice(0,50) || '—'}</div>
      </div>
      <div class="kalshi-detail-item">
        <div class="kalshi-detail-label">Kalshi Price</div>
        <div class="kalshi-detail-value" style="color:${price > 60 ? 'var(--green)' : price < 40 ? 'var(--red)' : 'var(--yellow)'}">${price !== null ? price + '%' : '—'}</div>
      </div>
      <div class="kalshi-detail-item">
        <div class="kalshi-detail-label">Divergence</div>
        <div class="kalshi-detail-value" style="color:${(cat.divergence||0) >= 15 ? 'var(--green)' : 'var(--text3)'}">${cat.divergence || 0}pts ${(cat.divergence||0) >= 15 ? '✓ threshold met' : '(need 15+)'}</div>
      </div>
      <div class="kalshi-detail-item">
        <div class="kalshi-detail-label">Days Until Event</div>
        <div class="kalshi-detail-value" style="color:${cat.withinWindow ? 'var(--green)' : 'var(--text3)'}">${cat.daysUntilEvent || '—'} ${cat.withinWindow ? '✓ in window' : '(need ≤72hrs)'}</div>
      </div>
      <div class="kalshi-detail-item">
        <div class="kalshi-detail-label">Liquidity</div>
        <div class="kalshi-detail-value" style="color:${cat.liquidity ? 'var(--green)' : 'var(--red)'}">${cat.liquidity ? 'PASS' : 'FAIL'}</div>
      </div>
      <div class="kalshi-detail-item">
        <div class="kalshi-detail-label">Edge Level</div>
        <div class="kalshi-detail-value">${cat.edge || '—'}</div>
      </div>
    </div>
    ${cat.note ? `<div style="margin-top:10px;font-size:11px;color:var(--text3);padding:8px;background:var(--bg3);border-radius:var(--radius-sm)">${cat.note}</div>` : ''}
    ${cat.meetsThreshold ? `<div style="margin-top:10px;padding:8px 12px;background:var(--green-bg);border:1px solid var(--green);border-radius:var(--radius-sm);color:var(--green);font-weight:700;font-size:12px">All 3 conditions met — get AI recommendation</div>` : ''}
  `;

  // Show Place Bet button if connected and market exists
  const betBtn = document.getElementById('kalshiBetBtn');
  if (betBtn && market?.ticker && price !== null) {
    fetch('/api/kalshi/auth-status').then(r => r.json()).then(d => {
      if (d.connected) {
        betBtn.classList.remove('hidden');
        betBtn.onclick = () => kalshiOpenBetModal(market.ticker, market.title, price, 'yes');
      } else {
        betBtn.classList.add('hidden');
      }
    }).catch(() => betBtn.classList.add('hidden'));
  }

  // Scroll to detail
  detailCard.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}
window.selectKalshiCategory = selectKalshiCategory;

async function getKalshiAiRecommendation() {
  if (!kalshiCurrentCategory) return;
  const btn = document.getElementById('kalshiAiBtn');
  const output = document.getElementById('kalshiAiOutput');
  btn.textContent = 'Analyzing...'; btn.disabled = true;
  output.classList.remove('hidden');
  output.innerHTML = '<div class="loading-row"><div class="spinner"></div><span>AI analyzing divergence...</span></div>';

  try {
    const res = await fetch('/api/kalshi/ai-recommend', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ category: kalshiCurrentCategory })
    });
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n'); buffer = lines.pop();
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const msg = JSON.parse(line.slice(6));
          if (msg.type === 'recommendation') {
            const r = msg.data;
            const actionClass = r.action?.includes('YES') ? 'bet-yes' : r.action?.includes('NO') ? 'bet-no' : r.action === 'WATCH' ? 'watch' : 'skip';
            output.innerHTML = `<div class="kalshi-ai-card">
              <div class="kalshi-ai-action ${actionClass}">${r.action}</div>
              <div class="kalshi-ai-reasoning">${r.reasoning}</div>
              <div class="kalshi-ai-meta">
                <div class="kalshi-ai-meta-item"><div class="kalshi-ai-meta-label">Position Size</div><div class="kalshi-ai-meta-value">${r.suggestedSize || '—'}</div></div>
                <div class="kalshi-ai-meta-item"><div class="kalshi-ai-meta-label">Time Window</div><div class="kalshi-ai-meta-value">${r.timeWindow || '—'}</div></div>
                <div class="kalshi-ai-meta-item"><div class="kalshi-ai-meta-label">Confidence</div><div class="kalshi-ai-meta-value">${r.confidence || '—'}%</div></div>
              </div>
              ${r.keyRisk ? `<div style="margin-top:8px;font-size:11px;color:var(--text3)"><b>Key risk:</b> ${r.keyRisk}</div>` : ''}
            </div>`;
          } else if (msg.type === 'error') {
            output.innerHTML = `<div class="empty-state red">${msg.text}</div>`;
          }
        } catch {}
      }
    }
  } catch (e) {
    output.innerHTML = `<div class="empty-state red">Failed: ${e.message}</div>`;
  } finally {
    btn.textContent = 'Get AI Recommendation'; btn.disabled = false;
  }
}

// ═══════════════════════════════════════════════════════════════════
// KALSHI TRADING — Connect, Balance, Positions, Place Bets
// ═══════════════════════════════════════════════════════════════════

let kalshiBetTicker = null;
let kalshiBetSide = 'yes';

document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('kalshiConnectBtn')?.addEventListener('click', kalshiConnect);
  document.getElementById('kalshiDisconnectBtn')?.addEventListener('click', kalshiDisconnect);
  document.getElementById('kalshiRefreshPositions')?.addEventListener('click', kalshiLoadPositions);
  document.getElementById('kalshiBetConfirm')?.addEventListener('click', kalshiPlaceBet);
  document.getElementById('kalshiBetPrice')?.addEventListener('input', kalshiUpdateBetCost);
  document.getElementById('kalshiBetCount')?.addEventListener('input', kalshiUpdateBetCost);

  // Load status on tab open
  document.querySelector('.tab-btn[data-tab="kalshi"]')?.addEventListener('click', kalshiCheckStatus);
});

async function kalshiCheckStatus() {
  try {
    const res = await fetch('/api/kalshi/auth-status');
    const data = await res.json();
    if (data.connected) {
      kalshiShowConnected(data);
      kalshiLoadBalance();
      kalshiLoadPositions();
    } else {
      kalshiShowDisconnected();
    }
  } catch {}
}

async function kalshiConnect() {
  const apiKeyId = document.getElementById('kalshiApiKeyId')?.value.trim();
  const privateKey = document.getElementById('kalshiPrivateKey')?.value.trim();
  if (!apiKeyId || !privateKey) { kalshiShowMsg('Enter both API Key ID and Private Key', 'error'); return; }

  const btn = document.getElementById('kalshiConnectBtn');
  btn.textContent = 'Connecting...'; btn.disabled = true;
  kalshiShowMsg('Saving credentials...', 'loading');

  try {
    // Save credentials
    const saveRes = await fetch('/api/kalshi/connect', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ apiKeyId, privateKey })
    });
    const saveData = await saveRes.json();
    if (!saveData.ok) { kalshiShowMsg(saveData.error || 'Failed to save', 'error'); return; }

    // Test by fetching balance
    kalshiShowMsg('Testing connection...', 'loading');
    const balRes = await fetch('/api/kalshi/balance');
    const balData = await balRes.json();
    if (balData.error) { kalshiShowMsg('Credentials saved but connection test failed: ' + balData.error, 'error'); return; }

    kalshiShowConnected({ apiKeyId });
    kalshiShowMsg('', '');
    document.getElementById('kalshiBalance').textContent = '$' + (balData.balance / 100).toFixed(2);
    kalshiLoadPositions();
  } catch (e) {
    kalshiShowMsg('Connection failed: ' + e.message, 'error');
  } finally {
    btn.textContent = 'Connect to Kalshi'; btn.disabled = false;
  }
}

async function kalshiDisconnect() {
  await fetch('/api/kalshi/disconnect', { method: 'POST' });
  kalshiShowDisconnected();
}

function kalshiShowConnected(data) {
  document.getElementById('kalshiStatusDot').className = 'tt-status-dot connected';
  document.getElementById('kalshiStatusLabel').textContent = 'Connected · ' + (data.apiKeyId?.slice(0,8) || '');
  document.getElementById('kalshiConnectedPanel').classList.remove('hidden');
  document.getElementById('kalshiSetupPanel').classList.add('hidden');
  document.getElementById('kalshiTradingPanel').classList.remove('hidden');
}

function kalshiShowDisconnected() {
  document.getElementById('kalshiStatusDot').className = 'tt-status-dot';
  document.getElementById('kalshiStatusLabel').textContent = 'Not Connected';
  document.getElementById('kalshiConnectedPanel').classList.add('hidden');
  document.getElementById('kalshiSetupPanel').classList.remove('hidden');
  document.getElementById('kalshiTradingPanel').classList.add('hidden');
}

function kalshiShowMsg(msg, type) {
  const el = document.getElementById('kalshiConnectMsg');
  if (!el) return;
  if (!msg) { el.classList.add('hidden'); return; }
  el.classList.remove('hidden');
  el.style.color = type === 'error' ? 'var(--red)' : type === 'loading' ? 'var(--text3)' : 'var(--green)';
  el.textContent = msg;
}

async function kalshiLoadBalance() {
  try {
    const res = await fetch('/api/kalshi/balance');
    const data = await res.json();
    if (data.balance !== undefined) {
      document.getElementById('kalshiBalance').textContent = '$' + (data.balance / 100).toFixed(2);
    }
  } catch {}
}

async function kalshiLoadPositions() {
  const posEl = document.getElementById('kalshiPositionsList');
  const ordEl = document.getElementById('kalshiOrdersList');
  const countEl = document.getElementById('kalshiPositionCount');

  try {
    const [posRes, ordRes] = await Promise.all([
      fetch('/api/kalshi/positions'),
      fetch('/api/kalshi/orders')
    ]);
    const posData = await posRes.json();
    const ordData = await ordRes.json();

    const positions = posData.positions || [];
    const orders = ordData.orders || [];

    if (countEl) countEl.textContent = positions.length;

    // Render positions
    posEl.innerHTML = positions.length ? positions.map(p => `
      <div class="kalshi-position-row">
        <div>
          <div class="kalshi-position-ticker">${p.ticker}</div>
          <div class="kalshi-position-title">${p.marketTitle || ''}</div>
        </div>
        <div style="text-align:right">
          <div style="font-weight:700">${p.yesContracts > 0 ? p.yesContracts + ' YES' : p.noContracts + ' NO'}</div>
          <div style="font-size:10px;color:${p.realizedPnl >= 0 ? 'var(--green)' : 'var(--red)'}">${p.realizedPnl >= 0 ? '+' : ''}$${(p.realizedPnl / 100).toFixed(2)}</div>
        </div>
      </div>
    `).join('') : '<div class="empty-state">No open positions.</div>';

    // Render orders
    ordEl.innerHTML = orders.length ? orders.map(o => `
      <div class="kalshi-order-row">
        <div>
          <div style="font-weight:700;font-size:12px">${o.ticker}</div>
          <div style="font-size:10px;color:var(--text3)">${o.side?.toUpperCase()} ${o.remaining_count} @ ${o.yes_price}¢</div>
        </div>
        <button class="btn-secondary btn-sm" onclick="kalshiCancelOrder('${o.order_id}')">Cancel</button>
      </div>
    `).join('') : '<div class="empty-state">No open orders.</div>';

  } catch (e) {
    posEl.innerHTML = `<div class="empty-state red">${e.message}</div>`;
  }
}
window.kalshiLoadPositions = kalshiLoadPositions;

async function kalshiCancelOrder(orderId) {
  try {
    await fetch(`/api/kalshi/order/${orderId}`, { method: 'DELETE' });
    kalshiLoadPositions();
    showAlertToast({ severity: 'info', title: 'Order cancelled', body: orderId });
  } catch (e) {
    alert('Failed to cancel: ' + e.message);
  }
}
window.kalshiCancelOrder = kalshiCancelOrder;

// ── Place Bet from AI recommendation ─────────────────────────────
function kalshiOpenBetModal(ticker, title, currentPrice, side) {
  kalshiBetTicker = ticker;
  kalshiBetSide = side || 'yes';

  document.getElementById('kalshiBetMarketInfo').innerHTML = `
    <div style="font-weight:700;margin-bottom:4px">${ticker}</div>
    <div style="color:var(--text2)">${title || ''}</div>
    <div style="margin-top:6px;display:flex;gap:12px">
      <span>YES: <b>${currentPrice}¢</b></span>
      <span>NO: <b>${100 - currentPrice}¢</b></span>
    </div>
  `;

  kalshiSelectSide(side || 'yes');
  document.getElementById('kalshiBetPrice').value = currentPrice || '';
  document.getElementById('kalshiBetCount').value = '1';
  kalshiUpdateBetCost();
  openModal('modalKalshiBet');
}
window.kalshiOpenBetModal = kalshiOpenBetModal;

function kalshiSelectSide(side) {
  kalshiBetSide = side;
  const yesBtn = document.getElementById('kalshiBetYes');
  const noBtn = document.getElementById('kalshiBetNo');
  if (yesBtn) { yesBtn.className = 'kalshi-side-btn' + (side === 'yes' ? ' active yes' : ''); }
  if (noBtn) { noBtn.className = 'kalshi-side-btn' + (side === 'no' ? ' active no' : ''); }
  kalshiUpdateBetCost();
}
window.kalshiSelectSide = kalshiSelectSide;

function kalshiUpdateBetCost() {
  const price = parseInt(document.getElementById('kalshiBetPrice')?.value) || 0;
  const count = parseInt(document.getElementById('kalshiBetCount')?.value) || 1;
  const costEl = document.getElementById('kalshiBetCost');
  if (costEl && price > 0) {
    const totalCents = price * count;
    const maxProfit = (100 - price) * count;
    costEl.textContent = `Cost: $${(totalCents / 100).toFixed(2)} · Max profit: $${(maxProfit / 100).toFixed(2)}`;
  }
}

async function kalshiPlaceBet() {
  const price = parseInt(document.getElementById('kalshiBetPrice')?.value);
  const count = parseInt(document.getElementById('kalshiBetCount')?.value) || 1;
  if (!kalshiBetTicker || !price) { alert('Enter a price'); return; }
  if (price < 1 || price > 99) { alert('Price must be 1-99 cents'); return; }

  const cost = (price * count / 100).toFixed(2);
  if (!confirm(`Place bet:\n${kalshiBetTicker} — ${kalshiBetSide.toUpperCase()}\n${count} contract(s) @ ${price}¢\nTotal cost: $${cost}\n\nThis uses REAL money on Kalshi. Confirm?`)) return;

  const btn = document.getElementById('kalshiBetConfirm');
  btn.textContent = 'Placing...'; btn.disabled = true;

  try {
    const res = await fetch('/api/kalshi/order', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ticker: kalshiBetTicker, side: kalshiBetSide, count, price })
    });
    const data = await res.json();
    if (data.error) { alert('Order failed: ' + data.error); return; }
    closeModal('modalKalshiBet');
    showAlertToast({ severity: 'info', title: 'Kalshi bet placed!', body: `${kalshiBetTicker} ${kalshiBetSide.toUpperCase()} ${count}x @ ${price}¢` });
    kalshiLoadPositions();
    kalshiLoadBalance();
  } catch (e) {
    alert('Failed: ' + e.message);
  } finally {
    btn.textContent = 'Place Bet'; btn.disabled = false;
  }
}

// ═══════════════════════════════════════════════════════════════════
// BACKTEST MODULE
// ═══════════════════════════════════════════════════════════════════

let btRange = '1y';
let btResult = null;
let btChartInstance = null;

async function btInit() {
  // Load strategies into select
  try {
    const strategies = await fetch('/api/backtest/strategies').then(r => r.json());
    const sel = document.getElementById('btStrategy');
    sel.innerHTML = strategies.map(s =>
      `<option value="${s.id}">${s.name}</option>`
    ).join('');
    btUpdateStrategyDesc(strategies);
    sel.addEventListener('change', () => btUpdateStrategyDesc(strategies));
  } catch (e) { console.error('btInit:', e); }

  // Wire up ticker search + validation (same pattern as rest of app)
  setupTickerSearch('btTicker', 'btTickerDropdown', 'btTickerMsg');

  // Period buttons
  document.querySelectorAll('.bt-period-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.bt-period-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      btRange = btn.dataset.range;
    });
  });

  // Run button — validate ticker first
  document.getElementById('btRunBtn')?.addEventListener('click', btRun);

  // AI button
  document.getElementById('btAiBtn')?.addEventListener('click', btAskAI);
}

function btUpdateStrategyDesc(strategies) {
  const sel = document.getElementById('btStrategy');
  const desc = document.getElementById('btStrategyDesc');
  const found = strategies.find(s => s.id === sel.value);
  if (found) {
    desc.textContent = `${found.description} — ${found.params}`;
    desc.classList.add('visible');
  } else {
    desc.classList.remove('visible');
  }
}

async function btRun() {
  const strategy = document.getElementById('btStrategy')?.value;
  const capital  = parseFloat(document.getElementById('btCapital')?.value) || 10000;
  const btn      = document.getElementById('btRunBtn');

  if (!strategy) { showToast('Select a strategy', 'warning'); return; }

  // Validate ticker using the same flow as rest of app
  const ticker = await validateAndGetTicker('btTicker', 'btTickerMsg');
  if (!ticker) return; // validateAndGetTicker shows the error message inline

  btn.textContent = 'Running...';
  btn.disabled = true;

  try {
    const res = await fetch('/api/backtest/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ticker, strategy, range: btRange, capital })
    });
    const data = await res.json();
    if (data.error) throw new Error(data.error);

    btResult = data;
    btRenderResults(data);
  } catch (e) {
    showToast('Backtest failed: ' + e.message, 'error');
  } finally {
    btn.textContent = '▶ Run Backtest';
    btn.disabled = false;
  }
}

function btRenderResults(data) {
  const { summary, trades, candles, ticker, strategy } = data;

  // Show cards
  document.getElementById('btStatsCard').style.display = '';
  document.getElementById('btTradesCard').style.display = '';
  document.getElementById('btAiCard').style.display = '';

  // Result label
  document.getElementById('btResultLabel').textContent =
    `${ticker} · ${strategy.replace('_', ' ').toUpperCase()} · ${data.range}`;

  // Stats grid
  const totalRetClass = summary.totalReturn >= 0 ? 'green' : 'red';
  const alphaClass    = summary.alpha >= 0 ? 'green' : 'red';
  const sharpeClass   = summary.sharpe >= 1 ? 'green' : summary.sharpe >= 0 ? 'yellow' : 'red';

  document.getElementById('btStatsGrid').innerHTML = `
    <div class="bt-stat-item">
      <div class="bt-stat-label">Total Return</div>
      <div class="bt-stat-value ${totalRetClass}">${summary.totalReturn >= 0 ? '+' : ''}${summary.totalReturn}%</div>
    </div>
    <div class="bt-stat-item">
      <div class="bt-stat-label">Final Value</div>
      <div class="bt-stat-value">$${summary.finalValue.toLocaleString()}</div>
    </div>
    <div class="bt-stat-item">
      <div class="bt-stat-label">Win Rate</div>
      <div class="bt-stat-value ${summary.winRate >= 50 ? 'green' : 'red'}">${summary.winRate}%</div>
    </div>
    <div class="bt-stat-item">
      <div class="bt-stat-label">Total Trades</div>
      <div class="bt-stat-value">${summary.totalTrades} <span style="font-size:11px;color:var(--text3)">(${summary.wins}W / ${summary.losses}L)</span></div>
    </div>
    <div class="bt-stat-item">
      <div class="bt-stat-label">Sharpe Ratio</div>
      <div class="bt-stat-value ${sharpeClass}">${summary.sharpe}</div>
    </div>
    <div class="bt-stat-item">
      <div class="bt-stat-label">Max Drawdown</div>
      <div class="bt-stat-value red">-${summary.maxDrawdown}%</div>
    </div>
    <div class="bt-stat-item">
      <div class="bt-stat-label">Alpha vs Buy&Hold</div>
      <div class="bt-stat-value ${alphaClass}">${summary.alpha >= 0 ? '+' : ''}${summary.alpha}%</div>
    </div>
    <div class="bt-stat-item">
      <div class="bt-stat-label">Avg Hold Days</div>
      <div class="bt-stat-value blue">${summary.avgHoldDays}d</div>
    </div>
  `;

  // Alpha comparison bar
  const maxVal = Math.max(Math.abs(summary.totalReturn), Math.abs(summary.buyHoldReturn), 1);
  const stratW = Math.min(100, (Math.abs(summary.totalReturn) / maxVal) * 100);
  const bhW    = Math.min(100, (Math.abs(summary.buyHoldReturn) / maxVal) * 100);
  document.getElementById('btAlphaBar').innerHTML = `
    <div class="bt-alpha-label">Strategy vs Buy & Hold</div>
    <div class="bt-alpha-row">
      <div class="bt-alpha-name">Strategy</div>
      <div class="bt-alpha-track"><div class="bt-alpha-fill-strategy" style="width:${stratW}%"></div></div>
      <div class="bt-alpha-pct ${totalRetClass}">${summary.totalReturn >= 0 ? '+' : ''}${summary.totalReturn}%</div>
    </div>
    <div class="bt-alpha-row">
      <div class="bt-alpha-name">Buy & Hold</div>
      <div class="bt-alpha-track"><div class="bt-alpha-fill-buyhold" style="width:${bhW}%"></div></div>
      <div class="bt-alpha-pct ${summary.buyHoldReturn >= 0 ? 'green' : 'red'}">${summary.buyHoldReturn >= 0 ? '+' : ''}${summary.buyHoldReturn}%</div>
    </div>
  `;

  // Chart
  btRenderChart(candles, trades);

  // Trade log
  document.getElementById('btTradeCount').textContent = `${trades.length} trades`;
  document.getElementById('btTradesBody').innerHTML = trades.map(t => {
    const cls = t.open ? 'bt-trade-open' : (t.win ? 'bt-trade-win' : 'bt-trade-loss');
    return `
      <tr>
        <td>${t.buyDate}</td>
        <td>${t.sellDate}</td>
        <td>$${t.buyPrice.toFixed(2)}</td>
        <td>$${t.sellPrice.toFixed(2)}</td>
        <td class="${cls}">${t.pnl >= 0 ? '+' : ''}$${t.pnl.toFixed(2)}</td>
        <td class="${cls}">${t.pnlPct >= 0 ? '+' : ''}${t.pnlPct}%</td>
        <td>${t.holdDays}d</td>
      </tr>
    `;
  }).join('');

  // Chart label
  document.getElementById('btChartLabel').textContent =
    `${candles.length} candles · ${candles[0]?.date} → ${candles[candles.length-1]?.date}`;

  // Reset AI output
  document.getElementById('btAiOutput').innerHTML =
    `<div class="empty-state">Click "Ask AI" to get analysis of these backtest results.</div>`;
}

function btRenderChart(candles, trades) {
  const wrap = document.querySelector('.bt-chart-wrap');
  const empty = document.getElementById('btChartEmpty');
  empty.style.display = 'none';

  const canvas = document.getElementById('btChart');
  const ctx = canvas.getContext('2d');

  // Set canvas size
  canvas.width  = wrap.clientWidth - 32;
  canvas.height = 240;

  const W = canvas.width, H = canvas.height;
  const pad = { top: 20, right: 20, bottom: 30, left: 55 };
  const chartW = W - pad.left - pad.right;
  const chartH = H - pad.top - pad.bottom;

  const closes = candles.map(c => c.close);
  const minP = Math.min(...closes) * 0.98;
  const maxP = Math.max(...closes) * 1.02;

  const xScale = i => pad.left + (i / (candles.length - 1)) * chartW;
  const yScale = p => pad.top + (1 - (p - minP) / (maxP - minP)) * chartH;

  // Clear
  ctx.clearRect(0, 0, W, H);

  // Grid lines
  ctx.strokeStyle = getComputedStyle(document.documentElement).getPropertyValue('--border').trim() || '#2a3347';
  ctx.lineWidth = 0.5;
  for (let i = 0; i <= 4; i++) {
    const y = pad.top + (i / 4) * chartH;
    ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(W - pad.right, y); ctx.stroke();
    const val = maxP - (i / 4) * (maxP - minP);
    ctx.fillStyle = '#5a6478';
    ctx.font = '10px Fira Code, monospace';
    ctx.textAlign = 'right';
    ctx.fillText('$' + val.toFixed(val > 100 ? 0 : 2), pad.left - 5, y + 4);
  }

  // Price line
  const grad = ctx.createLinearGradient(0, pad.top, 0, H - pad.bottom);
  grad.addColorStop(0, 'rgba(77,159,255,0.3)');
  grad.addColorStop(1, 'rgba(77,159,255,0.0)');

  ctx.beginPath();
  candles.forEach((c, i) => {
    const x = xScale(i), y = yScale(c.close);
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  });
  ctx.strokeStyle = '#4d9fff';
  ctx.lineWidth = 1.5;
  ctx.stroke();

  // Fill under line
  ctx.lineTo(xScale(candles.length - 1), H - pad.bottom);
  ctx.lineTo(xScale(0), H - pad.bottom);
  ctx.closePath();
  ctx.fillStyle = grad;
  ctx.fill();

  // Date labels (5 evenly spaced)
  ctx.fillStyle = '#5a6478';
  ctx.font = '10px Fira Sans, sans-serif';
  ctx.textAlign = 'center';
  [0, 0.25, 0.5, 0.75, 1].forEach(t => {
    const i = Math.round(t * (candles.length - 1));
    ctx.fillText(candles[i].date.slice(5), xScale(i), H - pad.bottom + 16);
  });

  // Trade signals
  trades.forEach(t => {
    // Buy signal
    const buyIdx = candles.findIndex(c => c.date === t.buyDate);
    if (buyIdx >= 0) {
      const x = xScale(buyIdx), y = yScale(candles[buyIdx].close);
      ctx.beginPath();
      ctx.arc(x, y, 5, 0, Math.PI * 2);
      ctx.fillStyle = '#22C55E';
      ctx.fill();
      ctx.strokeStyle = '#fff';
      ctx.lineWidth = 1.5;
      ctx.stroke();
    }
    // Sell signal
    if (t.sellDate !== 'Open') {
      const sellIdx = candles.findIndex(c => c.date === t.sellDate);
      if (sellIdx >= 0) {
        const x = xScale(sellIdx), y = yScale(candles[sellIdx].close);
        ctx.beginPath();
        ctx.arc(x, y, 5, 0, Math.PI * 2);
        ctx.fillStyle = '#ff4d6d';
        ctx.fill();
        ctx.strokeStyle = '#fff';
        ctx.lineWidth = 1.5;
        ctx.stroke();
      }
    }
  });

  // Legend
  ctx.font = '10px Fira Sans, sans-serif';
  ctx.textAlign = 'left';
  ctx.fillStyle = '#22C55E'; ctx.fillRect(pad.left, 6, 8, 8);
  ctx.fillStyle = '#8892a4'; ctx.fillText('Buy', pad.left + 11, 14);
  ctx.fillStyle = '#ff4d6d'; ctx.fillRect(pad.left + 45, 6, 8, 8);
  ctx.fillStyle = '#8892a4'; ctx.fillText('Sell', pad.left + 56, 14);
}

async function btAskAI() {
  if (!btResult) return;
  const { summary, ticker, strategy } = btResult;
  const btn = document.getElementById('btAiBtn');
  const out = document.getElementById('btAiOutput');

  btn.textContent = 'Analyzing...';
  btn.disabled = true;
  out.innerHTML = `<div class="loading-state"><div class="spinner"></div><span>AI analyzing your backtest...</span></div>`;

  const prompt = `You are an expert quant analyst reviewing a backtest result. Be concise and direct.

## Backtest: ${ticker} — ${strategy.replace(/_/g,' ').toUpperCase()}

- Total Return: ${summary.totalReturn >= 0 ? '+' : ''}${summary.totalReturn}%
- Buy & Hold Return: ${summary.buyHoldReturn >= 0 ? '+' : ''}${summary.buyHoldReturn}%
- Alpha vs Buy & Hold: ${summary.alpha >= 0 ? '+' : ''}${summary.alpha}%
- Win Rate: ${summary.winRate}%
- Total Trades: ${summary.totalTrades} (${summary.wins} wins / ${summary.losses} losses)
- Sharpe Ratio: ${summary.sharpe}
- Max Drawdown: -${summary.maxDrawdown}%
- Avg Hold Days: ${summary.avgHoldDays}
- Best Trade: +${summary.bestTrade}%
- Worst Trade: ${summary.worstTrade}%

Provide:
1. VERDICT: Is this strategy good for ${ticker}? (1 sentence)
2. STRENGTHS: What works well (2-3 bullets)
3. WEAKNESSES: What doesn't work (2-3 bullets)
4. SUGGESTION: One specific improvement to try (e.g. different parameters, combine with another indicator)

Keep it under 200 words. Plain text, no markdown.`;

  try {
    const res = await fetch('/api/ai/ask', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ prompt })
    });
    const data = await res.json();
    if (data.error) throw new Error(data.error);
    out.innerHTML = `<div class="analysis-summary" style="margin:12px 16px">${data.text.replace(/\n/g, '<br>')}</div>`;
  } catch (e) {
    out.innerHTML = `<div class="empty-state" style="color:var(--red)">${e.message}</div>`;
  } finally {
    btn.textContent = 'Ask AI';
    btn.disabled = false;
  }
}

// Init when Research tab or Backtest subtab is opened
function initBacktestIfNeeded() {
  if (!document.getElementById('btStrategy')?.options.length ||
      document.getElementById('btStrategy')?.options[0]?.value === '') {
    btInit();
  }
}
document.querySelector('[data-tab="research"]')?.addEventListener('click', initBacktestIfNeeded);
document.querySelector('[data-hubtab="backtest"]')?.addEventListener('click', initBacktestIfNeeded);

// ═══════════════════════════════════════════════════════════════════
// TRADING BIAS REPORT
// ═══════════════════════════════════════════════════════════════════
document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('biasReportBtn')?.addEventListener('click', runBiasReport);
});

async function runBiasReport() {
  const btn = document.getElementById('biasReportBtn');
  const out = document.getElementById('biasReportOutput');
  btn.textContent = 'Analyzing...';
  btn.disabled = true;
  out.innerHTML = `<div class="loading-state"><div class="spinner"></div><span>Analyzing your trading patterns...</span></div>`;

  try {
    const res = await fetch('/api/journal/bias-report', { method: 'POST', headers: { 'Content-Type': 'application/json' } });
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop();
      for (const line of lines) {
        if (!line.startsWith('data:')) continue;
        try {
          const msg = JSON.parse(line.slice(5).trim());
          if (msg.type === 'status') {
            out.innerHTML = `<div class="loading-state"><div class="spinner"></div><span>${msg.text}</span></div>`;
          } else if (msg.type === 'report') {
            renderBiasReport(out, msg.data);
          } else if (msg.type === 'error') {
            out.innerHTML = `<div class="empty-state" style="color:var(--red)">${msg.text}</div>`;
          }
        } catch {}
      }
    }
  } catch (e) {
    out.innerHTML = `<div class="empty-state" style="color:var(--red)">${e.message}</div>`;
  } finally {
    btn.textContent = 'Analyze All Trades';
    btn.disabled = false;
  }
}

function renderBiasReport(container, d) {
  const gradeColor = d.overallGrade === 'A' ? 'green' : d.overallGrade === 'B' ? 'blue' : d.overallGrade === 'C' ? 'yellow' : 'red';
  const dispSign = parseFloat(d.dispositionScore) > 0 ? `+${d.dispositionScore}d (holding losers longer)` : parseFloat(d.dispositionScore) < 0 ? `${d.dispositionScore}d (cutting losers fast ✓)` : 'Neutral';

  const biasesHtml = (d.biases || []).map(b => `
    <div class="bias-item">
      <div class="bias-item-header">
        <div class="bias-item-name">${b.name}</div>
        <div class="bias-severity ${b.severityColor}">${b.severity}</div>
      </div>
      <div class="bias-score-bar">
        <div class="bias-score-fill ${b.severityColor}" style="width:${b.score}%"></div>
      </div>
      <div class="bias-evidence">${b.evidence}</div>
      <div class="bias-fix">${b.fix}</div>
    </div>
  `).join('');

  const strengthsHtml = (d.strengths || []).map(s => `<div class="signal-reason-item"><span style="color:var(--green);margin-right:6px">✓</span>${s}</div>`).join('');

  container.innerHTML = `
    <div class="bias-header">
      <div class="bias-grade ${gradeColor}">${d.overallGrade}</div>
      <div class="bias-verdict">${d.overallVerdict}</div>
    </div>
    <div class="bias-stats-row">
      <div class="bias-stat">
        <div class="bias-stat-label">Trades</div>
        <div class="bias-stat-value">${d.totalTrades}</div>
      </div>
      <div class="bias-stat">
        <div class="bias-stat-label">Win Rate</div>
        <div class="bias-stat-value ${parseFloat(d.winRate)>=50?'green':'red'}">${d.winRate}%</div>
      </div>
      <div class="bias-stat">
        <div class="bias-stat-label">Profit Factor</div>
        <div class="bias-stat-value ${parseFloat(d.profitFactor)>=1?'green':'red'}">${d.profitFactor}x</div>
      </div>
      <div class="bias-stat">
        <div class="bias-stat-label">Avg Hold</div>
        <div class="bias-stat-value blue">${d.avgHoldDays}d</div>
      </div>
    </div>
    <div class="bias-section">
      <div class="bias-section-title">Behavioral Biases Detected</div>
      ${biasesHtml}
    </div>
    <div class="bias-section">
      <div class="bias-section-title">Disposition Effect</div>
      <div class="bias-insight">
        <div class="bias-insight-label">Hold Time Difference (Losers vs Winners)</div>
        ${dispSign}
        <br><span style="font-size:10px;color:var(--text3);margin-top:4px;display:block">Classic disposition effect: holding losing trades longer than winners to avoid realizing losses</span>
      </div>
    </div>
    <div class="bias-section">
      <div class="bias-section-title">Your Strengths</div>
      ${strengthsHtml}
    </div>
    <div class="bias-section" style="border-bottom:none">
      <div class="bias-section-title">Best vs Worst Setup</div>
      <div class="bias-insight"><div class="bias-insight-label">Best Setup</div>${d.bestSetup}</div>
      <div class="bias-insight"><div class="bias-insight-label">Worst Setup</div>${d.worstSetup}</div>
    </div>
    <div class="bias-target">
      <div class="bias-target-label">This Week's Focus</div>
      ${d.weeklyTarget}
      ${d.projectedImprovement ? `<div style="margin-top:6px;font-size:11px;color:var(--green)">${d.projectedImprovement}</div>` : ''}
    </div>
  `;
}

// ═══════════════════════════════════════════════════════════════════
// STATISTICAL BACKTEST VALIDATION
// ═══════════════════════════════════════════════════════════════════
async function btRunValidation() {
  if (!btResult) { showToast('Run a backtest first', 'warning'); return; }
  const btn = document.getElementById('btValidateBtn');
  const out = document.getElementById('btValidationOutput');
  btn.textContent = 'Running 1000 iterations...';
  btn.disabled = true;
  out.innerHTML = `<div class="loading-state"><div class="spinner"></div><span>Running Monte Carlo simulation...</span></div>`;

  try {
    const res = await fetch('/api/backtest/validate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ticker: btResult.ticker, strategy: btResult.strategy, range: btResult.range, capital: btResult.capital })
    });
    const data = await res.json();
    if (data.error) throw new Error(data.error);
    renderValidation(out, data);
  } catch (e) {
    out.innerHTML = `<div class="empty-state" style="color:var(--red)">${e.message}</div>`;
  } finally {
    btn.textContent = 'Run (1000 iterations)';
    btn.disabled = false;
  }
}

function renderValidation(container, d) {
  const mc = d.monteCarlo;
  const bs = d.bootstrap;
  const wf = d.walkForward;

  const mcColor   = mc.significant ? 'green' : parseFloat(mc.pValue) < 0.1 ? 'yellow' : 'red';
  const bsColor   = bs.sharpeLow > 0 ? 'green' : bs.sharpeLow > -0.5 ? 'yellow' : 'red';
  const wfColor   = wf.consistencyRate >= 75 ? 'green' : wf.consistencyRate >= 50 ? 'yellow' : 'red';

  const wfWindowsHtml = (wf.windows || []).map((w, i) => `
    <div class="bt-wf-row">
      <div class="bt-wf-label">Window ${i+1}</div>
      <div class="bt-wf-bar-track">
        <div class="bt-wf-bar-fill" style="width:${Math.min(100,Math.abs(w.totalReturn))}%;background:${w.profitable?'var(--green)':'var(--red)'}"></div>
      </div>
      <div style="font-size:11px;min-width:55px;text-align:right;font-family:var(--font-mono);color:${w.profitable?'var(--green)':'var(--red)'}">${w.totalReturn>=0?'+':''}${w.totalReturn}%</div>
      <div style="font-size:10px;color:var(--text3);min-width:40px;text-align:right">${w.trades}T</div>
    </div>
  `).join('');

  container.innerHTML = `
    <div class="bt-validation">
      <div class="bt-validation-title">
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M2 20h20M6 20V10l6-6 6 6v10"/></svg>
        Monte Carlo · Bootstrap · Walk-Forward
      </div>
      <div class="bt-val-grid">
        <div class="bt-val-item">
          <div class="bt-val-label">p-Value</div>
          <div class="bt-val-value" style="color:var(--${mcColor})">${mc.pValue ?? 'N/A'}</div>
          <div class="bt-val-sub">${mc.significant ? '✓ Significant' : '✗ Not significant'}</div>
        </div>
        <div class="bt-val-item">
          <div class="bt-val-label">Sharpe 95% CI</div>
          <div class="bt-val-value" style="color:var(--${bsColor});font-size:12px">[${bs.sharpeLow ?? '?'}, ${bs.sharpeHigh ?? '?'}]</div>
          <div class="bt-val-sub">Bootstrap</div>
        </div>
        <div class="bt-val-item">
          <div class="bt-val-label">Consistency</div>
          <div class="bt-val-value" style="color:var(--${wfColor})">${wf.consistencyRate ?? 'N/A'}%</div>
          <div class="bt-val-sub">${wf.profitableWindows ?? 0}/${wf.totalWindows ?? 0} windows</div>
        </div>
      </div>
      <div class="bt-val-verdict" style="margin-bottom:10px;border-left:3px solid var(--${mcColor});padding-left:10px">${mc.verdict}</div>
      <div class="bt-val-verdict" style="margin-bottom:10px;border-left:3px solid var(--${bsColor});padding-left:10px">${bs.verdict}</div>
      <div style="margin-bottom:6px;font-size:10px;color:var(--text3);text-transform:uppercase;letter-spacing:0.4px">Walk-Forward Windows</div>
      ${wfWindowsHtml}
      <div class="bt-val-verdict" style="margin-top:10px;border-left:3px solid var(--${wfColor});padding-left:10px">${wf.verdict}</div>
    </div>
  `;
}

// Wire validation button after btInit
const _origBtInit = btInit;
btInit = async function() {
  await _origBtInit();
  document.getElementById('btValidateBtn')?.addEventListener('click', btRunValidation);
};

// Also show validation card when results render
const _origBtRenderResults = btRenderResults;
btRenderResults = function(data) {
  _origBtRenderResults(data);
  document.getElementById('btValidationCard').style.display = '';
  document.getElementById('btValidationOutput').innerHTML =
    `<div class="empty-state" style="font-size:11px">Click "Run" to validate if this strategy's edge is statistically significant or just luck.</div>`;
};

// ═══════════════════════════════════════════════════════════════════
// SEC EDGAR ANALYZER
// ═══════════════════════════════════════════════════════════════════
let edgarInitialized = false;

function edgarInit() {
  if (edgarInitialized) return;
  edgarInitialized = true;
  setupTickerSearch('edgarTicker', 'edgarTickerDropdown', 'edgarTickerMsg');
  document.getElementById('edgarRunBtn')?.addEventListener('click', edgarRun);
}

async function edgarRun() {
  const ticker = await validateAndGetTicker('edgarTicker', 'edgarTickerMsg');
  if (!ticker) return;

  const insider   = document.getElementById('edgarInsider')?.checked;
  const filing8k  = document.getElementById('edgar8k')?.checked;
  const filing10k = document.getElementById('edgar10k')?.checked;

  const btn = document.getElementById('edgarRunBtn');
  const out = document.getElementById('edgarResultsBody');
  btn.textContent = 'Fetching...';
  btn.disabled = true;
  out.innerHTML = `<div class="loading-state"><div class="spinner"></div><span>Fetching EDGAR filings for ${ticker}...</span></div>`;
  document.getElementById('edgarScoreCard').style.display = 'none';
  document.getElementById('edgarAiCard').style.display = 'none';

  try {
    const res  = await fetch('/api/edgar/analyze', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ticker, insider, filing8k, filing10k })
    });
    const data = await res.json();
    if (data.error) throw new Error(data.error);
    edgarRender(data);
  } catch (e) {
    out.innerHTML = `<div class="empty-state" style="color:var(--red)">${e.message}</div>`;
  } finally {
    btn.textContent = 'Fetch EDGAR Data';
    btn.disabled = false;
  }
}

function edgarRender(d) {
  // Label
  document.getElementById('edgarResultLabel').textContent = `${d.companyName} · CIK ${d.cik}`;

  // Score card
  const scoreColor = d.score >= 3 ? 'green' : d.score <= -3 ? 'red' : 'yellow';
  const scoreLabel = d.score >= 3 ? 'Bullish Filing Signal' : d.score <= -3 ? 'Bearish Filing Signal' : 'Neutral Filing Signal';
  document.getElementById('edgarScoreCard').style.display = '';
  document.getElementById('edgarScoreBody').innerHTML = `
    <div class="edgar-score-ring">
      <div class="edgar-score-num" style="color:var(--${scoreColor})">${d.score > 0 ? '+' : ''}${d.score}</div>
      <div class="edgar-score-label">
        <div style="font-weight:700;color:var(--${scoreColor})">${scoreLabel}</div>
        <div style="margin-top:4px;font-size:10px;color:var(--text3)">Score range: −12 (very bearish) to +12 (very bullish)</div>
      </div>
    </div>
    ${(d.scoreBreakdown||[]).map(s => `
      <div style="display:flex;justify-content:space-between;align-items:center;padding:6px 16px;border-top:1px solid var(--border);font-size:11px">
        <span style="color:var(--text2)">${s.label}</span>
        <span style="font-weight:700;color:var(--${s.signal==='bullish'?'green':s.signal==='bearish'?'red':'text3'})">${s.points>0?'+':''}${s.points}</span>
      </div>
    `).join('')}
  `;

  // Insider trades
  let insiderHtml = '';
  if (d.insiderTrades?.length) {
    const buys  = d.insiderTrades.filter(t => t.type === 'buy');
    const sells = d.insiderTrades.filter(t => t.type === 'sell');
    insiderHtml = `
      <div style="padding:12px 16px;border-bottom:1px solid var(--border)">
        <div style="font-size:10px;font-weight:700;color:var(--text3);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:10px">
          Insider Activity — ${d.insiderBuys} purchases · ${d.insiderSells} sales (last 90 days)
        </div>
        ${d.insiderBuys >= 3 ? `<div class="edgar-filing-signal bullish" style="margin-bottom:8px">Cluster buying signal — ${d.insiderBuys} insiders purchased</div>` : ''}
        ${d.insiderTrades.slice(0,8).map(t => `
          <div class="edgar-insider-row">
            <div>
              <div class="edgar-insider-name" style="font-size:11px">${t.title}</div>
              <div class="edgar-insider-role">${t.date}</div>
            </div>
            <div class="${t.type==='buy'?'edgar-insider-buy':'edgar-insider-sell'}">${t.type.toUpperCase()}</div>
          </div>
        `).join('')}
        <div style="padding:6px 0;font-size:10px">
          <a href="https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${d.cik}&type=4&dateb=&owner=include&count=40" style="color:var(--blue);text-decoration:none" target="_blank">View all Form 4 filings on EDGAR →</a>
        </div>
      </div>
    `;
  }

  // Recent filings
  const formColors = { '8-K':'f8k', '10-K':'f10k', '10-Q':'f10q', '4':'f4', '13F':'f13f' };
  const filingsHtml = d.filings?.length ? d.filings.map(f => `
    <div class="edgar-filing-row">
      <div class="edgar-filing-form ${formColors[f.form]||'f10q'}">${f.form}</div>
      <div class="edgar-filing-info">
        <div class="edgar-filing-title">${f.title}</div>
        <div class="edgar-filing-date">${f.date}</div>
        <div class="edgar-filing-signal ${f.signal}">${f.signalLabel}</div>
        <div style="font-size:10px;color:var(--text3);margin-top:3px">${f.description}</div>
        <a href="${f.url}" style="font-size:10px;color:var(--blue);text-decoration:none" target="_blank">View on EDGAR →</a>
      </div>
    </div>
  `).join('') : '<div class="empty-state">No recent filings found</div>';

  document.getElementById('edgarResultsBody').innerHTML = `
    ${insiderHtml}
    <div style="padding:12px 16px">
      <div style="font-size:10px;font-weight:700;color:var(--text3);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:10px">Recent SEC Filings</div>
      ${filingsHtml}
    </div>
    <div style="padding:8px 16px;font-size:10px;color:var(--text3);border-top:1px solid var(--border)">
      Data source: SEC EDGAR public API (free, no key required) · CIK: ${d.cik}
    </div>
  `;

  // AI summary
  if (d.aiSummary) {
    document.getElementById('edgarAiCard').style.display = '';
    document.getElementById('edgarAiBody').innerHTML = `
      <div class="analysis-summary" style="margin:12px 16px">${d.aiSummary.replace(/\n/g,'<br>')}</div>
    `;
  }
}

// Init EDGAR when subtab opens
document.querySelector('[data-hubtab="edgar"]')?.addEventListener('click', edgarInit);

// ═══════════════════════════════════════════════════════════════════
// FIRST LAUNCH AI SETUP
// ═══════════════════════════════════════════════════════════════════
async function checkFirstLaunch() {
  // Only show if no AI provider has been explicitly configured
  try {
    const settings = await fetch('/api/ai/settings').then(r => r.json());
    const hasKey = settings.openai?.apiKey || settings.anthropic?.apiKey;
    const isDefaultProvider = settings.provider === 'ollama' || settings.provider === 'opencode';

    // Check if Ollama is already running
    const status = await fetch('/api/ai/status').then(r => r.json());
    const ollamaOk = status.ollama?.running && status.ollama?.availableModels?.length > 0;

    // Don't show if: Ollama is running, or they have a cloud key, or they've dismissed before
    const dismissed = localStorage.getItem('sf_setup_dismissed');
    if (dismissed || hasKey || ollamaOk) return;

    // Show setup overlay
    showAiSetup();
  } catch {}
}

async function showAiSetup() {
  const overlay = document.getElementById('aiSetupOverlay');
  overlay?.classList.remove('hidden');
  await checkOllamaSetup();
}

async function checkOllamaSetup() {
  const statusEl  = document.getElementById('setupOllamaStatus');
  const modelsEl  = document.getElementById('setupOllamaModels');
  const installEl = document.getElementById('setupOllamaInstall');
  const btn       = document.getElementById('setupOllamaBtn');

  if (statusEl) statusEl.textContent = 'Checking Ollama...';
  if (statusEl) statusEl.className = 'ai-setup-status';

  try {
    const data = await fetch('/api/ollama/models').then(r => r.json());

    if (data.running && data.models?.length > 0) {
      // Ollama is running with models
      statusEl.textContent = `✓ Ollama is running — ${data.models.length} model${data.models.length>1?'s':''} available`;
      statusEl.className = 'ai-setup-status ok';
      modelsEl?.classList.remove('hidden');
      installEl?.classList.add('hidden');
      if (btn) btn.style.display = '';

      // Populate model select
      const sel = document.getElementById('setupOllamaModelSelect');
      if (sel) {
        sel.innerHTML = data.models.map(m => `<option value="${m.name}">${m.name}</option>`).join('');
        // Pre-select the current model if set
        const current = (await fetch('/api/ai/settings').then(r=>r.json()))?.ollama?.model;
        if (current) sel.value = current;
      }
    } else if (data.running && data.models?.length === 0) {
      statusEl.textContent = '⚠ Ollama is running but no models installed. Run: ollama pull llama3.2';
      statusEl.className = 'ai-setup-status error';
      installEl?.classList.remove('hidden');
      modelsEl?.classList.add('hidden');
      if (btn) btn.style.display = 'none';
    } else {
      statusEl.textContent = '✗ Ollama not detected. Install it to use free local AI.';
      statusEl.className = 'ai-setup-status error';
      installEl?.classList.remove('hidden');
      modelsEl?.classList.add('hidden');
      if (btn) btn.style.display = 'none';
    }
  } catch {
    statusEl.textContent = '✗ Could not check Ollama status';
    statusEl.className = 'ai-setup-status error';
    installEl?.classList.remove('hidden');
    if (btn) btn.style.display = 'none';
  }
}

function dismissSetup() {
  document.getElementById('aiSetupOverlay')?.classList.add('hidden');
  localStorage.setItem('sf_setup_dismissed', '1');
}

async function saveSetupProvider(provider, extraSettings = {}) {
  try {
    const current = await fetch('/api/ai/settings').then(r => r.json());
    await fetch('/api/ai/settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ...current, provider, ...extraSettings })
    });
    dismissSetup();
    showToast(`AI provider set to ${provider}`, 'success');
    // Refresh AI status badge if it exists
    if (typeof loadAiStatus === 'function') loadAiStatus();
  } catch (e) {
    showToast('Failed to save: ' + e.message, 'error');
  }
}

// Wire up setup buttons on DOM ready
document.addEventListener('DOMContentLoaded', () => {
  // Ollama use button
  document.getElementById('setupOllamaBtn')?.addEventListener('click', async () => {
    const model = document.getElementById('setupOllamaModelSelect')?.value || 'llama3.2';
    await saveSetupProvider('ollama', { ollama: { url: 'http://localhost:11434', model } });
  });

  // Ollama retry
  document.getElementById('setupOllamaRetry')?.addEventListener('click', checkOllamaSetup);

  // Groq
  document.getElementById('setupGroqBtn')?.addEventListener('click', async () => {
    const key = document.getElementById('setupGroqKey')?.value.trim();
    if (!key) { showToast('Enter your Groq API key', 'warning'); return; }
    await saveSetupProvider('groq', { groq: { apiKey: key, model: 'llama-3.1-70b-versatile' } });
  });

  // OpenAI
  document.getElementById('setupOpenaiBtn')?.addEventListener('click', async () => {
    const key = document.getElementById('setupOpenaiKey')?.value.trim();
    if (!key) { showToast('Enter your OpenAI API key', 'warning'); return; }
    await saveSetupProvider('openai', { openai: { apiKey: key, model: 'gpt-4o' } });
  });

  // Anthropic
  document.getElementById('setupAnthropicBtn')?.addEventListener('click', async () => {
    const key = document.getElementById('setupAnthropicKey')?.value.trim();
    if (!key) { showToast('Enter your Anthropic API key', 'warning'); return; }
    await saveSetupProvider('anthropic', { anthropic: { apiKey: key, model: 'claude-sonnet-4-5' } });
  });

  // Skip
  // Finnhub key save
  document.getElementById('setupFinnhubSaveBtn')?.addEventListener('click', async () => {
    const key = document.getElementById('setupFinnhubKey')?.value.trim();
    const statusEl = document.getElementById('setupFinnhubStatus');
    if (!key) {
      if (statusEl) { statusEl.textContent = 'Using shared key — works fine for personal use'; statusEl.style.color = 'var(--text3)'; }
      return;
    }
    try {
      const current = await fetch('/api/ai/settings').then(r => r.json());
      await fetch('/api/ai/settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...current, finnhubKey: key })
      });
      if (statusEl) { statusEl.textContent = '✓ Your Finnhub key saved — using your personal rate limits'; statusEl.style.color = 'var(--green)'; }
    } catch {
      if (statusEl) { statusEl.textContent = '✗ Failed to save key'; statusEl.style.color = 'var(--red)'; }
    }
  });

  document.getElementById('setupSkipBtn')?.addEventListener('click', dismissSetup);

  // Check first launch after a short delay (let app load first)
  setTimeout(checkFirstLaunch, 1500);
});

// ═══════════════════════════════════════════════════════════════════
// AUTO MODE MODULE
// ═══════════════════════════════════════════════════════════════════
let autoSettings = null;
let autoSelectedAssets = new Set();

// ── Init ──────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('autoModeBtn')?.addEventListener('click', openAutoModal);
  document.getElementById('autoModalClose')?.addEventListener('click', closeAutoModal);
  document.getElementById('autoModalOverlay')?.addEventListener('click', e => {
    if (e.target.id === 'autoModalOverlay') closeAutoModal();
  });
  // Poll auto status every 30s to update titlebar badge
  setInterval(autoUpdateTitlebar, 30000);
  setTimeout(autoUpdateTitlebar, 3000);
});

async function openAutoModal() {
  document.getElementById('autoModalOverlay')?.classList.remove('hidden');
  await autoLoadSettings();
  await autoRenderAssets();
  autoRenderLog();
  autoWireButtons();
}

function closeAutoModal() {
  document.getElementById('autoModalOverlay')?.classList.add('hidden');
}

async function autoLoadSettings() {
  try {
    autoSettings = await fetch('/api/auto/settings').then(r => r.json());
    // Populate fields
    const gm = document.getElementById('autoGlobalMode');
    const sc = document.getElementById('autoSchedule');
    const mt = document.getElementById('autoMaxTrades');
    const mc = document.getElementById('autoMaxCapital');
    const mn = document.getElementById('autoMinConfidence');
    const mp = document.getElementById('autoMaxPositions');
    const mh = document.getElementById('autoMarketHours');
    if (gm) gm.value = autoSettings.globalMode || 'paper';
    if (sc) sc.value = String(autoSettings.schedule || 30);
    if (mt) mt.value = autoSettings.riskLimits?.maxTradesPerDay || 3;
    if (mc) mc.value = autoSettings.riskLimits?.maxCapitalPerTrade || 500;
    if (mn) mn.value = autoSettings.riskLimits?.minConfidence || 70;
    if (mp) mp.value = autoSettings.riskLimits?.maxOpenPositions || 5;
    if (mh) mh.checked = autoSettings.riskLimits?.marketHoursOnly !== false;
    // Kill switch
    autoUpdateKillSwitch(autoSettings.killSwitch);
  } catch (e) { console.error('autoLoadSettings:', e); }
}

function autoUpdateKillSwitch(active) {
  const btn = document.getElementById('autoKillSwitchBtn');
  const row = document.getElementById('autoKillSwitchRow');
  if (!btn) return;
  btn.textContent = active ? 'ON — TRADING STOPPED' : 'OFF';
  btn.className = `auto-killswitch-btn ${active ? 'active' : ''}`;
  if (row) row.style.borderColor = active ? 'var(--red)' : '';
}

async function autoRenderAssets() {
  const list = document.getElementById('autoAssetList');
  if (!list) return;

  list.innerHTML = '<div class="loading-row">Loading assets...</div>';

  // Gather all watchlist assets
  const data = await fetch('/api/data').then(r => r.json()).catch(() => ({}));
  const stocks  = [
    ...(data.daytrading?.watchlist || []),
    ...(data.longterm?.watchlist || []),
    ...(data.longterm?.portfolio || [])
  ];
  const cryptos = [
    ...(data.daytrading?.cryptoWatchlist || []),
    ...(data.longterm?.cryptoWatchlist || []),
    ...(data.longterm?.cryptoPortfolio || [])
  ];
  const allAssets = [
    ...stocks.map(s => ({ ticker: s.ticker, type: 'stock' })),
    ...cryptos.map(c => ({ ticker: c.ticker, type: 'crypto' }))
  ];

  // Dedupe
  const seen = new Set();
  const unique = allAssets.filter(a => {
    if (seen.has(a.ticker)) return false;
    seen.add(a.ticker);
    return true;
  });

  if (!unique.length) {
    list.innerHTML = '<div class="empty-state">No assets in your watchlists. Add stocks or crypto first.</div>';
    return;
  }

  // Fetch prices for ALL assets in one batch call
  const stockTickers  = unique.filter(a => a.type === 'stock').map(a => a.ticker);
  const cryptoTickers = unique.filter(a => a.type === 'crypto').map(a => a.ticker);

  const livePrices = {};
  try {
    if (stockTickers.length) {
      const res = await fetch('/api/prices/multi', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tickers: stockTickers })
      });
      const data = await res.json();
      data.forEach(p => { if (p.price > 0) livePrices[p.ticker] = p; });
    }
    if (cryptoTickers.length) {
      const res = await fetch('/api/crypto/prices/multi', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ coins: cryptoTickers })
      });
      const data = await res.json();
      data.forEach(p => { if (p.price > 0) livePrices[p.ticker] = p; });
    }
  } catch {}

  const assetSettings = autoSettings?.assets || {};

  list.innerHTML = unique.map(a => {
    const cfg      = assetSettings[a.ticker] || {};
    const enabled  = cfg.enabled || false;
    const p        = livePrices[a.ticker] || prices?.[a.ticker];
    const priceStr = p?.price ? `$${p.price.toFixed(2)}` : '—';
    const isSelected = autoSelectedAssets.has(a.ticker);
    return `
      <div class="auto-asset-row ${isSelected ? 'selected' : ''}" id="auto-row-${a.ticker}" onclick="autoToggleSelect('${a.ticker}')">
        <div class="auto-asset-check">
          ${isSelected ? '<svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="#fff" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>' : ''}
        </div>
        <span class="auto-asset-ticker">${a.type === 'crypto' ? '₿ ' : ''}${a.ticker}</span>
        <span class="auto-asset-price">${priceStr}</span>
        <button class="auto-asset-toggle ${enabled ? 'on' : ''}"
          onclick="event.stopPropagation(); autoToggleAsset('${a.ticker}', ${!enabled})"
        >${enabled ? 'Auto ON' : 'Auto OFF'}</button>
      </div>
    `;
  }).join('');
}

function autoToggleSelect(ticker) {
  if (autoSelectedAssets.has(ticker)) {
    autoSelectedAssets.delete(ticker);
  } else {
    autoSelectedAssets.add(ticker);
  }
  // Re-render row
  const row = document.getElementById(`auto-row-${ticker}`);
  if (row) {
    row.classList.toggle('selected', autoSelectedAssets.has(ticker));
    const check = row.querySelector('.auto-asset-check');
    if (check) check.innerHTML = autoSelectedAssets.has(ticker)
      ? '<svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="#fff" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>'
      : '';
  }
  // Update bulk bar
  const bar   = document.getElementById('autoBulkBar');
  const count = document.getElementById('autoBulkCount');
  if (bar)   bar.classList.toggle('hidden', autoSelectedAssets.size === 0);
  if (count) count.textContent = `${autoSelectedAssets.size} selected`;
}

async function autoToggleAsset(ticker, enabled) {
  try {
    await fetch('/api/auto/asset', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ticker, enabled, mode: autoSettings?.globalMode || 'paper', contracts: 1 })
    });
    if (!autoSettings) autoSettings = {};
    if (!autoSettings.assets) autoSettings.assets = {};
    autoSettings.assets[ticker] = { ...autoSettings.assets[ticker], enabled };
    // Update button
    const btn = document.querySelector(`#auto-row-${ticker} .auto-asset-toggle`);
    if (btn) {
      btn.textContent = enabled ? 'Auto ON' : 'Auto OFF';
      btn.className = `auto-asset-toggle ${enabled ? 'on' : ''}`;
    }
    autoUpdateTitlebar();
    if (enabled) showToast(`Auto mode enabled for ${ticker}`, 'success');
  } catch (e) { showToast('Failed to update: ' + e.message, 'error'); }
}

function autoWireButtons() {
  // Kill switch
  document.getElementById('autoKillSwitchBtn')?.addEventListener('click', async () => {
    const active = !(autoSettings?.killSwitch);
    await fetch('/api/auto/killswitch', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ active })
    });
    if (autoSettings) autoSettings.killSwitch = active;
    autoUpdateKillSwitch(active);
    autoUpdateTitlebar();
    showToast(active ? 'Kill switch activated — all auto trading stopped' : 'Kill switch deactivated', active ? 'warning' : 'success');
  });

  // Save settings
  document.getElementById('autoSaveSettings')?.addEventListener('click', async () => {
    const body = {
      globalMode: document.getElementById('autoGlobalMode')?.value,
      schedule:   parseInt(document.getElementById('autoSchedule')?.value) || 30,
      riskLimits: {
        maxTradesPerDay:    parseInt(document.getElementById('autoMaxTrades')?.value) || 3,
        maxCapitalPerTrade: parseInt(document.getElementById('autoMaxCapital')?.value) || 500,
        minConfidence:      parseInt(document.getElementById('autoMinConfidence')?.value) || 70,
        maxOpenPositions:   parseInt(document.getElementById('autoMaxPositions')?.value) || 5,
        marketHoursOnly:    document.getElementById('autoMarketHours')?.checked !== false
      }
    };
    try {
      const res = await fetch('/api/auto/settings', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      });
      autoSettings = (await res.json()).settings;
      showToast('Auto settings saved', 'success');
    } catch (e) { showToast('Save failed: ' + e.message, 'error'); }
  });

  // Select all / none
  document.getElementById('autoSelectAll')?.addEventListener('click', () => {
    document.querySelectorAll('.auto-asset-row').forEach(row => {
      const ticker = row.id.replace('auto-row-', '');
      autoSelectedAssets.add(ticker);
      row.classList.add('selected');
      const check = row.querySelector('.auto-asset-check');
      if (check) check.innerHTML = '<svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="#fff" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>';
    });
    const bar = document.getElementById('autoBulkBar');
    const count = document.getElementById('autoBulkCount');
    if (bar) bar.classList.remove('hidden');
    if (count) count.textContent = `${autoSelectedAssets.size} selected`;
  });

  document.getElementById('autoSelectNone')?.addEventListener('click', () => {
    autoSelectedAssets.clear();
    document.querySelectorAll('.auto-asset-row').forEach(row => {
      row.classList.remove('selected');
      const check = row.querySelector('.auto-asset-check');
      if (check) check.innerHTML = '';
    });
    document.getElementById('autoBulkBar')?.classList.add('hidden');
  });

  // Bulk enable/disable
  document.getElementById('autoBulkEnable')?.addEventListener('click', async () => {
    if (!autoSelectedAssets.size) return;
    const tickers = [...autoSelectedAssets];
    await fetch('/api/auto/assets/bulk', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ tickers, enabled: true, mode: autoSettings?.globalMode || 'paper', contracts: 1 })
    });
    tickers.forEach(t => {
      if (!autoSettings.assets) autoSettings.assets = {};
      autoSettings.assets[t] = { ...autoSettings.assets[t], enabled: true };
      const btn = document.querySelector(`#auto-row-${t} .auto-asset-toggle`);
      if (btn) { btn.textContent = 'Auto ON'; btn.className = 'auto-asset-toggle on'; }
    });
    showToast(`Auto enabled on ${tickers.length} asset(s)`, 'success');
    autoUpdateTitlebar();
  });

  document.getElementById('autoBulkDisable')?.addEventListener('click', async () => {
    if (!autoSelectedAssets.size) return;
    const tickers = [...autoSelectedAssets];
    await fetch('/api/auto/assets/bulk', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ tickers, enabled: false })
    });
    tickers.forEach(t => {
      if (autoSettings?.assets?.[t]) autoSettings.assets[t].enabled = false;
      const btn = document.querySelector(`#auto-row-${t} .auto-asset-toggle`);
      if (btn) { btn.textContent = 'Auto OFF'; btn.className = 'auto-asset-toggle'; }
    });
    showToast(`Auto disabled on ${tickers.length} asset(s)`, 'success');
    autoUpdateTitlebar();
  });

  // Scan now
  document.getElementById('autoScanNow')?.addEventListener('click', async () => {
    const btn = document.getElementById('autoScanNow');
    btn.textContent = 'Scanning...'; btn.disabled = true;
    try {
      const tickers = [...autoSelectedAssets];
      if (tickers.length === 1) {
        await fetch('/api/auto/scan-now', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ ticker: tickers[0] })
        });
      } else {
        await fetch('/api/auto/scan-now', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' });
      }
      await autoRenderLog();
      showToast('Scan complete — check the log below', 'success');
    } catch (e) { showToast('Scan failed: ' + e.message, 'error'); }
    finally { btn.textContent = 'Scan Now'; btn.disabled = false; }
  });

  // Refresh + clear log
  document.getElementById('autoRefreshLog')?.addEventListener('click', autoRenderLog);
  document.getElementById('autoClearLog')?.addEventListener('click', async () => {
    await fetch('/api/auto/log', { method: 'DELETE' });
    autoRenderLog();
  });
}

async function autoRenderLog() {
  const tbody = document.getElementById('autoLogBody');
  const countEl = document.getElementById('autoLogCount');
  if (!tbody) return;
  try {
    const log = await fetch('/api/auto/log').then(r => r.json());
    if (countEl) countEl.textContent = `${log.length} entries`;
    if (!log.length) {
      tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;color:var(--text3);padding:16px">No auto trades yet — enable assets and run a scan</td></tr>';
      return;
    }
    tbody.innerHTML = log.slice(0, 100).map(e => {
      const time    = e.timestamp ? new Date(e.timestamp).toLocaleTimeString() : '—';
      const date    = e.timestamp ? new Date(e.timestamp).toLocaleDateString() : '';
      const statusCls = `auto-log-status-${e.status}`;
      const action  = e.action || '—';
      const details = e.status === 'executed'
        ? `$${e.strike} ${e.expiry} · ${e.contracts}x @ $${e.premium?.toFixed(2)} = $${e.totalCost?.toFixed(0)}`
        : e.reason || e.thesis || '—';
      return `<tr>
        <td style="color:var(--text3);font-size:10px">${date}<br>${time}</td>
        <td style="font-weight:700">${e.ticker || '—'}</td>
        <td class="${statusCls}">${e.status}</td>
        <td>${action}</td>
        <td style="color:var(--text2);max-width:200px;overflow:hidden;text-overflow:ellipsis">${details}</td>
        <td>${e.mode || '—'}</td>
      </tr>`;
    }).join('');
  } catch { tbody.innerHTML = '<tr><td colspan="6" style="color:var(--red);padding:12px">Failed to load log</td></tr>'; }
}

async function autoUpdateTitlebar() {
  try {
    const s = await fetch('/api/auto/settings').then(r => r.json());
    const enabledCount = Object.values(s.assets || {}).filter(a => a.enabled).length;
    const isActive = enabledCount > 0 && !s.killSwitch;
    const btn   = document.getElementById('autoModeBtn');
    const badge = document.getElementById('autoActiveBadge');
    const label = document.getElementById('autoModeBtnLabel');
    if (btn)   btn.classList.toggle('active', isActive);
    if (badge) badge.classList.toggle('hidden', !isActive);
    if (label) label.textContent = isActive ? `Auto: ${enabledCount}` : 'Auto';
  } catch {}
}
