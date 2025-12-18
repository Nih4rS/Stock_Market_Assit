async function fetchJson(path) {
  const res = await fetch(path, { cache: 'no-store' });
  if (!res.ok) throw new Error(`${path}: ${res.status}`);
  return await res.json();
}

function fmtNum(x, digits = 2) {
  if (x === null || x === undefined || Number.isNaN(Number(x))) return '';
  const n = Number(x);
  return Number.isFinite(n) ? n.toFixed(digits) : '';
}

function setText(id, text) {
  const el = document.getElementById(id);
  if (el) el.textContent = text;
}

function clearChildren(el) {
  while (el.firstChild) el.removeChild(el.firstChild);
}

function rowToCells(row) {
  return [
    row.Ticker,
    row.Strategy,
    fmtNum(row.Score, 2),
    fmtNum(row.Close, 2),
    fmtNum(row.RSI14, 1),
    fmtNum(row.SMA50, 2),
    fmtNum(row.SMA200, 2),
    row.Dist_52wHigh !== undefined && row.Dist_52wHigh !== null ? fmtNum(row.Dist_52wHigh * 100, 2) + '%' : '',
    fmtNum(row.Vol5x20, 2),
  ];
}

function renderGoodStocks(rows) {
  const tbody = document.querySelector('#goodStocksTable tbody');
  clearChildren(tbody);

  for (const r of rows) {
    const tr = document.createElement('tr');
    for (const c of rowToCells(r)) {
      const td = document.createElement('td');
      td.textContent = c ?? '';
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
}

function renderNews(items) {
  const ul = document.getElementById('newsList');
  clearChildren(ul);
  for (const n of items) {
    const li = document.createElement('li');
    const a = document.createElement('a');
    a.href = n.link || '#';
    a.target = '_blank';
    a.rel = 'noreferrer';
    a.textContent = n.title || 'News item';

    const meta = document.createElement('span');
    meta.className = 'muted';
    const bits = [];
    if (n.ticker) bits.push(n.ticker);
    if (n.publisher) bits.push(n.publisher);
    if (n.published) bits.push(n.published);
    meta.textContent = bits.length ? ` (${bits.join(' • ')})` : '';

    li.appendChild(a);
    li.appendChild(meta);
    ul.appendChild(li);
  }
}

function normalizeTicker(t) {
  return t.trim().toUpperCase().replace(/\s+/g, '');
}

function downloadText(filename, text) {
  const blob = new Blob([text], { type: 'text/csv;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function filterCsvByDate(csvText, startDate, endDate, ticker) {
  const lines = csvText.split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) return [];
  const header = lines[0].split(',');
  const dateIdx = header.indexOf('Date');
  if (dateIdx === -1) return [];

  const out = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split(',');
    const d = cols[dateIdx];
    if (!d) continue;
    if (startDate && d < startDate) continue;
    if (endDate && d > endDate) continue;
    out.push({ ticker, header, cols });
  }
  return out;
}

async function handleDownload() {
  const status = document.getElementById('downloadStatus');
  const btn = document.getElementById('downloadBtn');
  const tickersRaw = document.getElementById('tickersInput').value || '';
  const startDate = document.getElementById('startDate').value;
  const endDate = document.getElementById('endDate').value;

  const tickers = tickersRaw
    .split(',')
    .map(normalizeTicker)
    .filter(Boolean);

  if (!tickers.length) {
    status.textContent = 'Enter at least one ticker.';
    return;
  }

  btn.disabled = true;
  status.textContent = 'Downloading...';

  try {
    const rows = [];
    let baseHeader = null;

    for (const t of tickers) {
      const path = `data/prices/${encodeURIComponent(t)}.csv`;
      const res = await fetch(path, { cache: 'no-store' });
      if (!res.ok) {
        status.textContent = `Missing data for ${t}. Ensure it exists in daily build.`;
        btn.disabled = false;
        return;
      }
      const csv = await res.text();
      const filtered = filterCsvByDate(csv, startDate, endDate, t);
      if (!filtered.length) continue;
      baseHeader = baseHeader || filtered[0].header;
      for (const r of filtered) rows.push(r);
    }

    if (!rows.length || !baseHeader) {
      status.textContent = 'No rows in that date range.';
      btn.disabled = false;
      return;
    }

    const outHeader = ['Ticker', ...baseHeader].join(',');
    const outLines = [outHeader];
    for (const r of rows) {
      outLines.push([r.ticker, ...r.cols].join(','));
    }

    const file = `historical_${tickers.join('-')}_${startDate || 'start'}_${endDate || 'end'}.csv`;
    downloadText(file, outLines.join('\n'));
    status.textContent = `Downloaded ${rows.length} rows.`;
  } catch (e) {
    status.textContent = String(e);
  } finally {
    btn.disabled = false;
  }
}

async function main() {
  try {
    const manifest = await fetchJson('data/manifest.json');
    setText('lastUpdated', `Last updated: ${manifest.last_updated_utc || 'unknown'}`);

    const good = await fetchJson('data/good_stocks.json');
    let rows = good.rows || [];
    renderGoodStocks(rows);

    const news = await fetchJson('data/news.json');
    renderNews(news.items || []);

    const filter = document.getElementById('filterTicker');
    filter.addEventListener('input', () => {
      const q = normalizeTicker(filter.value || '');
      if (!q) return renderGoodStocks(rows);
      renderGoodStocks(rows.filter(r => (r.Ticker || '').toUpperCase().includes(q)));
    });

    document.getElementById('downloadBtn').addEventListener('click', handleDownload);
  } catch (e) {
    setText('lastUpdated', `Error loading data: ${String(e)}`);
  }
}

main();
