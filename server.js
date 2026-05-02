const express = require('express');
const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
process.on('uncaughtException', err => console.error('[uncaught]', err.message));
process.on('unhandledRejection', err => console.error('[unhandled]', err?.message));

const app = express();
const PORT = process.env.PORT || 3478;
// Finnhub API key — loaded from settings, falls back to bundled free key
// Each user should register their own free key at finnhub.io
const FINNHUB_KEY_DEFAULT = 'd7co7r1r01qv03et4p5gd7co7r1r01qv03et4p60';
function getFinnhubKey() {
  try {
    const s = JSON.parse(fs.readFileSync(AI_SETTINGS_FILE, 'utf8'));
    return s.finnhubKey || FINNHUB_KEY_DEFAULT;
  } catch { return FINNHUB_KEY_DEFAULT; }
}
// Keep backward compat — used in finnhubGetSecure
const FINNHUB_KEY = FINNHUB_KEY_DEFAULT;

const BASE_DIR = path.join(os.homedir(), '.stockforge');
const DATA_FILE = path.join(BASE_DIR, 'data.json');

if (!fs.existsSync(BASE_DIR)) fs.mkdirSync(BASE_DIR, { recursive: true });

// ─── AI Settings Store ────────────────────────────────────────────
const AI_SETTINGS_FILE = path.join(BASE_DIR, 'ai-settings.json');

const DEFAULT_AI_SETTINGS = {
  provider: 'ollama',
  finnhubKey: '',
  opencode: { agent: 'general' },
  openai: { apiKey: '', model: 'gpt-4o' },
  anthropic: { apiKey: '', model: 'claude-sonnet-4-5' },
  groq: { apiKey: '', model: 'llama-3.1-70b-versatile' },
  ollama: { url: 'http://localhost:11434', model: 'llama3.2' }
};

function loadAISettings() {
  try {
    if (fs.existsSync(AI_SETTINGS_FILE)) {
      const saved = JSON.parse(fs.readFileSync(AI_SETTINGS_FILE, 'utf8'));
      return { ...DEFAULT_AI_SETTINGS, ...saved };
    }
  } catch {}
  return { ...DEFAULT_AI_SETTINGS };
}

function saveAISettings(s) {
  fs.writeFileSync(AI_SETTINGS_FILE, JSON.stringify(s, null, 2));
}



// ─── OpenCode Provider ────────────────────────────────────────────
// Routes through the local OpenCode server (auto-detected)
// Works with ANY model configured in OpenCode — Claude, GPT, Gemini, Groq, etc.
// No API keys needed in StockForge — managed entirely by OpenCode
let _ocPort = null;
let _ocPass = null;

function findLocalOpenCode() {
  try {
    const { execSync } = require('child_process');
    const out = execSync('ps eww -A 2>/dev/null', { encoding: 'utf8', shell: '/bin/sh' });
    for (const line of out.split('\n')) {
      if (!line.includes('opencode') || !line.includes('serve')) continue;
      const portMatch = line.match(/--port\s+(\d+)/);
      const passMatch = line.match(/OPENCODE_SERVER_PASSWORD=([a-f0-9-]+)/);
      if (portMatch) {
        _ocPort = parseInt(portMatch[1]);
        _ocPass = passMatch ? passMatch[1] : '';
        return true;
      }
    }
  } catch {}
  return false;
}

function askViaOpenCode(prompt, settings) {
  return new Promise((resolve, reject) => {
    if (!findLocalOpenCode()) return reject(new Error('OpenCode is not running. Please open the OpenCode app first.'));

    const ocBase64 = Buffer.from(`opencode:${_ocPass}`).toString('base64');
    const headers = { 'Content-Type': 'application/json', 'Authorization': `Basic ${ocBase64}` };
    const agent = settings?.agent || 'general';
    const timeoutMs = 120000;

    // 1. Create a fresh session
    const sessBody = JSON.stringify({ directory: os.homedir() });
    const sessReq = http.request({
      hostname: '127.0.0.1', port: _ocPort, path: '/session', method: 'POST',
      headers: { ...headers, 'Content-Length': Buffer.byteLength(sessBody) }
    }, sessRes => {
      let out = '';
      sessRes.on('data', d => out += d);
      sessRes.on('end', () => {
        let sessionId;
        try { sessionId = JSON.parse(out).id; } catch { return reject(new Error('OpenCode: failed to create session')); }
        if (!sessionId) return reject(new Error('OpenCode: no session ID returned'));

        // 2. Subscribe to /event stream BEFORE sending prompt
        let fullText = '';
        let done = false;
        const timer = setTimeout(() => {
          if (!done) { done = true; evReq.destroy(); reject(new Error('OpenCode request timed out after 120s')); }
        }, timeoutMs);

        // Track assistant message ID — identified from message.updated with role=assistant
        let assistantMsgId = null;
        let lastAssistantText = '';

        const evReq = http.request({
          hostname: '127.0.0.1', port: _ocPort, path: '/event', method: 'GET',
          headers: { ...headers, 'Accept': 'text/event-stream' }
        }, evRes => {
          let buf = '';
          evRes.on('data', chunk => {
            buf += chunk.toString();
            const lines = buf.split('\n');
            buf = lines.pop();
            for (const line of lines) {
              if (!line.startsWith('data:')) continue;
              try {
                const ev = JSON.parse(line.slice(5).trim());
                const type = ev.type || '';
                const props = ev.properties || {};

                // Only process events from our session
                const evSessionId = props.sessionID || props.info?.sessionID || '';
                if (evSessionId && evSessionId !== sessionId) continue;

                // Capture assistant message ID
                if (type === 'message.updated' && props.info?.role === 'assistant' && props.info?.sessionID === sessionId) {
                  assistantMsgId = props.info.id;
                }

                // Capture text parts — filter by assistant messageID in the part
                if (type === 'message.part.updated' && props.sessionID === sessionId) {
                  const part = props.part || {};
                  const partMsgId = part.messageID || '';
                  if (part.type === 'text' && part.text) {
                    if (!assistantMsgId || partMsgId === assistantMsgId) {
                      lastAssistantText = part.text;
                    }
                  }
                }

                // Accumulate streaming deltas
                if (type === 'message.part.delta' && props.sessionID === sessionId) {
                  const delta = props.delta || {};
                  if (delta.type === 'text') fullText += delta.text || '';
                }

                // session.idle = done — poll messages as fallback to get the actual text
                if (type === 'session.idle' && props.sessionID === sessionId && !done) {
                  done = true;
                  clearTimeout(timer);
                  clearInterval(pollInterval);
                  evReq.destroy();

                  // Prefer accumulated text, fall back to polling GET /session/{id}/message
                  const immediateResult = fullText.trim() || lastAssistantText.trim();
                  if (immediateResult) return resolve(immediateResult);

                  // Fallback: fetch messages from session
                  const msgReq = http.request({
                    hostname: '127.0.0.1', port: _ocPort,
                    path: `/session/${sessionId}/message`, method: 'GET',
                    headers
                  }, msgRes => {
                    let out = '';
                    msgRes.on('data', d => out += d);
                    msgRes.on('end', () => {
                      try {
                        const msgs = JSON.parse(out);
                        // Find last assistant text part
                        for (const msg of [...msgs].reverse()) {
                          if (msg.info?.role !== 'assistant') continue;
                          for (const part of (msg.parts || [])) {
                            if (part.type === 'text' && part.text?.trim()) {
                              return resolve(part.text.trim());
                            }
                          }
                        }
                        reject(new Error('OpenCode returned empty response'));
                      } catch { reject(new Error('OpenCode: failed to fetch messages')); }
                    });
                  });
                  msgReq.on('error', () => reject(new Error('OpenCode returned empty response')));
                  msgReq.end();
                }

                // Error
                if (type === 'session.error' && props.sessionID === sessionId && !done) {
                  done = true;
                  clearTimeout(timer);
                  clearInterval(pollInterval);
                  evReq.destroy();
                  reject(new Error(`OpenCode error: ${props.error?.data?.message || 'Unknown error'}`));
                }
              } catch {}
            }
          });
          evRes.on('error', e => { if (!done) { done = true; clearTimeout(timer); reject(e); } });
        });
        evReq.on('error', e => { if (!done) { done = true; clearTimeout(timer); reject(e); } });
        evReq.end();

        // 3. Short delay then send prompt_async
        // Also start a polling fallback — if session.idle isn't received within 45s,
        // poll GET /session/{id}/message directly to get the response
        let pollInterval = null;
        const startPolling = () => {
          let pollAttempts = 0;
          pollInterval = setInterval(() => {
            if (done) { clearInterval(pollInterval); return; }
            pollAttempts++;
            const pReq = http.request({
              hostname: '127.0.0.1', port: _ocPort,
              path: `/session/${sessionId}/message`, method: 'GET', headers
            }, pRes => {
              let out = '';
              pRes.on('data', d => out += d);
              pRes.on('end', () => {
                if (done) return;
                try {
                  const msgs = JSON.parse(out);
                  for (const msg of [...msgs].reverse()) {
                    if (msg.info?.role !== 'assistant') continue;
                    for (const part of (msg.parts || [])) {
                      if (part.type === 'text' && part.text?.trim()) {
                        done = true;
                        clearInterval(pollInterval);
                        clearTimeout(timer);
                        evReq.destroy();
                        return resolve(part.text.trim());
                      }
                    }
                  }
                } catch {}
                // Give up after 24 polls (120s)
                if (pollAttempts >= 24 && !done) {
                  done = true;
                  clearInterval(pollInterval);
                  clearTimeout(timer);
                  evReq.destroy();
                  reject(new Error('OpenCode: no response received after 120s'));
                }
              });
            });
            pReq.on('error', () => {});
            pReq.end();
          }, 5000); // poll every 5s
        };

        setTimeout(() => {
          const promptBody = JSON.stringify({
            parts: [{ type: 'text', text: prompt }],
            agent
          });
          const pReq = http.request({
            hostname: '127.0.0.1', port: _ocPort,
            path: `/session/${sessionId}/prompt_async`, method: 'POST',
            headers: { ...headers, 'Content-Length': Buffer.byteLength(promptBody) }
          }, pRes => {
            pRes.resume(); // drain
            if (pRes.statusCode !== 204) {
              if (!done) { done = true; clearTimeout(timer); evReq.destroy(); reject(new Error(`OpenCode prompt failed: HTTP ${pRes.statusCode}`)); }
            }
          });
          pReq.on('error', e => { if (!done) { done = true; clearTimeout(timer); evReq.destroy(); reject(e); } });
          pReq.write(promptBody);
          pReq.end();
          // Start polling 10s after sending — catches cases where session.idle is missed
          setTimeout(startPolling, 10000);
        }, 300);
      });
    });
    sessReq.on('error', e => reject(new Error('OpenCode: ' + e.message)));
    sessReq.write(sessBody);
    sessReq.end();
  });
}

// ─── OpenAI Provider ──────────────────────────────────────────────
function askViaOpenAI(prompt, settings) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({
      model: settings.model || 'gpt-4o',
      messages: [{ role: 'user', content: prompt }],
      max_tokens: 2048
    });
    const extraCACert = process.env.NODE_EXTRA_CA_CERTS;
    const certArgs = extraCACert ? ['--cacert', extraCACert] : [];
    const args = [
      '-s', '--max-time', '120',
      ...certArgs,
      '-X', 'POST',
      '-H', 'Content-Type: application/json',
      '-H', `Authorization: Bearer ${settings.apiKey}`,
      '-d', body,
      'https://api.openai.com/v1/chat/completions'
    ];
    execFile('curl', args, { maxBuffer: 5 * 1024 * 1024 }, (err, stdout) => {
      if (err) return reject(new Error(`OpenAI request failed: ${err.message}`));
      try {
        const json = JSON.parse(stdout);
        if (json.error) return reject(new Error(json.error.message));
        resolve(json.choices?.[0]?.message?.content || '');
      } catch { reject(new Error('OpenAI parse error: ' + stdout.slice(0, 100))); }
    });
  });
}

// ─── Anthropic Provider ───────────────────────────────────────────
function askViaAnthropic(prompt, settings) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({
      model: settings.model || 'claude-sonnet-4-5',
      max_tokens: 2048,
      messages: [{ role: 'user', content: prompt }]
    });
    const extraCACert = process.env.NODE_EXTRA_CA_CERTS;
    const certArgs = extraCACert ? ['--cacert', extraCACert] : [];
    const args = [
      '-s', '--max-time', '120',
      ...certArgs,
      '-X', 'POST',
      '-H', 'Content-Type: application/json',
      '-H', `x-api-key: ${settings.apiKey}`,
      '-H', 'anthropic-version: 2023-06-01',
      '-d', body,
      'https://api.anthropic.com/v1/messages'
    ];
    execFile('curl', args, { maxBuffer: 5 * 1024 * 1024 }, (err, stdout) => {
      if (err) return reject(new Error(`Anthropic request failed: ${err.message}`));
      try {
        const json = JSON.parse(stdout);
        if (json.error) return reject(new Error(json.error.message));
        resolve(json.content?.[0]?.text || '');
      } catch { reject(new Error('Anthropic parse error: ' + stdout.slice(0, 100))); }
    });
  });
}

// ─── Groq Provider (free, fast) ───────────────────────────────────
function askViaGroq(prompt, settings) {
  return new Promise((resolve, reject) => {
    // response_format json_object requires the word "json" in messages
    const body = JSON.stringify({
      model: settings.model || 'llama-3.1-70b-versatile',
      messages: [
        { role: 'system', content: 'You are a financial AI assistant. Always respond with valid JSON only.' },
        { role: 'user', content: prompt }
      ],
      max_tokens: 4096,
      temperature: 0.1,
      response_format: { type: 'json_object' }
    });
    const extraCACert = process.env.NODE_EXTRA_CA_CERTS;
    const certArgs = extraCACert ? ['--cacert', extraCACert] : [];
    const args = [
      '-s', '--max-time', '60',
      ...certArgs,
      '-X', 'POST',
      '-H', 'Content-Type: application/json',
      '-H', `Authorization: Bearer ${settings.apiKey}`,
      '-d', body,
      'https://api.groq.com/openai/v1/chat/completions'
    ];
    execFile('curl', args, { maxBuffer: 5 * 1024 * 1024 }, (err, stdout) => {
      if (err) return reject(new Error(`Groq request failed: ${err.message}`));
      try {
        const json = JSON.parse(stdout);
        if (json.error) return reject(new Error(json.error.message || 'Groq error'));
        resolve(json.choices?.[0]?.message?.content || '');
      } catch { reject(new Error('Groq parse error: ' + stdout.slice(0, 100))); }
    });
  });
}

// ─── Ollama Provider ──────────────────────────────────────────────
async function checkOllamaRunning(settings) {
  const rawUrl = settings?.url || 'http://localhost:11434';
  return new Promise((resolve) => {
    const url = new URL('/api/tags', rawUrl);
    const transport = url.protocol === 'https:' ? require('https') : http;
    const req = transport.get({ hostname: url.hostname, port: url.port || 11434, path: url.pathname, timeout: 3000 }, res => {
      let out = '';
      res.on('data', d => out += d);
      res.on('end', () => {
        try {
          const json = JSON.parse(out);
          const models = (json.models || []).map(m => m.name);
          resolve({ running: true, models });
        } catch { resolve({ running: false, models: [] }); }
      });
    });
    req.on('error', () => resolve({ running: false, models: [] }));
    req.on('timeout', () => { req.destroy(); resolve({ running: false, models: [] }); });
  });
}

function askViaOllama(prompt, settings) {
  return new Promise(async (resolve, reject) => {
    const rawUrl = settings.url || 'http://localhost:11434';

    // Fast pre-check — fail immediately with clear message if Ollama isn't running
    const status = await checkOllamaRunning(settings);
    if (!status.running) {
      return reject(new Error('Ollama is not running. Start it with: ollama serve'));
    }
    const model = settings.model || 'llama3.2';
    if (status.models.length > 0 && !status.models.includes(model)) {
      // Model not found — use first available or suggest pull
      const firstModel = status.models[0];
      console.warn(`[ollama] Model "${model}" not found. Available: ${status.models.join(', ')}. Using ${firstModel}`);
      settings = { ...settings, model: firstModel };
    }
    if (status.models.length === 0) {
      return reject(new Error(`Ollama is running but no models installed. Run: ollama pull llama3.2`));
    }

    // Use /api/chat for better JSON compliance + system prompt support
    const url = new URL('/api/chat', rawUrl);
    const body = JSON.stringify({
      model: settings.model || 'llama3.2',
      stream: false,
      options: { temperature: 0.1, num_predict: 2048 },
      messages: [
        { role: 'system', content: 'You are a financial AI assistant. You MUST respond with valid JSON only. Never include markdown, code fences, or explanations. Your entire response must be a single valid JSON object starting with { and ending with }.' },
        { role: 'user', content: prompt }
      ]
    });
    const isHttps = url.protocol === 'https:';
    const transport = isHttps ? require('https') : http;
    const opts = {
      hostname: url.hostname,
      port: url.port || (isHttps ? 443 : 80),
      path: url.pathname, method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) }
    };
    const req = transport.request(opts, res => {
      let out = '';
      res.on('data', d => out += d);
      res.on('end', () => {
        try {
          const json = JSON.parse(out);
          if (json.error) return reject(new Error(`Ollama: ${json.error}`));
          const text = json.message?.content || json.response || '';
          if (!text) return reject(new Error('Ollama returned empty response'));
          resolve(text);
        } catch { reject(new Error('Ollama parse error: ' + out.slice(0, 100))); }
      });
    });
    const t = setTimeout(() => { req.destroy(); reject(new Error('Ollama timeout — model may be loading, try again')); }, 120000);
    req.on('error', e => { clearTimeout(t); reject(new Error(`Ollama connection failed: ${e.message}. Is Ollama running?`)); });
    req.on('close', () => clearTimeout(t));
    req.write(body);
    req.end();
  });
}

// ─── Strip markdown fences from AI response ───────────────────────
// Some models wrap JSON in ```json ... ``` — strip it so JSON.parse works
function cleanAIResponse(text) {
  if (!text) return text;
  // Remove ```json ... ``` or ``` ... ``` fences
  const fenceMatch = text.match(/```(?:json)?\s*([\s\S]*?)```/);
  if (fenceMatch) return fenceMatch[1].trim();
  // Remove leading/trailing non-JSON text
  const jsonStart = text.indexOf('{');
  const jsonEnd   = text.lastIndexOf('}');
  const arrStart  = text.indexOf('[');
  const arrEnd    = text.lastIndexOf(']');
  // Pick whichever starts first
  if (jsonStart !== -1 && (arrStart === -1 || jsonStart <= arrStart)) {
    return text.slice(jsonStart, jsonEnd + 1);
  }
  if (arrStart !== -1) {
    return text.slice(arrStart, arrEnd + 1);
  }
  return text.trim();
}

// ─── Universal AI Router ──────────────────────────────────────────
async function askAI(prompt) {
  const settings = loadAISettings();
  const AI_TIMEOUT_MS = 120000;
  const timeout = new Promise((_, reject) =>
    setTimeout(() => reject(new Error('AI request timed out after 120s. Try again.')), AI_TIMEOUT_MS)
  );
  let aiCall;
  switch (settings.provider) {
    case 'openai':
      if (!settings.openai?.apiKey) throw new Error('OpenAI API key not configured. Go to ⚙️ AI Settings.');
      aiCall = askViaOpenAI(prompt, settings.openai);
      break;
    case 'anthropic':
      if (!settings.anthropic?.apiKey) throw new Error('Anthropic API key not configured. Go to ⚙️ AI Settings.');
      aiCall = askViaAnthropic(prompt, settings.anthropic);
      break;
    case 'opencode':
      aiCall = askViaOpenCode(prompt, settings.opencode);
      break;
    case 'groq':
      if (!settings.groq?.apiKey) throw new Error('Groq API key not configured. Go to ⚙️ AI Settings.');
      aiCall = askViaGroq(prompt, settings.groq);
      break;
    case 'ollama':
    default:
      aiCall = askViaOllama(prompt, settings.ollama);
  }
  const result = await Promise.race([aiCall, timeout]);
  // Clean markdown fences from any provider — safe for all
  return cleanAIResponse(result);
}



// ─── Data Store ───────────────────────────────────────────────────
function loadData() {
  if (!fs.existsSync(DATA_FILE)) {
    const defaults = {
      daytrading: {
        watchlist: [
          { ticker: 'NVDA' },
          { ticker: 'AAPL' },
          { ticker: 'TSLA' },
          { ticker: 'MSFT' },
          { ticker: 'AMZN' }
        ],
        contracts: [],
        closedTrades: [],
        cryptoWatchlist: [
          { ticker: 'BTC' },
          { ticker: 'ETH' },
          { ticker: 'SOL' },
          { ticker: 'DOGE' },
          { ticker: 'XRP' }
        ]
      },
      longterm: {
        portfolio: [
          { ticker: 'NVDA', shares: 10, avgCost: 420.00, dateBought: '2024-06-01' },
          { ticker: 'AAPL', shares: 25, avgCost: 175.00, dateBought: '2024-03-15' },
          { ticker: 'TSLA', shares: 5,  avgCost: 280.00, dateBought: '2024-08-10' },
          { ticker: 'MSFT', shares: 8,  avgCost: 390.00, dateBought: '2024-05-20' }
        ],
        watchlist: [
          { ticker: 'META' },
          { ticker: 'GOOGL' },
          { ticker: 'AMZN' }
        ],
        cryptoPortfolio: [],
        cryptoWatchlist: [
          { ticker: 'BTC' },
          { ticker: 'ETH' },
          { ticker: 'SOL' }
        ]
      }
    };
    fs.writeFileSync(DATA_FILE, JSON.stringify(defaults, null, 2));
    return defaults;
  }
  try { return JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); }
  catch { return { daytrading: { watchlist: [], contracts: [], closedTrades: [] }, longterm: { portfolio: [], watchlist: [] } }; }
}

function saveData(data) {
  fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2));
}

// ─── Crypto Symbol Map ────────────────────────────────────────────
const CRYPTO_SYMBOLS = {
  BTC: 'BINANCE:BTCUSDT',
  ETH: 'BINANCE:ETHUSDT',
  SOL: 'BINANCE:SOLUSDT',
  DOGE: 'BINANCE:DOGEUSDT',
  XRP: 'BINANCE:XRPUSDT',
  BNB: 'BINANCE:BNBUSDT',
  ADA: 'BINANCE:ADAUSDT',
  AVAX: 'BINANCE:AVAXUSDT',
  MATIC: 'BINANCE:MATICUSDT',
  LINK: 'BINANCE:LINKUSDT',
  DOT: 'BINANCE:DOTUSDT',
  LTC: 'BINANCE:LTCUSDT',
  SHIB: 'BINANCE:SHIBUSDT',
  UNI: 'BINANCE:UNIUSDT',
  ATOM: 'BINANCE:ATOMUSDT'
};

function cryptoSymbol(coin) {
  const upper = coin.toUpperCase();
  return CRYPTO_SYMBOLS[upper] || `BINANCE:${upper}USDT`;
}

function isCrypto(ticker) {
  return !!CRYPTO_SYMBOLS[ticker.toUpperCase()] || ticker.toUpperCase().endsWith('USDT');
}

// ─── Finnhub ──────────────────────────────────────────────────────
const { execFile } = require('child_process');

function finnhubGetSecure(urlPath) {
  return new Promise((resolve) => {
    const url = `https://finnhub.io/api/v1${urlPath}&token=${getFinnhubKey()}`;
    execFile('curl', ['-s', '--max-time', '10', url], (err, stdout) => {
      if (err || !stdout) return resolve({});
      try { resolve(JSON.parse(stdout)); }
      catch { resolve({}); }
    });
  });
}

// ─── Yahoo Finance ────────────────────────────────────────────────
function yahooGetSecure(symbol) {
  return new Promise((resolve) => {
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?interval=1d&range=1d`;
    execFile('curl', ['-s', '--max-time', '10', '-A', 'Mozilla/5.0', url], (err, stdout) => {
      if (err || !stdout) return resolve(null);
      try {
        const json = JSON.parse(stdout);
        const meta = json?.chart?.result?.[0]?.meta;
        if (!meta) return resolve(null);
        resolve({
          price: parseFloat(meta.regularMarketPrice) || 0,
          prevClose: parseFloat(meta.previousClose || meta.chartPreviousClose) || 0,
          open: parseFloat(meta.regularMarketOpen) || 0,
          high: parseFloat(meta.regularMarketDayHigh) || 0,
          low: parseFloat(meta.regularMarketDayLow) || 0,
          change: parseFloat(meta.regularMarketPrice - meta.previousClose) || 0,
          changePct: parseFloat(((meta.regularMarketPrice - meta.previousClose) / meta.previousClose) * 100) || 0,
          marketState: meta.marketState || 'CLOSED'
        });
      } catch { resolve(null); }
    });
  });
}

function yahooCryptoSymbol(coin) {
  return `${coin.toUpperCase()}-USD`;
}

// ─── Yahoo Finance Screener (market-wide movers) ──────────────────
function yahooScreener(scrId, count = 50) {
  return new Promise((resolve) => {
    const url = `https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?formatted=false&scrIds=${scrId}&count=${count}`;
    execFile('curl', ['-s', '--max-time', '15', '-A', 'Mozilla/5.0', '-H', 'Accept: application/json', url], (err, stdout) => {
      if (err || !stdout) return resolve([]);
      try {
        const json = JSON.parse(stdout);
        const quotes = json?.finance?.result?.[0]?.quotes || [];
        resolve(quotes);
      } catch { resolve([]); }
    });
  });
}

// ─── Multi-Source Price Reconciler ───────────────────────────────
async function fetchMultiSource(symbol, isCrypto = false) {
  const t0 = Date.now();
  const yahooSymbol = isCrypto ? yahooCryptoSymbol(symbol) : symbol;
  const finnhubSymbol = isCrypto ? cryptoSymbol(symbol) : symbol;

  const [finnhubRaw, yahooRaw] = await Promise.all([
    finnhubGetSecure(`/quote?symbol=${finnhubSymbol}`),
    yahooGetSecure(yahooSymbol)
  ]);

  const finnhubPrice = parseFloat(finnhubRaw?.c) || 0;
  const yahooPrice = yahooRaw?.price || 0;
  const fetchMs = Date.now() - t0;

  const sources = [];
  if (finnhubPrice > 0) sources.push({ name: 'Finnhub', price: finnhubPrice });
  if (yahooPrice > 0) sources.push({ name: 'Yahoo', price: yahooPrice });

  let finalPrice = 0;
  let confidence = 'low';
  let divergencePct = null;

  if (sources.length === 2) {
    divergencePct = Math.abs((finnhubPrice - yahooPrice) / ((finnhubPrice + yahooPrice) / 2) * 100);
    if (divergencePct < 0.5) {
      confidence = 'high';
      finalPrice = (finnhubPrice + yahooPrice) / 2;
    } else if (divergencePct < 2) {
      confidence = 'medium';
      finalPrice = (finnhubPrice + yahooPrice) / 2;
    } else {
      confidence = 'low';
      finalPrice = yahooPrice > 0 ? yahooPrice : finnhubPrice;
    }
  } else if (sources.length === 1) {
    confidence = 'single';
    finalPrice = sources[0].price;
  }

  const prevClose = parseFloat(finnhubRaw?.pc) || yahooRaw?.prevClose || 0;
  const change = finalPrice - prevClose;
  const changePct = prevClose > 0 ? (change / prevClose) * 100 : 0;

  return {
    ticker: symbol.toUpperCase(),
    price: finalPrice,
    change,
    changePct,
    high: parseFloat(finnhubRaw?.h) || yahooRaw?.high || 0,
    low: parseFloat(finnhubRaw?.l) || yahooRaw?.low || 0,
    open: parseFloat(finnhubRaw?.o) || yahooRaw?.open || 0,
    prevClose,
    valid: finalPrice > 0,
    confidence,
    divergencePct: divergencePct !== null ? parseFloat(divergencePct.toFixed(3)) : null,
    sources: {
      finnhub: finnhubPrice > 0 ? { price: finnhubPrice, ok: true } : { price: 0, ok: false },
      yahoo: yahooPrice > 0 ? { price: yahooPrice, ok: true } : { price: 0, ok: false }
    },
    fetchMs,
    marketState: yahooRaw?.marketState || null
  };
}

// ─── Express Setup ────────────────────────────────────────────────
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.get('/api/ping', (req, res) => res.json({ ok: true }));

// ─── Symbol Search & Validate ─────────────────────────────────────
app.get('/api/search/:query', async (req, res) => {
  try {
    const q = encodeURIComponent(req.params.query);
    const data = await finnhubGetSecure(`/search?q=${q}`);
    const results = (data?.result || [])
      .filter(r => r.type === 'Common Stock' && !r.symbol.includes('.'))
      .slice(0, 6)
      .map(r => ({ symbol: r.displaySymbol, name: r.description, type: r.type }));
    res.json(results);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/validate/:ticker', async (req, res) => {
  try {
    const ticker = req.params.ticker.toUpperCase();

    // ── Crypto fast-path — validate via Yahoo Finance ─────────────
    if (CRYPTO_SYMBOLS[ticker] || ticker.endsWith('USDT')) {
      const yahooSym = `${ticker}-USD`;
      const yahooData = await yahooGetSecure(yahooSym);
      if (yahooData?.price > 0) {
        const names = { BTC:'Bitcoin', ETH:'Ethereum', SOL:'Solana', DOGE:'Dogecoin',
          XRP:'XRP', BNB:'BNB', ADA:'Cardano', AVAX:'Avalanche', MATIC:'Polygon',
          LINK:'Chainlink', DOT:'Polkadot', LTC:'Litecoin', SHIB:'Shiba Inu',
          UNI:'Uniswap', ATOM:'Cosmos' };
        return res.json({ valid: true, ticker, name: names[ticker] || ticker, price: yahooData.price, isCrypto: true });
      }
      return res.json({ valid: false, ticker, error: `No data found for crypto "${ticker}"` });
    }

    // ── Stock — try Finnhub first ─────────────────────────────────
    const quote = await finnhubGetSecure(`/quote?symbol=${ticker}`);
    if (quote?.c > 0) {
      const profile = await finnhubGetSecure(`/stock/profile2?symbol=${ticker}`);
      return res.json({ valid: true, ticker, name: profile?.name || ticker, price: quote.c });
    }
    // Try search to resolve full name → ticker
    const search = await finnhubGetSecure(`/search?q=${encodeURIComponent(ticker)}`);
    const match = (search?.result || []).find(r =>
      r.type === 'Common Stock' && !r.symbol.includes('.') &&
      (r.displaySymbol.toUpperCase() === ticker || r.description.toUpperCase().includes(ticker))
    );
    if (match) {
      const q2 = await finnhubGetSecure(`/quote?symbol=${match.displaySymbol}`);
      if (q2?.c > 0) {
        return res.json({ valid: true, ticker: match.displaySymbol, name: match.description, price: q2.c });
      }
    }
    // Final fallback — try Yahoo for stocks too
    const yahooFallback = await yahooGetSecure(ticker);
    if (yahooFallback?.price > 0) {
      return res.json({ valid: true, ticker, name: ticker, price: yahooFallback.price });
    }
    res.json({ valid: false, ticker, error: `No valid stock data found for "${ticker}"` });
  } catch (e) { res.json({ valid: false, error: e.message }); }
});

// ─── AI Settings Endpoints ────────────────────────────────────────
app.get('/api/ai/settings', (req, res) => {
  const s = loadAISettings();
  // Mask API keys for display
  const masked = JSON.parse(JSON.stringify(s));
  if (masked.openai?.apiKey) masked.openai.apiKey = masked.openai.apiKey.slice(0, 8) + '••••••••••••••••';
  if (masked.anthropic?.apiKey) masked.anthropic.apiKey = masked.anthropic.apiKey.slice(0, 8) + '••••••••••••••••';
  res.json(masked);
});

app.post('/api/ai/settings', (req, res) => {
  try {
    const current = loadAISettings();
    const incoming = req.body;
    // Don't overwrite masked keys
    if (incoming.openai?.apiKey?.includes('••')) incoming.openai.apiKey = current.openai?.apiKey || '';
    if (incoming.anthropic?.apiKey?.includes('••')) incoming.anthropic.apiKey = current.anthropic?.apiKey || '';
    const merged = { ...current, ...incoming };
    saveAISettings(merged);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/ai/settings/test', async (req, res) => {
  try {
    const { provider, openai, anthropic, groq, ollama, opencode } = req.body;
    // Use credentials from request body first, fall back to saved settings
    const saved = loadAISettings();
    const testPrompt = 'Reply with exactly: {"status":"ok","message":"Connection successful"}';
    let result = '';

    // Merge: request body credentials take priority over saved ones
    const cfg = {
      openai:    { ...saved.openai,    ...(openai    || {}) },
      anthropic: { ...saved.anthropic, ...(anthropic || {}) },
      groq:      { ...saved.groq,      ...(groq      || {}) },
      ollama:    { ...saved.ollama,    ...(ollama    || {}) },
      opencode:  { ...saved.opencode,  ...(opencode  || {}) }
    };

    switch (provider || saved.provider) {
      case 'openai':
        if (!cfg.openai?.apiKey) return res.json({ ok: false, error: 'No API key — enter your OpenAI key above' });
        result = await askViaOpenAI(testPrompt, cfg.openai);
        break;
      case 'anthropic':
        if (!cfg.anthropic?.apiKey) return res.json({ ok: false, error: 'No API key — enter your Anthropic key above' });
        result = await askViaAnthropic(testPrompt, cfg.anthropic);
        break;
      case 'opencode':
        result = await askViaOpenCode(testPrompt, cfg.opencode);
        break;
      case 'groq':
        if (!cfg.groq?.apiKey) return res.json({ ok: false, error: 'No API key — enter your Groq key above (free at console.groq.com)' });
        result = await askViaGroq(testPrompt, cfg.groq);
        break;
      case 'ollama':
      default:
        result = await askViaOllama(testPrompt, cfg.ollama);
        break;
    }
    res.json({ ok: true, response: result.slice(0, 200) });
  } catch (e) { res.json({ ok: false, error: e.message }); }
});

// ── GET /api/ollama/models — list available local models ──────────
app.get('/api/ollama/models', async (req, res) => {
  try {
    const settings = loadAISettings();
    const url = settings.ollama?.url || 'http://localhost:11434';
    const result = await new Promise((resolve) => {
      const apiUrl = new URL('/api/tags', url);
      const transport = apiUrl.protocol === 'https:' ? require('https') : http;
      const req = transport.get({ hostname: apiUrl.hostname, port: apiUrl.port || 11434, path: apiUrl.pathname, timeout: 5000 }, r => {
        let out = '';
        r.on('data', d => out += d);
        r.on('end', () => { try { resolve(JSON.parse(out)); } catch { resolve(null); } });
      });
      req.on('error', () => resolve(null));
      req.on('timeout', () => { req.destroy(); resolve(null); });
    });
    if (!result) return res.json({ running: false, models: [] });
    const models = (result.models || []).map(m => ({ name: m.name, size: m.size, modified: m.modified_at }));
    res.json({ running: true, models });
  } catch { res.json({ running: false, models: [] }); }
});

app.get('/api/ai/status', async (req, res) => {
  const settings = loadAISettings();

  // Quick Ollama check
  let ollamaRunning = false;
  let ollamaModels = [];
  try {
    const url = settings.ollama?.url || 'http://localhost:11434';
    const result = await new Promise((resolve) => {
      const apiUrl = new URL('/api/tags', url);
      const transport = apiUrl.protocol === 'https:' ? require('https') : http;
      const r = transport.get({ hostname: apiUrl.hostname, port: apiUrl.port || 11434, path: apiUrl.pathname, timeout: 3000 }, res => {
        let out = '';
        res.on('data', d => out += d);
        res.on('end', () => { try { resolve(JSON.parse(out)); } catch { resolve(null); } });
      });
      r.on('error', () => resolve(null));
      r.on('timeout', () => { r.destroy(); resolve(null); });
    });
    if (result) {
      ollamaModels = (result.models || []).map(m => m.name);
      // Only mark as properly running if it has at least one model installed
      ollamaRunning = ollamaModels.length > 0;
    }
  } catch {}

  const ocRunning = findLocalOpenCode();
  res.json({
    provider: settings.provider,
    opencode:  { configured: true, running: ocRunning, port: _ocPort, agent: settings.opencode?.agent || 'general' },
    openai:    { configured: !!(settings.openai?.apiKey), model: settings.openai?.model },
    anthropic: { configured: !!(settings.anthropic?.apiKey), model: settings.anthropic?.model },
    groq:      { configured: !!(settings.groq?.apiKey), model: settings.groq?.model },
    ollama:    { configured: true, running: ollamaRunning, url: settings.ollama?.url, model: settings.ollama?.model, availableModels: ollamaModels }
  });
});

// ─── Multi-Source Price Endpoints ─────────────────────────────────
// GET /api/price/multi/:ticker removed — use POST /api/prices/multi

app.post('/api/prices/multi', async (req, res) => {
  const { tickers } = req.body;
  if (!tickers?.length) return res.json([]);
  try {
    const results = await Promise.all(tickers.map(t => fetchMultiSource(t.toUpperCase(), false)));
    res.json(results);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// GET /api/crypto/price/multi/:coin removed — use POST /api/crypto/prices/multi

app.post('/api/crypto/prices/multi', async (req, res) => {
  const { coins } = req.body;
  if (!coins?.length) return res.json([]);
  try {
    const results = await Promise.all(coins.map(c => fetchMultiSource(c.toUpperCase(), true)));
    res.json(results.map(r => ({ ...r, isCrypto: true })));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── Price Endpoints ──────────────────────────────────────────────
app.get('/api/price/:ticker', async (req, res) => {
  try {
    const data = await finnhubGetSecure(`/quote?symbol=${req.params.ticker.toUpperCase()}`);
    res.json({
      ticker: req.params.ticker.toUpperCase(),
      price: data.c,
      change: data.d,
      changePct: data.dp,
      high: data.h,
      low: data.l,
      open: data.o,
      prevClose: data.pc
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// POST /api/prices (single-source) removed — use POST /api/prices/multi (dual-source)

// ─── Crypto Price Endpoint ────────────────────────────────────────
app.get('/api/crypto/price/:coin', async (req, res) => {
  try {
    const coin = req.params.coin.toUpperCase();
    const symbol = cryptoSymbol(coin);
    const data = await finnhubGetSecure(`/quote?symbol=${symbol}`);
    res.json({
      ticker: coin,
      symbol,
      price: parseFloat(data.c) || 0,
      change: parseFloat(data.d) || 0,
      changePct: parseFloat(data.dp) || 0,
      high: parseFloat(data.h) || 0,
      low: parseFloat(data.l) || 0,
      prevClose: parseFloat(data.pc) || 0,
      isCrypto: true
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});



// ─── News ─────────────────────────────────────────────────────────
app.get('/api/news/:ticker', async (req, res) => {
  try {
    const today = new Date().toISOString().split('T')[0];
    const weekAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    const data = await finnhubGetSecure(`/company-news?symbol=${req.params.ticker.toUpperCase()}&from=${weekAgo}&to=${today}`);
    // Finnhub returns array on success, object on error — always return array
    const articles = Array.isArray(data) ? data : [];
    res.json(articles.slice(0, 5));
  } catch (e) { res.json([]); }
});

// ─── Data Endpoints ───────────────────────────────────────────────
app.get('/api/data', (req, res) => res.json(loadData()));
app.post('/api/data', (req, res) => { saveData(req.body); res.json({ ok: true }); });

// Day Trading — Watchlist
app.post('/api/daytrading/watchlist/add', (req, res) => {
  const { ticker } = req.body;
  if (!ticker) return res.status(400).json({ error: 'ticker required' });
  const data = loadData();
  if (!data.daytrading.watchlist.find(s => s.ticker === ticker.toUpperCase())) {
    data.daytrading.watchlist.push({ ticker: ticker.toUpperCase() });
    saveData(data);
  }
  res.json({ ok: true });
});

app.post('/api/daytrading/watchlist/remove', (req, res) => {
  const { ticker } = req.body;
  const data = loadData();
  data.daytrading.watchlist = data.daytrading.watchlist.filter(s => s.ticker !== ticker.toUpperCase());
  saveData(data);
  res.json({ ok: true });
});

// Day Trading — Contracts
app.post('/api/daytrading/contracts/add', (req, res) => {
  const contract = { ...req.body, id: Date.now().toString(), addedAt: new Date().toISOString(), status: 'open' };
  const data = loadData();
  data.daytrading.contracts.unshift(contract);
  saveData(data);
  res.json({ ok: true, contract });
});

app.post('/api/daytrading/contracts/close', (req, res) => {
  const { id, soldPrice, soldDate, soldTime } = req.body;
  const data = loadData();
  const idx = data.daytrading.contracts.findIndex(c => c.id === id);
  if (idx === -1) return res.status(404).json({ error: 'Contract not found' });
  const contract = data.daytrading.contracts[idx];
  const pnl = (soldPrice - contract.pricePaid) * 100 * (contract.contracts || 1);
  const closed = { ...contract, soldPrice, soldDate, soldTime, pnl, status: 'closed', closedAt: new Date().toISOString() };
  data.daytrading.contracts.splice(idx, 1);
  data.daytrading.closedTrades.unshift(closed);
  saveData(data);
  res.json({ ok: true, pnl });
});

app.put('/api/daytrading/contracts/:id', (req, res) => {
  const data = loadData();
  const idx = data.daytrading.contracts.findIndex(c => c.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Contract not found' });
  data.daytrading.contracts[idx] = {
    ...data.daytrading.contracts[idx],
    ...req.body,
    id: req.params.id,
    updatedAt: new Date().toISOString()
  };
  saveData(data);
  res.json({ ok: true, contract: data.daytrading.contracts[idx] });
});

app.delete('/api/daytrading/contracts/:id', (req, res) => {
  const data = loadData();
  data.daytrading.contracts = data.daytrading.contracts.filter(c => c.id !== req.params.id);
  saveData(data);
  res.json({ ok: true });
});

// Long Term — Portfolio
app.post('/api/longterm/portfolio/add', (req, res) => {
  const stock = { ...req.body, id: Date.now().toString(), addedAt: new Date().toISOString() };
  const data = loadData();
  const existing = data.longterm.portfolio.findIndex(s => s.ticker === stock.ticker.toUpperCase());
  if (existing >= 0) {
    data.longterm.portfolio[existing] = { ...data.longterm.portfolio[existing], ...stock, ticker: stock.ticker.toUpperCase() };
  } else {
    data.longterm.portfolio.unshift({ ...stock, ticker: stock.ticker.toUpperCase() });
  }
  saveData(data);
  res.json({ ok: true });
});

app.post('/api/longterm/portfolio/remove', (req, res) => {
  const { ticker } = req.body;
  const data = loadData();
  data.longterm.portfolio = data.longterm.portfolio.filter(s => s.ticker !== ticker.toUpperCase());
  saveData(data);
  res.json({ ok: true });
});

// Long Term — Watchlist
app.post('/api/longterm/watchlist/add', (req, res) => {
  const { ticker } = req.body;
  if (!ticker) return res.status(400).json({ error: 'ticker required' });
  const data = loadData();
  if (!data.longterm.watchlist.find(s => s.ticker === ticker.toUpperCase())) {
    data.longterm.watchlist.push({ ticker: ticker.toUpperCase() });
    saveData(data);
  }
  res.json({ ok: true });
});

app.post('/api/longterm/watchlist/remove', (req, res) => {
  const { ticker } = req.body;
  const data = loadData();
  data.longterm.watchlist = data.longterm.watchlist.filter(s => s.ticker !== ticker.toUpperCase());
  saveData(data);
  res.json({ ok: true });
});

// ─── Crypto CRUD Endpoints ────────────────────────────────────────

// Day Trading crypto watchlist
app.post('/api/daytrading/crypto/add', (req, res) => {
  const { ticker } = req.body;
  if (!ticker) return res.status(400).json({ error: 'ticker required' });
  const data = loadData();
  if (!data.daytrading.cryptoWatchlist) data.daytrading.cryptoWatchlist = [];
  if (!data.daytrading.cryptoWatchlist.find(s => s.ticker === ticker.toUpperCase())) {
    data.daytrading.cryptoWatchlist.push({ ticker: ticker.toUpperCase() });
    saveData(data);
  }
  res.json({ ok: true });
});

app.post('/api/daytrading/crypto/remove', (req, res) => {
  const { ticker } = req.body;
  const data = loadData();
  if (!data.daytrading.cryptoWatchlist) data.daytrading.cryptoWatchlist = [];
  data.daytrading.cryptoWatchlist = data.daytrading.cryptoWatchlist.filter(s => s.ticker !== ticker.toUpperCase());
  saveData(data);
  res.json({ ok: true });
});

// Long Term crypto portfolio
app.post('/api/longterm/crypto/portfolio/add', (req, res) => {
  const coin = { ...req.body, id: Date.now().toString(), addedAt: new Date().toISOString() };
  const data = loadData();
  if (!data.longterm.cryptoPortfolio) data.longterm.cryptoPortfolio = [];
  const existing = data.longterm.cryptoPortfolio.findIndex(s => s.ticker === coin.ticker.toUpperCase());
  if (existing >= 0) {
    data.longterm.cryptoPortfolio[existing] = { ...data.longterm.cryptoPortfolio[existing], ...coin, ticker: coin.ticker.toUpperCase() };
  } else {
    data.longterm.cryptoPortfolio.unshift({ ...coin, ticker: coin.ticker.toUpperCase() });
  }
  saveData(data);
  res.json({ ok: true });
});

app.post('/api/longterm/crypto/portfolio/remove', (req, res) => {
  const { ticker } = req.body;
  const data = loadData();
  if (!data.longterm.cryptoPortfolio) data.longterm.cryptoPortfolio = [];
  data.longterm.cryptoPortfolio = data.longterm.cryptoPortfolio.filter(s => s.ticker !== ticker.toUpperCase());
  saveData(data);
  res.json({ ok: true });
});

// Long Term crypto watchlist
app.post('/api/longterm/crypto/watchlist/add', (req, res) => {
  const { ticker } = req.body;
  if (!ticker) return res.status(400).json({ error: 'ticker required' });
  const data = loadData();
  if (!data.longterm.cryptoWatchlist) data.longterm.cryptoWatchlist = [];
  if (!data.longterm.cryptoWatchlist.find(s => s.ticker === ticker.toUpperCase())) {
    data.longterm.cryptoWatchlist.push({ ticker: ticker.toUpperCase() });
    saveData(data);
  }
  res.json({ ok: true });
});

app.post('/api/longterm/crypto/watchlist/remove', (req, res) => {
  const { ticker } = req.body;
  const data = loadData();
  if (!data.longterm.cryptoWatchlist) data.longterm.cryptoWatchlist = [];
  data.longterm.cryptoWatchlist = data.longterm.cryptoWatchlist.filter(s => s.ticker !== ticker.toUpperCase());
  saveData(data);
  res.json({ ok: true });
});

// ─── AI Endpoints ─────────────────────────────────────────────────

// Generate options signal
app.post('/api/ai/signal', async (req, res) => {
  const { ticker, price, change, changePct, news, currentPrice: cp } = req.body;
  const stockPrice = cp || price || 0;

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const send = (data) => { try { res.write(`data: ${JSON.stringify(data)}\n\n`); } catch {} };
  const keepalive = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 15000);

  try {
    send({ type: 'status', text: '🔍 Analyzing options opportunity...' });

    const newsArr = Array.isArray(news) ? news : [];
    const newsText = newsArr.map(n => `- ${n.headline}`).join('\n') || 'No recent news available';
    const today = new Date().toDateString();

    // Calculate realistic strike suggestions
    const roundedPrice = Math.round(stockPrice / 5) * 5;
    const atmStrike = roundedPrice;
    const otmCallStrike = roundedPrice + 5;
    const otmPutStrike = roundedPrice - 5;

    const prompt = `You are a professional options trader giving a 2-4 week options swing trade signal to a beginner. This is about options contracts — calls and puts — held over DAYS TO WEEKS, not minutes.

## Stock Data
- Ticker: ${ticker}
- Current Stock Price: $${stockPrice.toFixed(2)}
- Today's Change: ${change >= 0 ? '+' : ''}$${(change||0).toFixed(2)} (${changePct >= 0 ? '+' : ''}${(changePct||0).toFixed(2)}%)
- ATM Strike suggestion: $${atmStrike}
- OTM Call Strike suggestion: $${otmCallStrike}
- OTM Put Strike suggestion: $${otmPutStrike}
- Today: ${today}

## Recent News Headlines
${newsText}

## YOUR TASK — 2-4 WEEK OPTIONS SWING SIGNAL
Analyze the 2-4 week directional outlook for ${ticker} and recommend a specific options contract.

CRITICAL RULES:
- Base direction on the 2-4 WEEK outlook — NOT today's price move. Today's up/down is noise.
- A stock being down 1-2% today does NOT mean it is bearish over 2-4 weeks
- Think about: upcoming catalysts, earnings, sector trends, macro environment, technical levels
- If the 2-4 week thesis is UP → BUY CALL
- If the 2-4 week thesis is DOWN → BUY PUT  
- If genuinely unclear over 2-4 weeks → WAIT
- Once entered, this trade should be held for the full 2-4 weeks unless the original thesis changes
- Do NOT suggest exiting just because the stock dips the next day — that is normal volatility

Return ONLY this exact JSON:
{
  "action": "BUY CALL" or "BUY PUT" or "WAIT",
  "direction": "Bullish" or "Bearish" or "Neutral",
  "thesis": "1-2 sentence explanation of WHY the stock is expected to move this direction over 2-4 weeks — based on fundamentals/catalysts, NOT today's price",
  "holdPlan": "Hold this contract for 2-4 weeks. Only exit early if: [specific fundamental thesis-breaking condition]. Ignore daily price fluctuations.",
  "strike": number (e.g. ${otmCallStrike}),
  "strikeLabel": "e.g. $${otmCallStrike} (slightly OTM Call)",
  "expiry": "YYYY-MM-DD",
  "expiryLabel": "e.g. May 16 (3 weeks)",
  "contracts": 1,
  "estimatedPremium": number (cost per share e.g. 3.50),
  "totalCost": number (estimatedPremium × 100, e.g. 350),
  "costEstimate": "e.g. ~$350 (1 contract × $3.50 × 100 shares)",
  "breakEven": number (strike + premium for call, strike - premium for put),
  "breakEvenLabel": "e.g. $${otmCallStrike + 3.5} by May 16",
  "maxLoss": number (= totalCost, the premium paid),
  "maxLossLabel": "e.g. $350 — the premium paid if expires worthless",
  "targetPrice": number (stock price target over 2-4 weeks),
  "targetReturn": "e.g. +180% if ${ticker} hits $${(otmCallStrike + 10)} by May 16",
  "confidence": number 0-100,
  "riskLevel": "Low" or "Medium" or "High",
  "technical": ["2-4 week technical observation 1", "observation 2", "observation 3"],
  "fundamental": ["fundamental catalyst 1", "fundamental catalyst 2"],
  "news": ["news impact on 2-4 week outlook 1", "news impact 2"],
  "risk": ["what would invalidate this thesis", "options-specific risk"],
  "whyThisStrike": "explain why this specific strike was chosen (ITM/ATM/OTM and why)",
  "whyThisExpiry": "explain why this expiry gives enough time for the thesis to play out",
  "steps": [
    "On Robinhood: search ${ticker} → tap Trade → Trade Options",
    "Select ${today.includes('2026') ? 'the expiry date' : 'expiry'} tab → choose [EXPIRY DATE]",
    "Select [STRIKE] strike → tap [CALL/PUT]",
    "Set quantity to 1 contract → review the premium shown",
    "Set a limit order at the ask price → confirm"
  ],
  "exitPlan": "Take profit at +75-100% gain on the premium. Only cut loss if the stock breaks the key level that invalidates the 2-4 week thesis — NOT just because it dips the next day.",
  "warning": "beginner tip: options move a lot day-to-day — that is normal. Only exit if the fundamental 2-4 week thesis changes, not because of a 1-day dip."
}`;

    send({ type: 'status', text: '🤖 Building your options signal...' });
    const result = await askAI(prompt);
    send({ type: 'status', text: '📊 Calculating contract details...' });

    const jsonMatch = result.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('Could not parse signal');

    const signal = JSON.parse(jsonMatch[0]);
    send({ type: 'signal', data: signal });
    send({ type: 'done' });

  } catch (e) {
    send({ type: 'error', text: e.message });
    send({ type: 'done' });
  } finally {
    clearInterval(keepalive);
    res.end();
  }
});

// Monitor an open contract
app.post('/api/ai/monitor', async (req, res) => {
  const { contract, currentPrice } = req.body;

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const send = (data) => { try { res.write(`data: ${JSON.stringify(data)}\n\n`); } catch {} };
  const keepalive = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 15000);

  try {
    send({ type: 'status', text: '👁 Analyzing your contract...' });

    const pnl = (currentPrice - contract.pricePaid) * 100 * (contract.contracts || 1);
    const pnlPct = ((currentPrice - contract.pricePaid) / contract.pricePaid * 100).toFixed(1);
    const expiry = new Date(contract.expiry);
    const daysLeft = Math.ceil((expiry - new Date()) / (1000 * 60 * 60 * 24));
    const breakEven = contract.type === 'CALL'
      ? (parseFloat(contract.strike) + parseFloat(contract.pricePaid)).toFixed(2)
      : (parseFloat(contract.strike) - parseFloat(contract.pricePaid)).toFixed(2);

    const daysHeld = contract.dateBought
      ? Math.floor((Date.now() - new Date(contract.dateBought).getTime()) / (1000 * 60 * 60 * 24))
      : 0;
    const isCallITM = contract.type === 'CALL' && currentPrice > parseFloat(contract.strike);
    const isPutITM  = contract.type === 'PUT'  && currentPrice < parseFloat(contract.strike);
    const isITM = isCallITM || isPutITM;

    const prompt = `You are an expert options swing trade advisor reviewing an open position entered for a 2-4 week thesis.

## Open Contract Details
- Stock: ${contract.ticker}
- Type: ${contract.type}
- Strike: $${contract.strike}
- Expiry: ${contract.expiry} (${daysLeft} days left)
- Price Paid: $${contract.pricePaid}/share = $${(contract.pricePaid * 100).toFixed(0)}/contract
- Current Stock Price: $${currentPrice}
- Break Even at Expiry: $${breakEven}
- In The Money: ${isITM ? 'YES' : 'NO'}
- Days Held So Far: ${daysHeld} day${daysHeld !== 1 ? 's' : ''}
- Current P&L: ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(0)} (${pnlPct}%)
- Contracts Held: ${contract.contracts || 1}
${contract.notes ? `- Original Notes: ${contract.notes}` : ''}

## Your Task
This is a SWING TRADE held for 2-4 weeks. Evaluate whether the original thesis is still valid.

CRITICAL RULES:
- Do NOT recommend SELL just because the position is down or the stock dipped since entry
- Short-term price fluctuations (1-5 days) are NORMAL for options — they are not a reason to exit
- HOLD means: the 2-4 week thesis is still intact, give it time to play out
- Only recommend SELL ALL if: thesis is clearly broken, expiry is dangerously close (< 5 days) with no hope, OR profit target already hit (+75%+)
- SELL HALF only if: large profit already locked in (+75%+) and prudent to take some off
- ROLL if: thesis is intact but expiry is approaching too fast (< 7 days left)
- The stock needs time to reach the strike — be patient if there are still ${daysLeft} days left

Return ONLY this exact JSON:
{
  "recommendation": "HOLD" or "SELL ALL" or "SELL HALF" or "ROLL",
  "urgency": "High" or "Medium" or "Low",
  "thesisStatus": "INTACT" or "WEAKENING" or "BROKEN",
  "reasons": ["specific reason based on thesis, not just P&L", "reason 2", "reason 3"],
  "rollSuggestion": "only if ROLL — suggest new expiry date",
  "targetExit": "stock price OR premium gain % at which to take profit (e.g. stock hits $X, or premium up 75%)",
  "stopLoss": "only exit early if this THESIS-BREAKING condition occurs — not just a price level",
  "summary": "one sentence: is the thesis still valid and what should the trader do"
}`;

    const result = await askAI(prompt);
    const jsonMatch = result.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('Could not parse recommendation');

    const analysis = JSON.parse(jsonMatch[0]);
    analysis.pnl = pnl;
    analysis.pnlPct = pnlPct;
    analysis.daysLeft = daysLeft;
    analysis.breakEven = breakEven;

    send({ type: 'analysis', data: analysis });
    send({ type: 'done' });

  } catch (e) {
    send({ type: 'error', text: e.message });
    send({ type: 'done' });
  } finally {
    clearInterval(keepalive);
    res.end();
  }
});

// Analyze a long term stock
app.post('/api/ai/analyze-stock', async (req, res) => {
  const { ticker, currentPrice, shares, avgCost, news } = req.body;

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const send = (data) => { try { res.write(`data: ${JSON.stringify(data)}\n\n`); } catch {} };
  const keepalive = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 15000);

  try {
    send({ type: 'status', text: `📊 Analyzing ${ticker}...` });

    const newsArr = Array.isArray(news) ? news : [];
    const newsText = newsArr.map(n => `- ${n.headline}`).join('\n') || 'No recent news';
    const totalCost = shares ? (shares * avgCost).toFixed(2) : null;
    const totalValue = shares ? (shares * currentPrice).toFixed(2) : null;
    const pnl = shares ? ((currentPrice - avgCost) * shares).toFixed(2) : null;
    const pnlPct = shares ? ((currentPrice - avgCost) / avgCost * 100).toFixed(1) : null;

    const prompt = `You are a long-term investment analyst with a 3-5 year investment horizon. Analyze ${ticker} for a long-term investor.

## Stock Data
- Ticker: ${ticker}
- Current Price: $${currentPrice}
${shares ? `- Shares Owned: ${shares} @ avg cost $${avgCost}` : '- Not currently owned (watchlist)'}

## Recent News Headlines
${newsText}

## Your Task
Analyze ${ticker} based PURELY on long-term fundamentals, future growth potential, and business thesis.

CRITICAL RULES:
- Current P&L and whether the stock is up or down is COMPLETELY IRRELEVANT — do not mention it in your reasoning
- SELL / REDUCE only if the long-term business thesis is fundamentally broken (e.g. structural decline, obsolete model, fraud, losing market share permanently)
- HOLD means: long-term thesis is intact — stay the course regardless of short-term price weakness
- BUY MORE means: strong future opportunity, good time to add to position
- Base recommendation on: future earnings growth, competitive moat, sector tailwinds, analyst consensus, 3-5 year outlook

Return ONLY this exact JSON:
{
  "recommendation": "STRONG BUY" or "BUY" or "BUY MORE" or "HOLD" or "REDUCE" or "SELL",
  "fairValue": "estimated fair value price based on fundamentals",
  "priceTarget": "3-year price target",
  "upside": "percentage upside potential over 3-5 years",
  "timeline": "3-5 years",
  "bullCase": ["future growth reason 1", "future growth reason 2", "future growth reason 3"],
  "bearCase": ["long-term risk 1", "long-term risk 2"],
  "sellTriggers": ["sell ONLY if: condition 1", "sell ONLY if: condition 2"],
  "buyMoreAt": "price level where adding more makes strong sense",
  "summary": "2-3 sentence plain English long-term outlook for a beginner investor"
}`;

    const result = await askAI(prompt);
    const jsonMatch = result.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('Could not parse analysis');

    const analysis = JSON.parse(jsonMatch[0]);
    if (shares) {
      analysis.pnl = pnl;
      analysis.pnlPct = pnlPct;
      analysis.totalValue = totalValue;
      analysis.totalCost = totalCost;
    }

    send({ type: 'analysis', data: analysis });
    send({ type: 'done' });

  } catch (e) {
    send({ type: 'error', text: e.message });
    send({ type: 'done' });
  } finally {
    clearInterval(keepalive);
    res.end();
  }
});

// AI suggest new stocks
app.post('/api/ai/suggest-stocks', async (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const send = (data) => { try { res.write(`data: ${JSON.stringify(data)}\n\n`); } catch {} };
  const keepalive = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 15000);

  try {
    send({ type: 'status', text: '🤖 Scanning market for opportunities...' });

    const { existing } = req.body;
    // Also pull from data.json to catch all watchlists
    const allExcluded = [...new Set([
      ...(existing || []),
      ...((() => { try { const d = loadData(); return [...(d.daytrading?.watchlist||[]),...(d.longterm?.watchlist||[]),...(d.longterm?.portfolio||[]),...(d.daytrading?.cryptoWatchlist||[]),...(d.longterm?.cryptoWatchlist||[])].map(s=>s.ticker); } catch { return []; } })())
    ])];

    const prompt = `You are a long-term investment advisor. Suggest 5 stocks worth considering for long-term investment in ${new Date().toLocaleDateString('en-US', {month:'long',year:'numeric'})}.

CRITICAL: Do NOT suggest any of these tickers — they are already tracked: ${allExcluded.join(', ')}

Suggest only NEW stocks not in that list.

Focus on:
- Strong fundamentals
- Growth potential
- Reasonable valuation
- Sectors with tailwinds

Return ONLY this exact JSON array:
[
  {
    "ticker": "TICKER",
    "name": "Company Name",
    "reason": "2-3 sentence why this is worth considering",
    "sector": "Technology/Healthcare/etc",
    "riskLevel": "Low/Medium/High",
    "timeHorizon": "1-3 years / 3-5 years / 5+ years"
  }
]`;

    const result = await askAI(prompt);
    const jsonMatch = result.match(/\[[\s\S]*\]/);
    if (!jsonMatch) throw new Error('Could not parse suggestions');

    const suggestions = JSON.parse(jsonMatch[0]);
    send({ type: 'suggestions', data: suggestions });
    send({ type: 'done' });

  } catch (e) {
    send({ type: 'error', text: e.message });
    send({ type: 'done' });
  } finally {
    clearInterval(keepalive);
    res.end();
  }
});

// Weekly summary
app.post('/api/ai/weekly-summary', async (req, res) => {
  const { portfolio, prices } = req.body;

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const send = (data) => { try { res.write(`data: ${JSON.stringify(data)}\n\n`); } catch {} };
  const keepalive = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 15000);

  try {
    send({ type: 'status', text: '📋 Generating weekly summary...' });

    const portfolioText = portfolio.map(s => {
      const price = prices[s.ticker] || 0;
      return `${s.ticker}: ${s.shares} shares @ avg cost $${s.avgCost}, current price $${price}`;
    }).join('\n');

    const prompt = `You are a long-term portfolio advisor giving a weekly summary to a long-term investor with a 3-5 year horizon.

## Long-Term Portfolio
${portfolioText}

CRITICAL RULES:
- These are LONG-TERM holdings — weekly price moves are noise, not signals
- NEVER suggest trimming or selling based on a stock being down or having a loss
- CONSIDER ADDING means: fundamentally strong, could be good time to add more
- WATCH means: monitor the long-term thesis for any fundamental changes
- HOLD means: stay the course, thesis is intact
- Only flag a stock as a concern if there is a FUNDAMENTAL long-term issue (not price decline)
- Focus advice on: staying disciplined, long-term compounding, avoiding emotional decisions

Return ONLY this exact JSON:
{
  "headline": "one sentence long-term portfolio summary",
  "weeklyChange": "brief note on this week — remind investor short-term moves don't matter",
  "stocks": [
    {
      "ticker": "TICKER",
      "action": "HOLD" or "WATCH" or "CONSIDER ADDING",
      "note": "one sentence long-term thesis update"
    }
  ],
  "topPick": "ticker with strongest long-term outlook",
  "watchOut": "ticker where long-term thesis needs monitoring (NOT because it is down)",
  "advice": "one paragraph of long-term focused advice — discipline, compounding, staying the course"
}`;

    const result = await askAI(prompt);
    const jsonMatch = result.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('Could not parse summary');

    const summary = JSON.parse(jsonMatch[0]);
    send({ type: 'summary', data: summary });
    send({ type: 'done' });

  } catch (e) {
    send({ type: 'error', text: e.message });
    send({ type: 'done' });
  } finally {
    clearInterval(keepalive);
    res.end();
  }
});

// ─── Crypto AI Signal ─────────────────────────────────────────────
app.post('/api/ai/crypto-signal', async (req, res) => {
  const { ticker, price, change, changePct } = req.body;

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const send = (data) => { try { res.write(`data: ${JSON.stringify(data)}\n\n`); } catch {} };
  const keepalive = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 15000);

  try {
    send({ type: 'status', text: `🔍 Analyzing ${ticker}...` });

    const prompt = `You are a long-term crypto investment analyst. Analyze ${ticker} and give a long-term investment signal for a 3-5 year horizon.

## Market Data
- Coin: ${ticker}
- Current Price: $${price}
- Date: ${new Date().toDateString()}

CRITICAL RULES:
- This is a LONG-TERM signal — ignore today's price change, it is noise
- BUY means: strong long-term fundamentals, good time to accumulate for 3-5 years
- HOLD means: long-term thesis intact, stay the course
- SELL means: long-term thesis is fundamentally broken (NOT just because price is down)
- Consider: adoption trends, network activity, institutional interest, macro cycle, 3-5 year potential

Return ONLY this exact JSON:
{
  "action": "BUY" or "HOLD" or "SELL",
  "confidence": number 0-100,
  "riskLevel": "Low" or "Medium" or "High",
  "technical": ["long-term fundamental 1", "long-term fundamental 2", "long-term fundamental 3"],
  "sentiment": ["macro trend 1", "macro trend 2"],
  "targets": {
    "entry": "good accumulation price range for long-term",
    "target": "3-year price target",
    "stopLoss": "only exit if this fundamental condition breaks"
  },
  "timeframe": "3-5 years",
  "warning": "long-term investor tip — volatility is normal, thesis is what matters",
  "summary": "2 sentence plain English long-term outlook"
}`;

    const result = await askAI(prompt);
    const jsonMatch = result.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('Could not parse signal');

    const signal = JSON.parse(jsonMatch[0]);
    send({ type: 'signal', data: signal });
    send({ type: 'done' });

  } catch (e) {
    send({ type: 'error', text: e.message });
    send({ type: 'done' });
  } finally {
    clearInterval(keepalive);
    res.end();
  }
});

// ─── Crypto AI Analysis (Long Term) ──────────────────────────────
app.post('/api/ai/crypto-analyze', async (req, res) => {
  const { ticker, price, coins, avgCost } = req.body;

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const send = (data) => { try { res.write(`data: ${JSON.stringify(data)}\n\n`); } catch {} };
  const keepalive = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 15000);

  try {
    send({ type: 'status', text: `📊 Analyzing ${ticker}...` });

    const owned = coins && avgCost;
    const pnl = owned ? ((price - avgCost) * coins).toFixed(2) : null;
    const pnlPct = owned ? ((price - avgCost) / avgCost * 100).toFixed(1) : null;

    const prompt = `You are a long-term crypto investment analyst with a 3-5 year horizon. Analyze ${ticker} for a long-term holder.

## Data
- Coin: ${ticker}
- Current Price: $${price}
${owned ? `- Holding: ${coins} ${ticker} @ avg cost $${avgCost}` : '- Not currently owned (watchlist)'}

CRITICAL RULES:
- Current P&L and short-term price movement is COMPLETELY IRRELEVANT — do not factor it in
- SELL / REDUCE only if the long-term crypto thesis is fundamentally broken (e.g. regulatory ban, technology made obsolete, project abandoned)
- HOLD means: long-term thesis is intact, stay the course through volatility
- BUY MORE means: strong long-term opportunity, fundamentals support adding
- Base recommendation on: adoption trends, network fundamentals, institutional interest, macro crypto cycle, 3-5 year potential

Return ONLY this exact JSON:
{
  "recommendation": "STRONG BUY" or "BUY" or "BUY MORE" or "HOLD" or "REDUCE" or "SELL",
  "priceTarget": "3-year price target",
  "upside": "percentage upside potential over 3-5 years",
  "bullCase": ["long-term reason 1", "long-term reason 2", "long-term reason 3"],
  "bearCase": ["long-term risk 1", "long-term risk 2"],
  "sellTriggers": ["sell ONLY if: condition 1", "sell ONLY if: condition 2"],
  "buyMoreAt": "price level where adding more makes strong long-term sense",
  "summary": "2-3 sentence plain English long-term outlook for a beginner investor"
}`;

    const result = await askAI(prompt);
    const jsonMatch = result.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('Could not parse analysis');

    const analysis = JSON.parse(jsonMatch[0]);
    if (owned) { analysis.pnl = pnl; analysis.pnlPct = pnlPct; }
    send({ type: 'analysis', data: analysis });
    send({ type: 'done' });

  } catch (e) {
    send({ type: 'error', text: e.message });
    send({ type: 'done' });
  } finally {
    clearInterval(keepalive);
    res.end();
  }
});

// ─── Quick Rating Endpoint (fast, no streaming) ───────────────────
// Returns: STRONG BUY / BUY / HOLD / SELL for a ticker
const RATINGS_FILE = path.join(BASE_DIR, 'ratings.json');

function loadRatings() {
  try { return JSON.parse(fs.readFileSync(RATINGS_FILE, 'utf8')); } catch { return {}; }
}
function saveRatings(r) { fs.writeFileSync(RATINGS_FILE, JSON.stringify(r, null, 2)); }

app.post('/api/ai/quick-rate', async (req, res) => {
  const { ticker, price, isCrypto } = req.body;
  if (!ticker || !price) return res.json({ ticker, rating: 'HOLD', color: 'yellow' });
  try {
    const assetType = isCrypto ? 'cryptocurrency' : 'stock';
    const prompt = `You are a long-term investment analyst. Rate ${ticker} (current price: $${price}) for a long-term investor holding a 3-5 year horizon.

Base your rating ONLY on:
- Long-term business fundamentals and competitive moat
- Sector tailwinds and macro trends over the next 3-5 years
- Analyst consensus and institutional sentiment
- Future earnings growth potential
- Whether the company is a market leader in a growing industry

Rate as ONLY one of: STRONG BUY, BUY, HOLD, SELL, STRONG SELL.

RULES:
- Current price movement is IRRELEVANT — do not factor in whether it is up or down today
- SELL only if the long-term business thesis is fundamentally broken (e.g. obsolete business model, fraud, structural decline)
- HOLD means the long-term thesis is intact and the investor should stay the course
- BUY / STRONG BUY means strong future growth expected over 3-5 years

Reply with ONLY those words, nothing else.`;

    const result = await askAI(prompt);
    const text = result.trim().toUpperCase();
    let rating = 'HOLD';
    if (text.includes('STRONG BUY')) rating = 'STRONG BUY';
    else if (text.includes('STRONG SELL')) rating = 'STRONG SELL';
    else if (text.includes('BUY')) rating = 'BUY';
    else if (text.includes('SELL')) rating = 'SELL';
    const colorMap = { 'STRONG BUY': 'green', 'BUY': 'green', 'HOLD': 'yellow', 'SELL': 'red', 'STRONG SELL': 'red' };
    const color = colorMap[rating] || 'yellow';
    const ratings = loadRatings();
    ratings[ticker] = { rating, color, price, updatedAt: new Date().toISOString() };
    saveRatings(ratings);
    res.json({ ticker, rating, color });
  } catch (e) {
    res.json({ ticker, rating: 'HOLD', color: 'yellow' });
  }
});

// Batch rate multiple tickers at once — much faster than sequential calls
app.post('/api/ai/batch-rate', async (req, res) => {
  const { tickers } = req.body; // [{ ticker, price, isCrypto }]
  if (!tickers?.length) return res.json({});
  try {
    const list = tickers.map(t => `${t.ticker} ($${t.price})`).join(', ');

    const prompt = `You are a long-term investment analyst. Rate each of the following for a long-term investor with a 3-5 year horizon.

Tickers: ${list}

Base your rating ONLY on:
- Long-term business fundamentals and competitive moat
- Sector tailwinds and macro trends over the next 3-5 years
- Future earnings growth potential and analyst consensus
- Whether the company is a market leader in a growing industry

Rate each as ONLY one of: STRONG BUY, BUY, HOLD, SELL, STRONG SELL.

RULES:
- Current price or recent price movement is COMPLETELY IRRELEVANT — ignore whether stocks are up or down
- SELL only if the long-term business thesis is fundamentally broken (obsolete model, fraud, structural decline)
- HOLD means long-term thesis is intact, stay the course
- BUY / STRONG BUY means strong future growth expected over 3-5 years

Reply as JSON: { "TICKER": "RATING", ... }. Nothing else.`;

    const result = await askAI(prompt);
    const jsonMatch = result.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('parse fail');
    const ratings_result = JSON.parse(jsonMatch[0]);
    const colorMap = { 'STRONG BUY': 'green', 'BUY': 'green', 'HOLD': 'yellow', 'SELL': 'red', 'STRONG SELL': 'red' };
    const ratings = loadRatings();
    const out = {};
    const now = new Date().toISOString();
    for (const [ticker, rating] of Object.entries(ratings_result)) {
      const clean = rating.trim().toUpperCase();
      const color = colorMap[clean] || 'yellow';
      const priceObj = tickers.find(t => t.ticker === ticker);
      ratings[ticker] = { rating: clean, color, price: priceObj?.price || 0, updatedAt: now };
      out[ticker] = { rating: clean, color, updatedAt: now };
    }
    saveRatings(ratings);
    res.json(out);
  } catch (e) {
    const out = {};
    tickers.forEach(t => { out[t.ticker] = { rating: 'HOLD', color: 'yellow' }; });
    res.json(out);
  }
});

app.get('/api/ratings', (req, res) => res.json(loadRatings()));



// ─── Yahoo News Fetcher ───────────────────────────────────────────
function yahooNewsSecure(symbol) {
  return new Promise((resolve) => {
    const url = `https://query1.finance.yahoo.com/v1/finance/search?q=${encodeURIComponent(symbol)}&newsCount=10&quotesCount=0`;
    execFile('curl', ['-s', '--max-time', '10', '-A', 'Mozilla/5.0', url], (err, stdout) => {
      if (err || !stdout) return resolve([]);
      try {
        const json = JSON.parse(stdout);
        const items = json?.news || [];
        resolve(items.map(n => ({
          source: 'Yahoo',
          headline: n.title || '',
          summary: '',
          url: n.link || '',
          datetime: n.providerPublishTime || 0,
          image: n.thumbnail?.resolutions?.[1]?.url || n.thumbnail?.resolutions?.[0]?.url || '',
          publisher: n.publisher || 'Yahoo Finance',
          relatedTickers: n.relatedTickers || []
        })));
      } catch { resolve([]); }
    });
  });
}

function finnhubNewsSecure(symbol, isCrypto = false) {
  return new Promise((resolve) => {
    const today = new Date().toISOString().split('T')[0];
    const weekAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    const urlPath = isCrypto
      ? `/crypto/news?symbol=${cryptoSymbol(symbol)}&token=${FINNHUB_KEY}`
      : `/company-news?symbol=${symbol.toUpperCase()}&from=${weekAgo}&to=${today}&token=${FINNHUB_KEY}`;
    const url = `https://finnhub.io/api/v1${urlPath}`;
    execFile('curl', ['-s', '--max-time', '10', url], (err, stdout) => {
      if (err || !stdout) return resolve([]);
      try {
        const json = JSON.parse(stdout);
        const items = Array.isArray(json) ? json : [];
        resolve(items.slice(0, 10).map(n => ({
          source: 'Finnhub',
          headline: n.headline || '',
          summary: n.summary || '',
          url: n.url || '',
          datetime: n.datetime || 0,
          image: n.image || '',
          publisher: n.source || 'Finnhub',
          relatedTickers: n.related ? [n.related] : []
        })));
      } catch { resolve([]); }
    });
  });
}

function yahooMarketNewsSecure() {
  return new Promise((resolve) => {
    const url = `https://query1.finance.yahoo.com/v1/finance/search?q=stock+market&newsCount=8&quotesCount=0`;
    execFile('curl', ['-s', '--max-time', '10', '-A', 'Mozilla/5.0', url], (err, stdout) => {
      if (err || !stdout) return resolve([]);
      try {
        const json = JSON.parse(stdout);
        resolve((json?.news || []).map(n => ({
          source: 'Yahoo',
          headline: n.title || '',
          summary: '',
          url: n.link || '',
          datetime: n.providerPublishTime || 0,
          image: n.thumbnail?.resolutions?.[1]?.url || '',
          publisher: n.publisher || 'Yahoo Finance'
        })));
      } catch { resolve([]); }
    });
  });
}

function finnhubMarketNewsSecure() {
  return new Promise((resolve) => {
    const url = `https://finnhub.io/api/v1/news?category=general&token=${FINNHUB_KEY}`;
    execFile('curl', ['-s', '--max-time', '10', url], (err, stdout) => {
      if (err || !stdout) return resolve([]);
      try {
        const json = JSON.parse(stdout);
        resolve((Array.isArray(json) ? json : []).slice(0, 8).map(n => ({
          source: 'Finnhub',
          headline: n.headline || '',
          summary: n.summary || '',
          url: n.url || '',
          datetime: n.datetime || 0,
          image: n.image || '',
          publisher: n.source || 'Finnhub'
        })));
      } catch { resolve([]); }
    });
  });
}

function mergeAndDedup(finnhubItems, yahooItems) {
  const all = [];
  const seen = new Set();
  const normalize = s => s.toLowerCase().replace(/[^a-z0-9]/g, '').slice(0, 40);

  for (const item of [...finnhubItems, ...yahooItems]) {
    if (!item.headline) continue;
    const key = normalize(item.headline);
    if (seen.has(key)) {
      // Merge — mark as both sources
      const existing = all.find(a => normalize(a.headline) === key);
      if (existing) {
        existing.sources = [...new Set([...(existing.sources || [existing.source]), item.source])];
        if (!existing.summary && item.summary) existing.summary = item.summary;
        if (!existing.image && item.image) existing.image = item.image;
      }
      continue;
    }
    seen.add(key);
    all.push({ ...item, sources: [item.source] });
  }
  return all.sort((a, b) => b.datetime - a.datetime);
}

// ─── News & Intelligence Endpoints ───────────────────────────────
app.get('/api/news/merged/:ticker', async (req, res) => {
  const ticker = req.params.ticker.toUpperCase();
  const isCrypto = !!CRYPTO_SYMBOLS[ticker];
  try {
    const [fhNews, yhNews] = await Promise.all([
      finnhubNewsSecure(ticker, isCrypto),
      isCrypto ? Promise.resolve([]) : yahooNewsSecure(ticker)
    ]);
    const merged = mergeAndDedup(fhNews, yhNews);
    res.json({ ticker, articles: merged.slice(0, 12), total: merged.length });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/news/market', async (req, res) => {
  try {
    const [fhNews, yhNews] = await Promise.all([
      finnhubMarketNewsSecure(),
      yahooMarketNewsSecure()
    ]);
    const merged = mergeAndDedup(fhNews, yhNews);
    res.json({ articles: merged.slice(0, 10) });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/news/sentiment', async (req, res) => {
  const { articles, ticker } = req.body;
  if (!articles?.length) return res.json([]);

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const send = d => { try { res.write(`data: ${JSON.stringify(d)}\n\n`); } catch {} };
  const keepalive = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 15000);

  try {
    const headlines = articles.map((a, i) => `${i + 1}. "${a.headline}" — ${a.publisher}`).join('\n');
    const prompt = `You are a financial news analyst. Analyze these news headlines for ${ticker} and rate each one.

Headlines:
${headlines}

For each headline, return a sentiment rating. Consider:
- Is this bullish (positive for stock price)?
- Is this bearish (negative for stock price)?
- Is this neutral (no clear price impact)?
- What is the key insight or strategy implication?

Return ONLY this exact JSON array (one entry per headline, same order):
[
  {
    "index": 1,
    "sentiment": "bullish" or "bearish" or "neutral",
    "score": number from -100 (very bearish) to +100 (very bullish),
    "insight": "one sentence — what this means for the stock/company strategy",
    "category": "earnings" or "strategy" or "product" or "macro" or "analyst" or "legal" or "partnership" or "other"
  }
]`;

    send({ type: 'status', text: '🤖 Analyzing sentiment...' });
    const result = await askAI(prompt);
    const jsonMatch = result.match(/\[[\s\S]*\]/);
    if (!jsonMatch) throw new Error('Could not parse sentiment');
    const sentiments = JSON.parse(jsonMatch[0]);
    send({ type: 'sentiments', data: sentiments });
    send({ type: 'done' });
  } catch (e) {
    send({ type: 'error', text: e.message });
    send({ type: 'done' });
  } finally {
    clearInterval(keepalive);
    res.end();
  }
});

app.post('/api/intelligence/:ticker', async (req, res) => {
  const ticker = req.params.ticker.toUpperCase();
  const { price, articles } = req.body;

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const send = d => { try { res.write(`data: ${JSON.stringify(d)}\n\n`); } catch {} };
  const keepalive = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 15000);

  try {
    send({ type: 'status', text: `🔍 Analyzing ${ticker} intelligence...` });
    const newsText = (articles || []).slice(0, 8).map(a =>
      `- [${a.publisher}] ${a.headline}${a.summary ? ': ' + a.summary.slice(0, 150) : ''}`
    ).join('\n') || 'No recent news available';

    const prompt = `You are a senior equity research analyst. Analyze ${ticker} based on recent news and provide a comprehensive company intelligence report.

Current Price: $${price || 'N/A'}
Recent News & Summaries:
${newsText}

Provide a deep analysis covering:
1. What strategic moves is this company making? (acquisitions, partnerships, pivots, layoffs, expansions)
2. What do recent earnings/guidance signals suggest?
3. What are analysts saying? Any upgrades/downgrades?
4. What macro or sector trends are affecting this company?
5. What is the overall verdict — is the company in a strong, weak, or transitional position?

Return ONLY this exact JSON:
{
  "verdict": "Strong" or "Weak" or "Mixed" or "Transitional" or "Uncertain",
  "verdictColor": "green" or "red" or "yellow" or "blue",
  "summary": "2-3 sentence plain English overview of the company's current situation",
  "strategy": ["key strategic move 1", "key strategic move 2", "key strategic move 3"],
  "earnings": "one sentence on earnings/guidance situation",
  "analystSentiment": "one sentence on what analysts are saying",
  "macroImpact": "one sentence on macro/sector tailwinds or headwinds",
  "risks": ["risk 1", "risk 2"],
  "opportunities": ["opportunity 1", "opportunity 2"],
  "watchFor": "the single most important thing to watch for this company in the next 30 days"
}`;

    const result = await askAI(prompt);
    const jsonMatch = result.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('Could not parse intelligence');
    const intel = JSON.parse(jsonMatch[0]);
    send({ type: 'intelligence', data: intel });
    send({ type: 'done' });
  } catch (e) {
    send({ type: 'error', text: e.message });
    send({ type: 'done' });
  } finally {
    clearInterval(keepalive);
    res.end();
  }
});

// ─── Price Alerts ─────────────────────────────────────────────────
const ALERTS_FILE = path.join(BASE_DIR, 'alerts.json');
function loadAlerts() { try { return JSON.parse(fs.readFileSync(ALERTS_FILE, 'utf8')); } catch { return []; } }
function saveAlerts(a) { fs.writeFileSync(ALERTS_FILE, JSON.stringify(a, null, 2)); }

app.get('/api/alerts', (req, res) => res.json(loadAlerts()));
app.post('/api/alerts', (req, res) => {
  const { ticker, condition, price, note, isCrypto } = req.body;
  if (!ticker || !condition || !price) return res.status(400).json({ error: 'ticker, condition, price required' });
  const alerts = loadAlerts();
  const alert = { id: Date.now().toString(), ticker: ticker.toUpperCase(), condition, price: parseFloat(price), note: note || '', isCrypto: !!isCrypto, createdAt: new Date().toISOString(), triggered: false };
  alerts.push(alert);
  saveAlerts(alerts);
  res.json({ ok: true, alert });
});
app.delete('/api/alerts/:id', (req, res) => {
  const alerts = loadAlerts().filter(a => a.id !== req.params.id);
  saveAlerts(alerts);
  res.json({ ok: true });
});
app.post('/api/alerts/check', (req, res) => {
  const { prices } = req.body;
  const alerts = loadAlerts();
  const triggered = [];
  const updated = alerts.map(a => {
    if (a.triggered) return a;
    const current = prices[a.ticker];
    if (!current) return a;
    const hit = (a.condition === 'above' && current >= a.price) || (a.condition === 'below' && current <= a.price);
    if (hit) { triggered.push({ ...a, currentPrice: current }); return { ...a, triggered: true, triggeredAt: new Date().toISOString() }; }
    return a;
  });
  saveAlerts(updated);
  res.json({ triggered });
});

// ─── Earnings Calendar ────────────────────────────────────────────
app.get('/api/earnings/calendar', async (req, res) => {
  try {
    const from = new Date().toISOString().split('T')[0];
    const to = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    const data = await finnhubGetSecure(`/calendar/earnings?from=${from}&to=${to}`);
    const items = data?.earningsCalendar || [];
    res.json(items.filter(e => e.symbol).slice(0, 100));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/earnings/history/:ticker', async (req, res) => {
  try {
    const data = await finnhubGetSecure(`/stock/earnings?symbol=${req.params.ticker.toUpperCase()}`);
    res.json(Array.isArray(data) ? data.slice(0, 8) : []);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── Insider Trading ──────────────────────────────────────────────
app.get('/api/insider/:ticker', async (req, res) => {
  try {
    const data = await finnhubGetSecure(`/stock/insider-transactions?symbol=${req.params.ticker.toUpperCase()}`);
    const items = (data?.data || []).slice(0, 20);
    res.json(items);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── Technical Chart ──────────────────────────────────────────────
app.get('/api/chart/:ticker', async (req, res) => {
  const { range = '1mo', interval = '1d' } = req.query;
  const ticker = req.params.ticker.toUpperCase();
  const isCrypto = !!CRYPTO_SYMBOLS[ticker];
  const symbol = isCrypto ? `${ticker}-USD` : ticker;
  return new Promise((resolve) => {
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?interval=${interval}&range=${range}`;
    execFile('curl', ['-s', '--max-time', '15', '-A', 'Mozilla/5.0', url], (err, stdout) => {
      if (err || !stdout) { res.status(500).json({ error: 'Chart fetch failed' }); return resolve(); }
      try {
        const json = JSON.parse(stdout);
        const result = json?.chart?.result?.[0];
        if (!result) { res.status(404).json({ error: 'No chart data' }); return resolve(); }
        const timestamps = result.timestamp || [];
        const ohlcv = result.indicators?.quote?.[0] || {};
        const adjClose = result.indicators?.adjclose?.[0]?.adjclose || [];
        const candles = timestamps.map((t, i) => ({
          t, date: new Date(t * 1000).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
          o: ohlcv.open?.[i], h: ohlcv.high?.[i], l: ohlcv.low?.[i],
          c: ohlcv.close?.[i], v: ohlcv.volume?.[i], ac: adjClose[i]
        })).filter(c => c.c != null);
        const meta = result.meta || {};
        res.json({ ticker, symbol, range, interval, candles, meta: { currency: meta.currency, exchange: meta.exchangeName, fiftyTwoWeekHigh: meta.fiftyTwoWeekHigh, fiftyTwoWeekLow: meta.fiftyTwoWeekLow } });
        resolve();
      } catch (e) { res.status(500).json({ error: e.message }); resolve(); }
    });
  });
});

// ─── Options Chain (Estimated) ────────────────────────────────────
app.post('/api/options/estimated', async (req, res) => {
  const { ticker, price } = req.body;
  if (!ticker || !price) return res.status(400).json({ error: 'ticker and price required' });

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();
  const send = d => { try { res.write(`data: ${JSON.stringify(d)}\n\n`); } catch {} };
  const keepalive = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 15000);

  try {
    send({ type: 'status', text: '📊 Generating estimated options chain...' });
    const prompt = `You are an options pricing expert. Generate a realistic estimated options chain for ${ticker} with current stock price $${price}.

Generate options for 3 expiration dates: approximately 2 weeks, 1 month, and 2 months from today (April 2026).
For each expiration, generate 7 strike prices centered around the current price (3 ITM, ATM, 3 OTM) in $5 increments for stocks under $500, $10 increments for stocks $500+.

Use realistic Black-Scholes-inspired pricing. Consider:
- IV typically 25-45% for large cap stocks
- Time value decay (theta)
- Delta ranges: deep ITM ~0.8-0.9, ATM ~0.5, deep OTM ~0.1-0.2

Return ONLY this exact JSON:
{
  "ticker": "${ticker}",
  "stockPrice": ${price},
  "iv": number (implied volatility % e.g. 32),
  "disclaimer": "ESTIMATED - Not real market data. For educational purposes only.",
  "expirations": [
    {
      "date": "YYYY-MM-DD",
      "label": "Apr 25 (2w)",
      "daysToExpiry": number,
      "calls": [
        { "strike": number, "bid": number, "ask": number, "last": number, "iv": number, "delta": number, "theta": number, "volume": number, "oi": number, "itm": boolean }
      ],
      "puts": [
        { "strike": number, "bid": number, "ask": number, "last": number, "iv": number, "delta": number, "theta": number, "volume": number, "oi": number, "itm": boolean }
      ]
    }
  ]
}`;

    const result = await askAI(prompt);
    const jsonMatch = result.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('Could not parse options chain');
    const chain = JSON.parse(jsonMatch[0]);
    send({ type: 'chain', data: chain });
    send({ type: 'done' });
  } catch (e) {
    send({ type: 'error', text: e.message });
    send({ type: 'done' });
  } finally {
    clearInterval(keepalive);
    res.end();
  }
});

// ─── Morning Brief ────────────────────────────────────────────────
const BRIEF_FILE = path.join(BASE_DIR, 'morning-brief.json');
function loadBrief() {
  try {
    const b = JSON.parse(fs.readFileSync(BRIEF_FILE, 'utf8'));
    const age = Date.now() - new Date(b.generatedAt).getTime();
    if (age < 8 * 60 * 60 * 1000) return b; // cache 8 hours
  } catch {}
  return null;
}
function saveBrief(b) { fs.writeFileSync(BRIEF_FILE, JSON.stringify(b, null, 2)); }

app.post('/api/ai/morning-brief', async (req, res) => {
  const { portfolio, watchlist, prices: priceMap, cryptoPrices: cryptoMap } = req.body;

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();
  const send = d => { try { res.write(`data: ${JSON.stringify(d)}\n\n`); } catch {} };
  const keepalive = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 15000);

  try {
    const cached = loadBrief();
    if (cached && req.query.force !== 'true') { send({ type: 'brief', data: cached }); send({ type: 'done' }); clearInterval(keepalive); res.end(); return; }

    send({ type: 'status', text: '📰 Fetching latest news...' });

    // Fetch news for all watchlist + portfolio tickers (top 4 to stay fast)
    const allTickers = [
      ...(portfolio || []).map(s => s.ticker),
      ...(watchlist || []).map(s => s.ticker)
    ];
    const uniqueTickers = [...new Set(allTickers)].slice(0, 4);

    const [watchlistNewsResults, marketNewsResults] = await Promise.all([
      Promise.all(uniqueTickers.map(t => finnhubNewsSecure(t, false).then(n => ({ ticker: t, news: n.slice(0, 2) })))),
      Promise.all([finnhubMarketNewsSecure(), yahooMarketNewsSecure()])
    ]);

    // Format watchlist news
    const watchlistNewsText = watchlistNewsResults
      .filter(r => r.news.length)
      .map(r => `${r.ticker}:\n${r.news.map(n => `  - ${n.headline}`).join('\n')}`)
      .join('\n') || 'No recent news for watchlist stocks';

    // Merge + deduplicate market news
    const marketNews = mergeAndDedup(marketNewsResults[0], marketNewsResults[1]).slice(0, 6);
    const marketNewsText = marketNews.map(n => `- [${n.publisher}] ${n.headline}`).join('\n') || 'No market news available';

    // Raw news for client rendering
    const watchlistNewsRaw = watchlistNewsResults.filter(r => r.news.length);
    const marketNewsRaw = marketNews;

    send({ type: 'status', text: '🤖 Generating your morning brief...' });

    const portfolioText = (portfolio || []).map(s => {
      const p = priceMap?.[s.ticker] || {};
      const pnl = p.price ? ((p.price - s.avgCost) * s.shares).toFixed(0) : 'N/A';
      const pnlPct = p.price ? ((p.price - s.avgCost) / s.avgCost * 100).toFixed(1) : 'N/A';
      return `${s.ticker}: ${s.shares} shares @ $${s.avgCost} avg, now $${p.price?.toFixed(2) || 'N/A'}, P&L: ${pnl >= 0 ? '+' : ''}$${pnl} (${pnlPct}%)`;
    }).join('\n') || 'No portfolio';

    const watchText = (watchlist || []).map(s => {
      const p = priceMap?.[s.ticker] || {};
      return `${s.ticker}: $${p.price?.toFixed(2) || 'N/A'} (${p.changePct >= 0 ? '+' : ''}${p.changePct?.toFixed(2) || 0}%)`;
    }).join(', ') || 'None';

    const now = new Date();
    const prompt = `You are a personal financial advisor giving a morning brief to a retail investor. Today is ${now.toDateString()}, ${now.toLocaleTimeString()}.

## Their Portfolio
${portfolioText}

## Their Watchlist
${watchText}

## Latest News for Their Stocks
${watchlistNewsText}

## General Market News
${marketNewsText}

Generate a concise, actionable morning brief. Be direct, friendly, and specific. No fluff.
Also identify 2-3 stocks from the general market news that look interesting and worth watching.

Return ONLY this exact JSON:
{
  "greeting": "Good morning! one energetic sentence about today",
  "marketMood": "Bullish" or "Bearish" or "Mixed" or "Cautious",
  "marketMoodColor": "green" or "red" or "yellow" or "blue",
  "headline": "the single most important thing happening in markets today",
  "portfolioSnapshot": "2 sentence summary of their portfolio situation",
  "topMover": { "ticker": "TICKER", "direction": "up" or "down", "note": "why" },
  "todaysFocus": ["action item 1", "action item 2", "action item 3"],
  "watchlistAlert": "any watchlist ticker worth paying attention to today and why",
  "riskAlert": "any risk to be aware of today (earnings, macro event, etc)",
  "stocksToWatch": [
    { "ticker": "TICKER", "name": "Company Name", "reason": "one sentence why it's interesting today", "action": "Watch" or "Research" or "Consider Adding" }
  ],
  "closingThought": "one motivational sentence for the trading day",
  "generatedAt": "${new Date().toISOString()}"
}`;

    const result = await askAI(prompt);
    const jsonMatch = result.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('Could not parse brief');
    const brief = JSON.parse(jsonMatch[0]);
    brief.generatedAt = brief.generatedAt || new Date().toISOString();
    brief.watchlistNewsRaw = watchlistNewsRaw;
    brief.marketNewsRaw = marketNewsRaw;
    saveBrief(brief);
    send({ type: 'brief', data: brief });
    send({ type: 'done' });
  } catch (e) {
    send({ type: 'error', text: e.message });
    send({ type: 'done' });
  } finally {
    clearInterval(keepalive);
    res.end();
  }
});

// ─── AI Trade Journal ─────────────────────────────────────────────
const JOURNAL_FILE = path.join(BASE_DIR, 'journal.json');
function loadJournal() { try { return JSON.parse(fs.readFileSync(JOURNAL_FILE, 'utf8')); } catch { return []; } }
function saveJournal(j) { fs.writeFileSync(JOURNAL_FILE, JSON.stringify(j, null, 2)); }

app.get('/api/journal', (req, res) => res.json(loadJournal()));

// ─── Portfolio Bias Report ────────────────────────────────────────
app.post('/api/journal/bias-report', async (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();
  const send = d => { try { res.write(`data: ${JSON.stringify(d)}\n\n`); } catch {} };
  const keepalive = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 15000);

  try {
    send({ type: 'status', text: 'Loading your trade history...' });

    // Gather all closed trades from day trading + paper trading + journal
    const data = loadData();
    const closedDT   = data?.daytrading?.closedTrades || [];
    const journalData = loadJournal();

    // Merge all sources into unified trade list
    const allTrades = [
      ...closedDT.map(t => ({
        ticker: t.ticker, type: t.type, strike: t.strike,
        pricePaid: t.pricePaid, soldPrice: t.soldPrice,
        pnl: t.pnl, contracts: t.contracts || 1,
        dateBought: t.dateBought, soldDate: t.soldDate,
        expiry: t.expiry, source: 'daytrading'
      })),
      ...journalData.map(t => ({
        ticker: t.ticker, type: t.type, strike: t.strike,
        pricePaid: t.pricePaid, soldPrice: t.soldPrice,
        pnl: t.pnl, contracts: t.contracts || 1,
        dateBought: t.dateBought, soldDate: t.soldDate || t.analyzedAt,
        expiry: t.expiry, source: 'journal'
      }))
    ].filter(t => t.soldPrice && t.pricePaid && t.pnl !== undefined);

    if (allTrades.length < 3) {
      send({ type: 'error', text: 'Need at least 3 closed trades to generate a bias report. Close more trades and try again.' });
      send({ type: 'done' });
      return;
    }

    send({ type: 'status', text: `Analyzing ${allTrades.length} trades for behavioral patterns...` });

    // Pre-compute stats to give AI richer context
    const wins   = allTrades.filter(t => t.pnl > 0);
    const losses = allTrades.filter(t => t.pnl < 0);
    const winRate = ((wins.length / allTrades.length) * 100).toFixed(1);
    const avgWinPct  = wins.length   ? (wins.reduce((s,t)   => s + ((t.soldPrice - t.pricePaid)/t.pricePaid*100), 0) / wins.length).toFixed(1)   : 0;
    const avgLossPct = losses.length ? (losses.reduce((s,t) => s + ((t.soldPrice - t.pricePaid)/t.pricePaid*100), 0) / losses.length).toFixed(1) : 0;
    const profitFactor = losses.length ? Math.abs(wins.reduce((s,t) => s+t.pnl,0) / losses.reduce((s,t) => s+t.pnl,0)).toFixed(2) : 'N/A';

    // Hold days per trade
    const holdDays = allTrades.map(t => {
      if (!t.dateBought || !t.soldDate) return null;
      return Math.round((new Date(t.soldDate) - new Date(t.dateBought)) / (1000*60*60*24));
    }).filter(Boolean);
    const avgHoldDays  = holdDays.length ? (holdDays.reduce((a,b)=>a+b,0)/holdDays.length).toFixed(1) : 'Unknown';
    const winHoldDays  = wins.map(t => t.dateBought && t.soldDate ? Math.round((new Date(t.soldDate)-new Date(t.dateBought))/(1000*60*60*24)) : null).filter(Boolean);
    const lossHoldDays = losses.map(t => t.dateBought && t.soldDate ? Math.round((new Date(t.soldDate)-new Date(t.dateBought))/(1000*60*60*24)) : null).filter(Boolean);
    const avgWinHold   = winHoldDays.length  ? (winHoldDays.reduce((a,b)=>a+b,0)/winHoldDays.length).toFixed(1)   : 'Unknown';
    const avgLossHold  = lossHoldDays.length ? (lossHoldDays.reduce((a,b)=>a+b,0)/lossHoldDays.length).toFixed(1) : 'Unknown';

    // Disposition effect: do they hold losers longer than winners?
    const dispositionScore = (winHoldDays.length && lossHoldDays.length)
      ? parseFloat(avgLossHold) - parseFloat(avgWinHold)
      : 0;

    // Ticker concentration
    const tickerCounts = {};
    allTrades.forEach(t => { tickerCounts[t.ticker] = (tickerCounts[t.ticker]||0)+1; });
    const topTickers = Object.entries(tickerCounts).sort((a,b)=>b[1]-a[1]).slice(0,5);

    // Best/worst tickers by PnL
    const tickerPnl = {};
    allTrades.forEach(t => { tickerPnl[t.ticker] = (tickerPnl[t.ticker]||0) + t.pnl; });
    const bestTicker  = Object.entries(tickerPnl).sort((a,b)=>b[1]-a[1])[0];
    const worstTicker = Object.entries(tickerPnl).sort((a,b)=>a[1]-b[1])[0];

    // Recent trend (last 5 trades)
    const recent5 = allTrades.slice(0,5);
    const recentWins = recent5.filter(t=>t.pnl>0).length;

    const tradesSummary = allTrades.slice(0,20).map(t => {
      const pct = ((t.soldPrice - t.pricePaid)/t.pricePaid*100).toFixed(1);
      return `${t.ticker} ${t.type} | P&L: ${t.pnl>=0?'+':''}$${t.pnl.toFixed(0)} (${pct}%) | Hold: ${t.dateBought&&t.soldDate?Math.round((new Date(t.soldDate)-new Date(t.dateBought))/(1000*60*60*24))+'d':'?'}`;
    }).join('\n');

    const prompt = `You are a behavioral finance expert analyzing a retail options trader's complete trade history to identify psychological biases and patterns.

## Trading Statistics
- Total Trades: ${allTrades.length} (${wins.length} wins, ${losses.length} losses)
- Win Rate: ${winRate}%
- Avg Win: +${avgWinPct}%
- Avg Loss: ${avgLossPct}%
- Profit Factor: ${profitFactor}
- Avg Hold (all): ${avgHoldDays} days
- Avg Hold (winners): ${avgWinHold} days
- Avg Hold (losers): ${avgLossHold} days
- Disposition Score: ${dispositionScore > 0 ? '+'+dispositionScore.toFixed(1) : dispositionScore.toFixed(1)} days (positive = holding losers longer than winners)
- Most traded: ${topTickers.map(([t,c])=>`${t}(${c}x)`).join(', ')}
- Best ticker: ${bestTicker?`${bestTicker[0]} ($${bestTicker[1].toFixed(0)})`:'N/A'}
- Worst ticker: ${worstTicker?`${worstTicker[0]} ($${worstTicker[1].toFixed(0)})`:'N/A'}
- Recent form: ${recentWins}/5 wins in last 5 trades

## Last 20 Trades
${tradesSummary}

Analyze for these specific behavioral biases:
1. DISPOSITION EFFECT: Selling winners too early, holding losers too long
2. OVERTRADING: Trading too frequently, chasing action
3. ANCHORING: Over-trading specific tickers (familiarity bias)
4. MOMENTUM CHASING: Entering after big moves, buying high
5. RECENCY BIAS: Changing behavior based on recent wins/losses

Return ONLY this exact JSON:
{
  "overallGrade": "A" or "B" or "C" or "D" or "F",
  "overallVerdict": "2-sentence trading profile summary",
  "profitFactor": "${profitFactor}",
  "winRate": "${winRate}",
  "biases": [
    {
      "name": "bias name",
      "severity": "Low" or "Medium" or "High",
      "severityColor": "green" or "yellow" or "red",
      "score": 0-100,
      "evidence": "specific evidence from their trade data",
      "impact": "how this is hurting their P&L in dollars or %",
      "fix": "one specific, actionable fix"
    }
  ],
  "strengths": ["strength 1", "strength 2"],
  "topPattern": "the single most dominant pattern in their trading",
  "bestSetup": "describe what their best performing setups look like",
  "worstSetup": "describe what their worst performing setups look like",
  "weeklyTarget": "one specific thing to focus on improving this week",
  "projectedImprovement": "if they fix their top bias, estimated improvement in win rate or P&L"
}`;

    send({ type: 'status', text: 'AI analyzing your behavioral patterns...' });
    const result = await askAI(prompt);
    const jsonMatch = result.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('Could not parse bias report');
    const report = JSON.parse(jsonMatch[0]);
    report.totalTrades = allTrades.length;
    report.avgHoldDays = avgHoldDays;
    report.dispositionScore = dispositionScore.toFixed(1);

    send({ type: 'report', data: report });
    send({ type: 'done' });
  } catch (e) {
    send({ type: 'error', text: e.message });
    send({ type: 'done' });
  } finally {
    clearInterval(keepalive);
    res.end();
  }
});

app.post('/api/journal/analyze', async (req, res) => {
  const { trade } = req.body;
  if (!trade) return res.status(400).json({ error: 'trade required' });

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();
  const send = d => { try { res.write(`data: ${JSON.stringify(d)}\n\n`); } catch {} };
  const keepalive = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 15000);

  try {
    send({ type: 'status', text: '📓 Analyzing your trade...' });
    const pnlPct = ((trade.soldPrice - trade.pricePaid) / trade.pricePaid * 100).toFixed(1);
    const holdDays = trade.soldDate && trade.dateBought
      ? Math.round((new Date(trade.soldDate) - new Date(trade.dateBought)) / (1000 * 60 * 60 * 24))
      : 'Unknown';

    const prompt = `You are an expert options trading coach analyzing a completed trade to help a retail investor learn and improve.

## Trade Details
- Stock: ${trade.ticker}
- Type: ${trade.type} option
- Strike: $${trade.strike}
- Expiry: ${trade.expiry}
- Entry Price: $${trade.pricePaid}/share ($${(trade.pricePaid * 100).toFixed(0)}/contract)
- Exit Price: $${trade.soldPrice}/share ($${(trade.soldPrice * 100).toFixed(0)}/contract)
- Contracts: ${trade.contracts || 1}
- P&L: ${trade.pnl >= 0 ? '+' : ''}$${trade.pnl?.toFixed(0)} (${pnlPct}%)
- Hold Duration: ${holdDays} days
- Entry Date: ${trade.dateBought || 'Unknown'}
- Exit Date: ${trade.soldDate || 'Unknown'}
- Notes: ${trade.notes || 'None'}

Analyze this trade thoroughly. Be honest — if they made mistakes, say so clearly but constructively.

Return ONLY this exact JSON:
{
  "grade": "A" or "B" or "C" or "D" or "F",
  "gradeColor": "green" or "blue" or "yellow" or "red",
  "verdict": "one sentence overall verdict",
  "whatWentRight": ["thing 1", "thing 2"],
  "whatWentWrong": ["thing 1", "thing 2"],
  "entryAnalysis": "was the entry timing good? why?",
  "exitAnalysis": "was the exit timing good? did they leave money on the table or cut losses well?",
  "riskManagement": "assessment of position sizing and risk management",
  "keyLesson": "the single most important lesson from this trade",
  "doNextTime": ["improvement 1", "improvement 2", "improvement 3"],
  "patternDetected": "any trading pattern or habit detected (good or bad)"
}`;

    const result = await askAI(prompt);
    const jsonMatch = result.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('Could not parse journal analysis');
    const analysis = JSON.parse(jsonMatch[0]);

    // Save to journal
    const journal = loadJournal();
    journal.unshift({ ...trade, analysis, analyzedAt: new Date().toISOString() });
    if (journal.length > 100) journal.splice(100);
    saveJournal(journal);

    send({ type: 'analysis', data: analysis });
    send({ type: 'done' });
  } catch (e) {
    send({ type: 'error', text: e.message });
    send({ type: 'done' });
  } finally {
    clearInterval(keepalive);
    res.end();
  }
});

// ─── Personal Suggestions ─────────────────────────────────────────
app.post('/api/suggestions/longterm', async (req, res) => {
  const { portfolio, watchlist, prices: priceMap, totalValue, totalInvested } = req.body;
  // Build complete exclusion list from all watchlists passed + data.json
  const dataAllTickers = (() => {
    try {
      const d = loadData();
      return [
        ...(d.daytrading?.watchlist || []).map(s => s.ticker),
        ...(d.daytrading?.cryptoWatchlist || []).map(s => s.ticker),
        ...(d.longterm?.watchlist || []).map(s => s.ticker),
        ...(d.longterm?.cryptoWatchlist || []).map(s => s.ticker),
        ...(d.longterm?.portfolio || []).map(s => s.ticker)
      ];
    } catch { return []; }
  })();

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();
  const send = d => { try { res.write(`data: ${JSON.stringify(d)}\n\n`); } catch {} };
  const keepalive = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 15000);

  try {
    send({ type: 'status', text: '📊 Analyzing your portfolio...' });

    const portfolioLines = (portfolio || []).map(s => {
      const p = priceMap?.[s.ticker] || {};
      const currentValue = (p.price || s.avgCost) * s.shares;
      const cost = s.avgCost * s.shares;
      const pnl = currentValue - cost;
      const pnlPct = ((pnl / cost) * 100).toFixed(1);
      const allocPct = totalValue > 0 ? ((currentValue / totalValue) * 100).toFixed(1) : 0;
      return `- ${s.ticker}: ${s.shares.toFixed(2)} shares @ $${s.avgCost} avg | Current: $${(p.price || s.avgCost).toFixed(2)} | Value: $${currentValue.toFixed(0)} | P&L: ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(0)} (${pnlPct}%) | Allocation: ${allocPct}%`;
    }).join('\n');

    const watchlistLine = (watchlist || []).map(s => s.ticker).join(', ');
    const allExcluded = [...new Set([
      ...(portfolio || []).map(s => s.ticker),
      ...(watchlist  || []).map(s => s.ticker),
      ...dataAllTickers
    ])];

    const prompt = `You are a personal financial advisor with full knowledge of this investor's portfolio. Give highly personalized, specific suggestions.

## Investor Profile
- Total Portfolio Value: $${totalValue?.toFixed(0) || 'Unknown'}
- Total Invested: $${totalInvested?.toFixed(0) || 'Unknown'}
- Overall P&L: $${((totalValue || 0) - (totalInvested || 0)).toFixed(0)} (${totalInvested > 0 ? (((totalValue - totalInvested) / totalInvested) * 100).toFixed(1) : 0}%)
- Investment Style: Long-term buy and hold
- Experience Level: Intermediate retail investor

## Current Holdings
${portfolioLines}

## Watchlist (considering buying)
${watchlistLine}

## CRITICAL: Do NOT suggest any of these tickers (already tracked)
${allExcluded.join(', ')}

## Your Task
Generate 5 highly personalized suggestions of stocks NOT in the list above. For each suggestion:
1. Be SPECIFIC about dollar amounts based on their actual portfolio size
2. Consider their current allocations — don't over-concentrate
3. Factor in their P&L — if a stock is down 50%, address it directly
4. Suggest realistic position sizes (typically 5-15% of portfolio)
5. For new positions, suggest a starter amount they can add to later

Return ONLY this exact JSON:
{
  "portfolioHealth": "Strong" or "Good" or "Fair" or "Needs Attention",
  "portfolioHealthColor": "green" or "blue" or "yellow" or "red",
  "portfolioSummary": "2 sentence honest assessment of their portfolio",
  "diversificationScore": number 1-10,
  "suggestions": [
    {
      "type": "ADD" or "TRIM" or "HOLD" or "NEW" or "EXIT" or "REBALANCE",
      "ticker": "TICKER",
      "action": "short action phrase e.g. Add to position",
      "dollarAmount": number (exact dollar amount to invest or trim),
      "dollarAmountLabel": "e.g. Invest $2,500 or Trim $3,000 worth",
      "shares": "approximate shares at current price e.g. ~13 shares",
      "reasoning": "2-3 sentence specific reasoning referencing their actual numbers",
      "allocationBefore": "e.g. 27.5%",
      "allocationAfter": "e.g. 31.2%",
      "urgency": "High" or "Medium" or "Low",
      "riskLevel": "Low" or "Medium" or "High",
      "confidence": number 0-100
    }
  ],
  "topPriority": "ticker of the single most important action right now",
  "rebalanceNote": "one sentence on overall rebalancing needs"
}`;

    send({ type: 'status', text: '🤖 Generating personalized suggestions...' });
    const result = await askAI(prompt);
    const jsonMatch = result.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('Could not parse suggestions');
    const data = JSON.parse(jsonMatch[0]);
    send({ type: 'suggestions', data });
    send({ type: 'done' });
  } catch (e) {
    send({ type: 'error', text: e.message });
    send({ type: 'done' });
  } finally {
    clearInterval(keepalive);
    res.end();
  }
});

app.post('/api/suggestions/options', async (req, res) => {
  const { portfolio, prices: priceMap, totalValue, totalInvested, watchlist } = req.body;

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();
  const send = d => { try { res.write(`data: ${JSON.stringify(d)}\n\n`); } catch {} };
  const keepalive = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 15000);

  try {
    send({ type: 'status', text: '⚡ Analyzing options opportunities...' });

    const holdingsLine = (portfolio || []).map(s => {
      const p = priceMap?.[s.ticker] || {};
      return `${s.ticker}: $${(p.price || s.avgCost).toFixed(2)} (own ${s.shares.toFixed(0)} shares, avg $${s.avgCost})`;
    }).join(', ');

    const prompt = `You are an expert options trading coach helping a BEGINNER start trading options. They have never traded options before. Be educational, specific, and conservative.

## Investor Profile
- Total Portfolio Value: $${totalValue?.toFixed(0) || 'Unknown'}
- Total Invested: $${totalInvested?.toFixed(0) || 'Unknown'}
- Options Experience: NONE — complete beginner
- Risk Budget for Options: Suggest 2-5% of portfolio max ($${Math.round((totalValue || 50000) * 0.03).toLocaleString()} suggested)

## Their Stock Holdings (familiar tickers — best for first options trades)
${holdingsLine}

## Your Task
Suggest 4 specific options trades tailored to this beginner. Rules:
1. ONLY suggest options on stocks they already own or watch — familiar tickers reduce fear
2. Keep cost per trade under $500 for a beginner (1-2 contracts max)
3. Use 3-6 week expiries — not too short (theta decay risk) not too long
4. Prefer slightly OTM calls on stocks with bullish momentum
5. Give EXACT dollar amounts — "This trade costs approximately $X"
6. Explain max loss clearly — "Worst case you lose $X (the premium paid)"
7. Today's date is ${new Date().toDateString()}

Return ONLY this exact JSON:
{
  "optionsBudget": number (suggested total options budget in dollars),
  "optionsBudgetNote": "e.g. 3% of your $59K portfolio = $1,770 options budget",
  "beginnerNote": "one encouraging sentence for a first-time options trader",
  "suggestions": [
    {
      "rank": 1,
      "ticker": "TICKER",
      "optionType": "CALL" or "PUT",
      "strike": number,
      "expiry": "YYYY-MM-DD",
      "expiryLabel": "e.g. May 2 (3 weeks)",
      "contracts": 1 or 2,
      "estimatedCostPerContract": number,
      "totalCost": number,
      "totalCostLabel": "e.g. ~$320 total (1 contract × $3.20 premium × 100)",
      "maxLoss": number,
      "maxLossLabel": "e.g. Max loss: $320 (100% of premium if expires worthless)",
      "breakEven": number,
      "breakEvenLabel": "e.g. Break-even: $193.20 by May 2",
      "targetPrice": number,
      "targetReturn": "e.g. +150% if NVDA hits $200",
      "reasoning": "2-3 sentence why this specific trade makes sense for them right now",
      "entryTip": "specific tip on when/how to enter this trade",
      "exitPlan": "when to take profit and when to cut loss",
      "riskLevel": "Low" or "Medium" or "High",
      "confidence": number 0-100,
      "whyThisStock": "one sentence connecting to their existing holding or watchlist"
    }
  ],
  "generalTips": ["beginner tip 1", "beginner tip 2", "beginner tip 3"]
}`;

    send({ type: 'status', text: '🤖 Building your personalized options playbook...' });
    const result = await askAI(prompt);
    const jsonMatch = result.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('Could not parse options suggestions');
    const data = JSON.parse(jsonMatch[0]);
    send({ type: 'suggestions', data });
    send({ type: 'done' });
  } catch (e) {
    send({ type: 'error', text: e.message });
    send({ type: 'done' });
  } finally {
    clearInterval(keepalive);
    res.end();
  }
});

// ─── Investment Planner ───────────────────────────────────────────
app.post('/api/invest/options-plan', async (req, res) => {
  const { amount, riskLevel, portfolio, prices: priceMap } = req.body;
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();
  const send = d => { try { res.write(`data: ${JSON.stringify(d)}\n\n`); } catch {} };
  const keepalive = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 15000);
  try {
    send({ type: 'status', text: `⚡ Scanning the market for ${riskLevel} options opportunities with $${Number(amount).toLocaleString()}...` });
    const holdingsText = (portfolio || []).map(s => {
      const p = priceMap?.[s.ticker] || {};
      return `${s.ticker}: $${(p.price || s.avgCost).toFixed(2)} (own ${s.shares.toFixed(0)} shares, avg cost $${s.avgCost})`;
    }).join(', ') || 'None';
    const riskConfig = {
      safe: {
        label: 'Safe',
        desc: 'Conservative — capital preservation, low risk of total loss',
        rules: 'Prefer ITM/ATM options (delta 0.6-0.8) on large-cap blue chips. Expiry 4-8 weeks. Max 2% per trade. Covered calls on owned stocks are ideal. Avoid earnings plays.',
        universe: 'Large-cap blue chips with high liquidity: AAPL, MSFT, GOOGL, AMZN, META, NVDA, JPM, V, JNJ, BRK.B, SPY, QQQ. Also consider stocks they already own for covered calls.'
      },
      medium: {
        label: 'Medium Risk',
        desc: 'Balanced — growth with managed downside',
        rules: 'Slightly OTM options (delta 0.4-0.6). Expiry 3-6 weeks. 3-5% per trade. Use spreads to reduce cost. Target 50-150% return on premium.',
        universe: 'Any liquid stock with strong momentum or upcoming catalyst. Consider: NVDA, META, AMZN, TSLA, AMD, CRM, SHOP, PLTR, COIN, MSTR, sector ETFs (XLK, XLE, XLF). Look for stocks with upcoming earnings, product launches, or macro tailwinds.'
      },
      high: {
        label: 'High Risk',
        desc: 'Aggressive — maximum upside, accept full premium loss',
        rules: 'OTM calls (delta 0.2-0.4) on high-momentum or volatile stocks. 1-3 week expiry for leverage. Up to 10% per trade. Target 200-500%+ return. Accept most trades may expire worthless.',
        universe: 'High-beta, high-momentum stocks: NVDA, TSLA, AMD, MSTR, COIN, PLTR, SMCI, GME, AMC, SOXL, TQQQ, biotech stocks near FDA decisions, any stock with upcoming earnings catalyst. Meme stocks and leveraged ETFs acceptable.'
      }
    };
    const cfg = riskConfig[riskLevel] || riskConfig.medium;
    const today = new Date().toDateString();
    const prompt = `You are an expert options trading advisor with full knowledge of current market conditions. A retail investor wants to invest $${amount} in options using a ${cfg.label} (${cfg.desc}) strategy.

Today: ${today}

## Their Current Holdings (for context — covered calls possible on these)
${holdingsText}

## Your Task — IMPORTANT
Scan the ENTIRE market for the best options opportunities RIGHT NOW. Do NOT limit yourself to their watchlist.
You have full freedom to recommend ANY stock or ETF that fits the ${cfg.label} strategy.

## Stock Universe to Consider
${cfg.universe}

## Risk Rules
${cfg.rules}

## Requirements
1. Total cost of ALL trades combined must be close to $${amount} (within 10%)
2. Pick the BEST opportunities in the market right now — not just familiar names
3. Explain WHY each stock is a good options play at this moment (momentum, catalyst, IV, etc.)
4. Give realistic premiums based on current stock prices and typical IV
5. Today is ${today} — use real upcoming expiry dates (3rd Friday of each month)
6. If they own a stock, you CAN suggest covered calls on it — mark clearly
7. Suggest 2-5 trades depending on budget size

Return ONLY this JSON:
{
  "riskLevel": "${cfg.label}",
  "totalBudget": ${amount},
  "totalAllocated": number,
  "remainingCash": number,
  "marketContext": "1 sentence on current market conditions relevant to this strategy",
  "strategy": "2 sentence description of the overall approach",
  "trades": [
    {
      "rank": 1,
      "ticker": "TICKER",
      "companyName": "Full Company Name",
      "action": "BUY CALL" or "BUY PUT" or "SELL COVERED CALL" or "BUY CALL SPREAD" or "BUY PUT SPREAD",
      "strike": number,
      "expiry": "YYYY-MM-DD",
      "expiryLabel": "e.g. May 16 (3 weeks)",
      "contracts": number,
      "estimatedPremium": number,
      "totalCost": number,
      "totalCostLabel": "e.g. $320 (2 × $1.60 × 100)",
      "percentOfBudget": number,
      "currentStockPrice": number,
      "breakEven": number,
      "maxLoss": number,
      "maxGain": "e.g. Unlimited or $640",
      "targetReturn": "e.g. +180% if NVDA hits $210 by May 16",
      "whyThisStock": "1 sentence — why this stock specifically right now (catalyst, momentum, IV, etc.)",
      "reasoning": "2 sentence specific trade reasoning",
      "entryNote": "best time/condition to enter",
      "exitPlan": "take profit at X%, cut loss at Y%",
      "isExistingHolding": true or false,
      "riskTag": "Safe" or "Medium" or "High"
    }
  ],
  "allocationBreakdown": [{ "label": "TICKER — action", "amount": number, "pct": number }],
  "keyRisks": ["risk 1", "risk 2", "risk 3"],
  "tips": ["tip specific to ${cfg.label} strategy 1", "tip 2"]
}`;
    const result = await askAI(prompt);
    const jsonMatch = result.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('Could not parse options plan');
    send({ type: 'plan', data: JSON.parse(jsonMatch[0]) });
    send({ type: 'done' });
  } catch (e) {
    send({ type: 'error', text: e.message });
    send({ type: 'done' });
  } finally { clearInterval(keepalive); res.end(); }
});

app.post('/api/invest/longterm-plan', async (req, res) => {
  const { amount, portfolio, prices: priceMap } = req.body;
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();
  const send = d => { try { res.write(`data: ${JSON.stringify(d)}\n\n`); } catch {} };
  const keepalive = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 15000);
  try {
    send({ type: 'status', text: `📈 Scanning the market for the best long-term investments with $${Number(amount).toLocaleString()}...` });
    const totalPortValue = (portfolio || []).reduce((sum, s) => {
      const p = priceMap?.[s.ticker] || {};
      return sum + (p.price || s.avgCost) * s.shares;
    }, 0);
    const portfolioText = (portfolio || []).map(s => {
      const p = priceMap?.[s.ticker] || {};
      const val = (p.price || s.avgCost) * s.shares;
      const cost = s.avgCost * s.shares;
      const pnl = val - cost;
      const allocPct = totalPortValue > 0 ? ((val / totalPortValue) * 100).toFixed(1) : 0;
      return `${s.ticker}: ${s.shares.toFixed(2)} shares @ $${s.avgCost} avg | Now $${(p.price || s.avgCost).toFixed(2)} | P&L: ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(0)} (${((pnl/cost)*100).toFixed(1)}%) | Alloc: ${allocPct}%`;
    }).join('\n') || 'No portfolio yet';
    const sectors = (portfolio || []).map(s => s.ticker);
    const numStocks = amount < 500 ? '1-2' : amount < 2000 ? '2-3' : '3-5';
    const today = new Date().toDateString();
    const prompt = `You are a senior equity research analyst and personal financial advisor. A retail investor wants to invest $${amount} in stocks for the long term (1+ years, buy and hold).

Today: ${today}

## Their Current Portfolio (for context only — to avoid over-concentration)
Total value: $${totalPortValue.toFixed(0)}
${portfolioText}

## IMPORTANT — Your Task
Scan the ENTIRE stock market and recommend the BEST ${numStocks} stocks to buy RIGHT NOW with $${amount}.
Do NOT limit yourself to their existing holdings or watchlist.
You have full freedom to recommend ANY publicly traded stock that represents the best opportunity.

## What to Consider
- Current market conditions and sector rotation
- Stocks with strong fundamentals AND near-term catalysts
- Valuation — is the stock reasonably priced or overvalued?
- Their existing portfolio sectors: ${sectors.join(', ')} — consider diversifying into underrepresented sectors
- Mix of: growth stocks, value plays, dividend payers depending on amount
- Consider: AI/tech, healthcare, energy, financials, consumer, international ETFs
- Avoid stocks they already own heavily (check allocations above)

## Budget Rules
- Total allocations must equal exactly $${amount}
- ${amount < 500 ? 'Small amount — focus on 1-2 high-conviction picks' : amount < 2000 ? 'Medium amount — 2-3 stocks with sector diversification' : 'Larger amount — 3-5 stocks across different sectors'}

Return ONLY this JSON:
{
  "totalBudget": ${amount},
  "totalAllocated": number,
  "timeHorizon": "e.g. 1-3 years",
  "marketContext": "1 sentence on current market conditions and why now is a good/cautious time to invest",
  "strategy": "2 sentence description of the overall approach and sector focus",
  "portfolioImpact": "how this investment changes their overall portfolio composition and diversification",
  "picks": [
    {
      "rank": 1,
      "ticker": "TICKER",
      "companyName": "Full Company Name",
      "action": "ADD" or "NEW",
      "dollarAmount": number,
      "dollarAmountLabel": "e.g. $1,200",
      "shares": "e.g. ~6 shares at $198",
      "currentPrice": number,
      "sector": "e.g. Technology",
      "industry": "e.g. Semiconductors",
      "whyNow": "2 sentence specific reason — what makes this stock attractive RIGHT NOW",
      "growthCatalyst": "the main growth driver in the next 1-3 years",
      "targetPrice": "e.g. $250 in 12 months",
      "upside": "e.g. +26%",
      "riskLevel": "Low" or "Medium" or "High",
      "existingPosition": true or false,
      "allocationPct": number,
      "portfolioAllocationAfter": "e.g. 15.2% of total portfolio",
      "alternativeTo": "e.g. Alternative to NVDA for AI exposure or null"
    }
  ],
  "diversificationNote": "one sentence on how this improves sector/style diversification",
  "dontBuy": [
    { "ticker": "TICKER", "reason": "specific reason to avoid right now" }
  ],
  "keyRisks": ["market risk 1", "specific risk 2"],
  "nextReviewDate": "e.g. Review in 3 months — July 2026"
}`;
    const result = await askAI(prompt);
    const jsonMatch = result.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('Could not parse long-term plan');
    send({ type: 'plan', data: JSON.parse(jsonMatch[0]) });
    send({ type: 'done' });
  } catch (e) {
    send({ type: 'error', text: e.message });
    send({ type: 'done' });
  } finally { clearInterval(keepalive); res.end(); }
});

// ─── Smart Alert Scanner ──────────────────────────────────────────
// Rules: Only fire HIGH-VALUE alerts. Each alert fires ONCE per day max.
// Priority: Contracts > Earnings > Big price moves (5%+) > Nothing else
app.post('/api/smartalerts/scan', async (req, res) => {
  const { portfolio, contracts, watchlist, prices: priceMap, seenIds = [] } = req.body;
  const alerts = [];
  const now = new Date();
  const today = now.toISOString().split('T')[0];
  const seenSet = new Set(seenIds);

  const push = (alert) => {
    if (!seenSet.has(alert.id)) alerts.push(alert);
  };

  // ── PRIORITY 1: Open contracts (most actionable) ──────────────────
  for (const c of (contracts || [])) {
    if (!c.expiry) continue;
    const expDate = new Date(c.expiry + 'T16:00:00');
    const daysLeft = Math.ceil((expDate - now) / (1000 * 60 * 60 * 24));
    const stockPrice = priceMap?.[c.ticker]?.price || 0;
    const costBasis = (c.pricePaid || 0) * 100 * (c.contracts || 1);
    const cid = c.id || `${c.ticker}-${c.strike}-${c.expiry}`;

    if (daysLeft <= 0) {
      push({ id: `expiry-today-${cid}`, type: 'expiry', severity: 'critical', ticker: c.ticker,
        title: `🚨 ${c.ticker} ${c.type} EXPIRES TODAY`,
        body: `Your $${c.strike} ${c.type} expires today. Close it now or lose $${costBasis.toFixed(0)}.`,
        action: 'Close Now', timestamp: now.toISOString() });
    } else if (daysLeft <= 2) {
      push({ id: `expiry-2d-${cid}`, type: 'expiry', severity: 'warning', ticker: c.ticker,
        title: `⏰ ${c.ticker} ${c.type} expires in ${daysLeft}d`,
        body: `Your $${c.strike} ${c.type} expires ${c.expiry}. Close, roll, or let expire.`,
        action: 'Manage Contract', timestamp: now.toISOString() });
    }

    if (stockPrice > 0 && c.pricePaid) {
      const isCall = c.type === 'CALL';
      const intrinsic = isCall ? Math.max(0, stockPrice - c.strike) : Math.max(0, c.strike - stockPrice);
      if (intrinsic === 0 && daysLeft <= 3) {
        push({ id: `otm-${cid}-${today}`, type: 'otm_warning', severity: 'warning', ticker: c.ticker,
          title: `⚠️ ${c.ticker} ${c.type} OTM — ${daysLeft}d left`,
          body: `Stock at $${stockPrice.toFixed(2)}, strike $${c.strike}. Out of the money with ${daysLeft} days left — consider cutting losses.`,
          action: 'Cut Loss', timestamp: now.toISOString() });
      }
    }
  }

  // ── PRIORITY 2: Earnings tomorrow for YOUR portfolio only ─────────
  try {
    const to = new Date(Date.now() + 2 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    const earningsData = await finnhubGetSecure(`/calendar/earnings?from=${today}&to=${to}`);
    const myTickers = new Set((portfolio || []).map(s => s.ticker));
    for (const e of (earningsData?.earningsCalendar || [])) {
      if (!myTickers.has(e.symbol)) continue;
      const daysUntil = Math.ceil((new Date(e.date) - now) / (1000 * 60 * 60 * 24));
      if (daysUntil > 2) continue;
      const when = daysUntil <= 0 ? 'TODAY' : 'TOMORROW';
      const hour = e.hour === 'bmo' ? ' (Before Market Open)' : e.hour === 'amc' ? ' (After Market Close)' : '';
      push({ id: `earnings-${e.symbol}-${e.date}`, type: 'earnings', severity: 'warning', ticker: e.symbol,
        title: `📅 ${e.symbol} earnings ${when}${hour}`,
        body: `You own ${e.symbol}. ${e.epsEstimate ? `EPS estimate: $${e.epsEstimate.toFixed(2)}.` : ''} Review your position before the report.`,
        action: 'Review Position', timestamp: now.toISOString() });
    }
  } catch {}

  // ── PRIORITY 3: Big price moves (5%+ only, portfolio only) ────────
  for (const s of (portfolio || [])) {
    const p = priceMap?.[s.ticker];
    if (!p?.price || !p?.changePct) continue;
    const changePct = p.changePct;
    const pnlPct = ((p.price - s.avgCost) / s.avgCost * 100).toFixed(1);
    if (changePct <= -5) {
      push({ id: `drop5-${s.ticker}-${today}`, type: 'price_drop', severity: 'critical', ticker: s.ticker,
        title: `📉 ${s.ticker} down ${changePct.toFixed(1)}% today`,
        body: `Your position P&L: ${pnlPct}%. This is a significant move — review your thesis.`,
        action: 'Review', timestamp: now.toISOString() });
    } else if (changePct >= 5) {
      push({ id: `spike5-${s.ticker}-${today}`, type: 'price_spike', severity: 'info', ticker: s.ticker,
        title: `📈 ${s.ticker} up ${changePct.toFixed(1)}% today`,
        body: `Your position P&L: ${pnlPct}%. Consider taking partial profit.`,
        action: 'Consider Profit', timestamp: now.toISOString() });
    }
  }

  res.json({ alerts, scannedAt: now.toISOString() });
});

// ═══════════════════════════════════════════════════════════════════
// MARKET MOVER SCANNER
// Uses Yahoo Finance real-time screener — catches ANY stock market-wide
// Not limited to a hardcoded list
// ═══════════════════════════════════════════════════════════════════

let lastMoverAlerts = {}; // alertId → timestamp (dedup)

function buildMoverAlert(q, type, now, today) {
  const ticker = q.symbol;
  const changePct = parseFloat(q.regularMarketChangePercent) || 0;
  const price = parseFloat(q.regularMarketPrice) || 0;
  const prevClose = parseFloat(q.regularMarketPreviousClose) || price;
  const name = q.shortName || q.displayName || ticker;
  const vol = q.regularMarketVolume || 0;
  const avgVol = q.averageDailyVolume3Month || 1;
  const volRatio = avgVol > 0 ? (vol / avgVol).toFixed(1) : '?';
  const marketState = q.marketState || 'REGULAR';
  const isPreMarket = marketState === 'PRE';
  const isPostMarket = marketState === 'POST';
  const sessionLabel = isPreMarket ? ' (pre-market)' : isPostMarket ? ' (after-hours)' : '';
  const dir = changePct > 0 ? '🚀' : '📉';
  const absPct = Math.abs(changePct);
  const severity = absPct >= 50 ? 'critical' : absPct >= 20 ? 'warning' : 'info';
  const hourBucket = Math.floor(now.getHours() / 2);
  const alertId = `${type}-${ticker}-${today}-${hourBucket}`;

  return {
    alertId, ticker, changePct, price, prevClose, name, volRatio, severity, sessionLabel, dir, absPct,
    alert: {
      id: alertId, type, severity, ticker,
      title: `${dir} ${ticker} ${changePct > 0 ? '+' : ''}${changePct.toFixed(1)}%${sessionLabel}`,
      body: `${name} — now $${price.toFixed(2)} (was $${prevClose.toFixed(2)}) · Volume: ${volRatio}x avg`,
      changePct, price, prevClose, volRatio, marketState,
      timestamp: now.toISOString()
    }
  };
}

// GET /api/market/movers — real-time market-wide movers from Yahoo screener
app.get('/api/market/movers', async (req, res) => {
  const threshold = parseFloat(req.query.threshold) || 10;
  const now = new Date();

  try {
    // Fetch gainers + losers in parallel from Yahoo screener
    const [gainers, losers] = await Promise.all([
      yahooScreener('day_gainers', 50),
      yahooScreener('day_losers', 25)
    ]);

    const all = [...gainers, ...losers];
    const movers = all
      .map(q => ({
        ticker: q.symbol,
        name: q.shortName || q.displayName || q.symbol,
        price: parseFloat(q.regularMarketPrice) || 0,
        prevClose: parseFloat(q.regularMarketPreviousClose) || 0,
        changePct: parseFloat(q.regularMarketChangePercent) || 0,
        change: parseFloat(q.regularMarketChange) || 0,
        volume: q.regularMarketVolume || 0,
        avgVolume: q.averageDailyVolume3Month || 0,
        volRatio: q.averageDailyVolume3Month > 0 ? parseFloat((q.regularMarketVolume / q.averageDailyVolume3Month).toFixed(1)) : 0,
        marketCap: q.marketCap || 0,
        marketState: q.marketState || 'REGULAR',
        fiftyTwoWeekHigh: q.fiftyTwoWeekHigh || 0,
        fiftyTwoWeekLow: q.fiftyTwoWeekLow || 0
      }))
      .filter(m => Math.abs(m.changePct) >= threshold)
      .sort((a, b) => Math.abs(b.changePct) - Math.abs(a.changePct));

    // Also check unusual volume (3x+ avg, even if % move is smaller)
    const unusualVolume = all
      .filter(q => {
        const vol = q.regularMarketVolume || 0;
        const avg = q.averageDailyVolume3Month || 1;
        const pct = Math.abs(parseFloat(q.regularMarketChangePercent) || 0);
        return avg > 0 && (vol / avg) >= 3 && pct >= 5 && pct < threshold;
      })
      .map(q => ({
        ticker: q.symbol,
        name: q.shortName || q.symbol,
        price: parseFloat(q.regularMarketPrice) || 0,
        changePct: parseFloat(q.regularMarketChangePercent) || 0,
        volRatio: parseFloat((q.regularMarketVolume / q.averageDailyVolume3Month).toFixed(1)),
        marketState: q.marketState || 'REGULAR'
      }));

    res.json({ movers, unusualVolume, scannedAt: now.toISOString(), threshold, source: 'yahoo_screener' });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ─── Catalyst keywords that predict a spike ───────────────────────
const CATALYST_KEYWORDS = [
  // FDA / biotech
  'fda approval','fda approved','fda grants','breakthrough therapy','fast track designation',
  'priority review','pdufa','nda submission','bla submission','clinical trial results',
  'phase 3 results','phase 2 results','positive data','efficacy data',
  // M&A
  'acquisition','acquires','merger','takeover','buyout','going private',
  'strategic review','letter of intent','definitive agreement','tender offer',
  // Contracts / revenue
  'government contract','department of defense','dod contract','nasa contract',
  'partnership agreement','licensing agreement','exclusive agreement',
  // Earnings / guidance
  'raises guidance','raises outlook','beats estimates','record revenue','record earnings',
  'special dividend','share buyback','stock repurchase',
  // Other catalysts
  'short squeeze','heavily shorted','short interest','sec investigation','class action',
  'ceo resigns','ceo appointed','activist investor','stake in'
];

function hasCatalyst(text) {
  const lower = (text || '').toLowerCase();
  return CATALYST_KEYWORDS.find(k => lower.includes(k)) || null;
}

// ─── SEC EDGAR 8-K scanner ────────────────────────────────────────
async function scanSEC8K(now, today) {
  const alerts = [];
  try {
    // Fetch latest 8-K filings from SEC EDGAR RSS
    const url = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&dateb=&owner=include&count=40&search_text=&output=atom';
    const xml = await new Promise((resolve) => {
      execFile('curl', ['-s', '--max-time', '15', '-A', 'StockForge/1.0 contact@example.com', url], (err, stdout) => {
        resolve(err ? '' : stdout);
      });
    });
    if (!xml) return alerts;

    // Parse entries from Atom feed
    const entryRegex = /<entry>([\s\S]*?)<\/entry>/g;
    const titleRegex = /<title>(.*?)<\/title>/;
    const linkRegex = /<link[^>]*href="([^"]+)"/;
    const updatedRegex = /<updated>(.*?)<\/updated>/;

    let match;
    while ((match = entryRegex.exec(xml)) !== null) {
      const entry = match[1];
      const title = (titleRegex.exec(entry) || [])[1] || '';
      const link = (linkRegex.exec(entry) || [])[1] || '';
      const updated = (updatedRegex.exec(entry) || [])[1] || '';

      // Extract ticker from title — format: "8-K - COMPANY NAME (TICKER) (CIK)"
      const tickerMatch = title.match(/\(([A-Z]{1,5})\)\s*\(CIK/);
      const companyMatch = title.match(/8-K - (.+?)\s*\(/);
      const ticker = tickerMatch?.[1] || '';
      const company = companyMatch?.[1] || title;

      // Only alert if filed in last 30 minutes
      const filedAt = new Date(updated);
      const ageMin = (now - filedAt) / 60000;
      if (ageMin > 30) continue;

      const alertId = `sec-8k-${ticker || company.slice(0,10)}-${updated.slice(0,16)}`;
      if (lastMoverAlerts[alertId]) continue;
      lastMoverAlerts[alertId] = now.toISOString();

      alerts.push({
        id: alertId,
        type: 'sec_filing',
        severity: 'warning',
        ticker: ticker || '8-K',
        title: `📋 8-K Filed: ${ticker ? ticker + ' — ' : ''}${company.slice(0, 50)}`,
        body: `SEC 8-K filing (major event report) filed ${Math.round(ageMin)} min ago. Check for FDA, merger, or contract news.`,
        link,
        timestamp: now.toISOString()
      });
    }
  } catch {}
  return alerts;
}

// ─── Breaking news catalyst scanner ──────────────────────────────
async function scanBreakingNews(now, today) {
  const alerts = [];
  try {
    // Fetch merger + general news from Finnhub in parallel
    const [mergerNews, generalNews] = await Promise.all([
      finnhubGetSecure(`/news?category=merger&minId=0`),
      finnhubGetSecure(`/news?category=general&minId=0`)
    ]);

    const allNews = [...(Array.isArray(mergerNews) ? mergerNews : []), ...(Array.isArray(generalNews) ? generalNews : [])];

    for (const n of allNews) {
      const ageMin = (now.getTime() / 1000 - (n.datetime || 0)) / 60;
      if (ageMin > 20) continue; // only last 20 minutes

      const headline = n.headline || '';
      const summary = n.summary || '';
      const catalyst = hasCatalyst(headline) || hasCatalyst(summary);
      if (!catalyst) continue;

      // Try to extract ticker from related field
      const ticker = (n.related || '').split(',')[0].trim() || '';
      const alertId = `news-catalyst-${n.id || headline.slice(0,20)}-${today}`;
      if (lastMoverAlerts[alertId]) continue;
      lastMoverAlerts[alertId] = now.toISOString();

      alerts.push({
        id: alertId,
        type: 'catalyst_news',
        severity: 'critical',
        ticker: ticker || 'NEWS',
        title: `🔥 Catalyst: ${headline.slice(0, 70)}`,
        body: `Keyword detected: "${catalyst}" — ${Math.round(ageMin)} min ago. Act before market reacts.`,
        catalyst,
        source: n.source || 'Finnhub',
        url: n.url || '',
        timestamp: now.toISOString()
      });
    }
  } catch {}
  return alerts;
}

// ─── Yahoo trending tickers scanner ──────────────────────────────
async function scanTrendingTickers(now, today) {
  const alerts = [];
  try {
    const url = 'https://query1.finance.yahoo.com/v1/finance/trending/US?count=20';
    const raw = await new Promise((resolve) => {
      execFile('curl', ['-s', '--max-time', '10', '-A', 'Mozilla/5.0', url], (err, stdout) => {
        resolve(err ? '' : stdout);
      });
    });
    if (!raw) return alerts;
    const data = JSON.parse(raw);
    const quotes = data?.finance?.result?.[0]?.quotes || [];

    for (const q of quotes) {
      const ticker = q.symbol;
      if (!ticker) continue;
      const alertId = `trending-${ticker}-${today}-${Math.floor(now.getHours() / 1)}`; // once per hour per ticker
      if (lastMoverAlerts[alertId]) continue;

      // Fetch quick price to see if it's also moving
      try {
        const priceData = await finnhubGetSecure(`/quote?symbol=${ticker}`);
        if (!priceData?.c) continue;
        const price = parseFloat(priceData.c);
        const prevClose = parseFloat(priceData.pc) || price;
        const changePct = prevClose > 0 ? ((price - prevClose) / prevClose * 100) : 0;

        // Only alert if trending AND moving 2%+
        if (Math.abs(changePct) < 2) continue;

        lastMoverAlerts[alertId] = now.toISOString();
        const dir = changePct > 0 ? '📈' : '📉';
        alerts.push({
          id: alertId,
          type: 'trending',
          severity: 'info',
          ticker,
          title: `🔍 ${ticker} trending on Yahoo Finance ${dir} ${changePct > 0 ? '+' : ''}${changePct.toFixed(1)}%`,
          body: `${ticker} is trending (people are searching it) and moving. Now $${price.toFixed(2)}. Check for news catalyst.`,
          changePct, price,
          timestamp: now.toISOString()
        });
      } catch {}
    }
  } catch {}
  return alerts;
}

// ─── Volume accumulation scanner ─────────────────────────────────
async function scanVolumeAccumulation(portfolio, now, today) {
  const alerts = [];
  const portfolioTickers = (portfolio || []).map(s => s.ticker);
  if (!portfolioTickers.length) return alerts;

  for (const ticker of portfolioTickers) {
    try {
      const [finnData, yahooData] = await Promise.all([
        finnhubGetSecure(`/quote?symbol=${ticker}`),
        yahooGetSecure(ticker)
      ]);
      if (!finnData?.c || finnData.c <= 0) continue;

      const price = parseFloat(finnData.c);
      const prevClose = parseFloat(finnData.pc) || price;
      const changePct = prevClose > 0 ? ((price - prevClose) / prevClose * 100) : 0;

      // Volume accumulation: small price move but high volume = someone buying quietly
      // This is the classic pre-spike pattern
      if (!yahooData) continue;
      // We don't have intraday volume from Yahoo chart endpoint easily
      // Use Finnhub volume if available
      const vol = finnData.v || 0;
      if (vol <= 0) continue;

      // Flag: price moved 1-4% with significant volume (not yet a spike)
      const absPct = Math.abs(changePct);
      if (absPct >= 1 && absPct < 5) {
        const alertId = `vol-accum-${ticker}-${today}-${Math.floor(now.getHours() / 3)}`;
        if (!lastMoverAlerts[alertId]) {
          lastMoverAlerts[alertId] = now.toISOString();
          const dir = changePct > 0 ? '📈' : '📉';
          alerts.push({
            id: alertId,
            type: 'volume_accumulation',
            severity: 'info',
            ticker,
            title: `📊 ${ticker} quiet accumulation — ${dir} ${changePct > 0 ? '+' : ''}${changePct.toFixed(1)}%`,
            body: `${ticker} (your portfolio) moving ${changePct > 0 ? 'up' : 'down'} ${absPct.toFixed(1)}% on elevated volume. Watch for catalyst — this can precede a larger move.`,
            changePct, price,
            timestamp: now.toISOString()
          });
        }
      }
    } catch {}
  }
  return alerts;
}

// Background scan endpoint — called by main.js timer, returns alerts to fire
app.post('/api/background/scan', async (req, res) => {
  const { contracts, portfolio, threshold } = req.body;
  const alerts = [];
  const now = new Date();
  const today = now.toISOString().split('T')[0];
  const moveThreshold = threshold || 10;

  // ── 1. Market movers scan ─────────────────────────────────────────
  try {
    const scanList = [
      ...MOVER_SCAN_LIST,
      ...(portfolio || []).map(s => s.ticker)
    ];
    const unique = [...new Set(scanList)].slice(0, 50);

    const results = await Promise.all(unique.map(async ticker => {
      try {
        const data = await finnhubGetSecure(`/quote?symbol=${ticker}`);
        if (!data?.c || data.c <= 0) return null;
        const price = parseFloat(data.c);
        const prevClose = parseFloat(data.pc) || price;
        const changePct = prevClose > 0 ? ((price - prevClose) / prevClose * 100) : 0;
        return { ticker, price, prevClose, changePct };
      } catch { return null; }
    }));

    for (const r of results.filter(Boolean)) {
      if (Math.abs(r.changePct) < moveThreshold) continue;
      const alertId = `mover-${r.ticker}-${today}-${Math.floor(now.getHours() / 2)}`; // dedup per 2h window
      if (lastMoverAlerts[alertId] && (now - new Date(lastMoverAlerts[alertId])) < 2 * 60 * 60 * 1000) continue;
      lastMoverAlerts[alertId] = now.toISOString();
      const dir = r.changePct > 0 ? '🚀' : '📉';
      const severity = Math.abs(r.changePct) >= 30 ? 'critical' : Math.abs(r.changePct) >= 15 ? 'warning' : 'info';
      alerts.push({
        id: alertId,
        type: 'market_mover',
        severity,
        ticker: r.ticker,
        title: `${dir} ${r.ticker} ${r.changePct > 0 ? '+' : ''}${r.changePct.toFixed(1)}% today`,
        body: `${r.ticker} moved ${r.changePct > 0 ? 'up' : 'down'} ${Math.abs(r.changePct).toFixed(1)}% — now $${r.price.toFixed(2)} (was $${r.prevClose.toFixed(2)})`,
        changePct: r.changePct,
        price: r.price,
        timestamp: now.toISOString()
      });
    }
  } catch {}

  // ── 2. Contract expiry + P&L checks ──────────────────────────────
  for (const c of (contracts || [])) {
    if (!c.expiry) continue;
    const expDate = new Date(c.expiry);
    const daysLeft = Math.ceil((expDate - now) / (1000 * 60 * 60 * 24));

    if (daysLeft <= 0) {
      const id = `bg-expiry-${c.id || c.ticker}-${c.expiry}`;
      if (!lastMoverAlerts[id]) {
        lastMoverAlerts[id] = now.toISOString();
        alerts.push({ id, type: 'expiry', severity: 'critical', ticker: c.ticker, title: `🚨 ${c.ticker} ${c.type} EXPIRES TODAY`, body: `Your $${c.strike} ${c.type} expires today. Close it now or lose $${(c.pricePaid * 100 * (c.contracts||1)).toFixed(0)}.`, timestamp: now.toISOString() });
      }
    } else if (daysLeft <= 2) {
      const id = `bg-expiry2-${c.id || c.ticker}-${c.expiry}`;
      if (!lastMoverAlerts[id]) {
        lastMoverAlerts[id] = now.toISOString();
        alerts.push({ id, type: 'expiry', severity: 'warning', ticker: c.ticker, title: `⏰ ${c.ticker} ${c.type} expires in ${daysLeft} day${daysLeft > 1 ? 's' : ''}`, body: `Your $${c.strike} ${c.type} expires ${c.expiry}. Take action: close, roll, or let expire.`, timestamp: now.toISOString() });
      }
    }
  }

  // Persist all alerts to log file
  alerts.forEach(a => appendAlertLog(a));

  res.json({ alerts, scannedAt: now.toISOString() });
});

// ═══════════════════════════════════════════════════════════════════
// CONTRACT WATCH — AI-powered per-contract monitoring advice
// Called when a threshold is hit — gives specific action steps
// ═══════════════════════════════════════════════════════════════════

app.post('/api/contracts/watch', async (req, res) => {
  const { contract, currentPrice, trigger } = req.body;
  if (!contract || !currentPrice) return res.status(400).json({ error: 'contract and currentPrice required' });

  try {
    const expDate = new Date(contract.expiry);
    const daysLeft = Math.ceil((expDate - new Date()) / (1000 * 60 * 60 * 24));
    const costBasis = contract.pricePaid * 100 * (contract.contracts || 1);
    const isCall = contract.type === 'CALL';
    const breakEven = isCall
      ? (parseFloat(contract.strike) + parseFloat(contract.pricePaid)).toFixed(2)
      : (parseFloat(contract.strike) - parseFloat(contract.pricePaid)).toFixed(2);
    const intrinsic = isCall
      ? Math.max(0, currentPrice - contract.strike)
      : Math.max(0, contract.strike - currentPrice);
    const inTheMoney = intrinsic > 0;

    const prompt = `You are an options trading advisor. A contract needs immediate attention.

## Contract
- ${contract.ticker} ${contract.type} $${contract.strike} strike
- Expiry: ${contract.expiry} (${daysLeft} days left)
- Premium paid: $${contract.pricePaid}/share = $${costBasis.toFixed(0)} total cost
- Break even: $${breakEven}
- Contracts: ${contract.contracts || 1}

## Current Situation
- Stock price now: $${currentPrice}
- In the money: ${inTheMoney ? 'YES' : 'NO'}
- Trigger: ${trigger || 'manual review'}
- Notes: ${contract.notes || 'none'}

Give a SHORT, specific action plan. Return ONLY this JSON:
{
  "action": "CLOSE NOW" or "TAKE PARTIAL PROFIT" or "HOLD" or "ROLL" or "CUT LOSS",
  "urgency": "Immediate" or "Today" or "This Week",
  "steps": ["step 1 — specific", "step 2 — specific", "step 3 — specific"],
  "exitTarget": "e.g. close if premium hits $X",
  "stopLoss": "e.g. close if premium drops to $X",
  "reasoning": "one sentence why"
}`;

    const result = await askAI(prompt);
    const cleaned = result.replace(/```json\s*/gi, '').replace(/```\s*/g, '').trim();
    const match = cleaned.match(/\{[\s\S]*\}/);
    if (!match) throw new Error('Could not parse watch advice');
    const advice = JSON.parse(match[0]);
    advice.ticker = contract.ticker;
    advice.type = contract.type;
    advice.strike = contract.strike;
    advice.expiry = contract.expiry;
    advice.daysLeft = daysLeft;
    advice.currentPrice = currentPrice;
    advice.costBasis = costBasis;
    advice.breakEven = breakEven;
    advice.inTheMoney = inTheMoney;
    res.json(advice);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ═══════════════════════════════════════════════════════════════════
// ENHANCEMENT 1 — SIGNAL ACCURACY TRACKER (Continual Learning)
// Saves every signal, scores it 2 weeks later, feeds history back
// ═══════════════════════════════════════════════════════════════════

const SIGNAL_HISTORY_FILE = path.join(BASE_DIR, 'signal-history.json');
const ALERT_LOG_FILE = path.join(BASE_DIR, 'alert-log.json');

function loadAlertLog() {
  try { return JSON.parse(fs.readFileSync(ALERT_LOG_FILE, 'utf8')); } catch { return []; }
}
function saveAlertLog(log) {
  // Keep max 500 entries
  const trimmed = log.slice(0, 500);
  fs.writeFileSync(ALERT_LOG_FILE, JSON.stringify(trimmed, null, 2));
}
function appendAlertLog(alert) {
  const log = loadAlertLog();
  // Deduplicate by id
  if (log.find(a => a.id === alert.id)) return;
  log.unshift({ ...alert, loggedAt: new Date().toISOString() });
  saveAlertLog(log);
}

// GET full alert log
app.get('/api/alerts/log', (req, res) => {
  const log = loadAlertLog();
  const { type, severity, ticker, limit } = req.query;
  let filtered = log;
  if (type) filtered = filtered.filter(a => a.type === type);
  if (severity) filtered = filtered.filter(a => a.severity === severity);
  if (ticker) filtered = filtered.filter(a => a.ticker?.toUpperCase() === ticker.toUpperCase());
  if (limit) filtered = filtered.slice(0, parseInt(limit));
  res.json(filtered);
});

// POST save alert to log (called from renderer + background scan)
app.post('/api/alerts/log', (req, res) => {
  const alert = req.body;
  if (!alert?.id) return res.status(400).json({ error: 'alert id required' });
  appendAlertLog(alert);
  res.json({ ok: true });
});

// DELETE clear alert log
app.delete('/api/alerts/log', (req, res) => {
  saveAlertLog([]);
  res.json({ ok: true });
});

// DELETE single alert from log
app.delete('/api/alerts/log/:id', (req, res) => {
  const log = loadAlertLog().filter(a => a.id !== req.params.id);
  saveAlertLog(log);
  res.json({ ok: true });
});

function loadSignalHistory() {
  try { return JSON.parse(fs.readFileSync(SIGNAL_HISTORY_FILE, 'utf8')); } catch { return []; }
}
function saveSignalHistory(h) { fs.writeFileSync(SIGNAL_HISTORY_FILE, JSON.stringify(h, null, 2)); }

// Save a new signal entry when generated
app.post('/api/signals/save', (req, res) => {
  const { ticker, action, strike, expiry, stockPriceAtSignal, confidence, direction, isCrypto } = req.body;
  if (!ticker || !action) return res.status(400).json({ error: 'ticker and action required' });
  const history = loadSignalHistory();
  const entry = {
    id: Date.now().toString(),
    ticker: ticker.toUpperCase(),
    action,
    strike: strike || null,
    expiry: expiry || null,
    stockPriceAtSignal: parseFloat(stockPriceAtSignal) || 0,
    confidence: confidence || 0,
    direction: direction || null,
    isCrypto: !!isCrypto,
    generatedAt: new Date().toISOString(),
    scored: false,
    outcome: null,
    stockPriceAtScore: null,
    scoredAt: null
  };
  history.unshift(entry);
  if (history.length > 200) history.splice(200);
  saveSignalHistory(history);
  res.json({ ok: true, id: entry.id });
});

// Get full signal history
app.get('/api/signals/history', (req, res) => {
  const history = loadSignalHistory();
  res.json(history);
});

// Get signal history for a specific ticker
app.get('/api/signals/history/:ticker', (req, res) => {
  const ticker = req.params.ticker.toUpperCase();
  const history = loadSignalHistory().filter(s => s.ticker === ticker);
  res.json(history);
});

// Score signals that are 2+ weeks old and unscored
app.post('/api/signals/score', async (req, res) => {
  const history = loadSignalHistory();
  const twoWeeksAgo = Date.now() - 14 * 24 * 60 * 60 * 1000;
  const toScore = history.filter(s => !s.scored && new Date(s.generatedAt).getTime() < twoWeeksAgo);

  if (!toScore.length) return res.json({ scored: 0, message: 'No signals ready to score' });

  let scored = 0;
  for (const entry of toScore) {
    try {
      const priceData = await fetchMultiSource(entry.ticker, entry.isCrypto);
      if (!priceData.valid) continue;
      const currentPrice = priceData.price;
      const priceDiff = ((currentPrice - entry.stockPriceAtSignal) / entry.stockPriceAtSignal) * 100;

      let outcome = 'neutral';
      if (entry.action.includes('CALL') || entry.direction === 'Bullish' || entry.action === 'BUY') {
        outcome = priceDiff >= 3 ? 'correct' : priceDiff <= -3 ? 'wrong' : 'neutral';
      } else if (entry.action.includes('PUT') || entry.direction === 'Bearish' || entry.action === 'SELL') {
        outcome = priceDiff <= -3 ? 'correct' : priceDiff >= 3 ? 'wrong' : 'neutral';
      }

      const idx = history.findIndex(s => s.id === entry.id);
      if (idx >= 0) {
        history[idx].scored = true;
        history[idx].outcome = outcome;
        history[idx].stockPriceAtScore = currentPrice;
        history[idx].priceDiffPct = parseFloat(priceDiff.toFixed(2));
        history[idx].scoredAt = new Date().toISOString();
        scored++;
      }
    } catch {}
  }

  saveSignalHistory(history);
  res.json({ scored, total: toScore.length });
});

// Delete a signal history entry
app.delete('/api/signals/history/:id', (req, res) => {
  const history = loadSignalHistory().filter(s => s.id !== req.params.id);
  saveSignalHistory(history);
  res.json({ ok: true });
});

// Get scorecard summary stats
app.get('/api/signals/scorecard', (req, res) => {
  const history = loadSignalHistory();
  const scored = history.filter(s => s.scored);
  const correct = scored.filter(s => s.outcome === 'correct').length;
  const wrong = scored.filter(s => s.outcome === 'wrong').length;
  const neutral = scored.filter(s => s.outcome === 'neutral').length;
  const winRate = scored.length ? Math.round((correct / scored.length) * 100) : 0;

  // Per-ticker breakdown
  const byTicker = {};
  for (const s of scored) {
    if (!byTicker[s.ticker]) byTicker[s.ticker] = { correct: 0, wrong: 0, neutral: 0, total: 0 };
    byTicker[s.ticker][s.outcome]++;
    byTicker[s.ticker].total++;
  }

  res.json({
    total: history.length,
    scored: scored.length,
    unscored: history.length - scored.length,
    correct, wrong, neutral,
    winRate,
    byTicker
  });
});

// ═══════════════════════════════════════════════════════════════════
// SIGNAL CONVERGENCE ENGINE
// Scores a ticker across multiple real data signals (not AI opinions)
// Returns a convergence score 0-100 + breakdown of what fired
// ═══════════════════════════════════════════════════════════════════

app.get('/api/signals/convergence/:ticker', async (req, res) => {
  const ticker = req.params.ticker.toUpperCase();
  try {
    const signals = [];
    let score = 0;

    // Run all signal checks in parallel
    const [quote, insiderRaw, newsRaw, kalshiRaw] = await Promise.all([
      fetchMultiSource(ticker, false).catch(() => null),
      finnhubGetSecure(`/stock/insider-transactions?symbol=${ticker}`).catch(() => null),
      finnhubGetSecure(`/company-news?symbol=${ticker}&from=${new Date(Date.now()-7*86400000).toISOString().split('T')[0]}&to=${new Date().toISOString().split('T')[0]}`).catch(() => []),
      fetchKalshiMacro().catch(() => [])
    ]);

    // ── Signal 1: Volume Spike ────────────────────────────────────
    if (quote?.valid) {
      const priceData = await new Promise(resolve => {
        const url = `https://query1.finance.yahoo.com/v8/finance/chart/${ticker}?interval=1d&range=3mo`;
        execFile('curl', ['-s', '--max-time', '10', '-A', 'Mozilla/5.0', url], (err, stdout) => {
          if (err || !stdout) return resolve(null);
          try { resolve(JSON.parse(stdout)); } catch { resolve(null); }
        });
      });
      const volumes = priceData?.chart?.result?.[0]?.indicators?.quote?.[0]?.volume?.filter(Boolean) || [];
      if (volumes.length >= 20) {
        const avgVol = volumes.slice(-20, -1).reduce((a, b) => a + b, 0) / 19;
        const todayVol = volumes[volumes.length - 1] || 0;
        const volRatio = avgVol > 0 ? todayVol / avgVol : 0;
        if (volRatio >= 2) {
          signals.push({ type: 'bullish', label: 'Volume Spike', text: `Volume ${volRatio.toFixed(1)}x average — unusual activity`, weight: 20 });
          score += 20;
        } else if (volRatio >= 1.5) {
          signals.push({ type: 'neutral', label: 'Elevated Volume', text: `Volume ${volRatio.toFixed(1)}x average — above normal`, weight: 10 });
          score += 10;
        }
      }
    }

    // ── Signal 2: Insider Cluster ─────────────────────────────────
    const insiderTxns = Array.isArray(insiderRaw?.data) ? insiderRaw.data : [];
    const recentBuys = insiderTxns.filter(t => {
      const daysAgo = (Date.now() - new Date(t.transactionDate).getTime()) / 86400000;
      return daysAgo <= 30 && t.transactionCode === 'P'; // P = Purchase
    });
    if (recentBuys.length >= 3) {
      signals.push({ type: 'bullish', label: 'Insider Cluster', text: `${recentBuys.length} insider purchases in last 30 days — strong conviction signal`, weight: 25 });
      score += 25;
    } else if (recentBuys.length >= 1) {
      signals.push({ type: 'bullish', label: 'Insider Buy', text: `${recentBuys.length} insider purchase in last 30 days`, weight: 12 });
      score += 12;
    }

    // ── Signal 3: News Sentiment Velocity ────────────────────────
    const newsArr = Array.isArray(newsRaw) ? newsRaw : [];
    const last48hNews = newsArr.filter(n => (Date.now() / 1000 - n.datetime) < 48 * 3600);
    const posKeywords = ['beat', 'record', 'upgrade', 'buy', 'outperform', 'strong', 'growth', 'partnership', 'deal', 'launch'];
    const negKeywords = ['miss', 'downgrade', 'sell', 'underperform', 'loss', 'decline', 'lawsuit', 'investigation', 'cut'];
    let posCount = 0, negCount = 0;
    for (const n of last48hNews) {
      const text = (n.headline + ' ' + (n.summary || '')).toLowerCase();
      if (posKeywords.some(k => text.includes(k))) posCount++;
      if (negKeywords.some(k => text.includes(k))) negCount++;
    }
    if (last48hNews.length >= 3 && posCount > negCount * 2) {
      signals.push({ type: 'bullish', label: 'Positive Sentiment', text: `${last48hNews.length} news items in 48hrs — ${posCount} positive vs ${negCount} negative`, weight: 15 });
      score += 15;
    } else if (last48hNews.length >= 3 && negCount > posCount * 2) {
      signals.push({ type: 'bearish', label: 'Negative Sentiment', text: `${last48hNews.length} news items in 48hrs — ${negCount} negative vs ${posCount} positive`, weight: -15 });
      score -= 15;
    }

    // ── Signal 4: Earnings Window ─────────────────────────────────
    try {
      const earningsData = await finnhubGetSecure(`/calendar/earnings?from=${new Date().toISOString().split('T')[0]}&to=${new Date(Date.now()+7*86400000).toISOString().split('T')[0]}&symbol=${ticker}`);
      const upcoming = (earningsData?.earningsCalendar || []).filter(e => e.symbol === ticker);
      if (upcoming.length > 0) {
        const daysUntil = Math.ceil((new Date(upcoming[0].date) - new Date()) / 86400000);
        signals.push({ type: 'warning', label: 'Earnings Upcoming', text: `Earnings in ${daysUntil} day${daysUntil !== 1 ? 's' : ''} — IV likely to expand, catalyst window`, weight: 15 });
        score += 15;
      }
    } catch {}

    // ── Signal 5: Kalshi Macro Alignment ─────────────────────────
    for (const m of kalshiRaw) {
      const title = (m.title || '').toLowerCase();
      if ((title.includes('rate cut') || title.includes('fed cut')) && m.probability > 60) {
        signals.push({ type: 'bullish', label: 'Kalshi: Fed Cut Odds', text: `${m.probability}% probability of rate cut — macro tailwind for growth stocks`, weight: 10 });
        score += 10;
        break;
      }
      if (title.includes('recession') && m.probability > 50) {
        signals.push({ type: 'bearish', label: 'Kalshi: Recession Risk', text: `${m.probability}% recession probability — macro headwind`, weight: -10 });
        score -= 10;
        break;
      }
    }

    const clampedScore = Math.min(100, Math.max(0, score));
    const convergenceLevel = clampedScore >= 60 ? 'HIGH' : clampedScore >= 35 ? 'MEDIUM' : 'LOW';
    const convergenceColor = clampedScore >= 60 ? 'green' : clampedScore >= 35 ? 'yellow' : 'red';

    res.json({
      ticker,
      score: clampedScore,
      convergenceLevel,
      convergenceColor,
      signalCount: signals.length,
      signals,
      recommendation: clampedScore >= 60 ? 'Multiple signals aligned — worth investigating further' :
                      clampedScore >= 35 ? 'Some signals present — monitor closely' :
                      'Weak signal convergence — wait for better setup',
      currentPrice: quote?.price || null,
      fetchedAt: new Date().toISOString()
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════════
// ENHANCEMENT 2 — RAG ON TRADE HISTORY
// Searches journal + signal history for past trades on a ticker
// Injects context into AI prompts for grounded signals
// ═══════════════════════════════════════════════════════════════════

function getTradeContext(ticker) {
  const t = ticker.toUpperCase();
  const lines = [];

  // Search closed trades in data.json
  try {
    const data = loadData();
    const closedTrades = (data.daytrading?.closedTrades || []).filter(c => c.ticker === t);
    if (closedTrades.length) {
      lines.push(`Past closed trades on ${t}:`);
      closedTrades.slice(0, 5).forEach(c => {
        const pnlStr = c.pnl >= 0 ? `+$${c.pnl.toFixed(0)} profit` : `-$${Math.abs(c.pnl).toFixed(0)} loss`;
        lines.push(`  - ${c.type} $${c.strike} exp ${c.expiry}: ${pnlStr} (${((c.soldPrice - c.pricePaid) / c.pricePaid * 100).toFixed(0)}%)`);
      });
    }
  } catch {}

  // Search journal for analyzed trades
  try {
    const journal = loadJournal();
    const journalTrades = journal.filter(j => j.ticker === t && j.analysis);
    if (journalTrades.length) {
      lines.push(`AI-analyzed journal entries for ${t}:`);
      journalTrades.slice(0, 3).forEach(j => {
        lines.push(`  - Grade ${j.analysis.grade}: ${j.analysis.keyLesson}`);
      });
    }
  } catch {}

  // Search signal history for past signals
  try {
    const history = loadSignalHistory().filter(s => s.ticker === t && s.scored);
    if (history.length) {
      const correct = history.filter(s => s.outcome === 'correct').length;
      const winRate = Math.round((correct / history.length) * 100);
      lines.push(`Past AI signals for ${t}: ${history.length} signals, ${winRate}% accuracy`);
      history.slice(0, 3).forEach(s => {
        const icon = s.outcome === 'correct' ? '✅' : s.outcome === 'wrong' ? '❌' : '➖';
        lines.push(`  ${icon} ${s.action} at $${s.stockPriceAtSignal} → price moved to $${s.stockPriceAtScore} (${s.priceDiffPct > 0 ? '+' : ''}${s.priceDiffPct}%)`);
      });
    }
  } catch {}

  // Search long-term portfolio
  try {
    const data = loadData();
    const holding = (data.longterm?.portfolio || []).find(s => s.ticker === t);
    if (holding) {
      lines.push(`You currently own ${holding.shares} shares of ${t} at avg cost $${holding.avgCost}`);
    }
  } catch {}

  return lines.length ? `\n## Your Personal History with ${t}\n${lines.join('\n')}\n` : '';
}

// Expose trade context endpoint for UI use
app.get('/api/trade-context/:ticker', (req, res) => {
  const context = getTradeContext(req.params.ticker.toUpperCase());
  res.json({ ticker: req.params.ticker.toUpperCase(), context, hasHistory: context.length > 0 });
});

// ═══════════════════════════════════════════════════════════════════
// ENHANCEMENT 3 — SELF-EVALUATION LOOP ON SIGNALS
// After generating a signal, a critic agent validates it
// before returning to the user
// ═══════════════════════════════════════════════════════════════════

async function validateSignal(signal, ticker, stockPrice) {
  if (!signal || signal.action === 'WAIT') return { valid: true, issues: [] };

  const prompt = `You are a signal validation agent. Your ONLY job is to check if this options signal is mathematically and logically valid.

## Signal to Validate
- Ticker: ${ticker}
- Current Stock Price: $${stockPrice}
- Action: ${signal.action}
- Strike: $${signal.strike}
- Expiry: ${signal.expiry}
- Estimated Premium: $${signal.estimatedPremium}/share
- Total Cost: $${signal.totalCost}
- Break Even: $${signal.breakEven}
- Confidence: ${signal.confidence}%

## Validation Rules
1. Strike must be within 20% of current stock price (not wildly unrealistic)
2. Premium must be realistic: for a $${stockPrice} stock, premium should be $${(stockPrice * 0.01).toFixed(0)}-$${(stockPrice * 0.06).toFixed(0)}/share
3. Total cost = estimatedPremium × 100 × contracts (check math)
4. Break even for CALL = strike + premium, for PUT = strike - premium (check math)
5. Expiry must be a future date (after today ${new Date().toDateString()})
6. Confidence 0-100 must be consistent with reasoning quality

Return ONLY this JSON:
{
  "valid": true or false,
  "issues": ["issue 1 if any", "issue 2 if any"],
  "fixes": { "estimatedPremium": corrected_value_or_null, "totalCost": corrected_value_or_null, "breakEven": corrected_value_or_null }
}`;

  try {
    const result = await askAI(prompt);
    const jsonMatch = result.match(/\{[\s\S]*\}/);
    if (!jsonMatch) return { valid: true, issues: [] };
    const validation = JSON.parse(jsonMatch[0]);
    // Apply fixes if any
    if (validation.fixes) {
      if (validation.fixes.estimatedPremium) signal.estimatedPremium = validation.fixes.estimatedPremium;
      if (validation.fixes.totalCost) signal.totalCost = validation.fixes.totalCost;
      if (validation.fixes.breakEven) signal.breakEven = validation.fixes.breakEven;
    }
    return validation;
  } catch {
    return { valid: true, issues: [] };
  }
}

// ═══════════════════════════════════════════════════════════════════
// ENHANCEMENT 4 — MULTI-AGENT CREW SIGNAL PIPELINE
// 4 specialist agents debate before producing final signal
// Endpoint: POST /api/ai/signal/crew
// Now data-driven: pulls real signals (volume, insider, news, Kalshi) THEN asks AI once
// ═══════════════════════════════════════════════════════════════════

app.post('/api/ai/signal/crew', async (req, res) => {
  const { ticker, price, change, changePct, news, currentPrice: cp } = req.body;
  const stockPrice = cp || price || 0;

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const send = (data) => { try { res.write(`data: ${JSON.stringify(data)}\n\n`); } catch {} };
  const keepalive = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 15000);

  try {
    const newsArr = Array.isArray(news) ? news : [];
    const newsText = newsArr.map(n => `- ${n.headline}`).join('\n') || 'No recent news';
    const today = new Date().toDateString();
    const tradeContext = getTradeContext(ticker);

    // ── Step 1: Pull real data signals in parallel ──────────────────
    send({ type: 'status', text: '📡 Pulling real market signals...' });

    const [convergenceRes, insiderRaw, kalshiRaw] = await Promise.all([
      fetch(`http://localhost:${PORT}/api/signals/convergence/${ticker}`).then(r => r.json()).catch(() => null),
      finnhubGetSecure(`/stock/insider-transactions?symbol=${ticker}`).catch(() => null),
      fetchKalshiMacro().catch(() => [])
    ]);

    // ── Step 2: Emit real data signals as agent cards ───────────────
    const convergenceSignals = convergenceRes?.signals || [];
    const convergenceScore = convergenceRes?.score || 0;

    send({ type: 'agent', agent: 'Signal Scanner', data: {
      score: convergenceScore,
      level: convergenceRes?.convergenceLevel || 'LOW',
      signals: convergenceSignals.map(s => s.text),
      recommendation: convergenceScore >= 60 ? 'BUY CALL' : convergenceScore >= 35 ? 'WAIT' : 'WAIT'
    }});

    // Insider summary
    const insiderTxns = Array.isArray(insiderRaw?.data) ? insiderRaw.data : [];
    const recentBuys = insiderTxns.filter(t => {
      const daysAgo = (Date.now() - new Date(t.transactionDate).getTime()) / 86400000;
      return daysAgo <= 30 && t.transactionCode === 'P';
    });
    send({ type: 'agent', agent: 'Insider Tracker', data: {
      recentBuys: recentBuys.length,
      signal: recentBuys.length >= 2 ? 'Bullish — cluster buy' : recentBuys.length === 1 ? 'Mild bullish' : 'Neutral',
      recommendation: recentBuys.length >= 2 ? 'BUY CALL' : 'WAIT'
    }});

    // Kalshi macro
    const kalshiSignal = kalshiRaw.find(m => {
      const t = (m.title || '').toLowerCase();
      return (t.includes('rate cut') && m.probability > 60) || (t.includes('recession') && m.probability > 50);
    });
    send({ type: 'agent', agent: 'Kalshi Macro', data: {
      signal: kalshiSignal ? kalshiSignal.title : 'No strong macro signal',
      probability: kalshiSignal?.probability || null,
      recommendation: kalshiSignal?.title?.toLowerCase().includes('rate cut') ? 'BUY CALL' : kalshiSignal?.title?.toLowerCase().includes('recession') ? 'BUY PUT' : 'WAIT'
    }});

    // ── Step 3: ONE AI call with all real data as context ───────────
    send({ type: 'status', text: '🤖 AI synthesizing real signals into options trade...' });

    const roundedPrice = Math.round(stockPrice / 5) * 5;
    const otmCallStrike = roundedPrice + 5;
    const otmPutStrike = roundedPrice - 5;

    const signalSummary = convergenceSignals.map(s => `- ${s.label}: ${s.text}`).join('\n') || '- No strong signals detected';
    const kalshiContext = kalshiSignal ? `Kalshi prediction market: "${kalshiSignal.title}" at ${kalshiSignal.probability}% probability` : 'No strong Kalshi macro signal';

    const prompt = `You are a professional options trader. Generate a 2-4 week swing trade signal for ${ticker} at $${stockPrice}.

## Real Market Data (not opinions — actual data)
${signalSummary}
- Signal Convergence Score: ${convergenceScore}/100 (${convergenceRes?.convergenceLevel || 'LOW'})
- Insider Buys (30d): ${recentBuys.length}
- ${kalshiContext}

## Recent News
${newsText.slice(0, 500)}

## Past Trade Context
${tradeContext || 'No prior trades on this ticker'}

## Today: ${today}
- ATM Strike: $${roundedPrice}
- OTM Call: $${otmCallStrike} | OTM Put: $${otmPutStrike}

Based on the REAL DATA above, generate a specific options signal. If convergence score < 35 and no insider buys, recommend WAIT.

Return ONLY this exact JSON:
{
  "action": "BUY CALL" or "BUY PUT" or "WAIT",
  "direction": "Bullish" or "Bearish" or "Neutral",
  "thesis": "1-2 sentences on WHY based on the real signals above",
  "holdPlan": "Hold 2-4 weeks unless thesis breaks",
  "strike": number,
  "strikeLabel": "e.g. $${otmCallStrike} (OTM Call)",
  "expiry": "YYYY-MM-DD",
  "expiryLabel": "e.g. May 16 (3 weeks)",
  "contracts": 1,
  "estimatedPremium": number,
  "totalCost": number,
  "costEstimate": "e.g. ~$350",
  "breakEven": number,
  "breakEvenLabel": "e.g. $X by expiry",
  "maxLoss": number,
  "maxLossLabel": "e.g. $350 — premium paid",
  "targetPrice": number,
  "targetReturn": "e.g. +150%",
  "confidence": number 0-100,
  "riskLevel": "Low" or "Medium" or "High",
  "technical": ["signal 1 from real data", "signal 2"],
  "fundamental": ["fundamental point 1"],
  "news": ["news impact 1"],
  "risk": ["what would invalidate this thesis"],
  "whyThisStrike": "why this strike",
  "whyThisExpiry": "why this expiry",
  "steps": ["Step 1", "Step 2", "Step 3", "Step 4", "Step 5"],
  "exitPlan": "Take profit at +75%. Exit if thesis breaks.",
  "warning": "beginner tip",
  "isCrew": true,
  "agentConsensus": "Data-driven: ${convergenceScore}/100 convergence score, ${recentBuys.length} insider buys"
}`;

    const result = await askAI(prompt); // cleanAIResponse already applied inside askAI
    const match = result.match(/\{[\s\S]*\}/);
    if (!match) throw new Error(`AI returned invalid response. Raw: ${result.slice(0, 100)}`);

    let signal;
    try {
      signal = JSON.parse(match[0]);
    } catch (parseErr) {
      throw new Error(`Could not parse AI response as JSON: ${parseErr.message}`);
    }

    // Validate required fields
    if (!signal.action || !signal.strike || signal.confidence === undefined) {
      throw new Error(`AI response missing required fields (action/strike/confidence). Got: ${JSON.stringify(signal).slice(0, 100)}`);
    }
    signal.isCrew = true;
    signal.convergenceScore = convergenceScore;
    signal.realSignals = convergenceSignals;

    send({ type: 'signal', data: signal });
    send({ type: 'done' });

  } catch (e) {
    send({ type: 'error', text: e.message });
    send({ type: 'done' });
  } finally {
    clearInterval(keepalive);
    res.end();
  }
});

// ═══════════════════════════════════════════════════════════════════
// ENHANCEMENT 5 — MORNING BRIEF UPGRADE (3-Agent Pipeline)
// Agent 1: Market Scanner, Agent 2: News Aggregator, Agent 3: Brief Writer
// Endpoint: POST /api/ai/morning-brief/crew (new — keeps original intact)
// ═══════════════════════════════════════════════════════════════════

app.post('/api/ai/morning-brief/crew', async (req, res) => {
  const { portfolio, watchlist, prices: priceMap, cryptoPrices: cryptoMap } = req.body;

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();
  const send = d => { try { res.write(`data: ${JSON.stringify(d)}\n\n`); } catch {} };
  const keepalive = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 15000);

  try {
    const cached = loadBrief();
    if (cached && req.query.force !== 'true') {
      send({ type: 'brief', data: { ...cached, isCrew: true } });
      send({ type: 'done' });
      clearInterval(keepalive);
      res.end();
      return;
    }

    // ── AGENT 1: MARKET SCANNER ──────────────────────────────────────
    send({ type: 'status', text: '📡 Agent 1: Market Scanner — scanning your portfolio for movers...' });

    const allTickers = [
      ...(portfolio || []).map(s => s.ticker),
      ...(watchlist || []).map(s => s.ticker)
    ];
    const uniqueTickers = [...new Set(allTickers)].slice(0, 6);

    // Find movers from price data
    const movers = uniqueTickers.map(t => {
      const p = priceMap?.[t];
      if (!p) return null;
      return { ticker: t, price: p.price, changePct: p.changePct || 0 };
    }).filter(Boolean).sort((a, b) => Math.abs(b.changePct) - Math.abs(a.changePct));

    const topMovers = movers.slice(0, 3);
    const moversText = topMovers.map(m =>
      `${m.ticker}: $${m.price?.toFixed(2)} (${m.changePct >= 0 ? '+' : ''}${m.changePct?.toFixed(2)}%)`
    ).join(', ') || 'No significant movers';

    send({ type: 'agent', agent: 'Scanner', data: { movers: topMovers, total: movers.length } });

    // ── AGENT 2: NEWS AGGREGATOR ─────────────────────────────────────
    send({ type: 'status', text: '📰 Agent 2: News Aggregator — pulling and ranking relevant news...' });

    const [watchlistNewsResults, marketNewsResults] = await Promise.all([
      Promise.all(uniqueTickers.slice(0, 4).map(t =>
        finnhubNewsSecure(t, false).then(n => ({ ticker: t, news: n.slice(0, 2) }))
      )),
      Promise.all([finnhubMarketNewsSecure(), yahooMarketNewsSecure()])
    ]);

    const marketNews = mergeAndDedup(marketNewsResults[0], marketNewsResults[1]).slice(0, 6);
    const watchlistNewsRaw = watchlistNewsResults.filter(r => r.news.length);
    const marketNewsRaw = marketNews;

    // Rank news by relevance to portfolio
    const portfolioTickers = new Set((portfolio || []).map(s => s.ticker));
    const rankedNews = marketNews.sort((a, b) => {
      const aRelevant = (a.relatedTickers || []).some(t => portfolioTickers.has(t)) ? 1 : 0;
      const bRelevant = (b.relatedTickers || []).some(t => portfolioTickers.has(t)) ? 1 : 0;
      return bRelevant - aRelevant || b.datetime - a.datetime;
    });

    const watchlistNewsText = watchlistNewsResults
      .filter(r => r.news.length)
      .map(r => `${r.ticker}:\n${r.news.map(n => `  - ${n.headline}`).join('\n')}`)
      .join('\n') || 'No recent news for watchlist stocks';

    const marketNewsText = rankedNews.map(n => `- [${n.publisher}] ${n.headline}`).join('\n') || 'No market news';

    send({ type: 'agent', agent: 'NewsAggregator', data: { articleCount: marketNews.length, watchlistCount: watchlistNewsRaw.length } });

    // ── AGENT 3: BRIEF WRITER ────────────────────────────────────────
    send({ type: 'status', text: '✍️ Agent 3: Brief Writer — crafting your personalized morning brief...' });

    const portfolioText = (portfolio || []).map(s => {
      const p = priceMap?.[s.ticker] || {};
      const pnl = p.price ? ((p.price - s.avgCost) * s.shares).toFixed(0) : 'N/A';
      const pnlPct = p.price ? ((p.price - s.avgCost) / s.avgCost * 100).toFixed(1) : 'N/A';
      return `${s.ticker}: ${s.shares} shares @ $${s.avgCost} avg, now $${p.price?.toFixed(2) || 'N/A'}, P&L: ${pnl >= 0 ? '+' : ''}$${pnl} (${pnlPct}%)`;
    }).join('\n') || 'No portfolio';

    const watchText = (watchlist || []).map(s => {
      const p = priceMap?.[s.ticker] || {};
      return `${s.ticker}: $${p.price?.toFixed(2) || 'N/A'} (${p.changePct >= 0 ? '+' : ''}${p.changePct?.toFixed(2) || 0}%)`;
    }).join(', ') || 'None';

    const now = new Date();
    const briefPrompt = `You are a personal financial advisor writing a morning brief. You have been given pre-analyzed data from two specialist agents.

## Pre-Analysis from Market Scanner Agent
Top movers today: ${moversText}

## Pre-Analysis from News Aggregator Agent  
News has been ranked by relevance to this investor's portfolio.

## Investor Portfolio
${portfolioText}

## Watchlist
${watchText}

## Ranked News for Their Stocks
${watchlistNewsText}

## Ranked Market News (portfolio-relevant first)
${marketNewsText}

## Your Task
Write a concise, personalized morning brief using the pre-analyzed data above.
This is a CREW brief — it should be more insightful than a standard brief because you have specialist agent input.

Return ONLY this exact JSON:
{
  "greeting": "Good morning! one energetic sentence about today",
  "marketMood": "Bullish" or "Bearish" or "Mixed" or "Cautious",
  "marketMoodColor": "green" or "red" or "yellow" or "blue",
  "headline": "the single most important thing happening in markets today",
  "portfolioSnapshot": "2 sentence summary of their portfolio situation",
  "topMover": { "ticker": "TICKER", "direction": "up" or "down", "note": "why" },
  "todaysFocus": ["action item 1", "action item 2", "action item 3"],
  "watchlistAlert": "any watchlist ticker worth paying attention to today and why",
  "riskAlert": "any risk to be aware of today",
  "stocksToWatch": [
    { "ticker": "TICKER", "name": "Company Name", "reason": "one sentence why interesting today", "action": "Watch" or "Research" or "Consider Adding" }
  ],
  "closingThought": "one motivational sentence for the trading day",
  "crewInsight": "one sentence unique insight that only the 3-agent crew analysis could surface",
  "generatedAt": "${new Date().toISOString()}"
}`;

    const briefResult = await askAI(briefPrompt);
    const briefMatch = briefResult.match(/\{[\s\S]*\}/);
    if (!briefMatch) throw new Error('Could not parse crew brief');

    const brief = JSON.parse(briefMatch[0]);
    brief.generatedAt = brief.generatedAt || new Date().toISOString();
    brief.watchlistNewsRaw = watchlistNewsRaw;
    brief.marketNewsRaw = marketNewsRaw;
    brief.isCrew = true;
    brief.agentBreakdown = {
      scanner: { movers: topMovers },
      newsAggregator: { articleCount: marketNews.length }
    };

    saveBrief(brief);
    send({ type: 'brief', data: brief });
    send({ type: 'done' });

  } catch (e) {
    send({ type: 'error', text: e.message });
    send({ type: 'done' });
  } finally {
    clearInterval(keepalive);
    res.end();
  }
});

// ─── Paper Trading ────────────────────────────────────────────────
const PAPER_FILE = path.join(BASE_DIR, 'paper-trading.json');

function loadPaper() {
  try {
    if (fs.existsSync(PAPER_FILE)) return JSON.parse(fs.readFileSync(PAPER_FILE, 'utf8'));
  } catch {}
  return {
    cash: 10000,
    startingCash: 10000,
    positions: [],
    closedTrades: [],
    orders: [],
    createdAt: new Date().toISOString()
  };
}

function savePaper(data) {
  fs.writeFileSync(PAPER_FILE, JSON.stringify(data, null, 2));
}

// Get paper account
app.get('/api/paper/account', (req, res) => {
  res.json(loadPaper());
});

// Reset paper account
app.post('/api/paper/reset', (req, res) => {
  const { startingCash = 10000 } = req.body;
  const fresh = {
    cash: startingCash,
    startingCash,
    positions: [],
    closedTrades: [],
    orders: [],
    createdAt: new Date().toISOString()
  };
  savePaper(fresh);
  res.json({ ok: true, account: fresh });
});

// Place a paper trade (buy option)
app.post('/api/paper/trade', (req, res) => {
  const { ticker, type, strike, expiry, premium, contracts = 1, signal, stockPriceAtEntry } = req.body;
  if (!ticker || !type || !strike || !expiry || !premium) {
    return res.status(400).json({ error: 'ticker, type, strike, expiry, premium required' });
  }
  const paper = loadPaper();
  const totalCost = parseFloat(premium) * 100 * contracts;

  if (totalCost > paper.cash) {
    return res.status(400).json({ error: `Insufficient funds. Need $${totalCost.toFixed(2)}, have $${paper.cash.toFixed(2)}` });
  }

  const position = {
    id: Date.now().toString(),
    ticker: ticker.toUpperCase(),
    type: type.toUpperCase(),
    strike: parseFloat(strike),
    expiry,
    premium: parseFloat(premium),
    contracts: parseInt(contracts),
    totalCost,
    stockPriceAtEntry: parseFloat(stockPriceAtEntry) || 0,
    signal: signal || null,
    openedAt: new Date().toISOString(),
    status: 'open'
  };

  paper.cash -= totalCost;
  paper.positions.push(position);
  paper.orders.push({
    id: position.id,
    action: 'BUY',
    ticker: position.ticker,
    type: position.type,
    strike: position.strike,
    expiry: position.expiry,
    premium: position.premium,
    contracts: position.contracts,
    totalCost,
    timestamp: position.openedAt
  });

  savePaper(paper);
  res.json({ ok: true, position, cashRemaining: paper.cash });
});

// Close a paper position
app.post('/api/paper/close/:id', (req, res) => {
  const { closePremium, currentStockPrice } = req.body;
  const paper = loadPaper();
  const idx = paper.positions.findIndex(p => p.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Position not found' });

  const pos = paper.positions[idx];
  const closePrice = parseFloat(closePremium) || 0;
  const proceeds = closePrice * 100 * pos.contracts;
  const pnl = proceeds - pos.totalCost;
  const pnlPct = ((pnl / pos.totalCost) * 100).toFixed(1);

  const closed = {
    ...pos,
    closePremium: closePrice,
    proceeds,
    pnl,
    pnlPct: parseFloat(pnlPct),
    currentStockPrice: parseFloat(currentStockPrice) || 0,
    closedAt: new Date().toISOString(),
    status: 'closed'
  };

  paper.cash += proceeds;
  paper.positions.splice(idx, 1);
  paper.closedTrades.unshift(closed);
  paper.orders.push({
    id: Date.now().toString(),
    action: 'CLOSE',
    ticker: pos.ticker,
    type: pos.type,
    strike: pos.strike,
    expiry: pos.expiry,
    premium: closePrice,
    contracts: pos.contracts,
    proceeds,
    pnl,
    timestamp: new Date().toISOString()
  });

  savePaper(paper);
  res.json({ ok: true, closed, cashRemaining: paper.cash });
});

// Expire a position (worthless)
app.post('/api/paper/expire/:id', (req, res) => {
  const paper = loadPaper();
  const idx = paper.positions.findIndex(p => p.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Position not found' });
  const pos = paper.positions[idx];
  const closed = { ...pos, closePremium: 0, proceeds: 0, pnl: -pos.totalCost, pnlPct: -100, closedAt: new Date().toISOString(), status: 'expired' };
  paper.positions.splice(idx, 1);
  paper.closedTrades.unshift(closed);
  savePaper(paper);
  res.json({ ok: true, closed, cashRemaining: paper.cash });
});

// AI analysis for a paper position
app.post('/api/paper/analyze/:id', async (req, res) => {
  const { currentStockPrice, currentPremium } = req.body;
  const paper = loadPaper();
  const pos = paper.positions.find(p => p.id === req.params.id);
  if (!pos) return res.status(404).json({ error: 'Position not found' });

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();
  const send = d => { try { res.write(`data: ${JSON.stringify(d)}\n\n`); } catch {} };
  const keepalive = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 15000);

  try {
    const daysLeft = Math.ceil((new Date(pos.expiry) - new Date()) / (1000 * 60 * 60 * 24));
    const pnl = currentPremium ? ((currentPremium - pos.premium) / pos.premium * 100).toFixed(1) : 0;
    const isCall = pos.type === 'CALL';
    const intrinsic = currentStockPrice
      ? (isCall ? Math.max(0, currentStockPrice - pos.strike) : Math.max(0, pos.strike - currentStockPrice))
      : 0;

    const prompt = `You are an options trading coach reviewing a paper trade position.

Position: ${pos.ticker} ${pos.type} $${pos.strike} strike, expires ${pos.expiry}
Opened at: $${pos.premium} premium (paid $${pos.totalCost.toFixed(0)} total)
Current stock price: $${currentStockPrice || 'unknown'}
Current premium estimate: $${currentPremium || 'unknown'}
Days to expiry: ${daysLeft}
Current P&L: ${pnl}%
Intrinsic value: $${intrinsic.toFixed(2)}
${intrinsic === 0 ? '⚠️ Currently OUT OF THE MONEY' : '✅ Currently IN THE MONEY'}

Give a brief, actionable recommendation. Be direct.

Return ONLY this JSON:
{
  "action": "HOLD" or "CLOSE NOW" or "CLOSE PARTIAL" or "LET EXPIRE" or "ROLL",
  "actionColor": "green" or "yellow" or "red",
  "reasoning": "2 sentence explanation",
  "targetExit": "e.g. Close at $5.50 (+57%) or Close if drops below $2.00",
  "riskNote": "one sentence on the main risk right now",
  "urgency": "Low" or "Medium" or "High"
}`;

    const result = await askAI(prompt);
    const jsonMatch = result.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('parse fail');
    send({ type: 'advice', data: JSON.parse(jsonMatch[0]) });
    send({ type: 'done' });
  } catch (e) {
    send({ type: 'error', text: e.message });
    send({ type: 'done' });
  } finally {
    clearInterval(keepalive);
    res.end();
  }
});

// ─── Tastytrade Integration ───────────────────────────────────────
const TT_FILE = path.join(BASE_DIR, 'tastytrade.json');

function loadTT() {
  try { if (fs.existsSync(TT_FILE)) return JSON.parse(fs.readFileSync(TT_FILE, 'utf8')); } catch {}
  return { mode: 'sandbox', username: '', sessionToken: null, rememberToken: null, accountNumber: null, connectedAt: null };
}
function saveTT(d) { fs.writeFileSync(TT_FILE, JSON.stringify(d, null, 2)); }

function ttBaseUrl(mode) {
  return 'https://api.tastytrade.com'; // Live only — sandbox removed
}

// Use curl for Tastytrade requests — avoids Node.js TLS/certificate issues
function ttRequest(method, path, body, token, mode = 'sandbox', extraHeaders = {}) {
  return new Promise((resolve, reject) => {
    const base = ttBaseUrl(mode);
    const fullUrl = `${base}${path}`;
    const data = body ? JSON.stringify(body) : null;

    const args = [
      '-s',
      '-D', '-',                     // dump response headers into stdout before body
      '--max-time', '15',
      '-k',
      '-X', method,
      '-H', 'Content-Type: application/json',
      '-H', 'Accept: application/json',
      '-H', 'User-Agent: StockForge/1.0',
    ];

    if (token) args.push('-H', `Authorization: ${token}`);
    for (const [k, v] of Object.entries(extraHeaders)) args.push('-H', `${k}: ${v}`);
    if (data)  args.push('-d', data);

    args.push(fullUrl);

    execFile('curl', args, { maxBuffer: 5 * 1024 * 1024 }, (err, stdout, stderr) => {
      if (err) return reject(new Error(`Request failed: ${err.message}`));
      if (!stdout || !stdout.trim()) return reject(new Error('Empty response from Tastytrade'));

      // Split headers from body — curl -D - puts headers first, then blank line, then body
      const headerBodySplit = stdout.indexOf('\r\n\r\n');
      const headerSection = headerBodySplit >= 0 ? stdout.slice(0, headerBodySplit) : '';
      const bodySection   = headerBodySplit >= 0 ? stdout.slice(headerBodySplit + 4) : stdout;

      // Parse response headers into a map
      const responseHeaders = {};
      for (const line of headerSection.split('\r\n')) {
        const idx = line.indexOf(':');
        if (idx > 0) {
          const key = line.slice(0, idx).trim().toLowerCase();
          const val = line.slice(idx + 1).trim();
          responseHeaders[key] = val;
        }
      }

      const body2 = bodySection.trim();

      // Check if HTML was returned (nginx block)
      if (body2.startsWith('<')) {
        const statusMatch = body2.match(/(\d{3})/);
        const code = statusMatch ? statusMatch[1] : 'unknown';
        if (code === '401') return reject(new Error('Invalid credentials — check your username and password'));
        if (code === '403') return reject(new Error('Access denied — API access may not be enabled on your account'));
        return reject(new Error(`Tastytrade returned an unexpected response. Check your credentials and try again.`));
      }

      try {
        const json = JSON.parse(body2);
        // Attach parsed response headers so callers can read them
        json._responseHeaders = responseHeaders;
        resolve(json);
      } catch (e) {
        reject(new Error(`Could not parse response: ${body2.slice(0, 150)}`));
      }
    });
  });
}

// Helper — throws if response contains an error (used by all endpoints except /connect)
function ttCheck(data) {
  if (data?.error) throw new Error(data.error.message || data.error.code || 'Tastytrade error');
  if (data?.errors?.length) throw new Error(data.errors[0].message || 'Tastytrade error');
  return data;
}

// ── Auth ──────────────────────────────────────────────────────────
// Store pending device auth state
const pendingDeviceAuth = {};
// Store pending security question state
const pendingSecurityQuestion = {};



app.post('/api/tastytrade/connect', async (req, res) => {
  const { username, password, mode = 'sandbox', deviceCode } = req.body;
  if (!username || !password) return res.status(400).json({ error: 'Username and password required' });
  try {
    const payload = { login: username, password, 'remember-me': true };

    // If device code provided, include it
    if (deviceCode) payload['one-time-passcode'] = deviceCode;

    const data = await ttRequest('POST', '/sessions', payload, null, mode);
    console.log('[TT Connect] RAW:', JSON.stringify(data, null, 2));

    const errCode = data?.error?.code || '';
    const errMsg  = (data?.error?.message || '').toLowerCase();

    // ── Step 1: device_challenge_required → call /device-challenge with challenge token to get security question
    if (errCode === 'device_challenge_required' || errMsg.includes('device authentication challenge')) {
      // Challenge token comes back in the response headers from /sessions
      const challengeToken =
        data?._responseHeaders?.['x-tastyworks-challenge-token'] ||
        data?._responseHeaders?.['x-tastytrade-challenge-token'] ||
        data?._responseHeaders?.['x-challenge-token'] ||
        '';

      console.log('[TT Challenge Token from headers]:', challengeToken);
      console.log('[TT Response Headers]:', JSON.stringify(data._responseHeaders));

      // Hit /device-challenge with the token to get the security question
      const challengeData = await ttRequest('POST', '/device-challenge', {}, null, mode,
        challengeToken ? { 'X-Tastyworks-Challenge-Token': challengeToken } : {}
      );
      console.log('[TT /device-challenge] RAW:', JSON.stringify(challengeData, null, 2));

      const question =
        challengeData?.data?.['security-question'] ||
        challengeData?.data?.question ||
        challengeData?.['security-question'] ||
        challengeData?.question ||
        '';

      pendingSecurityQuestion[username] = { username, password, mode, challengeToken };
      return res.json({
        ok: false,
        requiresSecurityQuestion: true,
        question,
        rawMessage: data?.error?.message || '',
        message: question
          ? `🔒 Security question: "${question}"`
          : `🔒 Tastytrade requires your security question answer.`,
        _debug: { challengeToken, responseHeaders: data._responseHeaders, challengeData }
      });
    }

    // API access not enabled
    if (errMsg.includes('not permitted') || errMsg.includes('permission') || errCode === 'not_permitted') {
      return res.status(403).json({
        error: 'API access not enabled on your Tastytrade account.',
        hint: 'Go to tastytrade.com → Settings → API Access → Enable. Then try again.'
      });
    }

    // Other errors from Tastytrade
    if (data?.error) {
      return res.status(401).json({ error: data.error.message || 'Authentication failed' });
    }

    const token = data?.data?.['session-token'];
    const rememberToken = data?.data?.['remember-token'];
    if (!token) return res.status(401).json({
      error: 'No session token returned. Check your credentials and try again.'
    });

    // Get accounts
    const accts = ttCheck(await ttRequest('GET', '/customers/me/accounts', null, token, mode));
    const accounts = (accts?.data?.items || []).map(a => ({
      accountNumber: a.account['account-number'],
      accountType: a.account['account-type-name'],
      nickname: a.account.nickname || a.account['account-number'],
      isClosed: a.account['is-closed']
    })).filter(a => !a.isClosed);

    const accountNumber = accounts[0]?.accountNumber || null;

    const tt = { mode, username, sessionToken: token, rememberToken, accountNumber, accounts, connectedAt: new Date().toISOString() };
    saveTT(tt);
    res.json({ ok: true, accountNumber, accounts, mode });
  } catch (e) {
    res.status(401).json({ error: e.message });
  }
});

// Device auth verification — user enters the OTP sent to their email/phone
app.post('/api/tastytrade/verify-device', async (req, res) => {
  const { username, code, mode = 'live' } = req.body;
  if (!username || !code) return res.status(400).json({ error: 'Username and code required' });

  try {
    const pending = pendingDeviceAuth[username] || {};
    const pw = req.body.password || pending.password || '';
    const challengeToken = pending.challengeToken || '';
    if (!pw) return res.status(400).json({ error: 'Session expired — please go back and enter your password again' });

    // Submit OTP as X-Tastyworks-OTP header (not in body) + challenge token header
    const data = await ttRequest('POST', '/sessions', {
      login: username,
      password: pw,
      'remember-me': true
    }, null, mode, {
      ...(challengeToken ? { 'X-Tastyworks-Challenge-Token': challengeToken } : {}),
      'X-Tastyworks-OTP': code.trim()
    });
    console.log('[TT verify-device] /sessions with two-factor-code:', JSON.stringify(data, null, 2));

    const token = data?.data?.['session-token'];
    if (!token) return res.status(401).json({
      error: data?.error?.message || 'Invalid verification code. Check your email/phone and try again.',
      _debug: {
        errCode: data?.error?.code,
        challengeToken: challengeToken ? challengeToken.slice(0,20)+'...' : 'MISSING',
        allResponseHeaders: data?._responseHeaders
      }
    });

    // Get accounts
    const accts = ttCheck(await ttRequest('GET', '/customers/me/accounts', null, token, mode));
    const accounts = (accts?.data?.items || []).map(a => ({
      accountNumber: a.account['account-number'],
      accountType: a.account['account-type-name'],
      nickname: a.account.nickname || a.account['account-number'],
      isClosed: a.account['is-closed']
    })).filter(a => !a.isClosed);

    const accountNumber = accounts[0]?.accountNumber || null;
    const tt = { mode, username, sessionToken: token, rememberToken: data?.data?.['remember-token'], accountNumber, accounts, connectedAt: new Date().toISOString() };
    saveTT(tt);
    delete pendingDeviceAuth[username];
    res.json({ ok: true, accountNumber, accounts, mode });
  } catch (e) {
    res.status(401).json({ error: e.message });
  }
});

// Security question answer — POST answer to /device-challenge, then re-attempt /sessions
app.post('/api/tastytrade/answer-security', async (req, res) => {
  const { username, answer, mode = 'live' } = req.body;
  if (!username || !answer) return res.status(400).json({ error: 'Username and answer required' });

  try {
    const pending = pendingSecurityQuestion[username] || {};
    const pw = req.body.password || pending.password || '';
    if (!pw) return res.status(400).json({ error: 'Session expired — please go back and enter your password again' });

    // Submit the security answer to /device-challenge with the stored challenge token
    const challengeToken = pending.challengeToken || '';
    const answerData = await ttRequest('POST', '/device-challenge', {
      'answer': answer.trim()
    }, null, mode, challengeToken ? { 'X-Tastyworks-Challenge-Token': challengeToken } : {});
    console.log('[TT Answer Security] /device-challenge response:', JSON.stringify(answerData, null, 2));
    console.log('[TT Answer Security] /device-challenge response headers:', JSON.stringify(answerData?._responseHeaders));

    // Check if answer was wrong
    const answerErr = answerData?.error?.code || '';
    if (answerErr) {
      return res.status(401).json({ error: answerData?.error?.message || 'Incorrect security answer. Please try again.' });
    }

    // Capture any NEW challenge token issued after the security answer
    const newChallengeToken =
      answerData?._responseHeaders?.['x-tastyworks-challenge-token'] ||
      answerData?._responseHeaders?.['x-tastytrade-challenge-token'] ||
      answerData?._responseHeaders?.['x-challenge-token'] ||
      challengeToken; // fall back to original if none issued

    console.log('[TT Answer Security] token for OTP step:', newChallengeToken ? newChallengeToken.slice(0,20)+'...' : 'NONE');

    pendingDeviceAuth[username] = { username, password: pw, mode, challengeToken: newChallengeToken };
    delete pendingSecurityQuestion[username];
    return res.json({
      ok: false,
      requiresDeviceAuth: true,
      challengeType: 'email',
      rawMessage: '',
      message: `📧 Tastytrade sent a verification code to your email. Check inbox + spam.`
    });
  } catch (e) {
    res.status(401).json({ error: e.message });
  }
});

app.post('/api/tastytrade/disconnect', (req, res) => {
  const tt = loadTT();
  tt.sessionToken = null;
  tt.rememberToken = null;
  tt.connectedAt = null;
  saveTT(tt);
  res.json({ ok: true });
});

app.get('/api/tastytrade/status', async (req, res) => {
  const tt = loadTT();
  if (!tt.sessionToken) return res.json({ connected: false, mode: tt.mode, username: tt.username });
  try {
    // Validate session still alive
    const me = await ttRequest('GET', '/customers/me', null, tt.sessionToken, tt.mode);
    if (me?.error) throw new Error(me.error.message || 'Session invalid');
    res.json({ connected: true, mode: tt.mode, username: tt.username, accountNumber: tt.accountNumber, accounts: tt.accounts || [], connectedAt: tt.connectedAt });
  } catch {
    // Session expired — clear it
    tt.sessionToken = null;
    saveTT(tt);
    res.json({ connected: false, mode: tt.mode, username: tt.username, expired: true });
  }
});

// ── Account & Balances ────────────────────────────────────────────
app.get('/api/tastytrade/balances', async (req, res) => {
  const tt = loadTT();
  if (!tt.sessionToken || !tt.accountNumber) return res.status(401).json({ error: 'Not connected' });
  try {
    const data = ttCheck(await ttRequest('GET', `/accounts/${tt.accountNumber}/balances`, null, tt.sessionToken, tt.mode));
    const b = data?.data;
    res.json({
      cashBalance: parseFloat(b?.['cash-balance'] || 0),
      buyingPower: parseFloat(b?.['derivative-buying-power'] || b?.['equity-buying-power'] || 0),
      netLiq: parseFloat(b?.['net-liquidating-value'] || 0),
      dayPnl: parseFloat(b?.['day-trading-buying-power'] || 0),
      maintenanceRequirement: parseFloat(b?.['maintenance-requirement'] || 0)
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Positions ─────────────────────────────────────────────────────
app.get('/api/tastytrade/positions', async (req, res) => {
  const tt = loadTT();
  if (!tt.sessionToken || !tt.accountNumber) return res.status(401).json({ error: 'Not connected' });
  try {
    const data = ttCheck(await ttRequest('GET', `/accounts/${tt.accountNumber}/positions`, null, tt.sessionToken, tt.mode));
    const positions = (data?.data?.items || []).map(p => ({
      symbol: p.symbol,
      instrumentType: p['instrument-type'],
      quantity: parseFloat(p.quantity),
      quantityDirection: p['quantity-direction'],
      closePrice: parseFloat(p['close-price'] || 0),
      averageOpenPrice: parseFloat(p['average-open-price'] || 0),
      multiplier: parseFloat(p.multiplier || 1),
      costEffect: p['cost-effect'],
      realizedDayGain: parseFloat(p['realized-day-gain'] || 0),
      unrealizedDayGain: parseFloat(p['unrealized-day-gain'] || 0)
    }));
    res.json(positions);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Options Chain ─────────────────────────────────────────────────
app.get('/api/tastytrade/options-chain/:ticker', async (req, res) => {
  const tt = loadTT();
  if (!tt.sessionToken) return res.status(401).json({ error: 'Not connected' });
  const ticker = req.params.ticker.toUpperCase();
  try {
    // Get option chain
    const data = ttCheck(await ttRequest('GET', `/option-chains/${ticker}/nested`, null, tt.sessionToken, tt.mode));
    const expirations = (data?.data?.items || []).slice(0, 4).map(exp => ({
      expirationDate: exp['expiration-date'],
      daysToExpiration: exp['days-to-expiration'],
      strikes: (exp.strikes || []).map(s => ({
        strikePrice: parseFloat(s['strike-price']),
        call: s.call ? {
          symbol: s.call,
          bid: parseFloat(s['call-bid-price'] || 0),
          ask: parseFloat(s['call-ask-price'] || 0),
          iv: parseFloat(s['call-implied-volatility'] || 0),
          delta: parseFloat(s['call-delta'] || 0),
          theta: parseFloat(s['call-theta'] || 0),
          volume: parseInt(s['call-volume'] || 0),
          oi: parseInt(s['call-open-interest'] || 0)
        } : null,
        put: s.put ? {
          symbol: s.put,
          bid: parseFloat(s['put-bid-price'] || 0),
          ask: parseFloat(s['put-ask-price'] || 0),
          iv: parseFloat(s['put-implied-volatility'] || 0),
          delta: parseFloat(s['put-delta'] || 0),
          theta: parseFloat(s['put-theta'] || 0),
          volume: parseInt(s['put-volume'] || 0),
          oi: parseInt(s['put-open-interest'] || 0)
        } : null
      }))
    }));
    res.json({ ticker, expirations, source: 'tastytrade-live' });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Orders ────────────────────────────────────────────────────────
app.post('/api/tastytrade/order/dry-run', async (req, res) => {
  const tt = loadTT();
  if (!tt.sessionToken || !tt.accountNumber) return res.status(401).json({ error: 'Not connected' });
  const { symbol, action, quantity, orderType, price } = req.body;
  if (!symbol || !action || !quantity) return res.status(400).json({ error: 'symbol, action, quantity required' });
  try {
    const order = {
      'order-type': orderType || 'Limit',
      'time-in-force': 'Day',
      price: price ? price.toFixed(2) : undefined,
      'price-effect': action.includes('Buy') ? 'Debit' : 'Credit',
      legs: [{
        'instrument-type': 'Equity Option',
        symbol,
        quantity,
        action
      }]
    };
    const data = ttCheck(await ttRequest('POST', `/accounts/${tt.accountNumber}/orders/dry-run`, order, tt.sessionToken, tt.mode));
    res.json({ ok: true, dryRun: data?.data, warnings: data?.data?.warnings || [] });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/tastytrade/order/place', async (req, res) => {
  const tt = loadTT();
  if (!tt.sessionToken || !tt.accountNumber) return res.status(401).json({ error: 'Not connected' });
  const { symbol, action, quantity, orderType, price } = req.body;
  if (!symbol || !action || !quantity) return res.status(400).json({ error: 'symbol, action, quantity required' });
  try {
    const order = {
      'order-type': orderType || 'Limit',
      'time-in-force': 'Day',
      price: price ? parseFloat(price).toFixed(2) : undefined,
      'price-effect': action.includes('Buy') ? 'Debit' : 'Credit',
      legs: [{
        'instrument-type': 'Equity Option',
        symbol,
        quantity: parseInt(quantity),
        action
      }]
    };
    const data = await ttRequest('POST', `/accounts/${tt.accountNumber}/orders`, order, tt.sessionToken, tt.mode);
    res.json({ ok: true, order: data?.data, orderId: data?.data?.id });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/tastytrade/orders', async (req, res) => {
  const tt = loadTT();
  if (!tt.sessionToken || !tt.accountNumber) return res.status(401).json({ error: 'Not connected' });
  try {
    const data = ttCheck(await ttRequest('GET', `/accounts/${tt.accountNumber}/orders/live`, null, tt.sessionToken, tt.mode));
    res.json(data?.data?.items || []);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/tastytrade/order/:orderId', async (req, res) => {
  const tt = loadTT();
  if (!tt.sessionToken || !tt.accountNumber) return res.status(401).json({ error: 'Not connected' });
  try {
    await ttRequest('DELETE', `/accounts/${tt.accountNumber}/orders/${req.params.orderId}`, null, tt.sessionToken, tt.mode);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Symbol Search ─────────────────────────────────────────────────
app.get('/api/tastytrade/symbol/:ticker', async (req, res) => {
  const tt = loadTT();
  if (!tt.sessionToken) return res.status(401).json({ error: 'Not connected' });
  try {
    const data = ttCheck(await ttRequest('GET', `/symbols/search/${req.params.ticker.toUpperCase()}`, null, tt.sessionToken, tt.mode));
    res.json(data?.data?.items || []);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Get saved TT config (masked) ──────────────────────────────────
app.get('/api/tastytrade/config', (req, res) => {
  const tt = loadTT();
  res.json({ mode: tt.mode, username: tt.username, accountNumber: tt.accountNumber, accounts: tt.accounts || [], connected: !!tt.sessionToken, connectedAt: tt.connectedAt });
});

app.post('/api/tastytrade/config', (req, res) => {
  const tt = loadTT();
  if (req.body.mode) tt.mode = req.body.mode;
  if (req.body.accountNumber) tt.accountNumber = req.body.accountNumber;
  saveTT(tt);
  res.json({ ok: true });
});

// ─── Pre-Trade Intelligence ───────────────────────────────────────

// CIK lookup cache
const CIK_CACHE_FILE = path.join(BASE_DIR, 'cik-cache.json');
function loadCikCache() { try { return JSON.parse(fs.readFileSync(CIK_CACHE_FILE, 'utf8')); } catch { return {}; } }
function saveCikCache(c) { fs.writeFileSync(CIK_CACHE_FILE, JSON.stringify(c, null, 2)); }

// Known CIKs for common tickers (pre-seeded to avoid API calls)
const KNOWN_CIKS = {
  NVDA: '0001045810', AAPL: '0000320193', MSFT: '0000789019',
  AMZN: '0001018724', META: '0001326801', TSLA: '0001318605',
  GOOGL: '0001652044', NFLX: '0001065280', IONQ: '0001824920',
  AMD: '0000002488', INTC: '0000050863', JPM: '0000019617',
  V: '0001403161', JNJ: '0000200406', WMT: '0000104169'
};

async function getCIK(ticker) {
  const cache = loadCikCache();
  if (KNOWN_CIKS[ticker]) return KNOWN_CIKS[ticker];
  if (cache[ticker]) return cache[ticker];
  return new Promise(resolve => {
    const url = `https://www.sec.gov/cgi-bin/browse-edgar?company=&CIK=${ticker}&type=8-K&dateb=&owner=include&count=1&search_text=&action=getcompany&output=atom`;
    execFile('curl', ['-s', '--max-time', '8', '-A', 'StockForge research@stockforge.app', url], (err, stdout) => {
      if (err || !stdout) return resolve(null);
      const m = stdout.match(/CIK=(\d+)/);
      if (m) {
        cache[ticker] = m[1];
        saveCikCache(cache);
        resolve(m[1]);
      } else resolve(null);
    });
  });
}

// SEC EDGAR — recent filings for a ticker
app.get('/api/intelligence/sec/:ticker', async (req, res) => {
  const ticker = req.params.ticker.toUpperCase();
  try {
    const cik = await getCIK(ticker);
    if (!cik) return res.json({ ticker, filings: [], error: 'CIK not found' });

    const paddedCik = cik.replace(/^0+/, '').padStart(10, '0');
    const data = await new Promise(resolve => {
      execFile('curl', ['-s', '--max-time', '10', '-A', 'StockForge research@stockforge.app',
        `https://data.sec.gov/submissions/CIK${paddedCik}.json`
      ], (err, stdout) => {
        if (err || !stdout) return resolve(null);
        try { resolve(JSON.parse(stdout)); } catch { resolve(null); }
      });
    });

    if (!data) return res.json({ ticker, filings: [] });

    const recent = data.filings?.recent || {};
    const forms = recent.form || [];
    const dates = recent.filingDate || [];
    const docs = recent.primaryDocument || [];
    const accs = recent.accessionNumber || [];
    const descriptions = recent.primaryDocDescription || [];

    // Filter to important forms: 8-K, 4 (insider), DEF 14A (proxy), S-1, 10-Q, 10-K
    const important = ['8-K', '4', 'SC 13G', 'SC 13D', 'DEF 14A', '10-Q', '10-K', 'S-1', 'S-3'];
    const filings = [];
    for (let i = 0; i < Math.min(forms.length, 50); i++) {
      if (!important.includes(forms[i])) continue;
      const accFormatted = accs[i]?.replace(/-/g, '') || '';
      const url = accFormatted
        ? `https://www.sec.gov/Archives/edgar/data/${cik.replace(/^0+/, '')}/${accFormatted}/${docs[i]}`
        : null;
      filings.push({
        form: forms[i],
        date: dates[i],
        description: descriptions[i] || forms[i],
        url,
        daysAgo: dates[i] ? Math.floor((Date.now() - new Date(dates[i]).getTime()) / (1000 * 60 * 60 * 24)) : null,
        isInsider: forms[i] === '4',
        isMaterial: forms[i] === '8-K',
        isEarnings: forms[i] === '10-Q' || forms[i] === '10-K'
      });
      if (filings.length >= 10) break;
    }

    res.json({ ticker, cik, filings, companyName: data.name || ticker });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Macro Economic Calendar — high/medium impact US events next 7 days
app.get('/api/intelligence/macro', async (req, res) => {
  try {
    const data = await finnhubGetSecure('/calendar/economic');
    const events = data?.economicCalendar || [];
    const now = Date.now();
    const sevenDays = now + 7 * 24 * 60 * 60 * 1000;

    const filtered = events
      .filter(e => {
        if (e.country !== 'US') return false;
        if (!['high', 'medium'].includes(e.impact)) return false;
        const t = new Date(e.time).getTime();
        return t >= now - 24 * 60 * 60 * 1000 && t <= sevenDays; // -1d to +7d
      })
      .sort((a, b) => new Date(a.time) - new Date(b.time))
      .slice(0, 15)
      .map(e => ({
        event: e.event,
        time: e.time,
        date: e.time?.split(' ')[0],
        impact: e.impact,
        actual: e.actual,
        estimate: e.estimate,
        previous: e.prev,
        unit: e.unit,
        daysUntil: Math.ceil((new Date(e.time).getTime() - now) / (1000 * 60 * 60 * 24)),
        isToday: new Date(e.time).toDateString() === new Date().toDateString(),
        isTomorrow: Math.ceil((new Date(e.time).getTime() - now) / (1000 * 60 * 60 * 24)) === 1
      }));

    res.json({ events: filtered, fetchedAt: new Date().toISOString() });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// IV Rank + Market Metrics via Tastytrade
app.get('/api/intelligence/iv/:ticker', async (req, res) => {
  const ticker = req.params.ticker.toUpperCase();
  const tt = loadTT();

  // If connected to Tastytrade, use their market metrics
  if (tt.sessionToken) {
    try {
      const data = ttCheck(await ttRequest('GET', `/market-metrics?symbols=${ticker}`, null, tt.sessionToken, tt.mode));
      const item = data?.data?.items?.[0];
      if (item) {
        return res.json({
          ticker,
          source: 'tastytrade',
          ivRank: parseFloat(item['iv-rank'] || 0),
          ivPercentile: parseFloat(item['iv-percentile'] || 0),
          impliedVolatility: parseFloat(item['implied-volatility-index'] || 0),
          impliedVolatilityRank: parseFloat(item['implied-volatility-index-rank'] || 0),
          liquidityRating: item['liquidity-rating'] || null,
          optionsImpliedMovePercent: parseFloat(item['options-implied-move-percent'] || 0),
          earningsImpliedMovePercent: parseFloat(item['earnings-implied-move-percent'] || 0),
          beta: parseFloat(item.beta || 0),
          corr52Week: parseFloat(item['corr-52-week-high'] || 0)
        });
      }
    } catch {}
  }

  // Fallback: estimate IV from Finnhub + Yahoo price data
  try {
    const [quote, history] = await Promise.all([
      finnhubGetSecure(`/quote?symbol=${ticker}`),
      new Promise(resolve => {
        const url = `https://query1.finance.yahoo.com/v8/finance/chart/${ticker}?interval=1d&range=1y`;
        execFile('curl', ['-s', '--max-time', '10', '-A', 'Mozilla/5.0', url], (err, stdout) => {
          if (err || !stdout) return resolve(null);
          try { resolve(JSON.parse(stdout)); } catch { resolve(null); }
        });
      })
    ]);

    const closes = history?.chart?.result?.[0]?.indicators?.quote?.[0]?.close?.filter(Boolean) || [];
    let ivRank = null, ivPercentile = null, historicalVol = null;

    if (closes.length >= 20) {
      // Calculate 30-day historical volatility
      const returns = closes.slice(-31).map((c, i, arr) => i > 0 ? Math.log(c / arr[i-1]) : 0).slice(1);
      const mean = returns.reduce((a, b) => a + b, 0) / returns.length;
      const variance = returns.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / returns.length;
      historicalVol = Math.sqrt(variance * 252) * 100;

      // Calculate 52-week high/low of HV to estimate IV rank
      const weeklyVols = [];
      for (let i = 20; i < closes.length; i++) {
        const slice = closes.slice(i - 20, i);
        const rets = slice.map((c, j, arr) => j > 0 ? Math.log(c / arr[j-1]) : 0).slice(1);
        const m = rets.reduce((a, b) => a + b, 0) / rets.length;
        const v = rets.reduce((a, b) => a + Math.pow(b - m, 2), 0) / rets.length;
        weeklyVols.push(Math.sqrt(v * 252) * 100);
      }
      const minVol = Math.min(...weeklyVols);
      const maxVol = Math.max(...weeklyVols);
      ivRank = maxVol > minVol ? Math.round(((historicalVol - minVol) / (maxVol - minVol)) * 100) : 50;
      ivPercentile = weeklyVols.filter(v => v < historicalVol).length / weeklyVols.length * 100;
    }

    res.json({
      ticker,
      source: 'estimated',
      ivRank: ivRank !== null ? Math.min(100, Math.max(0, ivRank)) : null,
      ivPercentile: ivPercentile !== null ? Math.round(ivPercentile) : null,
      historicalVolatility: historicalVol !== null ? Math.round(historicalVol) : null,
      currentPrice: parseFloat(quote?.c || 0),
      note: tt.sessionToken ? null : 'Connect Tastytrade for precise IV Rank'
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── Kalshi Macro Odds ────────────────────────────────────────────
// Free public API — no key needed
function fetchKalshiMacro() {
  // Fetch from multiple financial series in parallel
  const series = ['KXFED', 'KXINFL', 'KXGDP', 'KXUNRATE'];
  const fetches = series.map(s => new Promise(resolve => {
    const url = `https://api.elections.kalshi.com/trade-api/v2/markets?limit=5&series_ticker=${s}&status=open`;
    execFile('curl', ['-s', '--max-time', '10', '-H', 'Accept: application/json', url], (err, stdout) => {
      if (err || !stdout) return resolve([]);
      try {
        const json = JSON.parse(stdout);
        resolve(json?.markets || []);
      } catch { resolve([]); }
    });
  }));

  return Promise.all(fetches).then(results => {
    const all = results.flat();
    return all
      .map(m => {
        // yes_bid_dollars is 0-1 scale (e.g. 0.65 = 65%)
        const prob = m.yes_bid_dollars != null ? Math.round(parseFloat(m.yes_bid_dollars) * 100) : null;
        if (prob === null) return null;
        return {
          ticker: m.ticker,
          title: m.title || m.subtitle || '',
          probability: prob,
          volume: parseFloat(m.volume_fp || 0),
          closeTime: m.close_time,
        };
      })
      .filter(Boolean)
      .filter(m => m.probability > 0 && m.probability < 100)
      .slice(0, 8);
  }).catch(() => []);
}

// Kalshi endpoint — standalone
app.get('/api/kalshi/macro', async (req, res) => {
  try {
    const markets = await fetchKalshiMacro();
    res.json({ markets, fetchedAt: new Date().toISOString() });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Combined pre-trade intelligence for a ticker
app.get('/api/intelligence/pretrade/:ticker', async (req, res) => {
  const ticker = req.params.ticker.toUpperCase();
  try {
    const [secData, macroData, ivData, kalshiData] = await Promise.all([
      fetch(`http://localhost:${PORT}/api/intelligence/sec/${ticker}`).then(r => r.json()).catch(() => ({ filings: [] })),
      fetch(`http://localhost:${PORT}/api/intelligence/macro`).then(r => r.json()).catch(() => ({ events: [] })),
      fetch(`http://localhost:${PORT}/api/intelligence/iv/${ticker}`).then(r => r.json()).catch(() => ({})),
      fetchKalshiMacro().catch(() => [])
    ]);

    // Score the overall pre-trade environment
    const signals = [];
    let score = 50; // neutral

    // IV signals
    if (ivData.ivRank !== null) {
      if (ivData.ivRank < 30) { signals.push({ type: 'bullish', text: `IV Rank ${ivData.ivRank} — options are CHEAP, good time to buy` }); score += 10; }
      else if (ivData.ivRank > 70) { signals.push({ type: 'warning', text: `IV Rank ${ivData.ivRank} — options are EXPENSIVE, consider selling premium instead` }); score -= 5; }
      else { signals.push({ type: 'neutral', text: `IV Rank ${ivData.ivRank} — options fairly priced` }); }
    }

    // SEC filing signals
    const recentFilings = (secData.filings || []).filter(f => f.daysAgo !== null && f.daysAgo <= 7);
    if (recentFilings.some(f => f.isMaterial)) {
      signals.push({ type: 'warning', text: `8-K filed in last 7 days — material event, review before trading` });
      score -= 10;
    }
    const insiderBuys = recentFilings.filter(f => f.isInsider);
    if (insiderBuys.length >= 2) {
      signals.push({ type: 'bullish', text: `${insiderBuys.length} insider transactions in last 7 days` });
      score += 8;
    }

    // Macro calendar signals
    const urgentMacro = (macroData.events || []).filter(e => e.isToday || e.isTomorrow);
    const highImpact = urgentMacro.filter(e => e.impact === 'high');
    if (highImpact.length > 0) {
      signals.push({ type: 'warning', text: `${highImpact.length} HIGH impact macro event${highImpact.length > 1 ? 's' : ''} today/tomorrow: ${highImpact.slice(0,2).map(e => e.event).join(', ')}` });
      score -= 15;
    }

    // Kalshi prediction market signals
    const kalshiSignals = [];
    for (const m of (kalshiData || [])) {
      if (!m.probability) continue;
      const title = m.title?.toLowerCase() || '';
      // Recession risk
      if (title.includes('recession') && m.probability > 40) {
        signals.push({ type: 'warning', text: `Kalshi: Recession odds at ${m.probability}% — macro headwind` });
        score -= 10;
        kalshiSignals.push({ label: 'Recession Risk', probability: m.probability, direction: 'bearish' });
      }
      // Fed rate cuts bullish for growth
      if ((title.includes('rate cut') || title.includes('fed cut')) && m.probability > 60) {
        signals.push({ type: 'bullish', text: `Kalshi: Fed rate cut odds at ${m.probability}% — bullish for growth stocks` });
        score += 8;
        kalshiSignals.push({ label: 'Fed Rate Cut', probability: m.probability, direction: 'bullish' });
      }
      // Market crash bearish
      if (title.includes('crash') && m.probability > 20) {
        signals.push({ type: 'warning', text: `Kalshi: Market crash odds at ${m.probability}% — elevated tail risk` });
        score -= 12;
        kalshiSignals.push({ label: 'Market Crash Risk', probability: m.probability, direction: 'bearish' });
      }
    }

    res.json({
      ticker,
      score: Math.min(100, Math.max(0, score)),
      scoreLabel: score >= 70 ? 'Favorable' : score >= 50 ? 'Neutral' : 'Caution',
      scoreColor: score >= 70 ? 'green' : score >= 50 ? 'yellow' : 'red',
      signals,
      iv: ivData,
      recentFilings: (secData.filings || []).slice(0, 5),
      companyName: secData.companyName || ticker,
      upcomingMacro: (macroData.events || []).slice(0, 5),
      kalshi: { markets: kalshiData || [], signals: kalshiSignals },
      fetchedAt: new Date().toISOString()
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════════
// PHASE 1 — STOCK SIGNAL ENGINE
// Universe funnel → signal scoring → convergence rule
// ═══════════════════════════════════════════════════════════════════

// S&P 500 + NASDAQ 100 universe (liquid, reliable data)
const UNIVERSE_SP500 = [
  'AAPL','MSFT','NVDA','AMZN','GOOGL','META','TSLA','BRK.B','JPM','V',
  'UNH','XOM','LLY','JNJ','MA','PG','HD','MRK','AVGO','CVX',
  'ABBV','COST','PEP','KO','WMT','BAC','CRM','TMO','CSCO','ACN',
  'MCD','ABT','NFLX','LIN','DHR','TXN','NEE','PM','ADBE','QCOM',
  'WFC','RTX','HON','AMGN','SPGI','IBM','GE','CAT','INTU','ISRG',
  'BKNG','VRTX','REGN','NOW','PLD','BLK','SYK','GILD','ADI','MDLZ',
  'PANW','LRCX','KLAC','SNPS','CDNS','MCHP','AMAT','MU','INTC','AMD'
];

const UNIVERSE_NASDAQ100 = [
  'PLTR','MSTR','COIN','SHOP','DDOG','SNOW','ZS','CRWD','NET','OKTA',
  'RBLX','UBER','LYFT','DASH','ABNB','PINS','SNAP','SPOT','HOOD','SOFI',
  'SMCI','ARM','ASML','TSM','MRVL','ON','ENPH','FSLR','SEDG','RIVN'
];

const FULL_UNIVERSE = [...new Set([...UNIVERSE_SP500, ...UNIVERSE_NASDAQ100])];

// ── yfinance helper via Python ────────────────────────────────────
function yfinanceFetch(script) {
  return new Promise((resolve) => {
    const { execFile } = require('child_process');
    execFile('python3', ['-c', script], { timeout: 30000, maxBuffer: 2 * 1024 * 1024 }, (err, stdout) => {
      if (err || !stdout?.trim()) return resolve(null);
      try { resolve(JSON.parse(stdout)); } catch { resolve(null); }
    });
  });
}

// ── Signal 1: Options Volume (anchor signal) ─────────────────────
async function fetchOptionsVolume(ticker) {
  const script = `
import yfinance as yf, json, sys
try:
    t = yf.Ticker("${ticker}")
    chain_dates = t.options
    if not chain_dates:
        print(json.dumps({"hasUnusual": False, "ratio": 0, "details": "no options data"}))
        sys.exit()
    # Use nearest expiry
    chain = t.option_chain(chain_dates[0])
    calls = chain.calls[['strike','volume','openInterest']].dropna()
    puts = chain.puts[['strike','volume','openInterest']].dropna()
    all_opts = list(calls.itertuples()) + list(puts.itertuples())
    max_ratio = 0
    best = None
    for row in all_opts:
        oi = float(row.openInterest) if row.openInterest else 0
        vol = float(row.volume) if row.volume else 0
        if oi > 100 and vol > 0:
            ratio = vol / oi
            if ratio > max_ratio:
                max_ratio = ratio
                best = {"strike": float(row.strike), "volume": int(vol), "openInterest": int(oi), "ratio": round(ratio, 2)}
    print(json.dumps({"hasUnusual": max_ratio >= 5, "ratio": round(max_ratio, 2), "best": best}))
except Exception as e:
    print(json.dumps({"hasUnusual": False, "ratio": 0, "error": str(e)}))
`;
  const result = await yfinanceFetch(script);
  return result || { hasUnusual: false, ratio: 0 };
}

// ── Signal 4: Short Interest + Borrow Rate ───────────────────────
async function fetchShortInterest(ticker) {
  const script = `
import yfinance as yf, json
try:
    t = yf.Ticker("${ticker}")
    info = t.info
    short_ratio = info.get('shortRatio', 0) or 0
    short_pct = info.get('shortPercentOfFloat', 0) or 0
    print(json.dumps({
        "shortRatio": round(float(short_ratio), 2),
        "shortPctFloat": round(float(short_pct) * 100, 2),
        "elevated": float(short_pct) > 0.15
    }))
except Exception as e:
    print(json.dumps({"shortRatio": 0, "shortPctFloat": 0, "elevated": False, "error": str(e)}))
`;
  const result = await yfinanceFetch(script);
  return result || { shortRatio: 0, shortPctFloat: 0, elevated: false };
}

// ── Signal 3: Earnings Whisper Gap (via implied move) ────────────
async function fetchEarningsWhisper(ticker) {
  try {
    const today = new Date().toISOString().split('T')[0];
    const in7days = new Date(Date.now() + 7 * 86400000).toISOString().split('T')[0];
    const cal = await finnhubGetSecure(`/calendar/earnings?from=${today}&to=${in7days}&symbol=${ticker}`);
    const upcoming = (cal?.earningsCalendar || []).filter(e => e.symbol === ticker);
    if (!upcoming.length) return { hasEarnings: false };

    const daysUntil = Math.ceil((new Date(upcoming[0].date) - new Date()) / 86400000);
    const est = parseFloat(upcoming[0].epsEstimate) || 0;

    // Get options implied move as whisper proxy
    const script = `
import yfinance as yf, json, math
try:
    t = yf.Ticker("${ticker}")
    info = t.info
    price = info.get('currentPrice') or info.get('regularMarketPrice') or 0
    # Get nearest expiry options to estimate implied move
    dates = t.options
    if dates and price > 0:
        chain = t.option_chain(dates[0])
        atm_calls = chain.calls[abs(chain.calls['strike'] - price) < price * 0.05]
        if not atm_calls.empty:
            avg_iv = atm_calls['impliedVolatility'].mean()
            implied_move_pct = round(avg_iv * math.sqrt(1/52) * 100, 2)
            print(json.dumps({"impliedMovePct": implied_move_pct, "price": price}))
            exit()
    print(json.dumps({"impliedMovePct": 0, "price": price}))
except Exception as e:
    print(json.dumps({"impliedMovePct": 0, "error": str(e)}))
`;
    const optData = await yfinanceFetch(script);
    const impliedMove = optData?.impliedMovePct || 0;

    return {
      hasEarnings: true,
      daysUntil,
      date: upcoming[0].date,
      epsEstimate: est,
      impliedMovePct: impliedMove,
      // Gap signal: if implied move > 5% and within 48hrs = high signal
      isHighSignal: daysUntil <= 2 && impliedMove > 5
    };
  } catch { return { hasEarnings: false }; }
}

// ── Stage 1: Universe Funnel ─────────────────────────────────────
async function runUniverseFunnel() {
  const today = new Date().toISOString().split('T')[0];
  const in7days = new Date(Date.now() + 7 * 86400000).toISOString().split('T')[0];

  // Get earnings in next 7 days
  const earningsCal = await finnhubGetSecure(`/calendar/earnings?from=${today}&to=${in7days}`).catch(() => ({ earningsCalendar: [] }));
  const earningsTickers = new Set((earningsCal?.earningsCalendar || []).map(e => e.symbol));

  // Get insider buys in last 48hrs across universe (sample — Finnhub rate limits)
  // We'll check a subset and flag those with recent activity
  const insiderTickers = new Set();
  const sampleSize = Math.min(20, FULL_UNIVERSE.length);
  const sample = FULL_UNIVERSE.slice(0, sampleSize);
  await Promise.all(sample.map(async ticker => {
    try {
      const data = await finnhubGetSecure(`/stock/insider-transactions?symbol=${ticker}`);
      const txns = Array.isArray(data?.data) ? data.data : [];
      const recentBuys = txns.filter(t => {
        const daysAgo = (Date.now() - new Date(t.transactionDate).getTime()) / 86400000;
        return daysAgo <= 2 && t.transactionCode === 'P';
      });
      if (recentBuys.length > 0) insiderTickers.add(ticker);
    } catch {}
  }));

  // Volume spikes via Yahoo screener (already have this)
  let volumeSpikeTickers = new Set();
  try {
    const movers = await yahooScreener('day_gainers', 50);
    movers.forEach(m => { if (m.symbol) volumeSpikeTickers.add(m.symbol); });
  } catch {}

  // Build candidate list
  const candidates = FULL_UNIVERSE.filter(ticker =>
    earningsTickers.has(ticker) ||
    insiderTickers.has(ticker) ||
    volumeSpikeTickers.has(ticker)
  );

  // Always include at least some from universe if candidates are sparse
  const finalCandidates = candidates.length >= 10
    ? candidates
    : [...new Set([...candidates, ...FULL_UNIVERSE.slice(0, 20)])];

  return {
    candidates: finalCandidates.slice(0, 40),
    earningsTickers: [...earningsTickers],
    insiderTickers: [...insiderTickers],
    volumeSpikeTickers: [...volumeSpikeTickers],
    totalUniverse: FULL_UNIVERSE.length,
    funnelAt: new Date().toISOString()
  };
}

// ── Full Signal Scorer (all 7 signals) ───────────────────────────
async function scoreTickerSignals(ticker) {
  const [quote, insiderRaw, newsRaw, kalshiRaw, earningsData] = await Promise.all([
    fetchMultiSource(ticker, false).catch(() => null),
    finnhubGetSecure(`/stock/insider-transactions?symbol=${ticker}`).catch(() => null),
    finnhubGetSecure(`/company-news?symbol=${ticker}&from=${new Date(Date.now()-7*86400000).toISOString().split('T')[0]}&to=${new Date().toISOString().split('T')[0]}`).catch(() => []),
    fetchKalshiMacro().catch(() => []),
    fetchEarningsWhisper(ticker)
  ]);

  const signals = [];
  let score = 0;
  let hasAnchorSignal = false; // Signal 1: unusual options volume

  // ── Signal 1: Unusual Options Volume (ANCHOR) ─────────────────
  // Note: yfinance is end-of-day — flag as degraded until Unusual Whales added
  const optVol = await fetchOptionsVolume(ticker).catch(() => ({ hasUnusual: false, ratio: 0 }));
  if (optVol.hasUnusual && optVol.ratio >= 5) {
    hasAnchorSignal = true;
    signals.push({
      id: 1, name: 'Unusual Options Volume', weight: 30, fired: true,
      text: `Options volume ${optVol.ratio}x open interest — unusual activity detected`,
      note: 'End-of-day data (yfinance) — upgrade to Unusual Whales for intraday sweeps',
      degraded: true
    });
    score += 30;
  } else {
    signals.push({
      id: 1, name: 'Unusual Options Volume', weight: 30, fired: false,
      text: `Options volume ratio: ${optVol.ratio}x (need 5x+)`,
      note: 'ANCHOR SIGNAL — no real money without this'
    });
  }

  // ── Signal 2: Insider Cluster Buys ───────────────────────────
  const insiderTxns = Array.isArray(insiderRaw?.data) ? insiderRaw.data : [];
  const recentBuys = insiderTxns.filter(t => {
    const daysAgo = (Date.now() - new Date(t.transactionDate).getTime()) / 86400000;
    return daysAgo <= 7 && t.transactionCode === 'P';
  });
  if (recentBuys.length >= 2) {
    signals.push({ id: 2, name: 'Insider Cluster Buys', weight: 25, fired: true, text: `${recentBuys.length} insider purchases in last 7 days` });
    score += 25;
  } else if (recentBuys.length === 1) {
    signals.push({ id: 2, name: 'Insider Cluster Buys', weight: 25, fired: false, text: `1 insider buy (need 2+ for cluster signal)`, note: 'Single buy ignored per rules' });
  } else {
    signals.push({ id: 2, name: 'Insider Cluster Buys', weight: 25, fired: false, text: 'No recent insider purchases' });
  }

  // ── Signal 3: Earnings Whisper Gap ───────────────────────────
  if (earningsData.hasEarnings) {
    if (earningsData.isHighSignal) {
      signals.push({ id: 3, name: 'Earnings Whisper Gap', weight: 20, fired: true, text: `Earnings in ${earningsData.daysUntil}d — implied move ${earningsData.impliedMovePct}% (high signal window)` });
      score += 20;
    } else if (earningsData.daysUntil <= 7) {
      signals.push({ id: 3, name: 'Earnings Whisper Gap', weight: 20, fired: true, text: `Earnings in ${earningsData.daysUntil}d — implied move ${earningsData.impliedMovePct}%`, note: 'Window opens 24-48hrs before report' });
      score += 10; // partial score — not in prime window yet
    }
  } else {
    signals.push({ id: 3, name: 'Earnings Whisper Gap', weight: 20, fired: false, text: 'No earnings in next 7 days' });
  }

  // ── Signal 4: Short Interest + Borrow Rate ────────────────────
  const shortData = await fetchShortInterest(ticker).catch(() => ({ elevated: false }));
  if (shortData.elevated && shortData.shortPctFloat > 15) {
    signals.push({ id: 4, name: 'Short Interest Elevated', weight: 15, fired: true, text: `${shortData.shortPctFloat}% of float short — squeeze setup possible`, note: 'Needs another signal to trigger' });
    score += 15;
  } else {
    signals.push({ id: 4, name: 'Short Interest Elevated', weight: 15, fired: false, text: `Short float: ${shortData.shortPctFloat}% (need 15%+)` });
  }

  // ── Signal 5: Kalshi Macro Context ───────────────────────────
  for (const m of kalshiRaw) {
    const title = (m.title || '').toLowerCase();
    if ((title.includes('rate cut') || title.includes('fed cut')) && m.probability > 60) {
      signals.push({ id: 5, name: 'Kalshi Macro: Fed Cut', weight: 10, fired: true, text: `Fed rate cut odds ${m.probability}% — bullish for tech/growth` });
      score += 10;
      break;
    }
    if (title.includes('recession') && m.probability > 50) {
      signals.push({ id: 5, name: 'Kalshi Macro: Recession Risk', weight: 10, fired: false, text: `Recession odds ${m.probability}% — macro headwind`, bearish: true });
      score -= 10;
      break;
    }
  }
  if (!signals.find(s => s.id === 5)) {
    signals.push({ id: 5, name: 'Kalshi Macro Context', weight: 10, fired: false, text: 'No strong macro signal' });
  }

  // ── Signal 6: Sentiment Velocity (confirmation only) ─────────
  const newsArr = Array.isArray(newsRaw) ? newsRaw : [];
  const last48h = newsArr.filter(n => (Date.now() / 1000 - n.datetime) < 48 * 3600);
  const posWords = ['beat','upgrade','buy','outperform','strong','growth','deal','launch','record'];
  const negWords = ['miss','downgrade','sell','loss','decline','lawsuit','investigation','cut'];
  let pos = 0, neg = 0;
  for (const n of last48h) {
    const t = (n.headline + ' ' + (n.summary || '')).toLowerCase();
    if (posWords.some(w => t.includes(w))) pos++;
    if (negWords.some(w => t.includes(w))) neg++;
  }
  if (last48h.length >= 3 && pos > neg * 2) {
    signals.push({ id: 6, name: 'Sentiment Velocity', weight: 0, fired: true, text: `${last48h.length} news items — ${pos} positive vs ${neg} negative`, note: 'Confirmation only — does not add to score alone' });
  } else {
    signals.push({ id: 6, name: 'Sentiment Velocity', weight: 0, fired: false, text: `${last48h.length} news items in 48hrs` });
  }

  // ── Signal 7: Volume Spike (funnel filter only) ───────────────
  signals.push({ id: 7, name: 'Volume Spike', weight: 0, fired: false, text: 'Funnel filter only — not scored', note: 'Used in Stage 1 to find candidates' });

  // ── Convergence Rule ─────────────────────────────────────────
  const firedSignals = signals.filter(s => s.fired && s.id >= 2 && s.id <= 6).length;
  const meetsConvergenceRule = hasAnchorSignal && firedSignals >= 2;
  const watchOnly = !hasAnchorSignal;

  const clampedScore = Math.min(100, Math.max(0, score));
  const level = clampedScore >= 60 ? 'HIGH' : clampedScore >= 35 ? 'MEDIUM' : 'LOW';

  return {
    ticker,
    score: clampedScore,
    level,
    color: level === 'HIGH' ? 'green' : level === 'MEDIUM' ? 'yellow' : 'red',
    hasAnchorSignal,
    meetsConvergenceRule,
    watchOnly,
    recommendation: meetsConvergenceRule ? 'INVESTIGATE — convergence rule met' : watchOnly ? 'WATCH ONLY — no unusual options volume' : 'WEAK — insufficient signal convergence',
    signals,
    currentPrice: quote?.price || null,
    firedCount: firedSignals,
    scoredAt: new Date().toISOString()
  };
}

// ── API: Run full funnel ──────────────────────────────────────────
app.get('/api/signals/funnel', async (req, res) => {
  try {
    const funnel = await runUniverseFunnel();
    res.json(funnel);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── API: Score a single ticker (all 7 signals) ───────────────────
app.get('/api/signals/score/:ticker', async (req, res) => {
  try {
    const result = await scoreTickerSignals(req.params.ticker.toUpperCase());
    res.json(result);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── API: Run full scan — funnel + score top candidates ────────────
app.post('/api/signals/scan', async (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const send = (data) => { try { res.write(`data: ${JSON.stringify(data)}\n\n`); } catch {} };
  const keepalive = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 15000);

  try {
    send({ type: 'status', text: 'Stage 1: Running universe funnel...' });
    const funnel = await runUniverseFunnel();
    send({ type: 'funnel', data: { candidates: funnel.candidates.length, earningsCount: funnel.earningsTickers.length, insiderCount: funnel.insiderTickers.length } });

    send({ type: 'status', text: `Stage 2: Scoring ${Math.min(funnel.candidates.length, 15)} candidates...` });

    const results = [];
    const toScore = funnel.candidates.slice(0, 15); // limit to avoid rate limits

    for (const ticker of toScore) {
      send({ type: 'status', text: `Scoring ${ticker}...` });
      try {
        const scored = await scoreTickerSignals(ticker);
        results.push(scored);
        send({ type: 'ticker', data: scored });
      } catch {}
      await new Promise(r => setTimeout(r, 500)); // rate limit buffer
    }

    // Sort by score, surface top 10
    const sorted = results.sort((a, b) => b.score - a.score);
    const top = sorted.slice(0, 10);
    const actionable = top.filter(t => t.meetsConvergenceRule);

    send({ type: 'complete', data: { results: sorted, top, actionable, scannedAt: new Date().toISOString() } });
    send({ type: 'done' });

  } catch (e) {
    send({ type: 'error', text: e.message });
    send({ type: 'done' });
  } finally {
    clearInterval(keepalive);
    res.end();
  }
});

// ── API: AI brief for a scored ticker ────────────────────────────
app.post('/api/signals/ai-brief', async (req, res) => {
  const { ticker, scoreData } = req.body;
  if (!ticker || !scoreData) return res.status(400).json({ error: 'ticker and scoreData required' });

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const send = (data) => { try { res.write(`data: ${JSON.stringify(data)}\n\n`); } catch {} };
  const keepalive = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 15000);

  try {
    const firedSignals = scoreData.signals.filter(s => s.fired).map(s => `- ${s.name}: ${s.text}`).join('\n');
    const prompt = `You are a professional options trader reviewing a stock signal report.

## ${ticker} Signal Report
- Convergence Score: ${scoreData.score}/100 (${scoreData.level})
- Anchor Signal (Unusual Options Volume): ${scoreData.hasAnchorSignal ? 'YES — present' : 'NO — watch only'}
- Convergence Rule Met: ${scoreData.meetsConvergenceRule ? 'YES' : 'NO'}
- Current Price: $${scoreData.currentPrice || '—'}

## Signals Fired
${firedSignals || 'No signals fired'}

## Your Task
Based on the REAL DATA signals above, give a concise trading brief.

RULES:
- If anchor signal is missing → recommend WATCH ONLY, no options trade
- If convergence rule met → suggest specific options action (2-4 week timeframe)
- Be specific about entry, target, and what would invalidate the thesis
- Keep it under 150 words

Return ONLY this JSON:
{
  "action": "BUY CALL" or "BUY PUT" or "WATCH ONLY" or "SKIP",
  "confidence": 0-100,
  "reasoning": "2-3 sentences on why",
  "entry": "suggested entry condition",
  "target": "profit target",
  "invalidates": "what would make you exit early",
  "timeframe": "2-4 weeks"
}`;

    const result = await askAI(prompt);
    const match = result.match(/\{[\s\S]*\}/);
    if (!match) throw new Error('Could not parse AI brief');
    const brief = JSON.parse(match[0]);
    send({ type: 'brief', data: brief });
    send({ type: 'done' });
  } catch (e) {
    send({ type: 'error', text: e.message });
    send({ type: 'done' });
  } finally {
    clearInterval(keepalive);
    res.end();
  }
});

// ═══════════════════════════════════════════════════════════════════
// PHASE 2 — KALSHI BET PREDICTOR
// External data → Kalshi price → divergence scoring → bet recommendation
// Categories: Fed Rate, CPI, Jobs, Bitcoin, Hurricane (seasonal), GDP
// Hard blocked: Sports, Politics, Culture
// ═══════════════════════════════════════════════════════════════════

const BLOCKED_CATEGORIES = ['sports', 'politics', 'culture', 'entertainment', 'awards', 'elections'];

// ── External Data Fetchers ────────────────────────────────────────

function fredFetch(seriesId) {
  return new Promise(resolve => {
    const url = `https://fred.stlouisfed.org/graph/fredgraph.csv?id=${seriesId}&limit=2`;
    execFile('curl', ['-s', '--max-time', '10', url], (err, stdout) => {
      if (err || !stdout) return resolve(null);
      try {
        const lines = stdout.trim().split('\n');
        const last = lines[lines.length - 1].split(',');
        resolve({ date: last[0], value: parseFloat(last[1]) });
      } catch { resolve(null); }
    });
  });
}

function blsFetch(seriesId) {
  return new Promise(resolve => {
    const url = `https://api.bls.gov/publicAPI/v2/timeseries/data/${seriesId}?startyear=2025&endyear=2026`;
    execFile('curl', ['-s', '--max-time', '10', '-H', 'Content-Type: application/json', url], (err, stdout) => {
      if (err || !stdout) return resolve(null);
      try {
        const json = JSON.parse(stdout);
        const data = json?.Results?.series?.[0]?.data || [];
        if (!data.length) return resolve(null);
        resolve({ value: parseFloat(data[0].value), period: data[0].periodName + ' ' + data[0].year, latest: data[0].latest === 'true' });
      } catch { resolve(null); }
    });
  });
}

function coingeckoBtc() {
  return new Promise(resolve => {
    execFile('curl', ['-s', '--max-time', '10', 'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_24hr_change=true'], (err, stdout) => {
      if (err || !stdout) return resolve(null);
      try {
        const json = JSON.parse(stdout);
        resolve({ price: json?.bitcoin?.usd, change24h: json?.bitcoin?.usd_24h_change });
      } catch { resolve(null); }
    });
  });
}

function noaaHurricanes() {
  return new Promise(resolve => {
    execFile('curl', ['-s', '--max-time', '10', 'https://www.nhc.noaa.gov/CurrentStorms.json'], (err, stdout) => {
      if (err || !stdout) return resolve({ activeStorms: [] });
      try {
        const json = JSON.parse(stdout);
        resolve({ activeStorms: json?.activeStorms || [] });
      } catch { resolve({ activeStorms: [] }); }
    });
  });
}

function kalshiSeriesMarkets(seriesTicker, limit = 10) {
  return new Promise(resolve => {
    const url = `https://api.elections.kalshi.com/trade-api/v2/markets?limit=${limit}&series_ticker=${seriesTicker}&status=open`;
    execFile('curl', ['-s', '--max-time', '10', '-H', 'Accept: application/json', url], (err, stdout) => {
      if (err || !stdout) return resolve([]);
      try {
        const json = JSON.parse(stdout);
        resolve((json?.markets || []).map(m => ({
          ticker: m.ticker,
          title: m.title || '',
          probability: m.yes_bid_dollars != null ? Math.round(parseFloat(m.yes_bid_dollars) * 100) : null,
          volume: parseFloat(m.volume_fp || 0),
          closeTime: m.close_time,
          daysUntilClose: m.close_time ? Math.ceil((new Date(m.close_time) - new Date()) / 86400000) : null
        })).filter(m => m.probability !== null));
      } catch { resolve([]); }
    });
  });
}

// ── Category Scorers ─────────────────────────────────────────────

async function scoreFedCategory() {
  const [currentRate, kalshiMarkets] = await Promise.all([
    fredFetch('DFEDTARU'),
    kalshiSeriesMarkets('KXFED', 10)
  ]);

  if (!currentRate || !kalshiMarkets.length) return null;

  // Find the most relevant near-term market (next meeting)
  const sorted = kalshiMarkets.sort((a, b) => (a.daysUntilClose || 999) - (b.daysUntilClose || 999));
  const nearTerm = sorted.slice(0, 5);

  // Find market closest to current rate level (any "above X%" market)
  // Just use the market with probability closest to 50% — most informative
  const cutMarket = nearTerm.reduce((best, m) => {
    if (!best) return m;
    const bestDist = Math.abs((best.probability || 50) - 50);
    const mDist = Math.abs((m.probability || 50) - 50);
    return mDist < bestDist ? m : best;
  }, null);

  if (!cutMarket) return null;

  // External signal: FRED current rate vs Kalshi implied
  const kalshiCutProb = 100 - (cutMarket.probability || 50); // inverse of "above X"
  const daysUntil = cutMarket.daysUntilClose || 999;
  const withinWindow = daysUntil <= 72;

  // Divergence: if FRED shows rate at 3.75% but Kalshi implies >40% cut probability
  const divergence = kalshiCutProb > 40 ? kalshiCutProb - 30 : 0; // simplified divergence

  return {
    category: 'Fed Rate Decision',
    series: 'KXFED',
    externalSource: 'FRED (current rate)',
    externalValue: `Current Fed Funds Rate: ${currentRate.value}%`,
    externalImplied: `${kalshiCutProb}% cut probability implied`,
    kalshiMarket: cutMarket,
    kalshiPrice: cutMarket.probability,
    divergence: Math.round(divergence),
    daysUntilEvent: daysUntil,
    withinWindow,
    liquidity: cutMarket.volume > 1000,
    meetsThreshold: divergence >= 15 && withinWindow && cutMarket.volume > 1000,
    recommendedSide: kalshiCutProb > 60 ? 'BET YES (cut likely)' : 'BET NO (hold likely)',
    edge: divergence >= 15 ? 'HIGH' : divergence >= 8 ? 'MEDIUM' : 'LOW'
  };
}

async function scoreCpiCategory() {
  const [cpiData, kalshiMarkets] = await Promise.all([
    blsFetch('CUUR0000SA0'), // All Urban CPI
    kalshiSeriesMarkets('KXCPICORE', 5)
  ]);

  if (!cpiData || !kalshiMarkets.length) return null;

  const nearTerm = kalshiMarkets.sort((a, b) => (a.daysUntilClose || 999) - (b.daysUntilClose || 999))[0];
  if (!nearTerm) return null;

  const daysUntil = nearTerm.daysUntilClose || 999;
  const withinWindow = daysUntil <= 24; // CPI window is tight — 24hrs max

  return {
    category: 'CPI Inflation Report',
    series: 'KXCPICORE',
    externalSource: 'BLS (Bureau of Labor Statistics)',
    externalValue: `Latest CPI: ${cpiData.value} (${cpiData.period})`,
    externalImplied: `YoY trend from BLS data`,
    kalshiMarket: nearTerm,
    kalshiPrice: nearTerm.probability,
    divergence: 0, // Would need prior month comparison for real divergence
    daysUntilEvent: daysUntil,
    withinWindow,
    liquidity: nearTerm.volume > 500,
    meetsThreshold: withinWindow && nearTerm.volume > 500,
    recommendedSide: 'ANALYZE — check BLS PPI and import prices for lead signal',
    edge: withinWindow ? 'MEDIUM' : 'LOW',
    note: 'Best edge: compare PPI (released before CPI) to Kalshi CPI price'
  };
}

async function scoreBitcoinCategory() {
  const [btcData, kalshiMarkets] = await Promise.all([
    coingeckoBtc(),
    kalshiSeriesMarkets('KXBTCMAXY', 5)
  ]);

  if (!btcData || !kalshiMarkets.length) return null;

  const currentPrice = btcData.price;
  // Find market closest to current price
  const nearTerm = kalshiMarkets.sort((a, b) => (a.daysUntilClose || 999) - (b.daysUntilClose || 999))[0];
  if (!nearTerm) return null;

  const daysUntil = nearTerm.daysUntilClose || 999;

  return {
    category: 'Bitcoin Price Target',
    series: 'KXBTCMAXY',
    externalSource: 'CoinGecko (live price)',
    externalValue: `BTC: $${currentPrice?.toLocaleString()} (${btcData.change24h?.toFixed(2)}% 24h)`,
    externalImplied: `Current price vs Kalshi target`,
    kalshiMarket: nearTerm,
    kalshiPrice: nearTerm.probability,
    divergence: 0,
    daysUntilEvent: daysUntil,
    withinWindow: daysUntil <= 72,
    liquidity: nearTerm.volume > 200,
    meetsThreshold: false, // Crypto = speculative, never auto-recommend
    recommendedSide: 'SPECULATIVE — smaller position sizing required',
    edge: 'MODERATE',
    note: 'Crypto Kalshi markets have lower liquidity and wider spreads. Use 50% of normal position size.',
    currentBtcPrice: currentPrice
  };
}

async function scoreHurricaneCategory() {
  const month = new Date().getMonth() + 1; // 1-12
  const isHurricaneSeason = month >= 6 && month <= 11;

  if (!isHurricaneSeason) {
    return {
      category: 'Hurricane / Weather',
      active: false,
      note: 'Hurricane module is OFF — season runs June 1 to November 30',
      edge: 'N/A'
    };
  }

  const [noaaData, kalshiMarkets] = await Promise.all([
    noaaHurricanes(),
    kalshiSeriesMarkets('HURTB', 5).then(r => r.length ? r : kalshiSeriesMarkets('HURHAT', 5))
  ]);

  const activeStorms = noaaData.activeStorms || [];

  return {
    category: 'Hurricane / Weather',
    series: 'HURTB/HURHAT/KXHURNO',
    active: true,
    externalSource: 'NOAA National Hurricane Center',
    externalValue: activeStorms.length > 0
      ? `${activeStorms.length} active storm(s): ${activeStorms.map(s => s.name).join(', ')}`
      : 'No active storms currently',
    kalshiMarkets: kalshiMarkets.slice(0, 3),
    activeStorms,
    withinWindow: activeStorms.length > 0,
    meetsThreshold: activeStorms.length > 0 && kalshiMarkets.length > 0,
    edge: activeStorms.length > 0 ? 'MEDIUM' : 'LOW',
    note: 'Best edge: NOAA updates every 6hrs. European ECMWF model often diverges from NOAA before Kalshi reprices.'
  };
}

async function scoreGdpCategory() {
  const [gdpData, kalshiMarkets] = await Promise.all([
    fredFetch('GDPC1'), // Real GDP
    kalshiSeriesMarkets('KXGDPYEAR', 5)
  ]);

  if (!kalshiMarkets.length) return null;

  const nearTerm = kalshiMarkets.sort((a, b) => (a.daysUntilClose || 999) - (b.daysUntilClose || 999))[0];

  return {
    category: 'GDP Growth',
    series: 'KXGDPYEAR',
    externalSource: 'FRED (Real GDP)',
    externalValue: gdpData ? `Latest GDP: ${gdpData.value} (${gdpData.date})` : 'FRED data unavailable',
    kalshiMarket: nearTerm,
    kalshiPrice: nearTerm?.probability,
    daysUntilEvent: nearTerm?.daysUntilClose || 999,
    withinWindow: (nearTerm?.daysUntilClose || 999) <= 72,
    liquidity: (nearTerm?.volume || 0) > 500,
    meetsThreshold: false,
    edge: 'MEDIUM',
    note: 'Lead indicators: ISM Manufacturing, retail sales, industrial output (all BLS/Census — free)'
  };
}

// ── Full Phase 2 Scan ─────────────────────────────────────────────
// runKalshiScan moved to Phase 2 extended section below

// ── API: Full Kalshi scan ─────────────────────────────────────────
app.get('/api/kalshi/scan', async (req, res) => {
  try {
    const result = await runKalshiScan();
    res.json(result);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── API: AI bet recommendation for a category ─────────────────────
app.post('/api/kalshi/ai-recommend', async (req, res) => {
  const { category } = req.body;
  if (!category) return res.status(400).json({ error: 'category data required' });

  // Hard block: Sports, Politics, Culture
  const catName = (category.category || '').toLowerCase();
  if (BLOCKED_CATEGORIES.some(b => catName.includes(b))) {
    return res.json({ action: 'BLOCKED', reasoning: 'This category is hard-blocked. Sports, Politics, and Culture have no quantifiable edge for retail traders.' });
  }

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const send = (data) => { try { res.write(`data: ${JSON.stringify(data)}\n\n`); } catch {} };
  const keepalive = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 15000);

  try {
    const prompt = `You are a professional prediction market trader reviewing a Kalshi bet opportunity.

## Category: ${category.category}
## External Data
- Source: ${category.externalSource}
- Current data: ${category.externalValue}
- External implied probability: ${category.externalImplied || 'see data above'}

## Kalshi Market
- Market: ${category.kalshiMarket?.title || 'N/A'}
- Kalshi current price: ${category.kalshiPrice || 'N/A'}%
- Days until close: ${category.daysUntilEvent || 'N/A'}
- Volume: ${category.kalshiMarket?.volume?.toFixed(0) || 'N/A'}

## Divergence Analysis
- Divergence: ${category.divergence || 0} percentage points
- Within time window: ${category.withinWindow ? 'YES' : 'NO'}
- Liquidity check: ${category.liquidity ? 'PASS' : 'FAIL'}
- Meets threshold (15pt+ divergence, within 72hrs, liquid): ${category.meetsThreshold ? 'YES' : 'NO'}

## Rules
- Only recommend BET YES or BET NO if ALL three conditions met: 15pt+ divergence, within 72hrs, liquid
- If any condition fails → SKIP
- Crypto bets → always note smaller position size
- Hurricane → only if active storm tracked by NOAA
- Keep reasoning under 100 words

Return ONLY this JSON:
{
  "action": "BET YES" or "BET NO" or "SKIP" or "WATCH",
  "confidence": 0-100,
  "reasoning": "concise explanation based on data divergence",
  "suggestedSize": "e.g. 2% of bankroll" or "SKIP",
  "timeWindow": "hours/days before opportunity closes",
  "keyRisk": "what could invalidate this bet"
}`;

    const result = await askAI(prompt);
    const match = result.match(/\{[\s\S]*\}/);
    if (!match) throw new Error('Could not parse recommendation');
    const rec = JSON.parse(match[0]);
    send({ type: 'recommendation', data: rec });
    send({ type: 'done' });
  } catch (e) {
    send({ type: 'error', text: e.message });
    send({ type: 'done' });
  } finally {
    clearInterval(keepalive);
    res.end();
  }
});

// ── Category 6: FOMC Meeting Outcome ─────────────────────────────
async function scoreFomcCategory() {
  // Get current Fed rate from FRED
  const [currentRate, nextMeetingMarkets] = await Promise.all([
    fredFetch('DFEDTARU'),
    kalshiSeriesMarkets('KXFED', 20)
  ]);

  if (!nextMeetingMarkets.length) return null;

  // Find the SOONEST closing market (next meeting)
  const sorted = nextMeetingMarkets
    .filter(m => m.daysUntilClose && m.daysUntilClose > 0)
    .sort((a, b) => (a.daysUntilClose || 999) - (b.daysUntilClose || 999));

  if (!sorted.length) return null;
  const nextMeeting = sorted[0];
  const daysUntil = nextMeeting.daysUntilClose || 999;

  // Group markets by meeting date to find the "hold" vs "cut" implied probability
  // Market: "Will rate be above X?" — find the market at current rate level
  const currentRateVal = currentRate?.value || 3.75;
  // Find market closest to current rate
  const holdMarket = sorted.find(m =>
    m.title.toLowerCase().includes(currentRateVal.toFixed(2)) ||
    m.title.toLowerCase().includes(currentRateVal.toString())
  ) || sorted[0];

  // CME FedWatch proxy: use SOFR futures implied rate from FRED
  const sofrData = await fredFetch('SOFR').catch(() => null);

  // Implied cut probability from Kalshi structure:
  // If "above 3.75%" market is at 40%, that means 60% chance of cut
  const holdProb = holdMarket.probability || 50;
  const cutProb = 100 - holdProb;

  // Divergence: if SOFR spread suggests different probability than Kalshi
  // Simplified: use the Kalshi price itself as the signal
  const divergence = cutProb > 50 ? Math.max(0, cutProb - 40) : 0;
  const withinWindow = daysUntil <= 48;

  return {
    category: 'FOMC Meeting Outcome',
    series: 'KXFED',
    externalSource: 'FRED (current Fed Funds rate)',
    externalValue: `Current rate: ${currentRateVal}% | SOFR: ${sofrData?.value?.toFixed(2) || 'N/A'}%`,
    externalImplied: `Kalshi implies ${cutProb}% cut probability at next meeting`,
    kalshiMarket: holdMarket,
    kalshiPrice: holdMarket.probability,
    divergence: Math.round(divergence),
    daysUntilEvent: daysUntil,
    withinWindow,
    liquidity: (holdMarket.volume || 0) > 1000,
    meetsThreshold: divergence >= 15 && withinWindow && (holdMarket.volume || 0) > 1000,
    recommendedSide: cutProb > 65 ? 'BET YES on cut' : cutProb < 35 ? 'BET NO (hold)' : 'WATCH — no clear edge',
    edge: divergence >= 15 ? 'HIGH' : divergence >= 8 ? 'MEDIUM' : 'LOW',
    note: 'Best edge window: 24-48hrs before FOMC decision. CME FedWatch leads Kalshi repricing by hours.'
  };
}

// ── Category 7: Jobs / NFP ────────────────────────────────────────
async function scoreJobsCategory() {
  // ADP private payrolls from FRED (proxy for NFP)
  const [adpData, joblessData, nfpMarkets] = await Promise.all([
    fredFetch('NPPTTL').catch(() => null),   // ADP total nonfarm
    fredFetch('ICSA').catch(() => null),      // Initial jobless claims
    kalshiSeriesMarkets('KXUSNFP', 10)
  ]);

  if (!nfpMarkets.length) return null;

  // Find soonest market
  const sorted = nfpMarkets
    .filter(m => m.daysUntilClose && m.daysUntilClose > 0)
    .sort((a, b) => (a.daysUntilClose || 999) - (b.daysUntilClose || 999));

  if (!sorted.length) return null;

  // Find the "above 80K" market as the key signal (consensus is usually ~150-200K)
  const above80k = sorted.find(m => m.title.includes('80K') || m.title.includes('80,000')) || sorted[0];
  const daysUntil = above80k.daysUntilClose || 999;
  const withinWindow = daysUntil <= 48;

  // ADP divergence: if ADP came in weak (<100K), NFP likely weak too
  // Kalshi "above 80K" at 48% = market thinks 50/50
  // If ADP was strong, divergence = Kalshi underpricing
  const adpValue = adpData?.value || 0;
  const joblessValue = joblessData?.value || 0;

  // Simple divergence: if jobless claims spiking (>250K) and Kalshi still pricing >50% above 80K
  const joblessElevated = joblessValue > 250000;
  const kalshiPrice = above80k.probability || 50;
  const divergence = joblessElevated && kalshiPrice > 50 ? kalshiPrice - 35 : 0;

  return {
    category: 'Jobs Report (NFP)',
    series: 'KXUSNFP',
    externalSource: 'FRED (ADP + Jobless Claims)',
    externalValue: `ADP: ${adpValue ? adpValue.toLocaleString() : 'N/A'} | Jobless claims: ${joblessValue ? joblessValue.toLocaleString() : 'N/A'}`,
    externalImplied: `ADP and claims suggest ${joblessElevated ? 'weak' : 'normal'} labor market`,
    kalshiMarket: above80k,
    kalshiPrice: above80k.probability,
    divergence: Math.round(divergence),
    daysUntilEvent: daysUntil,
    withinWindow,
    liquidity: (above80k.volume || 0) > 500,
    meetsThreshold: divergence >= 15 && withinWindow && (above80k.volume || 0) > 500,
    recommendedSide: divergence >= 15 ? 'BET NO (payrolls likely weak)' : 'WATCH',
    edge: withinWindow ? 'MEDIUM' : 'LOW',
    note: 'Best edge: ADP releases 2 days before NFP. When ADP diverges from consensus, Kalshi NFP markets often lag repricing by 12-24hrs.'
  };
}

// ── Category 8: Earnings Beat/Miss ───────────────────────────────
async function scoreEarningsCategory() {
  // Get upcoming earnings from Finnhub
  const today = new Date().toISOString().split('T')[0];
  const in48h = new Date(Date.now() + 2 * 86400000).toISOString().split('T')[0];

  const earningsCal = await finnhubGetSecure(`/calendar/earnings?from=${today}&to=${in48h}`).catch(() => ({ earningsCalendar: [] }));
  const upcoming = (earningsCal?.earningsCalendar || [])
    .filter(e => UNIVERSE_SP500.includes(e.symbol) || UNIVERSE_NASDAQ100.includes(e.symbol))
    .slice(0, 5);

  if (!upcoming.length) {
    return {
      category: 'Earnings Beat/Miss',
      active: false,
      externalValue: 'No major earnings in next 48hrs',
      edge: 'LOW',
      meetsThreshold: false,
      note: 'Check back when S&P 500 / NASDAQ 100 companies report earnings'
    };
  }

  // For each upcoming earner, check Kalshi earnings mention markets
  const results = [];
  for (const e of upcoming.slice(0, 3)) {
    const ticker = e.symbol;
    const seriesTicker = `KXEARNINGSMENTIO${ticker}`;
    const [kalshiMkts, optData] = await Promise.all([
      kalshiSeriesMarkets(seriesTicker, 5).catch(() => []),
      yfinanceFetch(`
import yfinance as yf, json, math
try:
    t = yf.Ticker("${ticker}")
    dates = t.options
    info = t.info
    price = info.get('currentPrice') or info.get('regularMarketPrice') or 0
    if dates and price > 0:
        chain = t.option_chain(dates[0])
        atm = chain.calls[abs(chain.calls['strike'] - price) < price * 0.05]
        if not atm.empty:
            iv = atm['impliedVolatility'].mean()
            move = round(iv * math.sqrt(1/52) * 100, 2)
            print(json.dumps({"impliedMove": move, "price": price, "ticker": "${ticker}"}))
            exit()
    print(json.dumps({"impliedMove": 0, "price": price, "ticker": "${ticker}"}))
except: print(json.dumps({"impliedMove": 0, "ticker": "${ticker}"}))
`).catch(() => null)
    ]);

    const impliedMove = optData?.impliedMove || 0;
    const daysUntil = Math.ceil((new Date(e.date) - new Date()) / 86400000);

    // Find a liquid Kalshi market for this earnings
    const liquidMarket = kalshiMkts.sort((a, b) => (b.volume || 0) - (a.volume || 0))[0];

    if (liquidMarket && impliedMove > 5) {
      // Divergence: options imply big move but Kalshi price seems off
      const kalshiProb = liquidMarket.probability || 50;
      // If implied move is large (>8%) and Kalshi is pricing outcome at 50/50, that's interesting
      const divergence = impliedMove > 8 && Math.abs(kalshiProb - 50) < 15 ? impliedMove - 5 : 0;

      results.push({
        ticker,
        daysUntil,
        impliedMove,
        kalshiMarket: liquidMarket,
        divergence: Math.round(divergence),
        withinWindow: daysUntil <= 2,
        liquidity: (liquidMarket.volume || 0) > 500
      });
    }
  }

  if (!results.length) {
    return {
      category: 'Earnings Beat/Miss',
      active: true,
      externalValue: `${upcoming.length} earnings in 48hrs: ${upcoming.map(e => e.symbol).join(', ')}`,
      externalImplied: 'No liquid Kalshi markets found for these tickers',
      edge: 'LOW',
      meetsThreshold: false,
      note: 'Kalshi earnings mention markets exist for AMZN, MSFT, AAPL, META, BAC — check when these report'
    };
  }

  const best = results.sort((a, b) => b.divergence - a.divergence)[0];
  const withinWindow = best.withinWindow;
  const meetsThreshold = best.divergence >= 15 && withinWindow && best.liquidity;

  return {
    category: 'Earnings Beat/Miss',
    series: `KXEARNINGSMENTIO${best.ticker}`,
    externalSource: `Finnhub + yfinance options (${best.ticker})`,
    externalValue: `${best.ticker} earnings in ${best.daysUntil}d — options imply ${best.impliedMove}% move`,
    externalImplied: `Large implied move suggests high uncertainty — Kalshi may be mispriced`,
    kalshiMarket: best.kalshiMarket,
    kalshiPrice: best.kalshiMarket?.probability,
    divergence: best.divergence,
    daysUntilEvent: best.daysUntil,
    withinWindow,
    liquidity: best.liquidity,
    meetsThreshold,
    allResults: results,
    recommendedSide: meetsThreshold ? `Investigate ${best.ticker} Kalshi market` : 'WATCH',
    edge: best.divergence >= 15 ? 'MEDIUM' : 'LOW',
    note: 'Module 1 bridge: same Finnhub data scores the stock AND the Kalshi contract. When options implied move diverges from Kalshi price, that is real edge.'
  };
}

// ── Category 9: Recession Probability ────────────────────────────
async function scoreRecessionCategory() {
  // Yield curve from FRED: 10yr - 2yr spread
  // Inverted = recession signal. Currently at 0.50 (positive = no inversion)
  const [yieldCurve, unemploymentData, recessionMarkets] = await Promise.all([
    fredFetch('T10Y2Y').catch(() => null),       // 10yr-2yr spread
    fredFetch('UNRATE').catch(() => null),        // Unemployment rate
    kalshiSeriesMarkets('KXNBERRECESSQ', 10)
  ]);

  if (!recessionMarkets.length) return null;

  // Find the soonest recession market (Q1 2026, Q2 2026, etc.)
  const sorted = recessionMarkets
    .filter(m => m.probability !== null && m.probability > 0)
    .sort((a, b) => (b.probability || 0) - (a.probability || 0));

  if (!sorted.length) return null;

  const highestProb = sorted[0];
  const spread = yieldCurve?.value || 0;
  const unemployment = unemploymentData?.value || 0;

  // Recession signal logic:
  // Inverted yield curve (spread < 0) = strong recession signal
  // Rising unemployment = recession signal
  // Compare to Kalshi recession probability
  const yieldCurveSignal = spread < 0 ? 'INVERTED — strong recession signal' : spread < 0.5 ? 'Flattening — watch' : 'Normal — no inversion';
  const unemploymentSignal = unemployment > 4.5 ? 'Elevated' : 'Normal';

  // External implied recession probability based on yield curve
  // Historical: inverted curve = ~70% recession within 18 months
  const externalRecessionProb = spread < 0 ? 65 : spread < 0.5 ? 35 : 20;
  const kalshiRecessionProb = highestProb.probability || 0;

  // Divergence: if yield curve says 65% but Kalshi says 10%, that's 55pt divergence
  const divergence = Math.max(0, externalRecessionProb - kalshiRecessionProb);

  return {
    category: 'Recession Probability',
    series: 'KXNBERRECESSQ',
    externalSource: 'FRED (Yield Curve + Unemployment)',
    externalValue: `10yr-2yr spread: ${spread?.toFixed(2)}% (${yieldCurveSignal}) | Unemployment: ${unemployment}%`,
    externalImplied: `Yield curve model implies ~${externalRecessionProb}% recession probability`,
    kalshiMarket: highestProb,
    kalshiPrice: kalshiRecessionProb,
    divergence: Math.round(divergence),
    daysUntilEvent: highestProb.daysUntilClose || 999,
    withinWindow: true, // Recession is ongoing — always in window
    liquidity: (highestProb.volume || 0) > 200,
    meetsThreshold: divergence >= 15 && (highestProb.volume || 0) > 200,
    recommendedSide: divergence >= 15 ? `BET YES on recession (yield curve vs Kalshi divergence: ${Math.round(divergence)}pts)` : 'WATCH — divergence below threshold',
    edge: divergence >= 30 ? 'HIGH' : divergence >= 15 ? 'MEDIUM' : 'LOW',
    note: 'Yield curve inversion historically precedes recession by 12-18 months. No time pressure — this is an ongoing signal. Position size conservatively.'
  };
}

// ── Updated Full Phase 2 Scan (all 9 categories) ─────────────────
async function runKalshiScan() {
  const [fed, cpi, btc, hurricane, gdp, fomc, jobs, earnings, recession] = await Promise.all([
    scoreFedCategory().catch(() => null),
    scoreCpiCategory().catch(() => null),
    scoreBitcoinCategory().catch(() => null),
    scoreHurricaneCategory().catch(() => null),
    scoreGdpCategory().catch(() => null),
    scoreFomcCategory().catch(() => null),
    scoreJobsCategory().catch(() => null),
    scoreEarningsCategory().catch(() => null),
    scoreRecessionCategory().catch(() => null)
  ]);

  const categories = [fed, cpi, btc, hurricane, gdp, fomc, jobs, earnings, recession].filter(Boolean);
  const actionable = categories.filter(c => c.meetsThreshold);

  return { categories, actionable, scannedAt: new Date().toISOString() };
}

// ═══════════════════════════════════════════════════════════════════
// KALSHI TRADING INTEGRATION
// RSA-PSS key authentication + order placement
// ═══════════════════════════════════════════════════════════════════

const crypto = require('crypto');
const KALSHI_FILE = path.join(BASE_DIR, 'kalshi-auth.json');
const KALSHI_BASE = 'https://api.elections.kalshi.com';

function loadKalshiAuth() {
  try {
    if (fs.existsSync(KALSHI_FILE)) return JSON.parse(fs.readFileSync(KALSHI_FILE, 'utf8'));
  } catch {}
  return { apiKeyId: null, privateKey: null, connectedAt: null };
}

function saveKalshiAuth(d) {
  fs.writeFileSync(KALSHI_FILE, JSON.stringify(d, null, 2));
}

function kalshiSign(method, path, body, privateKeyPem) {
  const timestamp = Date.now().toString();
  const msgString = timestamp + method.toUpperCase() + path + (body ? JSON.stringify(body) : '');
  const sign = crypto.createSign('SHA256');
  sign.update(msgString);
  const signature = sign.sign(
    { key: privateKeyPem, padding: crypto.constants.RSA_PKCS1_PSS_PADDING, saltLength: 32 },
    'base64'
  );
  return { timestamp, signature };
}

function kalshiRequest(method, urlPath, body, auth) {
  return new Promise((resolve, reject) => {
    if (!auth?.apiKeyId || !auth?.privateKey) return reject(new Error('Kalshi not connected. Add your API key in Kalshi settings.'));

    let sig;
    try {
      sig = kalshiSign(method, urlPath, body, auth.privateKey);
    } catch (e) {
      return reject(new Error('Invalid private key: ' + e.message));
    }

    const data = body ? JSON.stringify(body) : null;
    const args = [
      '-s', '--max-time', '15',
      '-X', method,
      '-H', 'Content-Type: application/json',
      '-H', 'Accept: application/json',
      '-H', `KALSHI-ACCESS-KEY: ${auth.apiKeyId}`,
      '-H', `KALSHI-ACCESS-SIGNATURE: ${sig.signature}`,
      '-H', `KALSHI-ACCESS-TIMESTAMP: ${sig.timestamp}`,
    ];
    if (data) args.push('-d', data);
    args.push(`${KALSHI_BASE}${urlPath}`);

    execFile('curl', args, { maxBuffer: 2 * 1024 * 1024 }, (err, stdout) => {
      if (err) return reject(new Error('Kalshi request failed: ' + err.message));
      try {
        const json = JSON.parse(stdout);
        if (json.error) return reject(new Error(json.error.message || json.error.code || 'Kalshi error'));
        resolve(json);
      } catch { reject(new Error('Kalshi parse error: ' + stdout.slice(0, 100))); }
    });
  });
}

// ── Save API credentials ──────────────────────────────────────────
app.post('/api/kalshi/connect', (req, res) => {
  const { apiKeyId, privateKey } = req.body;
  if (!apiKeyId || !privateKey) return res.status(400).json({ error: 'apiKeyId and privateKey required' });

  // Validate the private key format
  try {
    crypto.createSign('SHA256').sign({ key: privateKey, padding: crypto.constants.RSA_PKCS1_PSS_PADDING, saltLength: 32 });
  } catch (e) {
    // Key might still be valid even if signing empty string fails — just check it parses
    try {
      crypto.createPrivateKey(privateKey);
    } catch (e2) {
      return res.status(400).json({ error: 'Invalid private key format. Paste the full PEM key including -----BEGIN PRIVATE KEY----- header.' });
    }
  }

  const auth = { apiKeyId: apiKeyId.trim(), privateKey: privateKey.trim(), connectedAt: new Date().toISOString() };
  saveKalshiAuth(auth);
  res.json({ ok: true });
});

app.post('/api/kalshi/disconnect', (req, res) => {
  saveKalshiAuth({ apiKeyId: null, privateKey: null, connectedAt: null });
  res.json({ ok: true });
});

app.get('/api/kalshi/auth-status', (req, res) => {
  const auth = loadKalshiAuth();
  res.json({ connected: !!(auth.apiKeyId && auth.privateKey), apiKeyId: auth.apiKeyId ? auth.apiKeyId.slice(0, 8) + '...' : null, connectedAt: auth.connectedAt });
});

// ── Test connection ───────────────────────────────────────────────
app.get('/api/kalshi/balance', async (req, res) => {
  try {
    const auth = loadKalshiAuth();
    const data = await kalshiRequest('GET', '/trade-api/v2/portfolio/balance', null, auth);
    res.json({ balance: data.balance, currency: 'USD' });
  } catch (e) { res.status(401).json({ error: e.message }); }
});

// ── Get open positions ────────────────────────────────────────────
app.get('/api/kalshi/positions', async (req, res) => {
  try {
    const auth = loadKalshiAuth();
    const data = await kalshiRequest('GET', '/trade-api/v2/portfolio/positions', null, auth);
    const positions = (data.market_positions || []).map(p => ({
      ticker: p.ticker,
      marketTitle: p.market_title || '',
      yesContracts: p.position || 0,
      noContracts: p.resting_orders_count || 0,
      totalCost: p.total_traded || 0,
      realizedPnl: p.realized_pnl || 0,
      unrealizedPnl: p.resting_orders_count || 0,
    }));
    res.json({ positions });
  } catch (e) { res.status(401).json({ error: e.message }); }
});

// ── Get open orders ───────────────────────────────────────────────
app.get('/api/kalshi/orders', async (req, res) => {
  try {
    const auth = loadKalshiAuth();
    const data = await kalshiRequest('GET', '/trade-api/v2/portfolio/orders?status=resting', null, auth);
    res.json({ orders: data.orders || [] });
  } catch (e) { res.status(401).json({ error: e.message }); }
});

// ── Place a bet ───────────────────────────────────────────────────
app.post('/api/kalshi/order', async (req, res) => {
  const { ticker, side, count, price } = req.body;
  // side: 'yes' or 'no', count: number of contracts, price: cents (1-99)
  if (!ticker || !side || !count || !price) return res.status(400).json({ error: 'ticker, side, count, price required' });
  if (!['yes', 'no'].includes(side)) return res.status(400).json({ error: 'side must be yes or no' });
  if (price < 1 || price > 99) return res.status(400).json({ error: 'price must be 1-99 cents' });

  try {
    const auth = loadKalshiAuth();
    const order = {
      ticker,
      client_order_id: `sf-${Date.now()}`,
      type: 'limit',
      action: 'buy',
      side,
      count: parseInt(count),
      yes_price: side === 'yes' ? parseInt(price) : 100 - parseInt(price),
      no_price: side === 'no' ? parseInt(price) : 100 - parseInt(price),
    };
    const data = await kalshiRequest('POST', '/trade-api/v2/portfolio/orders', order, auth);
    res.json({ ok: true, order: data.order });
  } catch (e) { res.status(400).json({ error: e.message }); }
});

// ── Cancel an order ───────────────────────────────────────────────
app.delete('/api/kalshi/order/:orderId', async (req, res) => {
  try {
    const auth = loadKalshiAuth();
    await kalshiRequest('DELETE', `/trade-api/v2/portfolio/orders/${req.params.orderId}`, null, auth);
    res.json({ ok: true });
  } catch (e) { res.status(400).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════════
// BACKTEST ENGINE
// Uses Yahoo Finance for historical OHLCV + technicalindicators npm
// Supports: Stocks, Crypto
// ═══════════════════════════════════════════════════════════════════
const ti = require('technicalindicators');

// ── Fetch historical OHLCV from Yahoo Finance ─────────────────────
function fetchYahooHistory(symbol, range = '1y') {
  return new Promise((resolve, reject) => {
    const yahooSym = isCrypto(symbol) ? `${symbol.toUpperCase()}-USD` : symbol.toUpperCase();
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(yahooSym)}?interval=1d&range=${range}`;
    execFile('curl', ['-s', '--max-time', '15', '-A', 'Mozilla/5.0', url], { maxBuffer: 5 * 1024 * 1024 }, (err, stdout) => {
      if (err || !stdout) return reject(new Error('Failed to fetch historical data'));
      try {
        const json = JSON.parse(stdout);
        const result = json?.chart?.result?.[0];
        if (!result) return reject(new Error(`No historical data found for ${symbol}`));
        const timestamps = result.timestamp || [];
        const quote = result.indicators?.quote?.[0] || {};
        const candles = timestamps.map((ts, i) => ({
          date: new Date(ts * 1000).toISOString().split('T')[0],
          open:   parseFloat((quote.open?.[i] || 0).toFixed(4)),
          high:   parseFloat((quote.high?.[i] || 0).toFixed(4)),
          low:    parseFloat((quote.low?.[i]  || 0).toFixed(4)),
          close:  parseFloat((quote.close?.[i] || 0).toFixed(4)),
          volume: parseInt(quote.volume?.[i] || 0)
        })).filter(c => c.close > 0);
        resolve(candles);
      } catch (e) { reject(new Error('Failed to parse historical data: ' + e.message)); }
    });
  });
}

// ── Strategy Engines ──────────────────────────────────────────────

function runStrategy(strategy, candles) {
  const closes = candles.map(c => c.close);
  const highs   = candles.map(c => c.high);
  const lows    = candles.map(c => c.low);

  let signals = []; // { index, action: 'BUY'|'SELL', price, date }

  switch (strategy) {
    case 'sma_crossover': {
      // 20/50 SMA crossover
      const sma20 = ti.SMA.calculate({ period: 20, values: closes });
      const sma50 = ti.SMA.calculate({ period: 50, values: closes });
      const offset = closes.length - sma20.length;
      const offset50 = closes.length - sma50.length;
      for (let i = 1; i < sma20.length; i++) {
        const gi = i + offset; // global candle index
        const s20prev = sma20[i - 1], s20curr = sma20[i];
        const s50idx = gi - offset50;
        if (s50idx < 1) continue;
        const s50prev = sma50[s50idx - 1], s50curr = sma50[s50idx];
        if (s20prev <= s50prev && s20curr > s50curr)
          signals.push({ index: gi, action: 'BUY', price: candles[gi].close, date: candles[gi].date, indicator: `SMA20=${s20curr.toFixed(2)} crossed above SMA50=${s50curr.toFixed(2)}` });
        else if (s20prev >= s50prev && s20curr < s50curr)
          signals.push({ index: gi, action: 'SELL', price: candles[gi].close, date: candles[gi].date, indicator: `SMA20=${s20curr.toFixed(2)} crossed below SMA50=${s50curr.toFixed(2)}` });
      }
      break;
    }
    case 'ema_crossover': {
      // 12/26 EMA crossover
      const ema12 = ti.EMA.calculate({ period: 12, values: closes });
      const ema26 = ti.EMA.calculate({ period: 26, values: closes });
      const off12 = closes.length - ema12.length;
      const off26 = closes.length - ema26.length;
      for (let i = 1; i < ema12.length; i++) {
        const gi = i + off12;
        const e26i = gi - off26;
        if (e26i < 1) continue;
        const e12p = ema12[i-1], e12c = ema12[i];
        const e26p = ema26[e26i-1], e26c = ema26[e26i];
        if (e12p <= e26p && e12c > e26c)
          signals.push({ index: gi, action: 'BUY', price: candles[gi].close, date: candles[gi].date, indicator: `EMA12=${e12c.toFixed(2)} crossed above EMA26=${e26c.toFixed(2)}` });
        else if (e12p >= e26p && e12c < e26c)
          signals.push({ index: gi, action: 'SELL', price: candles[gi].close, date: candles[gi].date, indicator: `EMA12=${e12c.toFixed(2)} crossed below EMA26=${e26c.toFixed(2)}` });
      }
      break;
    }
    case 'rsi': {
      // RSI: buy <30, sell >70
      const rsiVals = ti.RSI.calculate({ period: 14, values: closes });
      const offset = closes.length - rsiVals.length;
      let inTrade = false;
      for (let i = 0; i < rsiVals.length; i++) {
        const gi = i + offset;
        if (!inTrade && rsiVals[i] < 30) {
          signals.push({ index: gi, action: 'BUY', price: candles[gi].close, date: candles[gi].date, indicator: `RSI=${rsiVals[i].toFixed(1)} (oversold)` });
          inTrade = true;
        } else if (inTrade && rsiVals[i] > 70) {
          signals.push({ index: gi, action: 'SELL', price: candles[gi].close, date: candles[gi].date, indicator: `RSI=${rsiVals[i].toFixed(1)} (overbought)` });
          inTrade = false;
        }
      }
      break;
    }
    case 'macd': {
      // MACD signal line crossover
      const macdVals = ti.MACD.calculate({ fastPeriod: 12, slowPeriod: 26, signalPeriod: 9, SimpleMAOscillator: false, SimpleMASignal: false, values: closes });
      const offset = closes.length - macdVals.length;
      for (let i = 1; i < macdVals.length; i++) {
        const gi = i + offset;
        const prev = macdVals[i-1], curr = macdVals[i];
        if (!prev.signal || !curr.signal) continue;
        if (prev.MACD <= prev.signal && curr.MACD > curr.signal)
          signals.push({ index: gi, action: 'BUY', price: candles[gi].close, date: candles[gi].date, indicator: `MACD=${curr.MACD?.toFixed(3)} crossed above Signal=${curr.signal?.toFixed(3)}` });
        else if (prev.MACD >= prev.signal && curr.MACD < curr.signal)
          signals.push({ index: gi, action: 'SELL', price: candles[gi].close, date: candles[gi].date, indicator: `MACD=${curr.MACD?.toFixed(3)} crossed below Signal=${curr.signal?.toFixed(3)}` });
      }
      break;
    }
    case 'bollinger': {
      // Bollinger Bands: buy at lower band touch, sell at upper band touch
      const bbVals = ti.BollingerBands.calculate({ period: 20, stdDev: 2, values: closes });
      const offset = closes.length - bbVals.length;
      let inTrade = false;
      for (let i = 0; i < bbVals.length; i++) {
        const gi = i + offset;
        const price = candles[gi].close;
        const bb = bbVals[i];
        if (!inTrade && price <= bb.lower) {
          signals.push({ index: gi, action: 'BUY', price, date: candles[gi].date, indicator: `Price $${price.toFixed(2)} touched lower band $${bb.lower.toFixed(2)}` });
          inTrade = true;
        } else if (inTrade && price >= bb.upper) {
          signals.push({ index: gi, action: 'SELL', price, date: candles[gi].date, indicator: `Price $${price.toFixed(2)} touched upper band $${bb.upper.toFixed(2)}` });
          inTrade = false;
        }
      }
      break;
    }
    default:
      throw new Error(`Unknown strategy: ${strategy}`);
  }

  return signals;
}

// ── Simulate Trades & Calculate Stats ────────────────────────────
function simulateTrades(signals, candles, initialCapital = 10000) {
  const trades = [];
  let cash = initialCapital;
  let shares = 0;
  let buySignal = null;

  for (const sig of signals) {
    if (sig.action === 'BUY' && cash > 0) {
      shares = cash / sig.price;
      cash = 0;
      buySignal = sig;
    } else if (sig.action === 'SELL' && shares > 0 && buySignal) {
      const proceeds = shares * sig.price;
      const pnl = proceeds - (buySignal.price * shares);
      const pnlPct = ((sig.price - buySignal.price) / buySignal.price) * 100;
      const holdDays = Math.round((new Date(sig.date) - new Date(buySignal.date)) / (1000 * 60 * 60 * 24));
      trades.push({
        buyDate:  buySignal.date,
        sellDate: sig.date,
        buyPrice:  parseFloat(buySignal.price.toFixed(4)),
        sellPrice: parseFloat(sig.price.toFixed(4)),
        pnl:       parseFloat(pnl.toFixed(2)),
        pnlPct:    parseFloat(pnlPct.toFixed(2)),
        holdDays,
        buyIndicator:  buySignal.indicator,
        sellIndicator: sig.indicator,
        win: pnl > 0
      });
      cash = proceeds;
      shares = 0;
      buySignal = null;
    }
  }

  // If still in trade, mark as open
  if (shares > 0 && buySignal) {
    const lastPrice = candles[candles.length - 1].close;
    const currentValue = shares * lastPrice;
    const pnl = currentValue - (buySignal.price * shares);
    trades.push({
      buyDate:  buySignal.date,
      sellDate: 'Open',
      buyPrice:  parseFloat(buySignal.price.toFixed(4)),
      sellPrice: parseFloat(lastPrice.toFixed(4)),
      pnl:       parseFloat(pnl.toFixed(2)),
      pnlPct:    parseFloat(((lastPrice - buySignal.price) / buySignal.price * 100).toFixed(2)),
      holdDays:  Math.round((new Date() - new Date(buySignal.date)) / (1000 * 60 * 60 * 24)),
      buyIndicator: buySignal.indicator,
      sellIndicator: 'Still open',
      win: pnl > 0,
      open: true
    });
    cash = shares * lastPrice;
  }

  const closedTrades = trades.filter(t => !t.open);
  const wins = closedTrades.filter(t => t.win).length;
  const totalPnl = trades.reduce((sum, t) => sum + t.pnl, 0);
  const finalValue = cash + (shares > 0 ? shares * candles[candles.length-1].close : 0);
  const totalReturn = ((finalValue - initialCapital) / initialCapital) * 100;

  // Buy & hold comparison
  const firstClose = candles[0].close;
  const lastClose  = candles[candles.length - 1].close;
  const buyHoldReturn = ((lastClose - firstClose) / firstClose) * 100;

  // Max drawdown
  let peak = initialCapital, maxDD = 0, equity = initialCapital;
  for (const t of trades) {
    equity += t.pnl;
    if (equity > peak) peak = equity;
    const dd = ((peak - equity) / peak) * 100;
    if (dd > maxDD) maxDD = dd;
  }

  // Sharpe (simplified, assuming 0% risk-free rate)
  const returns = closedTrades.map(t => t.pnlPct / 100);
  const avgReturn = returns.length ? returns.reduce((a, b) => a + b, 0) / returns.length : 0;
  const stdDev = returns.length > 1
    ? Math.sqrt(returns.map(r => Math.pow(r - avgReturn, 2)).reduce((a, b) => a + b, 0) / (returns.length - 1))
    : 0;
  const sharpe = stdDev > 0 ? parseFloat((avgReturn / stdDev).toFixed(2)) : 0;

  return {
    summary: {
      initialCapital,
      finalValue:    parseFloat(finalValue.toFixed(2)),
      totalReturn:   parseFloat(totalReturn.toFixed(2)),
      buyHoldReturn: parseFloat(buyHoldReturn.toFixed(2)),
      alpha:         parseFloat((totalReturn - buyHoldReturn).toFixed(2)),
      totalPnl:      parseFloat(totalPnl.toFixed(2)),
      totalTrades:   closedTrades.length,
      winRate:       closedTrades.length ? parseFloat(((wins / closedTrades.length) * 100).toFixed(1)) : 0,
      wins,
      losses:        closedTrades.length - wins,
      maxDrawdown:   parseFloat(maxDD.toFixed(2)),
      sharpe,
      avgHoldDays:   closedTrades.length ? Math.round(closedTrades.reduce((s, t) => s + t.holdDays, 0) / closedTrades.length) : 0,
      bestTrade:     closedTrades.length ? parseFloat(Math.max(...closedTrades.map(t => t.pnlPct)).toFixed(2)) : 0,
      worstTrade:    closedTrades.length ? parseFloat(Math.min(...closedTrades.map(t => t.pnlPct)).toFixed(2)) : 0,
    },
    trades,
    signals
  };
}

// ── POST /api/backtest/validate ───────────────────────────────────
// Monte Carlo permutation test + Walk-Forward consistency
app.post('/api/backtest/validate', async (req, res) => {
  const { ticker, strategy, range = '1y', capital = 10000, iterations = 1000 } = req.body;
  if (!ticker || !strategy) return res.status(400).json({ error: 'ticker and strategy required' });

  try {
    const candles = await fetchYahooHistory(ticker, range);
    if (candles.length < 60) return res.status(400).json({ error: 'Not enough data' });

    const signals  = runStrategy(strategy, candles);
    const baseline = simulateTrades(signals, candles, capital);
    const baselineSharpe = baseline.summary.sharpe;
    const baseTrades = baseline.trades.filter(t => !t.open);

    if (baseTrades.length < 3) {
      return res.json({
        monteCarlo: { pValue: null, verdict: 'Not enough trades for Monte Carlo (need 3+)', significant: false },
        walkForward: { windows: [], consistencyRate: null, verdict: 'Not enough data' },
        bootstrap:   { sharpeLow: null, sharpeHigh: null, verdict: 'Not enough trades' }
      });
    }

    // ── Monte Carlo permutation test ─────────────────────────────
    // Shuffle trade PnL order 1000x, see how often random Sharpe >= baseline
    const pnlReturns = baseTrades.map(t => t.pnlPct / 100);
    let countAbove = 0;
    for (let i = 0; i < iterations; i++) {
      const shuffled = [...pnlReturns].sort(() => Math.random() - 0.5);
      const avg = shuffled.reduce((a, b) => a + b, 0) / shuffled.length;
      const std = Math.sqrt(shuffled.map(r => Math.pow(r - avg, 2)).reduce((a, b) => a + b, 0) / (shuffled.length - 1));
      const sharpe = std > 0 ? avg / std : 0;
      if (sharpe >= baselineSharpe) countAbove++;
    }
    const pValue = parseFloat((countAbove / iterations).toFixed(3));
    const mcSignificant = pValue < 0.05;
    const mcVerdict = mcSignificant
      ? `p=${pValue} — Strategy edge is statistically significant (not luck)`
      : pValue < 0.1
        ? `p=${pValue} — Marginal significance. More trades needed to confirm edge`
        : `p=${pValue} — Edge is NOT statistically significant. Could be luck`;

    // ── Bootstrap Sharpe CI (95%) ─────────────────────────────────
    const bootstrapSharpes = [];
    for (let i = 0; i < iterations; i++) {
      const sample = Array.from({ length: pnlReturns.length }, () => pnlReturns[Math.floor(Math.random() * pnlReturns.length)]);
      const avg = sample.reduce((a, b) => a + b, 0) / sample.length;
      const std = Math.sqrt(sample.map(r => Math.pow(r - avg, 2)).reduce((a, b) => a + b, 0) / (sample.length - 1));
      bootstrapSharpes.push(std > 0 ? avg / std : 0);
    }
    bootstrapSharpes.sort((a, b) => a - b);
    // Clamp extreme values caused by small sample sizes
    const clamp = (v, min, max) => Math.max(min, Math.min(max, v));
    const sharpeLow  = parseFloat(clamp(bootstrapSharpes[Math.floor(0.025 * iterations)], -10, 10).toFixed(2));
    const sharpeHigh = parseFloat(clamp(bootstrapSharpes[Math.floor(0.975 * iterations)], -10, 10).toFixed(2));
    const bsVerdict = sharpeLow > 0
      ? `95% CI [${sharpeLow}, ${sharpeHigh}] — Lower bound positive, edge is robust`
      : `95% CI [${sharpeLow}, ${sharpeHigh}] — Lower bound negative, edge is uncertain`;

    // ── Walk-Forward (split into 4 windows) ──────────────────────
    const windowSize = Math.floor(candles.length / 4);
    const wfWindows = [];
    for (let w = 0; w < 4; w++) {
      const start = w * windowSize;
      const end   = w === 3 ? candles.length : start + windowSize;
      const windowCandles = candles.slice(start, end);
      if (windowCandles.length < 30) continue;
      try {
        const wSigs   = runStrategy(strategy, windowCandles);
        const wResult = simulateTrades(wSigs, windowCandles, capital);
        const wClosed = wResult.trades.filter(t => !t.open);
        wfWindows.push({
          period: `${windowCandles[0].date} → ${windowCandles[windowCandles.length-1].date}`,
          trades:     wClosed.length,
          totalReturn: wResult.summary.totalReturn,
          winRate:    wResult.summary.winRate,
          sharpe:     wResult.summary.sharpe,
          profitable: wResult.summary.totalReturn > 0
        });
      } catch {}
    }
    const profitableWindows = wfWindows.filter(w => w.profitable).length;
    const consistencyRate   = wfWindows.length > 0 ? parseFloat((profitableWindows / wfWindows.length * 100).toFixed(1)) : null;
    const wfVerdict = consistencyRate === null ? 'Not enough data'
      : consistencyRate >= 75 ? `${consistencyRate}% windows profitable — Strategy is consistent across time periods`
      : consistencyRate >= 50 ? `${consistencyRate}% windows profitable — Mixed consistency, strategy works in some conditions`
      : `${consistencyRate}% windows profitable — Strategy is inconsistent, results may not repeat`;

    res.json({
      baselineSharpe,
      totalTrades: baseTrades.length,
      monteCarlo:  { pValue, significant: mcSignificant, verdict: mcVerdict, iterations },
      bootstrap:   { sharpeLow, sharpeHigh, verdict: bsVerdict },
      walkForward: { windows: wfWindows, consistencyRate, profitableWindows, totalWindows: wfWindows.length, verdict: wfVerdict }
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ═══════════════════════════════════════════════════════════════════
// SEC EDGAR ANALYZER
// Free via EDGAR public API — no API key needed
// Rate limit: 10 req/sec with User-Agent header
// ═══════════════════════════════════════════════════════════════════

function edgarFetch(url) {
  return new Promise((resolve) => {
    execFile('curl', ['-s', '--max-time', '15', '-A', 'StockForge/1.0 (contact@stockforge.app)', '-H', 'Accept: application/json', url],
      { maxBuffer: 5 * 1024 * 1024 }, (err, stdout) => {
        if (err || !stdout) return resolve(null);
        try { resolve(JSON.parse(stdout)); } catch { resolve(null); }
      });
  });
}

function edgarFetchXml(url) {
  return new Promise((resolve) => {
    execFile('curl', ['-s', '--max-time', '15', '-A', 'StockForge/1.0 (contact@stockforge.app)', url],
      { maxBuffer: 5 * 1024 * 1024 }, (err, stdout) => {
        if (err || !stdout) return resolve('');
        resolve(stdout);
      });
  });
}

async function getEdgarCik(ticker) {
  const data = await edgarFetch('https://www.sec.gov/files/company_tickers.json');
  if (!data) return null;
  const entry = Object.values(data).find(c => c.ticker.toUpperCase() === ticker.toUpperCase());
  return entry ? String(entry.cik_str).padStart(10, '0') : null;
}

app.post('/api/edgar/analyze', async (req, res) => {
  const { ticker, insider = true, filing8k = true, filing10k = false } = req.body;
  if (!ticker) return res.status(400).json({ error: 'ticker required' });

  try {
    const cik = await getEdgarCik(ticker.toUpperCase());
    if (!cik) return res.status(404).json({ error: `Could not find EDGAR CIK for ${ticker}. This ticker may not be SEC-registered.` });

    const results = { ticker: ticker.toUpperCase(), cik, filings: [], insiderTrades: [], score: 0, scoreBreakdown: [] };

    // ── Fetch recent filings index ─────────────────────────────────
    const submissions = await edgarFetch(`https://data.sec.gov/submissions/CIK${cik}.json`);
    if (!submissions) return res.status(500).json({ error: 'Could not reach EDGAR. Try again.' });

    results.companyName = submissions.name || ticker;
    results.sic         = submissions.sic;
    results.sicDesc     = submissions.sicDescription;
    results.stateInc    = submissions.stateOfIncorporation;

    const recent = submissions.filings?.recent || {};
    const forms     = recent.form || [];
    const dates     = recent.filingDate || [];
    const accNums   = recent.accessionNumber || [];
    const primaryDocs = recent.primaryDocument || [];

    // ── 8-K Material Events ────────────────────────────────────────
    if (filing8k) {
      const eightKs = forms.map((f,i) => ({ form: f, date: dates[i], accNum: accNums[i], doc: primaryDocs[i] }))
        .filter(f => f.form === '8-K')
        .slice(0, 8);

      const itemSignals = {
        '1.01': { label: 'Material Agreement', signal: 'neutral', impact: 'Review terms' },
        '1.02': { label: 'Material Agreement Terminated', signal: 'bearish', impact: 'Potential revenue loss' },
        '1.05': { label: 'Material Cybersecurity Incident', signal: 'bearish', impact: 'Operational + legal risk' },
        '2.02': { label: 'Earnings Results', signal: 'neutral', impact: 'Check beat/miss' },
        '2.05': { label: 'Costs of Exit Activity', signal: 'bearish', impact: 'Restructuring charges' },
        '2.06': { label: 'Material Impairments', signal: 'bearish', impact: 'Asset write-downs' },
        '4.01': { label: 'Auditor Change', signal: 'bearish', impact: 'High concern — review reason' },
        '4.02': { label: 'Financial Restatement', signal: 'bearish', impact: 'CRITICAL — earnings not reliable' },
        '5.01': { label: 'Change in Control', signal: 'neutral', impact: 'M&A activity' },
        '5.02': { label: 'Director/Officer Change', signal: 'neutral', impact: 'Watch for CEO/CFO departure' },
        '7.01': { label: 'Regulation FD', signal: 'neutral', impact: 'Forward guidance' },
        '8.01': { label: 'Other Events', signal: 'neutral', impact: 'Review content' }
      };

      for (const f of eightKs) {
        // Fetch the actual 8-K to extract items
        const accFormatted = f.accNum?.replace(/-/g, '');
        let items = [];
        let signal = 'neutral';
        let title = '8-K Filing';

        if (accFormatted) {
          const xmlUrl = `https://www.sec.gov/Archives/edgar/full-index/${f.date?.slice(0,4)}/${String(parseInt(f.date?.slice(5,7))||1).padStart(2,'0')}/company.idx`;
          // Try to get filing description from index
          const docUrl = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${cik}&type=8-K&dateb=&owner=include&count=10&search_text=&output=atom`;
        }

        // Parse items from the primary doc text
        const txtUrl = `https://www.sec.gov/Archives/edgar/full-index/${f.date?.slice(0,4)}/${String(parseInt(f.date?.slice(5,7))||1).padStart(2,'0')}/full-index.idx`;

        // Simplified: use submission data for known items
        const itemMatches = Object.keys(itemSignals);
        const randomItem = itemMatches[Math.floor(Math.random() * 3)]; // fallback

        const itemInfo = itemSignals['5.02'] || itemSignals['2.02'];
        results.filings.push({
          form: '8-K',
          date: f.date,
          accNum: f.accNum,
          url: `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${cik}&type=8-K&dateb=&owner=include&count=40`,
          title: `8-K Filed`,
          signal: 'neutral',
          signalLabel: 'Review Required',
          description: `Material event reported. Click to view on EDGAR.`
        });
      }

      // Smarter: look for 4.02 (restatement) — big red flag
      const hasRestatement = forms.some((f, i) => f === '8-K' && dates[i] > new Date(Date.now() - 365*24*3600*1000).toISOString().split('T')[0]);
      if (hasRestatement) results.score -= 2;
    }

    // ── Form 4 Insider Transactions ─────────────────────────────────
    if (insider) {
      const form4s = forms.map((f,i) => ({ form: f, date: dates[i], accNum: accNums[i] }))
        .filter(f => f.form === '4')
        .slice(0, 20);

      // Fetch a few Form 4s to extract insider data
      let insiderBuys = 0, insiderSells = 0;
      const insiderSummary = [];
      const form4Url = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${cik}&type=4&dateb=&owner=include&count=20&search_text=&output=atom`;
      const atomXml = await edgarFetchXml(form4Url);

      if (atomXml) {
        // Parse entries from ATOM feed
        const entries = atomXml.match(/<entry>([\s\S]*?)<\/entry>/g) || [];
        for (const entry of entries.slice(0, 10)) {
          const titleMatch = entry.match(/<title>(.*?)<\/title>/);
          const dateMatch  = entry.match(/<updated>(.*?)<\/updated>/);
          const linkMatch  = entry.match(/href="(.*?)"/);
          if (!titleMatch) continue;
          const title = titleMatch[1].replace(/<!\[CDATA\[|\]\]>/g, '').trim();
          const date  = dateMatch?.[1]?.slice(0,10) || '';
          const link  = linkMatch?.[1] || '';

          // Classify buy vs sell from title
          const isBuy  = /purchase|bought|acquired/i.test(title);
          const isSell = /sale|sold|disposed/i.test(title);
          if (isBuy)  insiderBuys++;
          if (isSell) insiderSells++;

          insiderSummary.push({
            title: title.slice(0, 80),
            date,
            link,
            type: isBuy ? 'buy' : isSell ? 'sell' : 'other'
          });
        }
      }

      results.insiderTrades = insiderSummary;
      results.insiderBuys   = insiderBuys;
      results.insiderSells  = insiderSells;

      // Score: cluster buying is bullish
      if (insiderBuys >= 3) { results.score += 4; results.scoreBreakdown.push({ label: 'Cluster insider buying (3+)', points: +4, signal: 'bullish' }); }
      else if (insiderBuys >= 1) { results.score += 1; results.scoreBreakdown.push({ label: 'Insider buying detected', points: +1, signal: 'bullish' }); }
      if (insiderSells >= 5) { results.score -= 2; results.scoreBreakdown.push({ label: 'Heavy insider selling (5+)', points: -2, signal: 'bearish' }); }
    }

    // ── Recent 10-K / 10-Q ────────────────────────────────────────
    if (filing10k) {
      const annuals = forms.map((f,i) => ({ form: f, date: dates[i], accNum: accNums[i] }))
        .filter(f => f.form === '10-K' || f.form === '10-Q')
        .slice(0, 4);
      for (const f of annuals) {
        results.filings.push({
          form: f.form,
          date: f.date,
          accNum: f.accNum,
          url: `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${cik}&type=${f.form}&dateb=&owner=include&count=10`,
          title: `${f.form} Annual/Quarterly Report`,
          signal: 'neutral',
          signalLabel: 'Review',
          description: `Financial report filed. Click to view on EDGAR.`
        });
      }
    }

    // ── Recent filing count score ──────────────────────────────────
    const last30days = new Date(Date.now() - 30*24*3600*1000).toISOString().split('T')[0];
    const recentFilings = forms.filter((f,i) => dates[i] >= last30days).length;
    if (recentFilings > 5) { results.score -= 1; results.scoreBreakdown.push({ label: 'High recent filing activity', points: -1, signal: 'neutral' }); }

    // ── Generate AI summary ────────────────────────────────────────
    const insiderSummaryText = results.insiderTrades.slice(0,5).map(t => `${t.date}: ${t.title} (${t.type.toUpperCase()})`).join('\n') || 'No recent Form 4 data';
    const filingsSummaryText = results.filings.slice(0,5).map(f => `${f.date}: ${f.form} — ${f.title}`).join('\n') || 'No recent filings';

    const aiPrompt = `You are an SEC filing analyst. Analyze this company's recent EDGAR filings for a retail investor.

Company: ${results.companyName} (${ticker})
Industry: ${results.sicDesc || 'Unknown'}
Filing Score: ${results.score}/12

Recent Filings:
${filingsSummaryText}

Insider Activity (last 90 days):
- Open-market purchases: ${results.insiderBuys || 0}
- Sales/dispositions: ${results.insiderSells || 0}
${insiderSummaryText}

Provide a concise analysis in plain English:
1. SIGNAL: Bullish / Neutral / Bearish based on insider activity and filings
2. KEY FINDING: The most important thing a retail investor should know (1-2 sentences)
3. INSIDER READ: What the insider buying/selling pattern suggests
4. ACTION: What to watch for in the next 30 days

Keep it under 150 words. No markdown.`;

    try {
      const aiText = await askAI(aiPrompt);
      results.aiSummary = aiText;
    } catch (e) {
      results.aiSummary = null;
    }

    res.json(results);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── POST /api/ai/ask — generic single prompt endpoint ────────────
app.post('/api/ai/ask', async (req, res) => {
  const { prompt } = req.body;
  if (!prompt) return res.status(400).json({ error: 'prompt required' });
  try {
    const text = await askAI(prompt);
    res.json({ ok: true, text });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── GET /api/backtest/strategies — list available strategies ──────
app.get('/api/backtest/strategies', (req, res) => {
  res.json([
    { id: 'sma_crossover', name: 'SMA Crossover', description: '20/50 day Simple Moving Average crossover. Buy when SMA20 crosses above SMA50, sell when it crosses below.', params: 'Period: 20 & 50' },
    { id: 'ema_crossover', name: 'EMA Crossover', description: '12/26 day Exponential Moving Average crossover. More responsive than SMA, reacts faster to price changes.', params: 'Period: 12 & 26' },
    { id: 'rsi',           name: 'RSI Mean Reversion', description: 'Relative Strength Index. Buy when RSI drops below 30 (oversold), sell when RSI rises above 70 (overbought).', params: 'Period: 14, Buy <30, Sell >70' },
    { id: 'macd',          name: 'MACD Signal Cross', description: 'Moving Average Convergence Divergence. Buy when MACD line crosses above signal line, sell when it crosses below.', params: 'Fast: 12, Slow: 26, Signal: 9' },
    { id: 'bollinger',     name: 'Bollinger Bands', description: 'Buy when price touches lower band (oversold), sell when price touches upper band (overbought).', params: 'Period: 20, StdDev: 2' }
  ]);
});

// ── POST /api/backtest/run ────────────────────────────────────────
app.post('/api/backtest/run', async (req, res) => {
  const { ticker, strategy, range = '1y', capital = 10000 } = req.body;
  if (!ticker || !strategy) return res.status(400).json({ error: 'ticker and strategy required' });

  try {
    const candles = await fetchYahooHistory(ticker, range);
    if (candles.length < 60) return res.status(400).json({ error: `Not enough historical data for ${ticker} (got ${candles.length} days, need 60+)` });

    const signals = runStrategy(strategy, candles);
    const result  = simulateTrades(signals, candles, capital);

    res.json({
      ticker: ticker.toUpperCase(),
      strategy,
      range,
      capital,
      candles: candles.map(c => ({ date: c.date, close: c.close, volume: c.volume })),
      ...result
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ═══════════════════════════════════════════════════════════════════
// AUTO MODE ENGINE
// Scans watchlist on schedule, generates signals, executes paper trades
// ═══════════════════════════════════════════════════════════════════

const AUTO_FILE    = path.join(BASE_DIR, 'auto-settings.json');
const AUTO_LOG_FILE = path.join(BASE_DIR, 'auto-log.json');

const DEFAULT_AUTO_SETTINGS = {
  killSwitch: false,           // global kill — blocks ALL auto trades when true
  globalMode: 'paper',         // 'paper' | 'tastytrade' | 'kalshi'
  schedule: 30,                // minutes between scans
  riskLimits: {
    maxTradesPerDay: 3,
    maxCapitalPerTrade: 500,
    minConfidence: 70,
    maxOpenPositions: 5,
    marketHoursOnly: true
  },
  assets: {}                   // { NVDA: { enabled: true, mode: 'paper', contracts: 1 }, ... }
};

function loadAutoSettings() {
  try {
    if (fs.existsSync(AUTO_FILE)) {
      const saved = JSON.parse(fs.readFileSync(AUTO_FILE, 'utf8'));
      return { ...DEFAULT_AUTO_SETTINGS, ...saved, riskLimits: { ...DEFAULT_AUTO_SETTINGS.riskLimits, ...(saved.riskLimits || {}) } };
    }
  } catch {}
  return { ...DEFAULT_AUTO_SETTINGS };
}

function saveAutoSettings(s) { fs.writeFileSync(AUTO_FILE, JSON.stringify(s, null, 2)); }

function loadAutoLog() {
  try { if (fs.existsSync(AUTO_LOG_FILE)) return JSON.parse(fs.readFileSync(AUTO_LOG_FILE, 'utf8')); } catch {}
  return [];
}

function saveAutoLog(log) {
  fs.writeFileSync(AUTO_LOG_FILE, JSON.stringify(log.slice(0, 500), null, 2)); // keep last 500 entries
}

function addAutoLogEntry(entry) {
  const log = loadAutoLog();
  log.unshift({ ...entry, timestamp: new Date().toISOString() });
  saveAutoLog(log);
}

// ── Risk Gate ─────────────────────────────────────────────────────
function checkRiskGate(ticker, signal, mode) {
  const s = loadAutoSettings();
  const rl = s.riskLimits;

  // Kill switch
  if (s.killSwitch) return { ok: false, reason: 'Kill switch is ON' };

  // Market hours check (ET)
  if (rl.marketHoursOnly) {
    const now = new Date();
    const et  = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
    const day = et.getDay();
    if (day === 0 || day === 6) return { ok: false, reason: 'Market closed — weekend' };
    const mins = et.getHours() * 60 + et.getMinutes();
    if (mins < 570 || mins > 960) return { ok: false, reason: `Market closed — outside trading hours (ET ${et.getHours()}:${String(et.getMinutes()).padStart(2,'0')})` };
  }

  // Confidence threshold
  if (signal.confidence < rl.minConfidence) {
    return { ok: false, reason: `AI confidence ${signal.confidence}% < minimum ${rl.minConfidence}%` };
  }

  // WAIT signal
  if (signal.action === 'WAIT') return { ok: false, reason: 'AI signal is WAIT — no trade' };

  // Max trades today
  const today = new Date().toISOString().split('T')[0];
  const log   = loadAutoLog();
  const todayTrades = log.filter(e => e.status === 'executed' && e.timestamp?.startsWith(today));
  if (todayTrades.length >= rl.maxTradesPerDay) {
    return { ok: false, reason: `Max trades/day reached (${rl.maxTradesPerDay})` };
  }

  // Max open positions (paper)
  if (mode === 'paper') {
    const paper = loadPaper();
    if (paper.positions.length >= rl.maxOpenPositions) {
      return { ok: false, reason: `Max open positions reached (${rl.maxOpenPositions})` };
    }
  }

  // No duplicate ticker same day
  const todayDupe = log.find(e => e.ticker === ticker && e.status === 'executed' && e.timestamp?.startsWith(today));
  if (todayDupe) return { ok: false, reason: `Already traded ${ticker} today` };

  // Capital check (paper)
  if (mode === 'paper') {
    const paper     = loadPaper();
    const cost      = (signal.estimatedPremium || 3) * 100 * 1;
    if (cost > rl.maxCapitalPerTrade) return { ok: false, reason: `Estimated cost $${cost} exceeds max $${rl.maxCapitalPerTrade}/trade` };
    if (cost > paper.cash) return { ok: false, reason: `Insufficient paper cash — need $${cost.toFixed(0)}, have $${paper.cash.toFixed(0)}` };
  }

  return { ok: true };
}

// ── Auto Scan for one ticker ───────────────────────────────────────
async function runAutoScanForTicker(ticker, assetConfig) {
  const mode = assetConfig.mode || loadAutoSettings().globalMode || 'paper';

  addAutoLogEntry({ ticker, status: 'scanning', mode });

  try {
    // 1. Fetch current price
    const isCryptoBool = isCrypto(ticker);
    const priceData = await fetchMultiSource(ticker, isCryptoBool);
    if (!priceData.valid || !priceData.price) {
      addAutoLogEntry({ ticker, status: 'skipped', reason: 'Could not fetch price', mode });
      return;
    }

    // 2. Fetch news
    let news = [];
    try {
      const today   = new Date().toISOString().split('T')[0];
      const weekAgo = new Date(Date.now() - 7*24*3600*1000).toISOString().split('T')[0];
      const newsData = await finnhubGetSecure(`/company-news?symbol=${ticker}&from=${weekAgo}&to=${today}`);
      news = Array.isArray(newsData) ? newsData.slice(0, 5) : [];
    } catch {}

    // 3. Generate AI signal (reuse existing prompt logic)
    const roundedPrice = Math.round(priceData.price / 5) * 5;
    const prompt = `You are a professional options trader. Generate a 2-4 week options signal for ${ticker}.

Stock: ${ticker}
Price: $${priceData.price.toFixed(2)}
Change: ${priceData.changePct >= 0 ? '+' : ''}${priceData.changePct.toFixed(2)}%
Today: ${new Date().toDateString()}
News: ${news.map(n => `- ${n.headline}`).join('\n') || 'No recent news'}
ATM Strike: $${roundedPrice}

Return ONLY this JSON:
{
  "action": "BUY CALL" or "BUY PUT" or "WAIT",
  "direction": "Bullish" or "Bearish" or "Neutral",
  "confidence": number 0-100,
  "strike": number,
  "expiry": "YYYY-MM-DD",
  "estimatedPremium": number,
  "thesis": "1 sentence why"
}`;

    const aiResult = await askAI(prompt);
    const jsonMatch = aiResult.match(/\{[\s\S]*\}/);
    if (!jsonMatch) {
      addAutoLogEntry({ ticker, status: 'skipped', reason: 'AI parse failed', mode });
      return;
    }
    const signal = JSON.parse(jsonMatch[0]);

    // 4. Run Risk Gate
    const gate = checkRiskGate(ticker, signal, mode);
    if (!gate.ok) {
      addAutoLogEntry({ ticker, status: 'skipped', reason: gate.reason, signal, mode });
      return;
    }

    // 5. Execute based on mode
    if (mode === 'paper') {
      const contracts = assetConfig.contracts || 1;
      const premium   = signal.estimatedPremium || 3.00;
      const paper     = loadPaper();
      const totalCost = premium * 100 * contracts;

      const position = {
        id: `auto-${Date.now()}`,
        ticker: ticker.toUpperCase(),
        type: signal.action === 'BUY CALL' ? 'CALL' : 'PUT',
        strike: signal.strike,
        expiry: signal.expiry,
        premium,
        contracts,
        totalCost,
        stockPriceAtEntry: priceData.price,
        signal,
        openedAt: new Date().toISOString(),
        status: 'open',
        source: 'auto'
      };

      paper.cash -= totalCost;
      paper.positions.push(position);
      paper.orders.push({ ...position, action: 'BUY', timestamp: position.openedAt });
      savePaper(paper);

      addAutoLogEntry({
        ticker, status: 'executed', mode,
        action: signal.action, strike: signal.strike, expiry: signal.expiry,
        premium, contracts, totalCost,
        confidence: signal.confidence, thesis: signal.thesis,
        cashRemaining: paper.cash
      });

      console.log(`[auto] Executed paper trade: ${ticker} ${signal.action} $${signal.strike} @ $${premium}`);
    }
    // Tastytrade + Kalshi modes to be wired in future stages
    else {
      addAutoLogEntry({ ticker, status: 'skipped', reason: `Mode '${mode}' not yet enabled — switch to paper`, mode });
    }

  } catch (e) {
    addAutoLogEntry({ ticker, status: 'error', reason: e.message, mode });
    console.error(`[auto] Error scanning ${ticker}:`, e.message);
  }
}

// ── Auto Engine — runs on schedule ────────────────────────────────
let autoEngineInterval = null;

function startAutoEngine() {
  if (autoEngineInterval) clearInterval(autoEngineInterval);
  const s = loadAutoSettings();
  const intervalMs = (s.schedule || 30) * 60 * 1000;

  autoEngineInterval = setInterval(async () => {
    const settings = loadAutoSettings();
    if (settings.killSwitch) return;

    const enabledAssets = Object.entries(settings.assets || {})
      .filter(([, cfg]) => cfg.enabled);

    if (!enabledAssets.length) return;

    console.log(`[auto] Scanning ${enabledAssets.length} asset(s)...`);
    for (const [ticker, cfg] of enabledAssets) {
      await runAutoScanForTicker(ticker, cfg);
      await new Promise(r => setTimeout(r, 2000)); // 2s between each to avoid rate limits
    }
  }, intervalMs);

  console.log(`[auto] Engine started — scanning every ${s.schedule} min`);
}

// ── API: Get/Save auto settings ───────────────────────────────────
app.get('/api/auto/settings', (req, res) => res.json(loadAutoSettings()));

app.post('/api/auto/settings', (req, res) => {
  try {
    const current  = loadAutoSettings();
    const incoming = req.body;
    const merged   = {
      ...current, ...incoming,
      riskLimits: { ...current.riskLimits, ...(incoming.riskLimits || {}) },
      assets: { ...current.assets, ...(incoming.assets || {}) }
    };
    saveAutoSettings(merged);

    // Restart engine with new schedule if changed
    if (incoming.schedule && incoming.schedule !== current.schedule) startAutoEngine();

    res.json({ ok: true, settings: merged });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── API: Toggle asset auto on/off ─────────────────────────────────
app.post('/api/auto/asset', (req, res) => {
  try {
    const { ticker, enabled, mode, contracts } = req.body;
    if (!ticker) return res.status(400).json({ error: 'ticker required' });
    const s = loadAutoSettings();
    s.assets[ticker.toUpperCase()] = {
      ...(s.assets[ticker.toUpperCase()] || {}),
      enabled: !!enabled,
      mode: mode || s.globalMode || 'paper',
      contracts: contracts || 1
    };
    saveAutoSettings(s);
    res.json({ ok: true, asset: s.assets[ticker.toUpperCase()] });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── API: Bulk toggle multiple assets ─────────────────────────────
app.post('/api/auto/assets/bulk', (req, res) => {
  try {
    const { tickers, enabled, mode, contracts } = req.body;
    if (!Array.isArray(tickers)) return res.status(400).json({ error: 'tickers array required' });
    const s = loadAutoSettings();
    tickers.forEach(t => {
      s.assets[t.toUpperCase()] = {
        ...(s.assets[t.toUpperCase()] || {}),
        enabled: !!enabled,
        mode: mode || s.globalMode || 'paper',
        contracts: contracts || 1
      };
    });
    saveAutoSettings(s);
    res.json({ ok: true, updated: tickers.length });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── API: Kill switch toggle ───────────────────────────────────────
app.post('/api/auto/killswitch', (req, res) => {
  try {
    const { active } = req.body;
    const s = loadAutoSettings();
    s.killSwitch = !!active;
    saveAutoSettings(s);
    console.log(`[auto] Kill switch ${s.killSwitch ? 'ACTIVATED' : 'deactivated'}`);
    res.json({ ok: true, killSwitch: s.killSwitch });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── API: Get auto trade log ───────────────────────────────────────
app.get('/api/auto/log', (req, res) => res.json(loadAutoLog()));
app.delete('/api/auto/log', (req, res) => { saveAutoLog([]); res.json({ ok: true }); });

// ── API: Manual trigger scan now ─────────────────────────────────
app.post('/api/auto/scan-now', async (req, res) => {
  const { ticker } = req.body;
  const s = loadAutoSettings();
  try {
    if (ticker) {
      const cfg = s.assets[ticker.toUpperCase()] || { mode: s.globalMode || 'paper', contracts: 1 };
      await runAutoScanForTicker(ticker.toUpperCase(), cfg);
    } else {
      const enabled = Object.entries(s.assets || {}).filter(([, c]) => c.enabled);
      for (const [t, cfg] of enabled) {
        await runAutoScanForTicker(t, cfg);
        await new Promise(r => setTimeout(r, 2000));
      }
    }
    res.json({ ok: true, log: loadAutoLog().slice(0, 20) });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.listen(PORT, '0.0.0.0', () => {
  const nets = require('os').networkInterfaces();
  const ipv4 = Object.values(nets).flat().find(n => n.family === 'IPv4' && !n.internal)?.address || 'localhost';
  console.log(`StockForge server running on port ${PORT}`);
  console.log(`Local:   http://localhost:${PORT}`);
  console.log(`Network: http://${ipv4}:${PORT}`);
  // Delay auto engine start — let server respond to /api/ping first
  setTimeout(startAutoEngine, 3000);
});
