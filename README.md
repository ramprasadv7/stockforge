# StockForge Mentor

**AI-powered stock trading assistant for retail traders.**

Built on Electron + Node.js. Ships as a native macOS desktop app with a full Express API backend, live market data, AI signal generation, options tracking, backtesting, and auto trading mode.

---

## Screenshots

> Coming soon

---

## Features

### Trading
- **Live prices** — Dual-source (Finnhub + Yahoo Finance) with confidence scoring
- **Watchlist** — Stocks + crypto with real-time price updates
- **Day Trading** — AI options signal generator (BUY CALL/PUT with strike, expiry, premium, break-even)
- **Contract Monitor** — AI-powered HOLD/SELL/ROLL recommendations on open positions
- **Paper Trading** — Virtual $10,000 account to practice before going live
- **Long Term Portfolio** — Holdings tracker with AI analysis and weekly summary
- **Brokerage** — Tastytrade integration (place real options orders)
- **Kalshi** — Prediction markets integration

### AI & Research
- **5 Backtest strategies** — SMA crossover, EMA crossover, RSI, MACD, Bollinger Bands
- **Statistical Validation** — Monte Carlo p-value, Bootstrap Sharpe CI, Walk-Forward consistency
- **SEC EDGAR Analyzer** — Insider buying clusters, 8-K events, composite filing score
- **Trading Bias Report** — Disposition effect, overtrading, anchoring detection across your trade history
- **Signal Scorecard** — Track AI signal accuracy over time
- **Morning Brief** — Daily AI market summary for your watchlist
- **Smart Alerts** — Contract expiry warnings, earnings alerts, 5%+ price moves

### Auto Mode
- **Multi-select assets** from your watchlist
- **Risk Gate** — 7 checks before every trade (kill switch, market hours, confidence threshold, max trades/day, max positions, no duplicates, capital check)
- **Auto Trade Log** — Full history of every scan: executed, skipped + reason, errors
- **Kill Switch** — Instant stop for all auto trading
- **Background mode** — Stays alive in macOS tray when window is closed

### AI Providers
- **Ollama** (recommended) — Free, local, private. No API key needed
- **OpenCode** — Routes through your local OpenCode app — works with any model configured there
- **OpenAI** — GPT-4o (requires API key)
- **Anthropic** — Claude (requires API key)

---

## Installation

### Download (macOS Apple Silicon)

Download the latest DMG from [Releases](https://github.com/ramprasadv7/stockforge/releases).

```
StockForge-1.3.2-arm64.dmg
```

1. Open the DMG → drag to `/Applications`
2. Right-click → Open (first launch only, app is not notarized)
3. Or run: `xattr -cr "/Applications/StockForge.app"`

### Build from Source

**Prerequisites:**
- Node.js v18+
- npm

```bash
git clone https://github.com/ramprasadv7/stockforge.git
cd stockforge
npm install
```

**Run as desktop app (Electron):**
```bash
npm start
```

**Run as web app only (no Electron):**
```bash
node server.js
# Open http://localhost:3478
```

**Build DMG:**
```bash
npm run build:dmg        # arm64 (Apple Silicon)
npm run build:universal  # Universal (Intel + Apple Silicon)
```

---

## AI Setup

On first launch, StockForge shows a setup screen to configure your AI provider.

### Option 1 — Ollama (Recommended, Free)

No API key needed. Runs entirely on your Mac.

```bash
# Install Ollama
brew install ollama
# or download from https://ollama.com

# Pull a model (3.8GB)
ollama pull llama3.2

# Start Ollama
ollama serve
```

Then click **Retry** in the StockForge setup screen.

### Option 2 — OpenCode

If you have [OpenCode](https://opencode.ai) installed, StockForge automatically detects it and routes AI calls through it — using whatever model you have configured (Claude, GPT-4, Gemini, Groq, etc.).

Just open OpenCode before starting StockForge.

### Option 3 — OpenAI / Anthropic

Paste your API key in the setup screen or in **AI Settings**.

---

## Data Sources

| Source | Used for | Cost |
|--------|----------|------|
| Yahoo Finance | Historical OHLCV, live prices, crypto | Free |
| Finnhub | Live quotes, news, stock search | Free (shared key included, or add your own at [finnhub.io](https://finnhub.io/register)) |
| SEC EDGAR | Insider filings, 8-K events | Free (public API) |
| Kalshi | Prediction market prices | Free (own account to trade) |
| Tastytrade | Options order execution | Own account required |

---

## Project Structure

```
stockforge/
├── main.js           # Electron entry point, tray, background scan
├── preload.js        # Electron context bridge
├── server.js         # Express API server (all endpoints)
├── package.json
├── public/
│   ├── index.html    # Single-page UI
│   ├── app.js        # All frontend JavaScript (~8,600 lines)
│   ├── style.css     # All styles
│   └── fonts/        # Fira Sans + Fira Code (bundled, offline)
└── assets/
    ├── icon.icns     # macOS app icon
    └── icon.svg      # Source icon (AI-native circuit + chart design)
```

---

## Auto Mode

Auto Mode scans your watchlist on a schedule, generates AI signals, and executes paper trades automatically.

**To use:**
1. Click **⚡ Auto** in the titlebar
2. Select assets from your watchlist (multi-select supported)
3. Configure risk limits (max trades/day, max capital/trade, min confidence)
4. Set mode to **Paper Trading** (safe for testing)
5. Click **Enable Auto** or **Scan Now**

**Risk limits (configurable):**
- Max 3 trades per day
- Max $500 per trade
- Min 70% AI confidence
- Max 5 open positions
- Market hours only (9:30am–4pm ET)
- No duplicate ticker same day

**Kill Switch** — instantly stops all auto trading from the modal.

When the window is closed with auto mode active, the app continues running in the macOS tray.

---

## Backtesting

Navigate to **Research → Backtest**.

1. Enter a ticker (validated against Finnhub + Yahoo)
2. Select a strategy (SMA, EMA, RSI, MACD, Bollinger)
3. Choose period (6M, 1Y, 2Y)
4. Click **Run Backtest**

Results show:
- Total return vs Buy & Hold (alpha)
- Win rate, Sharpe ratio, max drawdown
- Price chart with buy (green) / sell (red) signals
- Full trade log with P&L per trade
- **Validate** button — Monte Carlo p-value, Bootstrap Sharpe CI, Walk-Forward consistency

---

## User Data

All user data is stored locally on your Mac at `~/.stockforge/`:

```
~/.stockforge/
├── data.json          # Watchlists, portfolio, contracts
├── paper-trading.json # Paper trading account
├── ai-settings.json   # AI provider settings
├── auto-settings.json # Auto mode config + asset settings
├── auto-log.json      # Auto trade history
├── journal.json       # Trade journal entries
├── alerts.json        # Price alerts
└── ratings.json       # AI stock ratings cache
```

No data is sent to any server. Everything stays on your machine.

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Desktop app | Electron 28 |
| Backend | Node.js + Express |
| Frontend | Vanilla JS + HTML + CSS |
| Fonts | Fira Sans + Fira Code (bundled) |
| Backtesting | technicalindicators npm |
| AI (local) | Ollama via HTTP |
| AI (cloud) | OpenAI / Anthropic / OpenCode APIs |
| Market data | Finnhub API + Yahoo Finance |
| SEC data | EDGAR public API |

---

## Supported Platforms

| Platform | Status |
|----------|--------|
| macOS Apple Silicon (M1/M2/M3/M4) | ✅ Supported |
| macOS Intel | Build with `npm run build:dmg -- --x64` |
| Windows | Not tested |
| Linux | Not tested |

---

## License

MIT — see [LICENSE](LICENSE)

---

## Disclaimer

StockForge is for educational and informational purposes only. It is not financial advice. Options trading involves significant risk and is not suitable for all investors. Past performance of AI signals does not guarantee future results. Always do your own research before making any investment decisions.
