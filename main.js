const { app, BrowserWindow, Menu, shell, dialog, Notification, ipcMain, Tray, nativeImage } = require('electron');
const path = require('path');
const { fork } = require('child_process');
const http = require('http');
const fs = require('fs');
const os = require('os');

const PORT = 3478;
let mainWindow = null;
let serverProcess = null;
let tray = null;
let backgroundScanInterval = null;
const SCAN_INTERVAL_MARKET_MS = 2 * 60 * 1000;  // 2 min during market hours
const SCAN_INTERVAL_OFF_MS    = 10 * 60 * 1000; // 10 min outside market hours

function isMarketHours() {
  const now = new Date();
  const et = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const day = et.getDay(); // 0=Sun, 6=Sat
  if (day === 0 || day === 6) return false;
  const h = et.getHours();
  const m = et.getMinutes();
  const mins = h * 60 + m;
  return mins >= 390 && mins <= 960; // 6:30am–4:00pm ET (includes pre-market)
}
const DATA_FILE = path.join(os.homedir(), '.stockforge', 'data.json');

function loadDataForScan() {
  try { return JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); } catch { return null; }
}

function fireNotification(title, body) {
  if (!Notification.isSupported()) return;
  const n = new Notification({ title, body, silent: false });
  n.on('click', () => {
    if (mainWindow) { mainWindow.show(); mainWindow.focus(); }
    else createWindow();
  });
  n.show();
}

async function runBackgroundScan() {
  try {
    const data = loadDataForScan();
    const contracts = data?.daytrading?.contracts || [];
    const portfolio = data?.longterm?.portfolio || [];

    const res = await new Promise((resolve, reject) => {
      const body = JSON.stringify({ contracts, portfolio, threshold: 10 });
      const req = http.request({
        hostname: 'localhost', port: PORT,
        path: '/api/background/scan', method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) }
      }, res => {
        let out = '';
        res.on('data', d => out += d);
        res.on('end', () => { try { resolve(JSON.parse(out)); } catch { resolve(null); } });
      });
      req.on('error', reject);
      req.write(body);
      req.end();
    });

    if (!res?.alerts?.length) return;

    for (const alert of res.alerts) {
      if (alert.severity === 'critical' || alert.severity === 'warning' || alert.type === 'market_mover') {
        fireNotification(alert.title, alert.body);
      }
      // Forward to renderer if window is open
      if (mainWindow && !mainWindow.isDestroyed()) {
        mainWindow.webContents.send('background-alert', alert);
      }
    }
  } catch (e) {
    console.error('[background scan]', e.message);
  }
}

function startBackgroundScan() {
  if (backgroundScanInterval) return;
  // First scan after 30s (let server warm up)
  setTimeout(runBackgroundScan, 30000);
  // Dynamic interval — check every 30s which interval to use
  backgroundScanInterval = setInterval(() => {
    const interval = isMarketHours() ? SCAN_INTERVAL_MARKET_MS : SCAN_INTERVAL_OFF_MS;
    const now = Date.now();
    if (!startBackgroundScan._lastRun || (now - startBackgroundScan._lastRun) >= interval) {
      startBackgroundScan._lastRun = now;
      runBackgroundScan();
    }
  }, 30000); // check every 30s, run based on interval
  console.log('[background] Scan started — 2min market hours, 10min off-hours');
}

function createTray() {
  try {
    // Use a simple template image (works without custom icon)
    const icon = nativeImage.createFromPath(path.join(__dirname, 'assets', 'icon.icns'));
    const resized = icon.isEmpty() ? nativeImage.createEmpty() : icon.resize({ width: 16, height: 16 });
    tray = new Tray(resized);
    tray.setToolTip('StockForge Mentor — monitoring markets');
    tray.setContextMenu(Menu.buildFromTemplate([
      { label: 'Open StockForge', click: () => { if (mainWindow) { mainWindow.show(); mainWindow.focus(); } else createWindow(); } },
      { label: 'Scan Now', click: () => runBackgroundScan() },
      { type: 'separator' },
      { label: 'Quit', click: () => app.quit() }
    ]));
    tray.on('click', () => { if (mainWindow) { mainWindow.show(); mainWindow.focus(); } else createWindow(); });
  } catch (e) {
    console.error('[tray]', e.message);
  }
}

function killPort(port) {
  // Synchronously kill anything on this port before we start
  try {
    const { execSync } = require('child_process');
    const pids = execSync(`lsof -ti tcp:${port} 2>/dev/null || true`, { encoding: 'utf8' }).trim();
    if (pids) {
      pids.split('\n').filter(Boolean).forEach(pid => {
        try { process.kill(parseInt(pid), 'SIGKILL'); } catch {}
      });
    }
  } catch {}
}

function killServerProcess() {
  if (!serverProcess) return;
  try { serverProcess.kill('SIGKILL'); } catch {}
  serverProcess = null;
}

function startServer() {
  return new Promise((resolve, reject) => {
    // Kill anything already on the port BEFORE forking
    killPort(PORT);

    const serverPath = path.join(__dirname, 'server.js');
    let serverError = '';
    let serverReady = false;
    let resolved = false;

    const done = (err) => {
      if (resolved) return;
      resolved = true;
      if (err) reject(err); else resolve();
    };

    serverProcess = fork(serverPath, [], {
      env: { ...process.env, PORT: String(PORT) },
      silent: true
    });

    serverProcess.stdout.on('data', (data) => {
      const msg = data.toString();
      console.log('[server]', msg.trim());
      if (msg.includes('running on port') && !serverReady) {
        serverReady = true;
        setTimeout(() => done(null), 300);
      }
    });

    serverProcess.stderr.on('data', (data) => {
      const msg = data.toString().trim();
      serverError += msg + '\n';
      console.error('[server error]', msg);
    });

    serverProcess.on('error', (e) => {
      done(new Error(`Failed to start server process: ${e.message}`));
    });

    serverProcess.on('exit', (code, signal) => {
      if (!serverReady) {
        const errDetail = serverError.slice(0, 400) || `exit code ${code}`;
        done(new Error(`Server stopped unexpectedly.\n${errDetail}`));
      }
    });

    // Ping-based fallback — start after 1s, try for 20s
    const startTime = Date.now();
    const pingCheck = () => {
      if (resolved) return;
      if (Date.now() - startTime > 20000) {
        done(new Error(`Server took too long to start.\n${serverError.slice(0, 400) || 'No error details.'}`));
        return;
      }
      http.get(`http://localhost:${PORT}/api/ping`, (res) => {
        if (res.statusCode === 200 && !serverReady) {
          serverReady = true;
          done(null);
        } else if (!resolved) {
          setTimeout(pingCheck, 500);
        }
      }).on('error', () => {
        if (!resolved) setTimeout(pingCheck, 500);
      });
    };
    setTimeout(pingCheck, 1000);
  });
}

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1400,
    height: 900,
    minWidth: 1000,
    minHeight: 700,
    titleBarStyle: 'hiddenInset',
    trafficLightPosition: { x: 14, y: 16 },
    backgroundColor: '#0f1117',
    icon: path.join(__dirname, 'assets', 'icon.icns'),
    resizable: true,
    movable: true,
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
      preload: path.join(__dirname, 'preload.js')
    },
    show: false
  });

  mainWindow.loadURL(`http://localhost:${PORT}`);

  mainWindow.once('ready-to-show', () => {
    mainWindow.show();
    mainWindow.focus();
  });

  mainWindow.webContents.setWindowOpenHandler(({ url }) => {
    shell.openExternal(url);
    return { action: 'deny' };
  });

  // IPC: send macOS notification from renderer
  ipcMain.on('notify', (event, title, body) => {
    if (Notification.isSupported()) {
      new Notification({ title, body, silent: false }).show();
    }
  });

  mainWindow.on('closed', () => {
    mainWindow = null;
  });

  buildMenu();
}

function buildMenu() {
  const template = [
    {
      label: 'StockForge',
      submenu: [
        { label: 'About StockForge', role: 'about' },
        { type: 'separator' },
        { label: 'Hide', accelerator: 'Cmd+H', role: 'hide' },
        { type: 'separator' },
        { label: 'Quit', accelerator: 'Cmd+Q', role: 'quit' }
      ]
    },
    {
      label: 'Edit',
      submenu: [
        { role: 'undo' }, { role: 'redo' }, { type: 'separator' },
        { role: 'cut' }, { role: 'copy' }, { role: 'paste' }, { role: 'selectAll' }
      ]
    },
    {
      label: 'View',
      submenu: [
        { label: 'Reload', accelerator: 'Cmd+R', click: () => mainWindow?.reload() },
        { type: 'separator' },
        { role: 'togglefullscreen' },
        { label: 'Developer Tools', accelerator: 'Cmd+Alt+I', click: () => mainWindow?.webContents.toggleDevTools() }
      ]
    }
  ];
  Menu.setApplicationMenu(Menu.buildFromTemplate(template));
}

app.whenReady().then(async () => {
  try {
    await startServer();
    createWindow();
    createTray();
    startBackgroundScan();
  } catch (err) {
    killServerProcess();
    dialog.showErrorBox('StockForge — Startup Error', err.message);
    app.quit();
  }
});

function cleanupAndQuit() {
  if (backgroundScanInterval) { clearInterval(backgroundScanInterval); backgroundScanInterval = null; }
  killServerProcess();
  // Also kill the port directly as a safety net
  killPort(PORT);
}

// Always quit completely when window is closed
app.on('window-all-closed', () => {
  cleanupAndQuit();
  app.quit();
});

app.on('activate', () => { if (mainWindow === null) createWindow(); });

app.on('before-quit', () => {
  app.isQuitting = true;
  cleanupAndQuit();
});
