const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('stockforge', {
  version: '1.4.2'
});

contextBridge.exposeInMainWorld('electronAPI', {
  notify: (title, body) => ipcRenderer.send('notify', title, body),
  onBackgroundAlert: (callback) => ipcRenderer.on('background-alert', (event, alert) => callback(alert))
});
