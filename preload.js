const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('stockforge', {
  version: '1.5.1'
});

contextBridge.exposeInMainWorld('electronAPI', {
  notify: (title, body) => ipcRenderer.send('notify', title, body),
  onBackgroundAlert: (callback) => ipcRenderer.on('background-alert', (event, alert) => callback(alert))
});
