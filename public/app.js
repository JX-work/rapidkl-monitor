// Shared client logic: clock, WS connection, notifications, modal.
// Each page provides its own render() function.

window.RapidKL = (function() {
  // ── clock ────────────────────────────────────────────────────────────
  const clockEl = document.getElementById('clock');
  function tickClock() {
    if (!clockEl) return;
    clockEl.textContent = new Intl.DateTimeFormat('en-GB', {
      hour:'2-digit', minute:'2-digit', second:'2-digit',
      hour12: false, timeZone:'Asia/Kuala_Lumpur',
    }).format(new Date());
  }
  tickClock(); setInterval(tickClock, 1000);

  // ── notifications + sound ────────────────────────────────────────────
  if ('Notification' in window && Notification.permission === 'default') {
    setTimeout(() => Notification.requestPermission(), 2000);
  }
  let audioCtx = null;
  function beep(freq=880, ms=200) {
    try {
      audioCtx = audioCtx || new (window.AudioContext || window.webkitAudioContext)();
      const o = audioCtx.createOscillator(), g = audioCtx.createGain();
      o.frequency.value = freq; o.type = 'sine'; g.gain.value = 0.0001;
      o.connect(g); g.connect(audioCtx.destination); o.start();
      g.gain.exponentialRampToValueAtTime(0.15, audioCtx.currentTime + 0.02);
      g.gain.exponentialRampToValueAtTime(0.0001, audioCtx.currentTime + ms/1000);
      o.stop(audioCtx.currentTime + ms/1000);
    } catch {}
  }
  document.addEventListener('click',
    () => { if (audioCtx && audioCtx.state === 'suspended') audioCtx.resume(); },
    { once: true });

  function notify(title, body, level, link) {
    if ('Notification' in window && Notification.permission === 'granted') {
      const n = new Notification(title, { body, tag: title });
      if (link) n.onclick = () => window.open(link, '_blank');
    }
    if (level === 'error') { beep(440,180); setTimeout(()=>beep(330,220), 220); }
    else if (level === 'warn') { beep(660,150); }
  }

  function showModal(title, body, link) {
    const m = document.getElementById('modal');
    if (!m) { notify(title, body, 'error', link); return; }
    document.getElementById('modal-title').textContent = title;
    document.getElementById('modal-body').textContent = body;
    const a = document.getElementById('modal-link');
    if (link) { a.href = link; a.style.display = 'inline-block'; a.textContent = 'Read full announcement →'; }
    else { a.style.display = 'none'; }
    m.classList.add('on');
  }
  window.dismissModal = () => document.getElementById('modal')?.classList.remove('on');

  // ── ws connection ────────────────────────────────────────────────────
  let ws, retry = 1000;
  const handlers = { snapshot: [], 'new-announcements': [] };
  function on(type, fn) { (handlers[type] || (handlers[type] = [])).push(fn); }
  function connect() {
    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    ws = new WebSocket(`${proto}://${location.host}/ws`);
    ws.onopen    = () => retry = 1000;
    ws.onmessage = ev => {
      const m = JSON.parse(ev.data);
      (handlers[m.type] || []).forEach(fn => fn(m.payload));
    };
    ws.onclose = () => {
      document.getElementById('pulse')?.classList.add('off');
      setTimeout(connect, retry); retry = Math.min(retry*1.5, 15000);
    };
    ws.onerror = () => ws.close();
  }
  connect();

  // ── handle alert popups for new high-priority announcements ──────────
  on('new-announcements', items => {
    for (const it of items) {
      const level = it.tone === 'error' ? 'error' : 'warn';
      notify(it.title, it.categoryLabel || 'Announcement', level, it.link);
      if (it.tone === 'error') showModal(it.title, it.categoryLabel || 'Service Disruption', it.link);
    }
  });

  // ── utils exposed to pages ───────────────────────────────────────────
  return {
    on,
    notify, beep,
    fmtAgo(ts) {
      if (!ts) return '—';
      const s = Math.floor((Date.now() - ts)/1000);
      if (s < 60)   return s + 's ago';
      if (s < 3600) return Math.floor(s/60) + 'm ago';
      if (s < 86400) return Math.floor(s/3600) + 'h ago';
      return Math.floor(s/86400) + 'd ago';
    },
    fmtDate(ts) {
      if (!ts) return '—';
      return new Intl.DateTimeFormat('en-GB', {
        day:'2-digit', month:'short', year:'numeric',
        hour:'2-digit', minute:'2-digit', hour12: false,
        timeZone:'Asia/Kuala_Lumpur',
      }).format(new Date(ts));
    },
  };
})();
