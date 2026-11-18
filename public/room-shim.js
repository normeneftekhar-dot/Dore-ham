/* Browser adapter for Kasi Injast: Vercel WebSocket Function + Redis-backed rooms. */
(function () {
  "use strict";
  if (window.claude && window.claude.use) return;

  var socket = null;
  var connected = false;
  var peerId = null;
  var retry = 700;
  var rooms = Object.create(null);
  var reconnectTimer = null;
  var closedByUser = false;

  function status(value, message) {
    window.dispatchEvent(new CustomEvent("room-connection", {
      detail: { connected: value, message: message || "" }
    }));
  }

  function send(packet) {
    if (!socket || !connected || socket.readyState !== WebSocket.OPEN) return false;
    try { socket.send(JSON.stringify(packet)); return true; }
    catch (_) { return false; }
  }

  function scheduleReconnect() {
    if (closedByUser || reconnectTimer) return;
    reconnectTimer = setTimeout(function () {
      reconnectTimer = null;
      connect();
    }, retry);
    retry = Math.min(retry * 1.8, 12000);
  }

  function connect() {
    if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) return;
    if (!/^https?:$/.test(location.protocol)) {
      status(false, "برای اتصال، سایت باید از طریق HTTP یا HTTPS اجرا شود.");
      return;
    }
    closedByUser = false;
    var scheme = location.protocol === "https:" ? "wss://" : "ws://";
    try { socket = new WebSocket(scheme + location.host + "/api/ws"); }
    catch (_) { status(false, "ساخت اتصال ممکن نشد؛ دوباره تلاش می‌کنیم."); scheduleReconnect(); return; }

    socket.onopen = function () {
      connected = true;
      retry = 700;
      status(true, "متصل به اتاق");
      Object.keys(rooms).forEach(function (key) {
        send({ t: "join", room: key });
      });
    };

    socket.onmessage = function (event) {
      var m;
      try { m = JSON.parse(event.data); } catch (_) { return; }
      if (m.t === "hello") {
        peerId = m.id;
        Object.keys(rooms).forEach(function (key) { rooms[key]._resend(); });
        return;
      }
      var target = m.room && rooms[m.room];
      if (m.t === "error") {
        if (target) target._error(m.code || "server_error");
        if (m.room === "lobby") {
          var message = m.code === "room_full" ? "اتاق عمومی پر است." :
            m.code === "redis_not_configured" ? "سرویس اتاق هنوز پیکربندی نشده است." :
            "ارتباط با اتاق مشترک مشکل دارد؛ در حال تلاش دوباره…";
          status(false, message);
        }
        return;
      }
      if (!target) return;
      if (m.t === "peers") { target._others(m.list); if (m.room === "lobby") status(true, "متصل به اتاق"); }
      else if (m.t === "msg") target._msg(m);
    };

    socket.onerror = function () {
      status(false, "خطا در ارتباط؛ تلاش برای اتصال دوباره…");
      try { socket.close(); } catch (_) {}
    };

    socket.onclose = function () {
      connected = false;
      peerId = null;
      socket = null;
      Object.keys(rooms).forEach(function (key) { rooms[key]._others([]); });
      status(false, "ارتباط قطع شد؛ تلاش برای اتصال دوباره…");
      scheduleReconnect();
    };
  }

  function sizeOf(value) {
    try { return new Blob([JSON.stringify(value === undefined ? null : value)]).size; }
    catch (_) { return Infinity; }
  }

  function makeRoom(key) {
    if (rooms[key]) return rooms[key];
    var mine = {};
    var others = [];
    var handlers = Object.create(null);
    var subscribers = [];
    var errorHandlers = [];
    var cache = Object.create(null);
    var previous = Object.create(null);
    var snapshot = Object.freeze([]);
    var changeTimer = null;
    var presenceTimer = null;
    var pendingPatch = null;

    function build() {
      var list = (peerId ? [{ peer: peerId, presence: mine, self: true }] : [])
        .concat(others.filter(function (p) { return p.peer !== peerId; }));
      var next = Object.create(null), out = [];
      list.forEach(function (p) {
        var raw;
        try { raw = JSON.stringify(p.presence || {}); } catch (_) { raw = "{}"; }
        var c = cache[p.peer];
        if (!c || c.raw !== raw || c.self !== !!p.self) {
          c = cache[p.peer] = { raw: raw, self: !!p.self, value: Object.freeze({
            peer: p.peer, by: null, isMe: !!p.self, sameTab: !!p.self,
            kind: "viewer", guest: false,
            presence: Object.freeze(JSON.parse(raw)), updatedAt: Date.now()
          }) };
        }
        next[p.peer] = c.value;
        out.push(c.value);
      });
      Object.keys(cache).forEach(function (id) { if (!next[id]) delete cache[id]; });
      var joined = [], left = [], updated = [];
      out.forEach(function (p) {
        if (!previous[p.peer]) joined.push(p);
        else if (previous[p.peer] !== p) updated.push(p);
      });
      Object.keys(previous).forEach(function (id) { if (!next[id]) left.push(previous[id]); });
      previous = next;
      snapshot = Object.freeze(out);
      return { peers: snapshot, joined: joined, left: left, updated: updated };
    }

    function changed() {
      if (changeTimer) return;
      changeTimer = setTimeout(function () {
        changeTimer = null;
        var delta = build();
        if (!delta.joined.length && !delta.left.length && !delta.updated.length) return;
        subscribers.slice().forEach(function (fn) {
          try { fn(delta); } catch (e) { console.error(e); }
        });
      }, 25);
    }

    var api = {
      emit: function (topic, data) {
        if (typeof topic !== "string" || !/^[a-z][a-z0-9_.-]{0,47}$/.test(topic) || sizeOf(data) > 4096)
          return Promise.reject({ code: "invalid_argument" });
        if (!send({ t: "emit", room: key, topic: topic, data: data }))
          return Promise.reject({ code: "not_connected" });
        return Promise.resolve();
      },
      on: function (topic, fn) {
        if (typeof fn !== "function") throw new TypeError("handler");
        (handlers[topic] = handlers[topic] || []).push(fn);
        return function () {
          var list = handlers[topic] || [], i = list.indexOf(fn);
          if (i >= 0) list.splice(i, 1);
        };
      },
      presence: function (patch) {
        if (!patch || typeof patch !== "object" || Array.isArray(patch))
          return Promise.reject({ code: "invalid_argument" });
        var next = Object.assign({}, mine);
        Object.keys(patch).forEach(function (key) {
          if (patch[key] === null) delete next[key];
          else next[key] = patch[key];
        });
        if (sizeOf(next) > 4096) return Promise.reject({ code: "invalid_argument" });
        mine = next;
        pendingPatch = Object.assign(pendingPatch || {}, patch);
        changed();
        if (!presenceTimer) presenceTimer = setTimeout(function () {
          presenceTimer = null;
          var delta = pendingPatch;
          pendingPatch = null;
          if (delta) send({ t: "pres", room: key, patch: delta });
        }, 55);
        return Promise.resolve();
      },
      peers: function () { return snapshot; },
      onPeers: function (fn, onError) {
        if (typeof fn !== "function") throw new TypeError("handler");
        subscribers.push(fn);
        if (typeof onError === "function") errorHandlers.push(onError);
        Promise.resolve().then(function () {
          if (subscribers.indexOf(fn) < 0) return;
          var delta = build();
          try { fn({ peers: snapshot, joined: snapshot, left: [], updated: [] }); }
          catch (e) { console.error(e); }
        });
        return function () {
          var i = subscribers.indexOf(fn);
          if (i >= 0) subscribers.splice(i, 1);
          if (onError) {
            i = errorHandlers.indexOf(onError);
            if (i >= 0) errorHandlers.splice(i, 1);
          }
        };
      },
      connected: function () { return connected; },
      leave: function () {
        send({ t: "leave", room: key });
        delete rooms[key];
        subscribers = [];
        handlers = Object.create(null);
        errorHandlers = [];
        if (presenceTimer) clearTimeout(presenceTimer);
        if (changeTimer) clearTimeout(changeTimer);
        return Promise.resolve();
      },
      _others: function (list) { others = Array.isArray(list) ? list : []; changed(); },
      _resend: function () {
        if (send({ t: "join", room: key }) && Object.keys(mine).length)
          send({ t: "pres", room: key, patch: mine });
      },
      _error: function (code) {
        errorHandlers.slice().forEach(function (fn) {
          try { fn({ code: code }); } catch (_) {}
        });
      },
      _msg: function (m) {
        (handlers[m.topic] || []).slice().forEach(function (fn) {
          try {
            fn({ topic: m.topic, data: m.data, peer: m.peer,
              by: null, isMe: m.peer === peerId, sameTab: m.peer === peerId,
              kind: "viewer", guest: false });
          } catch (e) { console.error(e); }
        });
      }
    };

    rooms[key] = api;
    send({ t: "join", room: key });
    return api;
  }

  window.addEventListener("pagehide", function () {
    closedByUser = true;
    if (reconnectTimer) clearTimeout(reconnectTimer);
    if (socket) { try { socket.close(); } catch (_) {} }
  });

  window.claude = {
    use: async function (name) {
      if (name !== "room") return null;
      if (!socket) connect();
      var lobby = makeRoom("lobby");
      lobby.join = async function (code) {
        code = String(code || "");
        if (!/^[a-z0-9]{20}$/.test(code)) throw new Error("bad room");
        return makeRoom("p:" + code);
      };
      return lobby;
    }
  };
})();
