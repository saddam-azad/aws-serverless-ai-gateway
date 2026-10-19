/**
 * Browser demo for the AI gateway: sign in with Cognito, open one AppSync Events
 * WebSocket per user, publish to /requests/submit and render the streamed answer.
 *
 * Plain JavaScript, loaded by index.html as a classic script (after config.js,
 * which scripts/setup.ts writes). No build step.
 */
const $ = (id) => document.getElementById(id);
function start(cfg) {
  const tokens = {};
  const sessions = {};
  let active = null;
  const userSelect = $("user");
  for (const u of cfg.users) userSelect.add(new Option(`${u.username} (${u.tenant}/${u.app})`, u.username));
  const b64url = (s) => btoa(s).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
  async function signIn(username) {
    const cached = tokens[username];
    if (cached && cached.exp > Date.now() / 1000 + 60) return cached;
    const res = await fetch(`https://cognito-idp.${cfg.region}.amazonaws.com/`, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-amz-json-1.1",
        "X-Amz-Target": "AWSCognitoIdentityProviderService.InitiateAuth",
      },
      body: JSON.stringify({
        AuthFlow: "USER_PASSWORD_AUTH",
        ClientId: cfg.clientId,
        AuthParameters: { USERNAME: username, PASSWORD: cfg.password },
      }),
    });
    const body = (await res.json());
    if (!res.ok) throw new Error(body.message || "Sign-in failed");
    const idToken = body.AuthenticationResult.IdToken;
    const payload = idToken.split(".")[1].replace(/-/g, "+").replace(/_/g, "/");
    const claims = JSON.parse(atob(payload));
    const token = { idToken, sub: claims.sub, exp: claims.exp };
    tokens[username] = token;
    return token;
  }
  function setStat(id, text, cls = "") {
    const el = $(id);
    el.textContent = text;
    el.className = cls;
  }
  // One long-lived connection per user, subscribed once to /responses/{sub}/*.
  // A fresh subscription per request lost ~14% of events in our load tests.
  function session(username) {
    const existing = sessions[username];
    if (existing) return existing;
    const promise = (async () => {
      const user = await signIn(username);
      const auth = { host: cfg.httpDns, Authorization: user.idToken };
      const sessionId = crypto.randomUUID();
      const socket = new WebSocket(`wss://${cfg.realtimeDns}/event/realtime`, [
        "aws-appsync-event-ws",
        `header-${b64url(JSON.stringify(auth))}`,
      ]);
      await new Promise((resolve, reject) => {
        socket.onopen = () => socket.send(JSON.stringify({ type: "connection_init" }));
        socket.onerror = () => reject(new Error("WebSocket error"));
        socket.onclose = () => {
          delete sessions[username];
        };
        socket.onmessage = (m) => {
          const msg = JSON.parse(m.data);
          if (msg.type === "connection_ack") {
            socket.send(JSON.stringify({
              type: "subscribe",
              id: crypto.randomUUID(),
              channel: `/responses/${user.sub}/${sessionId}/*`,
              authorization: auth,
            }));
          } else if (msg.type === "subscribe_success") {
            resolve();
          } else if (msg.type === "data" && msg.event !== undefined) {
            let ev = JSON.parse(msg.event);
            if (typeof ev === "string") ev = JSON.parse(ev);
            if (active && ev.request_id === active.id) onEvent(ev);
          } else if (/error/.test(msg.type)) {
            reject(new Error(JSON.stringify(msg.errors || msg)));
            if (active) fail(msg.type, JSON.stringify(msg.errors || msg, null, 2));
          }
        };
      });
      return { socket, auth, sessionId };
    })();
    const guarded = promise.catch((e) => {
      delete sessions[username];
      throw e;
    });
    sessions[username] = guarded;
    return guarded;
  }
  function onEvent(ev) {
    const req = active;
    if (!req) return;
    const ms = Math.round(performance.now() - req.sentAt);
    if (ev.type === "token") {
      if (req.done) return;
      if (!req.gotFirst) {
        req.gotFirst = true;
        setStat("sTtft", `${ms} ms`);
        setStat("sStatus", "streaming");
      }
      req.parts[ev.seq] = ev.text; // indexed by seq: AppSync Events does not guarantee order
      $("answer").textContent = req.parts.join("");
    } else if (ev.type === "done") {
      if (req.done) return; // the gateway sends `done` twice; delivery is best effort
      const done = ev;
      req.done = done;
      // `done` carries the full answer, which repairs any chunk that never arrived.
      if (done.text !== undefined) $("answer").textContent = done.text;
      setStat("sStatus", "200 landed", "ok");
      setStat("sLatency", `${ms} ms`);
      setStat("sModel", String(done.model));
      setStat("sTokens", `${done.input_tokens} / ${done.output_tokens}`);
      setStat("sRoute", `${done.cache}${done.fallback ? " / fallback" : ""}`, done.fallback ? "warn" : "");
      $("send").disabled = false;
    } else if (ev.type === "error") {
      fail(`${ev.status}`, ev.message ?? "", ev.status === 429 || ev.status === 403 ? "warn" : "bad");
    }
  }
  function fail(status, message, cls = "bad") {
    setStat("sStatus", status, cls);
    $("answer").textContent = message;
    $("send").disabled = false;
  }
  // Connect as soon as a passenger is chosen, so the first request is not slowed by the handshake.
  const warm = () => {
    session(userSelect.value).catch(() => { });
  };
  userSelect.onchange = warm;
  warm();
  $("send").onclick = async () => {
    const sendButton = $("send");
    sendButton.disabled = true;
    $("answer").textContent = "";
    for (const id of ["sStatus", "sTtft", "sLatency", "sModel", "sTokens", "sRoute"]) setStat(id, "–");
    const id = crypto.randomUUID();
    $("rid").textContent = `request_id: ${id}`;
    setStat("sStatus", "boarding…");
    try {
      const { socket, auth, sessionId } = await session(userSelect.value);
      const payload = {
        request_id: id,
        session_id: sessionId,
        prompt: $("prompt").value,
        max_tokens: Number($("maxTokens").value),
        cacheable: $("cacheable").checked,
      };
      const model = $("model").value;
      if (model) payload.model = model;
      active = { id, parts: [], gotFirst: false, done: null, sentAt: performance.now() };
      socket.send(JSON.stringify({
        type: "publish",
        id: crypto.randomUUID(),
        channel: "/requests/submit",
        events: [JSON.stringify(payload)],
        authorization: auth,
      }));
    } catch (e) {
      fail("error", e.message);
    }
  };
}
const cfg = window.GATEWAY_CONFIG;
if (!cfg) {
  $("answer").textContent = "Missing config.js. Run: node scripts/setup.ts";
  $("send").disabled = true;
} else {
  start(cfg);
}
