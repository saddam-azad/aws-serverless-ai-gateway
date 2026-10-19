// k6 load test for the AI gateway: the full journey, not just Lambda.
//
// Like a real chat app, each iteration is one user session: open a WebSocket to
// AppSync Events, subscribe once to /responses/{sub}/{session_id}/*, then send
// several requests over that connection and time each streamed answer.
//
//   ./run.sh baseline
//
// Scenarios: baseline | load | burst | streaming | quota | policy
// Env: MODEL (default: tenant default, a mock model), LOAD_MAX, BURST_MAX,
//      REQUESTS_PER_SESSION, TIMEOUT_MS
import ws from "k6/ws";
import http from "k6/http";
import encoding from "k6/encoding";
import { check } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";
const cfg = JSON.parse(open("./config.json"));
const SCENARIO = __ENV.SCENARIO || "baseline";
const MODEL = __ENV.MODEL || "";
const LOAD_MAX = Number(__ENV.LOAD_MAX || 500);
const BURST_MAX = Number(__ENV.BURST_MAX || 500);
const TIMEOUT_MS = Number(__ENV.TIMEOUT_MS || 60000);
const ttft = new Trend("gw_ttft", true);
const latency = new Trend("gw_latency", true);
const tokensPerSec = new Trend("gw_tokens_per_sec");
const success = new Rate("gw_success");
const quota429 = new Counter("gw_quota_429");
const denied403 = new Counter("gw_denied_403");
const unexpected = new Counter("gw_unexpected");
const truncated = new Counter("gw_truncated");
const cacheHits = new Counter("gw_cache_hits");
const fallbacks = new Counter("gw_fallbacks");
// AppSync Events is best effort: count what was published vs. what arrived.
const eventsExpected = new Counter("gw_events_expected");
const eventsLost = new Counter("gw_events_lost");
const repaired = new Counter("gw_answers_repaired");
const SCENARIOS = {
  // 10 users x 10 requests.
  baseline: { executor: "per-vu-iterations", vus: 10, iterations: 1, maxDuration: "10m" },
  // 10 -> 50 -> 100 -> LOAD_MAX concurrent users: where does latency start to bend?
  load: {
    executor: "ramping-vus",
    startVUs: 10,
    stages: [
      { duration: "1m", target: 10 },
      { duration: "1m", target: 50 },
      { duration: "1m", target: 100 },
      { duration: "2m", target: LOAD_MAX },
      { duration: "30s", target: 0 },
    ],
    gracefulRampDown: "90s",
  },
  // A sudden spike of users: 10 -> 100 -> BURST_MAX within seconds, then back down.
  burst: {
    executor: "ramping-vus",
    startVUs: 10,
    stages: [
      { duration: "30s", target: 10 },
      { duration: "2s", target: 100 },
      { duration: "30s", target: 100 },
      { duration: "2s", target: BURST_MAX },
      { duration: "30s", target: BURST_MAX },
      { duration: "2s", target: 10 },
      { duration: "60s", target: 10 },
    ],
    gracefulRampDown: "90s",
  },
  // Long answers under load: dropped connections, truncated answers?
  streaming: { executor: "constant-vus", vus: Number(__ENV.VUS || 50), duration: "3m" },
  // One small tenant pushed past its hourly token limit: clean 429s, every single time.
  quota: { executor: "constant-vus", vus: Number(__ENV.VUS || 20), duration: "1m" },
  // The knowledge bot asks for a model nobody approved: 403.
  policy: { executor: "per-vu-iterations", vus: 1, iterations: 1 },
};
const SESSION_REQUESTS = {
  baseline: 10,
  load: 5,
  burst: 3,
  streaming: 2,
  quota: 5,
  policy: 5,
};
const REQUESTS_PER_SESSION = Number(__ENV.REQUESTS_PER_SESSION || SESSION_REQUESTS[SCENARIO] || 5);
const THRESHOLDS = {
  quota: { gw_unexpected: ["count==0"], gw_quota_429: ["count>0"] },
  policy: { gw_unexpected: ["count==0"], gw_denied_403: ["count==5"] },
  default: { gw_unexpected: ["count==0"], gw_truncated: ["count==0"], gw_success: ["rate>0.99"] },
};
export const options = {
  scenarios: { [SCENARIO]: SCENARIOS[SCENARIO] },
  thresholds: THRESHOLDS[SCENARIO] || THRESHOLDS.default,
  summaryTrendStats: ["avg", "min", "med", "p(90)", "p(95)", "p(99)", "max"],
};
const PERSONAL_PROMPTS = [
  "Summarize this customer's last five tickets and suggest a next step.",
  "Draft a reply to a customer whose order arrived damaged.",
  "Explain what Kubernetes is to a new support engineer.",
  "Summarize the attached incident timeline in three bullet points.",
  "Suggest a polite follow-up for a customer who has not replied in a week.",
];
const FAQ_PROMPT = "What's your refund policy?";
const pick = (arr) => arr[Math.floor(Math.random() * arr.length)];
export function setup() {
  const users = {};
  for (const u of cfg.users) {
    const res = http.post(`https://cognito-idp.${cfg.region}.amazonaws.com/`, JSON.stringify({
      AuthFlow: "USER_PASSWORD_AUTH",
      ClientId: cfg.clientId,
      AuthParameters: { USERNAME: u.username, PASSWORD: cfg.password },
    }), {
      headers: {
        "Content-Type": "application/x-amz-json-1.1",
        "X-Amz-Target": "AWSCognitoIdentityProviderService.InitiateAuth",
      },
    });
    if (res.status !== 200) throw new Error(`Sign-in failed for ${u.username}: ${res.status} ${res.body}`);
    const idToken = res.json("AuthenticationResult.IdToken");
    const claims = JSON.parse(encoding.b64decode(idToken.split(".")[1], "rawurl", "s"));
    users[u.username] = { ...u, idToken, sub: claims.sub };
  }
  return users;
}
function pickUser(users) {
  switch (SCENARIO) {
    case "quota":
      return users["fabrikam-docs"];
    case "policy":
      return users["contoso-knowledge"];
    case "streaming":
      return users["contoso-support"];
    default:
      // A real model run uses the support assistant only; the knowledge bot is not allowed model-a.
      return MODEL
        ? users["contoso-support"]
        : pick([users["contoso-support"], users["contoso-devassist"], users["contoso-knowledge"]]);
  }
}
function nextRequest() {
  const prompt = pick(PERSONAL_PROMPTS);
  const model = MODEL || undefined;
  switch (SCENARIO) {
    case "quota":
      return { payload: { prompt, max_tokens: 300, model }, expected: ["done", 429] };
    case "policy":
      return { payload: { prompt, max_tokens: 50, model: "model-a" }, expected: [403] };
    case "streaming":
      return { payload: { prompt, max_tokens: 1500, model }, expected: ["done"] };
    default: {
      const faq = Math.random() < 0.3;
      return {
        payload: { prompt: faq ? FAQ_PROMPT : prompt, cacheable: faq, max_tokens: 300, model },
        expected: ["done"],
      };
    }
  }
}
function uuid() {
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    return (c === "x" ? r : (r & 0x3) | 0x8).toString(16);
  });
}
function record(user, req) {
  const tags = { first: String(req.index === 0) };
  success.add(req.outcome === "done", tags);
  if (req.outcome === 429) quota429.add(1);
  if (req.outcome === 403) denied403.add(1);
  if (!req.expected.includes(req.outcome)) {
    unexpected.add(1, { outcome: String(req.outcome), ...tags });
    console.warn(`UNEXPECTED ${req.outcome} request=${req.id} user=${user.username} n=${req.index + 1} ` +
      `cacheable=${!!req.payload.cacheable} accepted=${req.accepted} chunks=${req.chunks} ${req.message || ""}`);
  }
  if (req.outcome === "done") {
    const d = req.done;
    if (!d) return;
    const ms = req.latencyMs ?? 0;
    if (req.ttftMs !== undefined) ttft.add(req.ttftMs, tags);
    latency.add(ms, tags);
    if (d.cache === "hit") cacheHits.add(1);
    if (d.fallback) fallbacks.add(1);
    if (d.output_tokens && req.ttftMs !== undefined && ms > req.ttftMs) {
      tokensPerSec.add(d.output_tokens / ((ms - req.ttftMs) / 1000));
    }
  }
  check(req, { "got an expected outcome": (x) => x.expected.includes(x.outcome) });
}
// Runs at the end of the session, so events that arrived late count as delivered, not lost.
function recordDelivery(req) {
  const d = req.done;
  if (req.outcome !== "done" || !d) return;
  const tags = { first: String(req.index === 0) };
  eventsExpected.add(d.chunks + 2, tags); // chunks + two copies of done
  const lost = d.chunks - req.chunks + (2 - req.doneCopies);
  if (lost > 0) eventsLost.add(lost, tags);
  if (req.chunks < d.chunks && d.text !== undefined) repaired.add(1, tags);
  // Truncated means the user still ends up with an incomplete answer.
  const finalChars = d.text !== undefined ? d.text.length : req.chars;
  if (finalChars !== d.chars) {
    truncated.add(1, tags);
    console.warn(`TRUNCATED request=${req.id} final ${finalChars}/${d.chars} chars`);
  }
}
// One user session: one connection, one wildcard subscription, many requests.
function runSession(user, count) {
  const auth = { host: cfg.httpDns, Authorization: user.idToken };
  const headerProtocol = "header-" + encoding.b64encode(JSON.stringify(auth), "rawurl");
  const requests = {};
  const sessionId = uuid();
  let current = null;
  let sent = 0;
  const res = ws.connect(`wss://${cfg.realtimeDns}/event/realtime`, { headers: { "Sec-WebSocket-Protocol": `aws-appsync-event-ws, ${headerProtocol}` } }, (socket) => {
    const finish = (req) => {
      if (req.finished) return;
      req.finished = true;
      record(user, req);
      if (sent < count) send();
      else socket.setTimeout(() => socket.close(), 1500); // let late events arrive for loss accounting
    };
    const send = () => {
      const { payload, expected } = nextRequest();
      const req = {
        id: uuid(),
        publishId: uuid(),
        index: sent,
        payload,
        expected,
        accepted: "no reply",
        outcome: "timeout",
        chunks: 0,
        chars: 0,
        doneCopies: 0,
        seen: {},
        sentAt: Date.now(),
        finished: false,
      };
      requests[req.id] = req;
      current = req;
      sent += 1;
      socket.send(JSON.stringify({
        type: "publish",
        id: req.publishId,
        channel: "/requests/submit",
        events: [JSON.stringify({ request_id: req.id, session_id: sessionId, ...payload })],
        authorization: auth,
      }));
      socket.setTimeout(() => finish(req), TIMEOUT_MS);
    };
    socket.on("open", () => socket.send(JSON.stringify({ type: "connection_init" })));
    socket.on("message", (raw) => {
      const msg = JSON.parse(raw);
      if (msg.type === "connection_ack") {
        socket.send(JSON.stringify({
          type: "subscribe",
          id: uuid(),
          channel: `/responses/${user.sub}/${sessionId}/*`,
          authorization: auth,
        }));
      } else if (msg.type === "subscribe_success") {
        send();
      } else if (msg.type === "publish_success") {
        const cur = current;
        // Did AppSync accept the request? Tells "never reached Lambda" apart from "answer lost".
        if (cur && msg.id === cur.publishId) {
          cur.accepted = (msg.failed || []).length ? `failed ${JSON.stringify(msg.failed)}` : "yes";
        }
      } else if (msg.type === "data" && msg.event !== undefined) {
        let ev = JSON.parse(msg.event);
        if (typeof ev === "string") ev = JSON.parse(ev);
        const req = requests[ev.request_id];
        if (!req) return;
        const now = Date.now();
        if (ev.type === "token" && !req.seen[ev.seq]) {
          req.seen[ev.seq] = true;
          if (req.ttftMs === undefined) req.ttftMs = now - req.sentAt;
          req.chunks += 1;
          req.chars += ev.text.length;
        } else if (ev.type === "done") {
          req.doneCopies += 1;
          if (!req.done) {
            // `done` carries the full answer, so the request is complete now.
            Object.assign(req, { outcome: "done", latencyMs: now - req.sentAt, done: ev });
            finish(req);
          }
        } else if (ev.type === "error" && !req.finished) {
          Object.assign(req, { outcome: ev.status, message: ev.message });
          finish(req);
        }
      } else if (["subscribe_error", "publish_error", "error", "connection_error"].includes(msg.type ?? "")) {
        const cur = current;
        if (cur && !cur.finished) {
          Object.assign(cur, { outcome: "ws_error", message: JSON.stringify(msg.errors ?? msg) });
          finish(cur);
        } else {
          console.warn(`session error: ${JSON.stringify(msg)}`);
        }
      }
    });
  });
  if (!res || res.status !== 101) console.warn(`connect failed: ${res && res.status}`);
  // Anything still open when the socket closed never completed.
  for (const req of Object.values(requests)) {
    if (!req.finished) {
      req.finished = true;
      record(user, req);
    }
    recordDelivery(req);
  }
}
export default function (users) {
  runSession(pickUser(users), REQUESTS_PER_SESSION);
}
