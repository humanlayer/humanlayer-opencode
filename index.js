// @bun
// apps/riptide-opencode-plugin/src/index.ts
import { hostname } from "os";

// packages/session-sdk-base/src/channels.ts
var ALL_CHANNELS = ["prod", "beta", "dev", "local"];
function isChannel(value) {
  return value !== undefined && ALL_CHANNELS.includes(value);
}
var CHANNEL_DEFAULTS = {
  prod: {
    api: "https://riptide-api.humanlayer.com",
    sync: "https://sync.humanlayer.com",
    app: "https://app.humanlayer.com",
    clientId: "client_01KGBPX6V78MDGNF006SE6NYTG"
  },
  beta: {
    api: "https://riptide-api.codelayer.cloud",
    sync: "https://sync.codelayer.cloud",
    app: "https://app.codelayer.cloud",
    clientId: "client_01K84ASX66BNXTMD842AHWBKNG"
  },
  dev: {
    api: "https://riptide-api.dev.codelayer.gg",
    sync: "https://sync.dev.codelayer.gg",
    app: "https://app.dev.codelayer.gg",
    clientId: "client_01K84ASWYHMC34NMFHN6MBXP8N"
  },
  local: {
    api: "http://localhost:8700",
    sync: "http://localhost:8888",
    app: "http://localhost:3000",
    clientId: "client_01K84ASWYHMC34NMFHN6MBXP8N"
  }
};
var DEFAULT_WORKOS_URL = "https://api.workos.com";
function getChannelConfig(channel, env = process.env) {
  const base = CHANNEL_DEFAULTS[channel];
  return {
    channel,
    api: env.HUMANLAYER_API_URL || base.api,
    sync: env.HUMANLAYER_SYNC_URL || base.sync,
    app: env.HUMANLAYER_APP_URL || base.app,
    workos: env.HUMANLAYER_WORKOS_URL || DEFAULT_WORKOS_URL,
    clientId: base.clientId
  };
}
// packages/session-sdk-base/src/files.ts
import { randomUUID } from "crypto";
import { appendFile, mkdir, open, readFile, rename, stat, unlink, writeFile } from "fs/promises";
import { dirname } from "path";
function errorMessage(err) {
  return err instanceof Error ? err.message : String(err);
}
function sleepUnref(ms, signal) {
  return new Promise((resolve) => {
    const done = () => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", done);
      resolve();
    };
    const timer = setTimeout(done, ms);
    timer.unref();
    if (signal?.aborted)
      done();
    else
      signal?.addEventListener("abort", done, { once: true });
  });
}
async function readJsonFile(path) {
  try {
    return JSON.parse(await readFile(path, "utf8"));
  } catch {
    return null;
  }
}
async function writeJsonFileAtomic(path, data) {
  await mkdir(dirname(path), { recursive: true, mode: 448 });
  const tmp = `${path}.${process.pid}.${randomUUID()}.tmp`;
  await writeFile(tmp, JSON.stringify(data, null, 2), { mode: 384 });
  await rename(tmp, path);
}
var LOCK_STALE_MS = 30000;
var LOCK_TIMEOUT_MS = 15000;
var LOCK_POLL_MS = 100;
async function withFileLock(lockPath, fn) {
  await mkdir(dirname(lockPath), { recursive: true, mode: 448 });
  const start = Date.now();
  for (;; ) {
    try {
      const handle = await open(lockPath, "wx", 384);
      await handle.close();
      break;
    } catch (err) {
      if (err.code !== "EEXIST")
        throw err;
      const info = await stat(lockPath).catch(() => null);
      const age = Date.now() - (info?.mtimeMs ?? Date.now());
      if (age > LOCK_STALE_MS)
        await unlink(lockPath).catch(() => {});
      else if (Date.now() - start > LOCK_TIMEOUT_MS)
        throw new Error(`HumanLayer: lock timed out: ${lockPath}`);
      else
        await sleepUnref(LOCK_POLL_MS);
    }
  }
  try {
    return await fn();
  } finally {
    await unlink(lockPath).catch(() => {});
  }
}
function decodeJwtPayload(jwt) {
  const part = jwt.split(".")[1];
  if (!part)
    throw new Error("Malformed JWT: no payload segment");
  const parsed = JSON.parse(Buffer.from(part, "base64url").toString("utf8"));
  if (typeof parsed !== "object" || parsed === null)
    throw new Error("Malformed JWT: payload is not an object");
  return parsed;
}
function jwtClaim(jwt, key) {
  try {
    const value = decodeJwtPayload(jwt)[key];
    return typeof value === "string" && value ? value : undefined;
  } catch {
    return;
  }
}
var logChain = Promise.resolve();
function logLine(path, line) {
  logChain = logChain.then(async () => {
    try {
      const info = await stat(path).catch(() => null);
      if (info && info.size > 5 * 1024 * 1024)
        await rename(path, `${path}.1`).catch(() => {});
      await mkdir(dirname(path), { recursive: true, mode: 448 });
      await appendFile(path, line.endsWith(`
`) ? line : `${line}
`, { mode: 384 });
    } catch {}
  });
}
// packages/session-sdk-base/src/rpc.ts
class RpcError extends Error {
  status;
  code;
  data;
  constructor(message, status, code, data) {
    super(message);
    this.name = "RpcError";
    this.status = status;
    this.code = code;
    this.data = data;
  }
}

class LoginRequiredError extends Error {
  constructor(message) {
    super(message);
    this.name = "LoginRequiredError";
  }
}
function timeout(ms, signal) {
  return signal ? AbortSignal.any([AbortSignal.timeout(ms), signal]) : AbortSignal.timeout(ms);
}
function isErrorBody(json) {
  return typeof json === "object" && json !== null;
}
async function rpcCall(call) {
  const { api, plane, path, body, cred, signal, timeoutMs = 15000, onDone } = call;
  const headers = new Headers({ "content-type": "application/json" });
  if (cred && plane === "api")
    headers.set("authorization", `Bearer ${cred}`);
  if (cred && plane === "daemon")
    headers.set("x-daemon-authorization", cred);
  const data = JSON.stringify(body ?? {});
  const start = Date.now();
  const done = (outcome) => onDone?.(outcome, Date.now() - start, Buffer.byteLength(data));
  const res = await fetch(`${api}/rpc/${plane}/v1/${path}`, {
    method: "POST",
    headers,
    body: data,
    signal: timeout(timeoutMs, signal)
  }).catch((err) => {
    done(err instanceof Error ? err.message : String(err));
    throw err;
  });
  const json = await res.json().catch(() => null);
  done(res.status);
  if (!res.ok) {
    const err = isErrorBody(json) ? json : undefined;
    throw new RpcError(err?.message ?? `HTTP ${res.status}`, res.status, err?.code, err?.data);
  }
  return json;
}
function isTokenRefused(err) {
  return err instanceof RpcError && (err.status === 401 || err.status === 403 && err.code === "UNAUTHORIZED");
}
function classifyRpcError(err, plane) {
  if (err instanceof LoginRequiredError)
    return { kind: "login-required" };
  if (!(err instanceof RpcError))
    return { kind: "retry-forever" };
  const { status } = err;
  if (status === 408 || status === 425 || status === 429 || status === 502 || status === 503 || status === 504) {
    return { kind: "retry-forever" };
  }
  if (status === 500)
    return { kind: "retry-limited", maxAttempts: 5 };
  if (plane === "daemon" && isTokenRefused(err))
    return { kind: "login-required" };
  if (status === 402)
    return { kind: "stop-all" };
  if (status === 403 || status === 404)
    return { kind: "stop-binding" };
  return { kind: "skip" };
}
function isRetry(action) {
  return action.kind === "retry-forever" || action.kind === "retry-limited";
}
function isTransient(err) {
  if (err instanceof LoginRequiredError)
    return false;
  if (!(err instanceof RpcError))
    return true;
  return err.status === 408 || err.status === 425 || err.status === 429 || err.status >= 500;
}

// packages/session-sdk-base/src/outbox.ts
function backoffMs(b, n) {
  const first = b.initialBackoffMs ?? 500;
  const max = b.maxBackoffMs ?? 30000;
  return Math.min(max, first * 2 ** Math.min(n - 1, 16));
}
var live = new Set;
function resumeAll() {
  for (const member of [...live])
    member.resume();
}

class Outbox {
  queue = [];
  opts;
  byteTotal = 0;
  pauseReason;
  stopped = false;
  running = false;
  lastFailure;
  member = {
    pause: (reason) => this.pause(reason),
    resume: () => this.resume(),
    stop: (reason) => this.halt(reason, this.opts.onStopAll)
  };
  constructor(opts) {
    this.opts = opts;
    live.add(this.member);
  }
  get length() {
    return this.queue.length;
  }
  get items() {
    return this.queue;
  }
  get failure() {
    return this.lastFailure;
  }
  get isPaused() {
    return this.pauseReason !== undefined;
  }
  get isStopped() {
    return this.stopped;
  }
  push(item) {
    if (this.stopped)
      return;
    this.queue.push(item);
    this.byteTotal += item.sizeBytes;
    this.enforceCap();
    this.kick();
  }
  pause(reason) {
    if (this.pauseReason !== undefined)
      return;
    this.pauseReason = reason;
    this.opts.onPause?.(reason);
  }
  resume() {
    if (this.pauseReason === undefined)
      return;
    this.pauseReason = undefined;
    this.opts.onResume?.();
    this.kick();
  }
  async drain(ms) {
    const waitForEmpty = (async () => {
      while (this.queue.length > 0 && !this.stopped && this.pauseReason === undefined) {
        await sleepUnref(50);
      }
    })();
    await Promise.race([waitForEmpty, sleepUnref(ms)]);
  }
  close() {
    this.halt("closed");
  }
  enforceCap() {
    const maxItems = this.opts.maxItems ?? 5000;
    const maxBytes = this.opts.maxBytes ?? 50 * 1024 * 1024;
    let i = 1;
    for (let item = this.queue[i];item && (this.queue.length > maxItems || this.byteTotal > maxBytes); item = this.queue[i]) {
      if (item.keep) {
        i++;
        continue;
      }
      this.queue.splice(i, 1);
      this.byteTotal -= item.sizeBytes;
      this.opts.onDrop?.(item);
    }
  }
  kick() {
    if (!this.running)
      this.runLoop().catch(() => {});
  }
  async runLoop() {
    this.running = true;
    try {
      for (let item = this.queue[0];item && !this.stopped && this.pauseReason === undefined; item = this.queue[0]) {
        const done = await this.attempt(item);
        if (done && this.queue[0] === item) {
          this.queue.shift();
          this.byteTotal -= item.sizeBytes;
        }
      }
    } finally {
      this.running = false;
    }
  }
  async attempt(item) {
    for (let attempts = 1;!this.stopped && this.pauseReason === undefined; attempts++) {
      try {
        await this.opts.send(item);
        this.lastFailure = undefined;
        return true;
      } catch (err) {
        const action = (this.opts.classify ?? classifyRpcError)(err, item.plane);
        const reason = errorMessage(err);
        this.lastFailure = reason;
        const note = (what) => this.opts.log?.(`outbox #${item.id} ${what}: ${reason}`);
        if (action.kind === "retry-forever" || action.kind === "retry-limited" && attempts <= action.maxAttempts) {
          const ms = backoffMs(this.opts, attempts);
          note(`retry in ${ms}ms`);
          await sleepUnref(ms);
          continue;
        }
        if (action.kind === "login-required") {
          note("pause all for login");
          for (const member of [...live])
            member.pause("login required");
          return false;
        }
        if (action.kind === "stop-all") {
          note("stop all");
          this.halt(reason, this.opts.onStopAll);
          for (const member of [...live])
            member.stop(reason);
          return false;
        }
        if (action.kind === "stop-binding") {
          note("stop");
          this.halt(reason, this.opts.onStopBinding);
          return false;
        }
        note("skip");
        this.opts.onSkip?.(item, err);
        return true;
      }
    }
    return false;
  }
  halt(reason, notify) {
    if (this.stopped)
      return;
    this.stopped = true;
    live.delete(this.member);
    this.queue.length = 0;
    this.byteTotal = 0;
    notify?.(reason);
  }
}
// packages/session-sdk-base/src/text.ts
import { createHash } from "crypto";
function sha256Hex(input) {
  return createHash("sha256").update(input).digest("hex");
}
function cutUtf8(s, maxBytes) {
  const buf = Buffer.from(s, "utf8");
  if (buf.byteLength <= maxBytes)
    return s;
  let end = maxBytes;
  while (end > 0 && ((buf[end] ?? 0) & 192) === 128)
    end--;
  const removed = buf.byteLength - end;
  return `${buf.subarray(0, end).toString("utf8")}\u2026[truncated ${removed} bytes]`;
}
// packages/session-sdk-sessions/src/claims.ts
import { mkdir as mkdir2, readdir, rm, stat as stat2, writeFile as writeFile2 } from "fs/promises";
import { join } from "path";
var KEEP_MS = 24 * 60 * 60 * 1000;
async function claim(dir, key) {
  await mkdir2(dir, { recursive: true, mode: 448 });
  try {
    await writeFile2(join(dir, key.replace(/[^\w.-]/g, "_")), "", { flag: "wx", mode: 384 });
    return true;
  } catch (err) {
    if (err instanceof Error && "code" in err && err.code === "EEXIST")
      return false;
    throw err;
  }
}
async function pruneClaims(dir, now = Date.now()) {
  for (const name of await readdir(dir).catch(() => [])) {
    const path = join(dir, name);
    const info = await stat2(path).catch(() => {
      return;
    });
    if (info && now - info.mtimeMs > KEEP_MS)
      await rm(path, { force: true }).catch(() => {});
  }
}
// packages/session-sdk-sessions/src/commands.ts
function sessionCommand(change) {
  if (change.type === "delete")
    return;
  const { value, previousValue } = change;
  const moved = change.type === "insert" || previousValue?.status !== value.status;
  if (!moved)
    return;
  if (value.status === "resuming") {
    const resume = change.type === "update" && previousValue?.status === "lost" ? "lost-resume" : "ordinary";
    return { kind: "continue", sessionId: value.id, prompt: value.prompt ?? null, resume };
  }
  if (value.status === "interrupt_requested")
    return { kind: "interrupt", sessionId: value.id };
  return;
}
// packages/session-sdk-sessions/src/events.ts
import { createHash as createHash2 } from "crypto";
var EVENT_LIMITS = {
  contentBytes: 1024 * 1024,
  hiddenBytes: 256 * 1024,
  idChars: 256
};
function systemFields(payload) {
  return { eventType: "system", role: "system", content: JSON.stringify(payload) };
}
function fitEvent(event) {
  const fitted = { ...event };
  fitted.content &&= cutUtf8(fitted.content, EVENT_LIMITS.contentBytes);
  fitted.toolResultContent &&= cutUtf8(fitted.toolResultContent, EVENT_LIMITS.contentBytes);
  fitted.toolCallId &&= fitted.toolCallId.slice(0, EVENT_LIMITS.idChars);
  fitted.toolResultForId &&= fitted.toolResultForId.slice(0, EVENT_LIMITS.idChars);
  if (fitted.toolInputJson !== undefined)
    fitted.toolInputJson = cutStrings(fitted.toolInputJson);
  return fitted;
}
function cutStrings(value) {
  if (typeof value === "string")
    return cutUtf8(value, EVENT_LIMITS.contentBytes);
  if (Array.isArray(value))
    return value.map(cutStrings);
  if (typeof value !== "object" || value === null)
    return value;
  return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, cutStrings(item)]));
}
var NAME_UUID_NAMESPACE = Buffer.from("c7b1b7b063e4de4a9c8a2f6b7a4e9d31", "hex");
function eventUuid(name) {
  const hash = createHash2("sha1").update(Buffer.concat([NAME_UUID_NAMESPACE, Buffer.from(name, "utf8")])).digest();
  const bytes = hash.subarray(0, 16);
  bytes[6] = (bytes[6] ?? 0) & 15 | 80;
  bytes[8] = (bytes[8] ?? 0) & 63 | 128;
  const hex = Buffer.from(bytes).toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
// packages/session-sdk-sessions/src/inbox.ts
import { createHash as createHash3 } from "crypto";

// packages/session-sdk-sessions/src/shape.ts
class ShapeReader {
  opts;
  handle;
  offset = "-1";
  cursor;
  live = false;
  seeded = false;
  rows = new Map;
  constructor(opts) {
    this.opts = { liveTimeoutMs: 60000, fetchTimeoutMs: 20000, ...opts };
  }
  get isSeeded() {
    return this.seeded;
  }
  get current() {
    return this.rows;
  }
  async poll(headers, signal) {
    const url = new URL(this.opts.url);
    url.searchParams.set("offset", this.offset);
    if (this.handle)
      url.searchParams.set("handle", this.handle);
    if (this.live) {
      url.searchParams.set("live", "true");
      if (this.cursor)
        url.searchParams.set("cursor", this.cursor);
    }
    const res = await fetch(url, {
      headers,
      signal: timeout(this.live ? this.opts.liveTimeoutMs : this.opts.fetchTimeoutMs, signal)
    });
    if (res.status === 409) {
      await res.body?.cancel();
      this.refetch(res.headers.get("electric-handle") ?? undefined);
      return [];
    }
    if (!res.ok) {
      await res.body?.cancel();
      throw new RpcError(`shape HTTP ${res.status}`, res.status, undefined);
    }
    this.handle = res.headers.get("electric-handle") ?? this.handle;
    this.offset = res.headers.get("electric-offset") ?? this.offset;
    this.cursor = res.headers.get("electric-cursor") ?? this.cursor;
    const messages = res.status === 204 ? [] : await res.json();
    const changes = [];
    for (const m of messages) {
      const change = this.apply(m);
      if (change)
        changes.push(change);
    }
    return changes;
  }
  refetch(handle) {
    this.handle = handle;
    this.offset = "-1";
    this.cursor = undefined;
    this.live = false;
  }
  apply(m) {
    const { control, operation } = m.headers;
    if (control === "up-to-date") {
      this.live = true;
      this.seeded = true;
      return;
    }
    if (control === "must-refetch") {
      this.refetch(undefined);
      return;
    }
    const key = m.key ?? m.value?.id;
    if (!operation || !key)
      return;
    const previousValue = this.rows.get(key);
    const value = operation === "insert" ? m.value : { ...previousValue, ...m.value };
    if (operation === "delete")
      this.rows.delete(key);
    else
      this.rows.set(key, value);
    const type = operation === "insert" && previousValue ? "update" : operation;
    return { type, key, value, previousValue, initial: !this.seeded };
  }
}

// packages/session-sdk-sessions/src/inbox.ts
function follow(name, url, o, handle) {
  const stopper = new AbortController;
  const signal = AbortSignal.any([o.signal, stopper.signal]);
  const shape = new ShapeReader({ url });
  const state = { stopped: false };
  (async () => {
    let failures = 0;
    while (!signal.aborted) {
      try {
        const changes = await o.client.withDaemonToken(o.channel, signal, (token) => shape.poll({ "x-daemon-authorization": token }, signal));
        failures = 0;
        await handle(changes, shape);
      } catch (err) {
        if (signal.aborted)
          return;
        const action = classifyRpcError(err, "daemon").kind;
        if (action === "stop-all" || action === "stop-binding" || action === "skip") {
          o.log(`${name} stopped: ${errorMessage(err)}`);
          state.stopped = true;
          return;
        }
        if (failures === 0)
          o.log(`${name}: ${errorMessage(err)}`);
        failures++;
        await sleepUnref(backoffMs({}, failures), signal);
      }
    }
  })();
  return {
    close: () => stopper.abort(),
    get isStopped() {
      return state.stopped;
    }
  };
}
function claimKey(row) {
  const prompt = createHash3("sha256").update(row.prompt ?? "").digest("hex").slice(0, 16);
  return `${row.id}-${row.status}-${row.updated_at ?? ""}-${prompt}`;
}
var WAITING = new Set(["resuming", "interrupt_requested"]);
function followSessions(o) {
  let caughtUp = false;
  const act = async (change) => {
    const command = sessionCommand(change);
    if (!command || command.kind === "continue" && !command.prompt)
      return;
    const target = await o.target(command.sessionId);
    if (!target || !await o.claim(claimKey(change.value)))
      return;
    if (command.kind === "continue" && command.prompt)
      target.web(command.prompt);
    else if (command.kind === "interrupt")
      target.stop();
  };
  const url = `${getChannelConfig(o.channel).sync}/v1/sessions/${o.hostId}`;
  return follow("inbox", url, o, async (changes, shape) => {
    if (!caughtUp && shape.isSeeded) {
      caughtUp = true;
      const waiting = [...shape.current.values()].filter((row) => WAITING.has(row.status));
      for (const value of waiting)
        await act({ type: "insert", value });
    }
    for (const change of changes)
      if (!change.initial)
        await act(change);
  });
}
function followApprovals(o) {
  const url = `${getChannelConfig(o.channel).sync}/v1/daemon/approvals`;
  return follow("approvals", url, o, async (changes) => {
    for (const { value } of changes) {
      if (value.status !== "pending")
        o.answered(value.id, value.status === "approved", value.comment ?? undefined, value.session_id ?? undefined);
    }
  });
}
// packages/session-sdk-sessions/src/prepare.ts
import { execFile } from "child_process";
import { basename, dirname as dirname2 } from "path";
import { promisify } from "util";
var execFileAsync = promisify(execFile);
async function git(cwd, ...args) {
  try {
    return (await execFileAsync("git", args, { cwd, timeout: 5000 })).stdout.trim();
  } catch {
    return;
  }
}
async function gitInfo(cwd) {
  const root = await git(cwd, "rev-parse", "--show-toplevel");
  if (!root)
    return;
  const [headSha, branch, remoteUrl] = await Promise.all([
    git(root, "rev-parse", "--verify", "-q", "HEAD"),
    git(root, "symbolic-ref", "--short", "-q", "HEAD"),
    git(root, "remote", "get-url", "origin")
  ]);
  const info = { root, branch: branch ?? "" };
  if (headSha)
    info.headSha = headSha;
  if (remoteUrl)
    info.remoteUrl = stripUserinfo(remoteUrl);
  return info;
}
function stripUserinfo(url) {
  try {
    const parsed = new URL(url);
    if (!parsed.username && !parsed.password)
      return url;
    parsed.username = "";
    parsed.password = "";
    return parsed.toString();
  } catch {
    return url;
  }
}
function promptText(text2, imageCount) {
  const parts = [text2, ...Array.from({ length: imageCount }, () => "[image]")].filter((part) => part.trim());
  return cutUtf8(parts.join(`
`), 1024 * 1024) || "(empty prompt)";
}
function prepareBody(f) {
  const session = {
    hostId: f.hostId,
    sessionName: f.title,
    prompt: f.prompt,
    workingDirectory: f.cwd,
    codingAgent: f.codingAgent,
    permissionsMode: "bypass"
  };
  if (f.model) {
    session.provider = f.model.provider;
    session.model = f.model.id;
  }
  if (f.pick.taskMode === "use")
    return { ...session, taskMode: "use", taskIdOrSlug: f.pick.taskIdOrSlug };
  const task = { name: f.title, workflowType: "freeform", worktreeTiming: "never" };
  if (f.git)
    task.workspaceState = workspaceState(f.git);
  return { ...session, taskMode: "ensure", slug: f.pick.slug, task };
}
function workspaceState(git2) {
  const repo = { path: basename(git2.root), sourceRef: "HEAD", branch: git2.branch, primary: true };
  if (git2.headSha)
    repo.sourceCommit = git2.headSha;
  if (git2.remoteUrl)
    repo.remoteUrl = git2.remoteUrl;
  return { workspaceBaseDirectory: dirname2(git2.root), repos: [repo] };
}
function repositoriesReport(git2) {
  const repo = { localPath: git2.root };
  if (git2.remoteUrl)
    repo.remoteUrl = git2.remoteUrl;
  if (git2.branch)
    repo.branch = git2.branch;
  return { repositories: [repo] };
}
// packages/session-sdk-sessions/src/task-pick.ts
import { appendFile as appendFile2, mkdir as mkdir3, readdir as readdir2, readFile as readFile2, readlink, realpath } from "fs/promises";
import { basename as basename2, dirname as dirname3, join as join2 } from "path";
var UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
function isUuid(value) {
  return UUID.test(value);
}
async function pickTask(o) {
  if (o.attach === "new")
    return newTask(o.prefix, o.freshId?.() ?? `${Date.now()}`);
  const chosen = o.attach || o.chosen?.trim();
  if (chosen === "new")
    return newTask(o.prefix, o.sessionId);
  if (chosen)
    return { taskMode: "use", taskIdOrSlug: chosen, taskSlug: isUuid(chosen) ? undefined : chosen };
  const link = await taskLink(o.cwd, o.linksPath);
  if (link)
    return { taskMode: "use", taskIdOrSlug: link.taskId, taskSlug: link.slug, auto: true };
  return newTask(o.prefix, o.sessionId);
}
function newTask(prefix, sessionId) {
  const tail = sessionId.replace(/[^a-z0-9]/gi, "").slice(-12).toLowerCase();
  return { taskMode: "ensure", slug: `${prefix}-${tail}` };
}
async function taskLink(cwd, linksPath) {
  const dir = await realpath(join2(cwd, ".humanlayer", "tasks")).catch(() => "");
  let links = [];
  for (const name of await readdir2(dir).catch(() => [])) {
    const target = await readlink(join2(dir, name)).catch(() => "");
    const taskId = /\/artifacts\/([^/]+)\/?$/.exec(target)?.[1];
    if (taskId && UUID.test(taskId))
      links.push({ taskId, slug: name });
  }
  if (links.length > 0) {
    const own = new Set((await readFile2(linksPath, "utf8").catch(() => "")).split(`
`));
    links = links.filter((link) => !own.has(linkKey(join2(dir, link.slug), link.taskId)));
  }
  return links.length === 1 ? links[0] : undefined;
}
function linkKey(path, taskId) {
  return JSON.stringify([path, taskId]);
}
async function recordLink(linksPath, path, taskId) {
  const key = linkKey(join2(await realpath(dirname3(path)), basename2(path)), taskId);
  await mkdir3(dirname3(linksPath), { recursive: true, mode: 448 });
  await appendFile2(linksPath, `
${key}
`, { mode: 384 });
}
// packages/session-sdk-tools/src/tools.ts
function formatThread(comment, replies) {
  const lines = [`<comment id="${comment.truncatedId}">`];
  if (comment.previousBlockText || comment.quotedText || comment.nextBlockText) {
    for (const line of comment.previousBlockText?.split(`
`) ?? [])
      lines.push(`  | ${line}`);
    for (const line of comment.quotedText?.split(`
`) ?? [])
      lines.push(`> | ${line}`);
    for (const line of comment.nextBlockText?.split(`
`) ?? [])
      lines.push(`  | ${line}`);
    lines.push("");
  }
  lines.push(`${comment.userName}${comment.isResolved ? " [RESOLVED]" : ""}: ${comment.contentText}`);
  for (const reply of replies.get(comment.id) ?? []) {
    lines.push(`  ${reply.userName}${reply.isResolved ? " [RESOLVED]" : ""}: ${reply.contentText}`);
  }
  lines.push("</comment>");
  return lines.join(`
`);
}
function formatArtifactComments(filename, comments, offset, limit) {
  const page = comments.slice(offset, offset + limit);
  if (page.length === 0)
    return `No comments found on ${filename}`;
  const replies = new Map;
  for (const c of page) {
    if (!c.replyToCommentId)
      continue;
    const list = replies.get(c.replyToCommentId) ?? [];
    list.push(c);
    replies.set(c.replyToCommentId, list);
  }
  const threads = page.filter((c) => !c.replyToCommentId).map((c) => formatThread(c, replies));
  const result = `<comments for="${filename}">
${threads.join(`

`)}
</comments>`;
  const remaining = Math.max(0, comments.length - offset - page.length);
  return remaining > 0 ? `${result}
(${remaining} additional comments not returned)` : result;
}
function escapeXml(value) {
  return value.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;").replaceAll("'", "&apos;");
}
function formatDiffEntry(tag, c) {
  const author = c.createdByAgent ? `${c.createdByUserId}'s agent` : c.createdByUserId;
  const text2 = c.isDeleted ? "Comment deleted" : c.contentText;
  return `  <${tag} id="${c.truncatedId}" author="${escapeXml(author)}">${escapeXml(text2)}</${tag}>`;
}
function formatDiffComments(threads, offset, limit) {
  const page = threads.slice(offset, offset + limit);
  if (page.length === 0)
    return "No diff comments found for this task";
  const blocks = page.map((t) => {
    const a = t.anchor;
    return [
      `<diff_comment id="${t.root.truncatedId}" repo="${escapeXml(a.repoId)}" path="${escapeXml(a.path)}" patch_hash="${escapeXml(a.patchHash)}" start_side="${a.start.side}" start_line="${a.start.line}" end_side="${a.end.side}" end_line="${a.end.line}" resolved="${t.isResolved}">`,
      formatDiffEntry("comment", t.root),
      ...t.replies.map((r) => formatDiffEntry("reply", r)),
      "</diff_comment>"
    ].join(`
`);
  });
  const result = `<diff_comments>
${blocks.join(`
`)}
</diff_comments>`;
  const remaining = Math.max(0, threads.length - offset - page.length);
  return remaining > 0 ? `${result}
(${remaining} additional threads not returned)` : result;
}
function formatUpdate(result) {
  const messages = [];
  if (result.updated.length > 0)
    messages.push(`Updated ${result.updated.length} comment(s)`);
  if (result.failed.length > 0) {
    messages.push(`Failed: ${result.failed.map((f) => `${f.truncatedId}: ${f.reason}`).join(", ")}`);
  }
  return messages.join(". ") || "No changes made";
}
function validArtifacts(err) {
  if (!(err instanceof RpcError) || typeof err.data !== "object" || err.data === null)
    return;
  const list = err.data.validArtifacts;
  return Array.isArray(list) ? list.map(String) : undefined;
}
function tool(description, params, run) {
  return { description, params, run: (p, taskId, call) => run(p, taskId, call) };
}
function toJsonSchema(params) {
  const string = (p) => ({ type: "string", ...p });
  const properties = Object.fromEntries(Object.entries(params).map(([name, p]) => {
    const { optional: _optional, type, ...rest } = p;
    if (type === "true")
      return [name, { type: "boolean", const: true, description: p.description }];
    if (type === "strings") {
      const { items, ...array } = rest;
      return [name, { type: "array", items: string(items ?? {}), ...array }];
    }
    return [name, { type, ...rest }];
  }));
  const required = Object.entries(params).filter(([, p]) => !p.optional).map(([name]) => name);
  return { type: "object", properties, required, additionalProperties: false };
}
var DIFF_SUFFIX = {
  minLength: 8,
  maxLength: 12,
  pattern: "^[0-9a-fA-F]+$",
  description: "Unique 8-12 character right-hand UUID suffix from get_diff_comments"
};
var paging = (what) => ({
  limit: { type: "integer", minimum: 1, optional: true, description: `Max ${what} to return (default: 20)` },
  offset: { type: "integer", minimum: 0, optional: true, description: `Number of ${what} to skip (default: 0)` }
});
function requireChange(p) {
  if (p.resolved === undefined && p.deleted === undefined) {
    throw new Error("At least one of resolved or deleted must be provided");
  }
}
var TOOLS = {
  get_artifact_comments: tool("Get all comments on an artifact (plan.md, research.md, etc). Returns threaded comments in XML format with truncated IDs for referencing in update/reply tools.", {
    artifact_filename: {
      type: "string",
      description: 'Filename of the artifact, e.g. "plan.md", "research.md"'
    },
    include_resolved: {
      type: "boolean",
      optional: true,
      description: "Include resolved comments (default: true)"
    },
    ...paging("comments")
  }, async (p, taskId, call) => {
    const out = await call("comments/get", {
      taskId,
      artifactFilename: p.artifact_filename,
      includeResolved: p.include_resolved ?? true
    });
    return formatArtifactComments(out.artifactFilename, out.comments, p.offset ?? 0, p.limit ?? 20);
  }),
  update_artifact_comments: tool("Update comments on an artifact - mark as resolved or deleted. Use truncated IDs from get_artifact_comments.", {
    artifact_filename: { type: "string", description: 'Filename of the artifact, e.g. "plan.md"' },
    comment_ids: { type: "strings", minItems: 1, description: "Truncated comment IDs (last 12 chars)" },
    resolved: { type: "boolean", optional: true, description: "Set resolved state for all specified comments" },
    deleted: { type: "boolean", optional: true, description: "Set deleted state for all specified comments" }
  }, async (p, taskId, call) => {
    requireChange(p);
    const body = { taskId, artifactFilename: p.artifact_filename, truncatedCommentIds: p.comment_ids };
    return formatUpdate(await call("comments/update", { ...body, resolved: p.resolved, deleted: p.deleted }));
  }),
  reply_to_artifact_comment: tool("Reply to a comment on an artifact. Use truncated ID from get_artifact_comments.", {
    artifact_filename: { type: "string", description: 'Filename of the artifact, e.g. "plan.md"' },
    comment_id: { type: "string", description: "Truncated comment ID to reply to" },
    content: { type: "string", minLength: 1, description: "Reply text content (markdown supported)" }
  }, async (p, taskId, call) => {
    const out = await call("comments/reply", {
      taskId,
      artifactFilename: p.artifact_filename,
      truncatedCommentId: p.comment_id,
      contentText: p.content
    });
    return `Reply added (id: ${out.commentId.slice(-12)})`;
  }),
  get_diff_comments: tool("Get ordered diff comment threads for the current task, including code anchors and exact short IDs for reply and update tools.", {
    include_resolved: {
      type: "boolean",
      optional: true,
      description: "Include resolved threads (default: false)"
    },
    ...paging("threads")
  }, async (p, taskId, call) => {
    const out = await call("diffComments/get", { taskId, includeResolved: p.include_resolved ?? false });
    return formatDiffComments(out.comments, p.offset ?? 0, p.limit ?? 20);
  }),
  reply_to_diff_comment: tool("Reply to a diff comment thread in the current task. The reply is added to the root thread and reopens it.", {
    thread_id: {
      type: "string",
      ...DIFF_SUFFIX,
      description: "Root or reply ID suffix for the thread to reply to"
    },
    content: { type: "string", minLength: 1, description: "Reply text content (Markdown supported)" }
  }, async (p, taskId, call) => {
    const out = await call("diffComments/reply", {
      taskId,
      truncatedCommentId: p.thread_id,
      contentText: p.content.trim()
    });
    return `Reply added (id: ${out.commentId.slice(-12)})`;
  }),
  update_diff_comments: tool("Resolve, reopen, or soft-delete diff comments in the current task. Resolve a thread only when the user asks you to resolve it or feedback delivery used Send & Resolve. Never resolve a thread just because you replied or changed code. Deletion is limited to comments authored by the driving user.", {
    comment_ids: {
      type: "strings",
      minItems: 1,
      items: DIFF_SUFFIX,
      description: "Comment or thread ID suffixes to update"
    },
    resolved: {
      type: "boolean",
      optional: true,
      description: "Resolve (true) or reopen (false) each matched thread"
    },
    deleted: {
      type: "true",
      optional: true,
      description: "Soft-delete matched comments authored by the driving user"
    }
  }, async (p, taskId, call) => {
    requireChange(p);
    const body = { taskId, truncatedCommentIds: p.comment_ids };
    return formatUpdate(await call("diffComments/update", { ...body, resolved: p.resolved, deleted: p.deleted }));
  }),
  library_researcher: tool("Research documentation for a library or package to answer questions about usage, APIs, and best practices. Use this when you need up-to-date information about a library or dependency.", {
    question: {
      type: "string",
      description: "The specific question you want answered about the library. Be detailed and specific. " + "Good: 'How do I configure authentication middleware in Express.js 5?' " + "Bad: 'express auth'"
    },
    package_name: {
      type: "string",
      description: "The exact name of the npm package, PyPI package, or library to research. " + "Use the canonical package name as it appears in the package registry if possible. " + "Examples: 'react', 'express', 'drizzle-orm', '@tanstack/react-query'"
    },
    language: {
      type: "string",
      description: "The programming language(s) in question. E.g. typescript, python, elixir, java, javascript, C, c++, etc"
    }
  }, async (p, _taskId, call) => {
    const out = await call("agents/research", {
      question: p.question,
      packageName: p.package_name,
      language: p.language
    });
    return out.response;
  })
};
var HUMANLAYER_TOOLS = Object.keys(TOOLS);
function toolFailure(name, err) {
  const artifacts = validArtifacts(err);
  if (artifacts)
    return new Error(`Artifact not found. Valid artifacts: ${artifacts.join(", ")}`);
  if (err instanceof RpcError)
    return new Error(`${name} failed: ${err.message}`);
  return err instanceof Error ? err : new Error(String(err));
}
var BIND_WAIT_MS = 15000;
async function settledTarget(target, signal) {
  const deadline = Date.now() + BIND_WAIT_MS;
  let t = target();
  while ("pending" in t && t.pending && Date.now() < deadline && !signal?.aborted) {
    await sleepUnref(100);
    t = target();
  }
  return t;
}
async function runTool(name, params, target, daemonCall, signal) {
  const t = await settledTarget(target, signal);
  if ("reason" in t)
    throw new Error(t.reason);
  const call = (path, body) => daemonCall(t.channel, path, body, signal);
  try {
    return await TOOLS[name].run(params, t.taskId, call);
  } catch (err) {
    throw toolFailure(name, err);
  }
}
// apps/riptide-opencode-plugin/src/index.ts
import { Plugin } from "@opencode/plugin";

// apps/riptide-opencode-plugin/src/config.ts
import { existsSync } from "fs";
import { homedir } from "os";
import { join as join4 } from "path";
import { fileURLToPath } from "url";

// packages/session-sdk-auth/src/client.ts
import { randomUUID as randomUUID2 } from "crypto";
import { join as join3 } from "path";
function authPaths(dir) {
  return {
    config: join3(dir, "config.json"),
    host: (channel) => join3(dir, `host-${channel}.json`),
    session: (channel) => join3(dir, `session-${channel}.json`),
    lock: (channel) => join3(dir, `session-${channel}.json.lock`)
  };
}
function createSessionClient(opts) {
  const paths = () => authPaths(opts.dir());
  const notSignedIn = `Not signed in to HumanLayer. ${opts.loginHint}`;
  const stashKey = Symbol.for(opts.stashKey);
  function rpc2(channel, plane, path, body, cred, signal) {
    return rpcCall({
      api: getChannelConfig(channel).api,
      plane,
      path,
      body,
      cred,
      signal,
      onDone: (outcome, ms, bytes) => opts.log(`${plane} ${path} ${outcome} ${ms}ms ${bytes}B`)
    });
  }
  async function resolveChannel() {
    const envChannel = process.env.HUMANLAYER_CHANNEL;
    if (isChannel(envChannel))
      return envChannel;
    const saved = await readJsonFile(paths().config);
    if (isChannel(saved?.channel))
      return saved.channel;
    return "prod";
  }
  async function saveChannel(channel) {
    await writeJsonFileAtomic(paths().config, { channel });
  }
  const hostIds = new Map;
  function hostId(channel) {
    const file = paths().host(channel);
    let id = hostIds.get(file);
    if (!id) {
      id = withFileLock(`${file}.lock`, async () => {
        const existing = await readJsonFile(file);
        if (existing && typeof existing.hostId === "string" && existing.hostId.length > 0)
          return existing.hostId;
        const made = randomUUID2();
        await writeJsonFileAtomic(file, { hostId: made });
        return made;
      }).finally(() => hostIds.delete(file));
      hostIds.set(file, id);
    }
    return id;
  }
  function stash() {
    const saved = Reflect.get(globalThis, stashKey);
    if (saved)
      return saved;
    const fresh = {};
    Reflect.set(globalThis, stashKey, fresh);
    return fresh;
  }
  function getPat() {
    const s = stash();
    if (s.pat === undefined) {
      const fromEnv = process.env.HUMANLAYER_PAT;
      if (fromEnv !== undefined) {
        s.pat = fromEnv;
        delete process.env.HUMANLAYER_PAT;
      }
    }
    return s.pat;
  }
  function readCreds(channel) {
    return readJsonFile(paths().session(channel));
  }
  async function identity(channel) {
    if (getPat())
      return { channel, source: "pat" };
    const creds = await readCreds(channel);
    if (!creds)
      return null;
    return {
      channel,
      source: "device",
      email: creds.email,
      orgName: creds.orgName,
      orgId: creds.orgId,
      userId: creds.userId
    };
  }
  function isFresh(creds) {
    const claims2 = decodeJwtPayload(creds.accessToken);
    const exp = typeof claims2.exp === "number" ? claims2.exp : 0;
    return exp * 1000 - Date.now() > 60000;
  }
  async function refreshAndSave(channel, current) {
    let refreshed;
    try {
      refreshed = await rpc2(channel, "api", "auth/token/refresh", {
        refreshToken: current.refreshToken,
        organizationId: current.workosOrgId
      });
    } catch (err) {
      if (err instanceof RpcError && (err.status === 400 || err.status === 401)) {
        throw new LoginRequiredError(`HumanLayer sign-in expired. ${opts.loginHint}`);
      }
      throw err;
    }
    const updated = { ...current, accessToken: refreshed.accessToken, refreshToken: refreshed.refreshToken };
    await writeJsonFileAtomic(paths().session(channel), updated);
    return updated.accessToken;
  }
  async function refreshAccessToken(channel) {
    const first = await readCreds(channel);
    if (!first)
      throw new LoginRequiredError(notSignedIn);
    if (isFresh(first))
      return first.accessToken;
    return withFileLock(paths().lock(channel), async () => {
      const current = await readCreds(channel);
      if (!current)
        throw new LoginRequiredError(notSignedIn);
      if (isFresh(current))
        return current.accessToken;
      return refreshAndSave(channel, current);
    });
  }
  async function refreshAccessTokenFrom(channel, staleToken) {
    return withFileLock(paths().lock(channel), async () => {
      const current = await readCreds(channel);
      if (!current)
        throw new LoginRequiredError(notSignedIn);
      if (current.accessToken !== staleToken)
        return current.accessToken;
      return refreshAndSave(channel, current);
    });
  }
  async function apiRpc(channel, path, body, signal) {
    const pat = getPat();
    const bearer = pat ?? await refreshAccessToken(channel);
    try {
      return await rpc2(channel, "api", path, body, bearer, signal);
    } catch (err) {
      if (pat) {
        if (err instanceof RpcError && err.status === 401) {
          throw new LoginRequiredError("HUMANLAYER_PAT was rejected (401). Set a valid HUMANLAYER_PAT.");
        }
        throw err;
      }
      if (!(err instanceof RpcError) || err.status !== 401)
        throw err;
      const retried = await refreshAccessTokenFrom(channel, bearer);
      return rpc2(channel, "api", path, body, retried, signal);
    }
  }
  async function mintDaemonToken(channel, host, signal) {
    const minted = await apiRpc(channel, "auth/daemon/token/create", { hostId: host }, signal);
    return minted.token;
  }
  async function remintPatDaemonToken(channel, signal) {
    if (!getPat())
      throw new LoginRequiredError("HUMANLAYER_PAT not set");
    const token = await mintDaemonToken(channel, await hostId(channel), signal);
    const s = stash();
    if (!s.patDaemonTokens)
      s.patDaemonTokens = {};
    s.patDaemonTokens[channel] = token;
    return token;
  }
  async function daemonToken(channel, signal) {
    if (getPat())
      return stash().patDaemonTokens?.[channel] ?? remintPatDaemonToken(channel, signal);
    const creds = await readCreds(channel);
    if (!creds)
      throw new LoginRequiredError(notSignedIn);
    return creds.daemonToken;
  }
  async function daemonOrgId(channel, signal) {
    return jwtClaim(await daemonToken(channel, signal), "organizationId");
  }
  async function remintDaemonToken(channel, signal) {
    if (getPat())
      return remintPatDaemonToken(channel, signal);
    const before = await readCreds(channel);
    if (!before)
      throw new LoginRequiredError(notSignedIn);
    const token = await mintDaemonToken(channel, before.daemonHostId, signal);
    return withFileLock(paths().lock(channel), async () => {
      const current = await readCreds(channel);
      if (!current)
        throw new LoginRequiredError(notSignedIn);
      if (current.daemonToken !== before.daemonToken)
        return current.daemonToken;
      await writeJsonFileAtomic(paths().session(channel), { ...current, daemonToken: token });
      return token;
    });
  }
  async function withDaemonToken(channel, signal, request) {
    try {
      return await request(await daemonToken(channel, signal));
    } catch (err) {
      if (!isTokenRefused(err))
        throw err;
      return request(await remintDaemonToken(channel, signal));
    }
  }
  function daemonCall(channel, path, body, signal) {
    return withDaemonToken(channel, signal, (token) => rpc2(channel, "daemon", path, body, token, signal));
  }
  function prepare2(channel, body, signal) {
    return apiRpc(channel, "automation/run/prepare", body, signal);
  }
  return {
    paths,
    loginHint: opts.loginHint,
    log: opts.log,
    rpc: rpc2,
    resolveChannel,
    saveChannel,
    hostId,
    getPat,
    readCreds,
    identity,
    apiRpc,
    daemonToken,
    daemonOrgId,
    remintDaemonToken,
    withDaemonToken,
    daemonCall,
    prepare: prepare2
  };
}
// packages/session-sdk-auth/src/login.ts
import { spawn } from "child_process";
import { unlink as unlink2 } from "fs/promises";
var CANCEL_MESSAGE = "login cancelled";
var TIMEOUT_MESSAGE = "the code expired";
var SLOW_DOWN_TIMEOUT_MESSAGE = "timed out after one or more slow_down responses. This is often caused by clock drift " + "in WSL or VM environments. Please sync or restart the VM clock and try again.";
var MINIMUM_INTERVAL_MS = 1000;
var DEFAULT_POLL_INTERVAL_SECONDS = 5;
var SLOW_DOWN_INTERVAL_INCREMENT_MS = 5000;
function abortableSleep(ms, signal, cancelMessage) {
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(new Error(cancelMessage));
      return;
    }
    const onAbort = () => {
      clearTimeout(timeout2);
      reject(new Error(cancelMessage));
    };
    const timeout2 = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    timeout2.unref();
    signal.addEventListener("abort", onAbort, { once: true });
  });
}
async function pollDeviceCodeFlow(options) {
  const deadline = typeof options.expiresInSeconds === "number" ? Date.now() + options.expiresInSeconds * 1000 : Number.POSITIVE_INFINITY;
  let intervalMs = Math.max(MINIMUM_INTERVAL_MS, Math.floor((options.intervalSeconds ?? DEFAULT_POLL_INTERVAL_SECONDS) * 1000));
  let slowDownResponses = 0;
  if (options.waitBeforeFirstPoll) {
    const remainingMs = deadline - Date.now();
    if (remainingMs > 0)
      await abortableSleep(Math.min(intervalMs, remainingMs), options.signal, CANCEL_MESSAGE);
  }
  while (Date.now() < deadline) {
    if (options.signal.aborted)
      throw new Error(CANCEL_MESSAGE);
    const result = await options.poll();
    if (result.status === "complete")
      return result.value;
    if (result.status === "failed")
      throw new Error(result.message);
    if (result.status === "slow_down") {
      slowDownResponses += 1;
      intervalMs = typeof result.intervalSeconds === "number" && Number.isFinite(result.intervalSeconds) && result.intervalSeconds > 0 ? Math.max(MINIMUM_INTERVAL_MS, Math.floor(result.intervalSeconds * 1000)) : Math.max(MINIMUM_INTERVAL_MS, intervalMs + SLOW_DOWN_INTERVAL_INCREMENT_MS);
    }
    const remainingMs = deadline - Date.now();
    if (remainingMs <= 0)
      break;
    await abortableSleep(Math.min(intervalMs, remainingMs), options.signal, CANCEL_MESSAGE);
  }
  throw new Error(slowDownResponses > 0 ? SLOW_DOWN_TIMEOUT_MESSAGE : TIMEOUT_MESSAGE);
}
function isRecord(value) {
  return typeof value === "object" && value !== null;
}
function workosForm(cfg, path, form, signal) {
  return fetch(`${cfg.workos}/user_management/${path}`, {
    method: "POST",
    headers: new Headers({ "content-type": "application/x-www-form-urlencoded" }),
    body: new URLSearchParams({ ...form, client_id: cfg.clientId }),
    signal: timeout(15000, signal)
  });
}
async function startDeviceAuthorize(cfg, signal) {
  const res = await workosForm(cfg, "authorize/device", {}, signal);
  if (!res.ok)
    throw new Error(`could not start: HTTP ${res.status}`);
  return await res.json();
}
async function pollWorkosToken(cfg, deviceCode, signal) {
  const res = await workosForm(cfg, "authenticate", { grant_type: "urn:ietf:params:oauth:grant-type:device_code", device_code: deviceCode }, signal);
  const json = await res.json().catch(() => null);
  if (res.ok && isRecord(json) && typeof json.access_token === "string" && typeof json.refresh_token === "string" && isRecord(json.user) && typeof json.user.email === "string") {
    return {
      status: "complete",
      value: { accessToken: json.access_token, refreshToken: json.refresh_token, email: json.user.email }
    };
  }
  const error = isRecord(json) && typeof json.error === "string" ? json.error : undefined;
  if (error === "authorization_pending")
    return { status: "pending" };
  if (error === "slow_down")
    return { status: "slow_down" };
  return { status: "failed", message: error ?? `HTTP ${res.status}` };
}
function openBrowser(url) {
  const cmd = process.platform === "darwin" ? "open" : process.platform === "linux" ? "xdg-open" : undefined;
  if (!cmd)
    return;
  try {
    const child = spawn(cmd, [url], { detached: true, stdio: "ignore" });
    child.unref();
    child.on("error", () => {});
  } catch {}
}
function createDeviceLogin(client, opts = {}) {
  const logins = new Map;
  function pendingLogin(channel) {
    return logins.get(channel);
  }
  function abortLogin(channel) {
    logins.get(channel)?.controller.abort();
    logins.delete(channel);
  }
  async function startDeviceLogin(channel, ui) {
    abortLogin(channel);
    const login = { controller: new AbortController };
    logins.set(channel, login);
    const signal = login.controller.signal;
    const paths = client.paths();
    try {
      const approved = await approve(channel, login, ui);
      const org = await pickOrg(channel, approved, ui, signal);
      if (!org)
        return;
      const tokens = await scopeTo(channel, approved, org, signal);
      const creds = await credsFor(channel, tokens, org, signal);
      await withFileLock(paths.lock(channel), async () => {
        signal.throwIfAborted();
        await writeJsonFileAtomic(paths.session(channel), creds);
      });
      await client.saveChannel(channel);
      signal.throwIfAborted();
      return creds;
    } catch (err) {
      if (signal.aborted)
        return;
      throw err;
    } finally {
      if (logins.get(channel) === login)
        logins.delete(channel);
    }
  }
  async function approve(channel, login, ui) {
    const cfg = getChannelConfig(channel);
    const signal = login.controller.signal;
    const dc = await startDeviceAuthorize(cfg, signal);
    signal.throwIfAborted();
    login.url = dc.verification_uri_complete;
    login.code = dc.user_code;
    ui.showCode(dc.verification_uri_complete, dc.user_code);
    if (!opts.noBrowser?.())
      openBrowser(dc.verification_uri_complete);
    return pollDeviceCodeFlow({
      intervalSeconds: dc.interval,
      expiresInSeconds: dc.expires_in,
      waitBeforeFirstPoll: true,
      signal,
      poll: () => pollWorkosToken(cfg, dc.device_code, signal)
    });
  }
  async function pickOrg(channel, tokens, ui, signal) {
    const { organizations: orgs } = await client.rpc(channel, "api", "auth/user/organizations/list", {}, tokens.accessToken, signal);
    if (orgs.length === 0)
      throw new Error("you have no organization. Create or join one in the web app.");
    const savedOrgId = (await client.readCreds(channel))?.workosOrgId;
    const byId = (id) => orgs.find((o) => o.organizationId === id);
    const current = byId(savedOrgId) ?? byId(jwtClaim(tokens.accessToken, "org_id"));
    return orgs.length > 1 && ui.hasUI ? ui.pickOrg(orgs, current) : current ?? orgs[0];
  }
  async function scopeTo(channel, tokens, org, signal) {
    if (jwtClaim(tokens.accessToken, "org_id") === org.organizationId)
      return tokens;
    const refreshed = await client.rpc(channel, "api", "auth/token/refresh", { refreshToken: tokens.refreshToken, organizationId: org.organizationId }, undefined, signal);
    return { email: tokens.email, accessToken: refreshed.accessToken, refreshToken: refreshed.refreshToken };
  }
  async function credsFor(channel, tokens, org, signal) {
    const claim2 = (key) => {
      const value = jwtClaim(tokens.accessToken, key);
      if (!value)
        throw new Error(`the token is missing claim "${key}"`);
      return value;
    };
    const userId = claim2("sub");
    const orgId = claim2("internal_org_id");
    const workosOrgId = claim2("org_id");
    const daemonHostId = await client.hostId(channel);
    const daemon = await client.rpc(channel, "api", "auth/daemon/token/create", { hostId: daemonHostId }, tokens.accessToken, signal);
    return {
      version: 1,
      channel,
      email: tokens.email,
      userId,
      orgId,
      workosOrgId,
      orgName: org.organizationName,
      accessToken: tokens.accessToken,
      refreshToken: tokens.refreshToken,
      daemonToken: daemon.token,
      daemonHostId
    };
  }
  async function logout(channel) {
    abortLogin(channel);
    const paths = client.paths();
    await withFileLock(paths.lock(channel), () => unlink2(paths.session(channel)).catch(() => {}));
  }
  return { pendingLogin, abortLogin, startDeviceLogin, logout };
}
// apps/riptide-opencode-plugin/src/config.ts
function riptideHome() {
  return process.env.HUMANLAYER_RIPTIDE_HOME || join4(homedir(), ".humanlayer", "riptide");
}
function opencodeDir() {
  return join4(riptideHome(), "opencode");
}
function artifactsDir(taskId) {
  return join4(riptideHome(), "artifacts", taskId);
}
function bindingsDir(channel) {
  return join4(opencodeDir(), "bindings", channel);
}
function claimsDir() {
  return join4(opencodeDir(), "claims");
}
function linksPath() {
  return join4(opencodeDir(), "task-links.jsonl");
}
function bundledSkillsDir() {
  const built = fileURLToPath(new URL("./skills/", import.meta.url));
  return existsSync(built) ? built : fileURLToPath(new URL("../../riptide-pi-extension/skills/", import.meta.url));
}
function logFilePath() {
  return join4(opencodeDir(), "logs", "opencode-humanlayer.log");
}
function log(line) {
  logLine(logFilePath(), `${new Date().toISOString()} ${line}`);
}
async function guard(name, fn) {
  try {
    return await fn();
  } catch (err) {
    log(`${name}: ${errorMessage(err)}`);
    return;
  }
}
var client2 = createSessionClient({
  dir: opencodeDir,
  log,
  loginHint: "Run `humanlayer-opencode login`.",
  stashKey: "humanlayer.opencode.v1"
});
var login2 = createDeviceLogin(client2, {
  noBrowser: () => process.env.HUMANLAYER_OPENCODE_NO_BROWSER === "1"
});
function isDisabled() {
  return process.env.HUMANLAYER_OPENCODE_DISABLE === "1";
}
function taskFromEnv() {
  return process.env.HUMANLAYER_TASK?.trim() || undefined;
}
var FLUSH_MS = 5000;
var HEARTBEAT_MS = 15000;

// apps/riptide-opencode-plugin/src/mapper.ts
import { isAbsolute, resolve } from "path";
function createMapperState(sessionId, cwd) {
  return { sessionId, cwd, model: undefined, done: new Set, tools: new Map, openTools: new Set };
}
function promptText2(prompt) {
  const files2 = Array.from({ length: prompt.files?.length ?? 0 }, () => "[file]");
  return [prompt.text, ...files2].filter((s) => s.trim()).join(`
`);
}
function mapPrompt(messageID, content, state) {
  const out = { events: [] };
  if (content)
    once(state, `${messageID}:user`, out, { eventType: "message", role: "user", content });
  return out;
}
function mapEvent(event, state) {
  const out = { events: [] };
  switch (event.type) {
    case "session.text.ended": {
      const d = event.data;
      if (d.text.trim())
        once(state, `${d.assistantMessageID}:text:${d.ordinal}`, out, {
          eventType: "message",
          role: "assistant",
          content: d.text
        });
      break;
    }
    case "session.reasoning.ended": {
      const d = event.data;
      if (d.text.trim())
        once(state, `${d.assistantMessageID}:reasoning:${d.ordinal}`, out, {
          eventType: "thinking",
          role: "assistant",
          content: d.text
        });
      break;
    }
    case "session.tool.input.started":
      state.tools.set(event.data.id, { name: event.data.name });
      break;
    case "session.tool.called": {
      const d = event.data;
      const name = state.tools.get(d.id)?.name ?? "tool";
      const [toolName, toolInputJson] = shapeToolCall(name, d.input, state.cwd);
      state.tools.set(d.id, { name, input: toolInputJson });
      if (once(state, `${d.id}:call`, out, {
        eventType: "tool_call",
        role: "assistant",
        toolCallId: d.id,
        toolName,
        toolInputJson
      }))
        state.openTools.add(d.id);
      break;
    }
    case "session.tool.success":
    case "session.tool.failed": {
      const d = event.data;
      const call = state.tools.get(d.id);
      if (call && !state.done.has(`${d.id}:call`)) {
        const [toolName, toolInputJson] = shapeToolCall(call.name, call.input ?? {}, state.cwd);
        once(state, `${d.id}:call`, out, {
          eventType: "tool_call",
          role: "assistant",
          toolCallId: d.id,
          toolName,
          toolInputJson
        });
      }
      const text2 = contentText(d.content);
      const result = event.type === "session.tool.failed" ? [event.data.error.message, text2].filter(Boolean).join(`
`) : resultText(call?.name, call?.input, text2, d.metadata);
      once(state, `${d.id}:result`, out, {
        eventType: "tool_result",
        role: "user",
        toolResultForId: d.id,
        toolResultContent: result
      });
      state.openTools.delete(d.id);
      if (call?.name === "bash" || call?.name === "shell")
        out.scan = true;
      const path = call?.input?.file_path;
      if (event.type === "session.tool.success" && typeof path === "string")
        out.touched = [path];
      break;
    }
    case "session.shell.ended": {
      const { shell, output } = event.data;
      const toolCallId = `shell-${shell.id}`;
      once(state, `${toolCallId}:call`, out, {
        eventType: "tool_call",
        role: "assistant",
        toolCallId,
        toolName: "bash",
        toolInputJson: { command: shell.command }
      });
      const code = shell.exit ?? (shell.status === "exited" ? 0 : 1);
      const toolResultContent = `${code ? `Exit code ${code}
` : ""}${output.output}`;
      once(state, `${toolCallId}:result`, out, {
        eventType: "tool_result",
        role: "user",
        toolResultForId: toolCallId,
        toolResultContent
      });
      out.scan = true;
      break;
    }
    case "session.step.ended": {
      const d = event.data;
      if (d.files?.length)
        out.touched = d.files.map((f) => isAbsolute(f) ? f : resolve(state.cwd, f));
      const usage = usageOf(event.id, d.tokens, d.cost, state);
      if (usage && !state.done.has(event.id)) {
        state.done.add(event.id);
        out.usage = usage;
      }
      break;
    }
    case "session.step.failed": {
      const d = event.data;
      if (/interrupt|abort/i.test(`${d.error.type} ${d.error.message}`))
        break;
      out.errorMessage = d.error.message;
      once(state, `${d.assistantMessageID}:error`, out, {
        eventType: "message",
        role: "assistant",
        content: `**Error:** ${d.error.message}`
      });
      break;
    }
    case "session.compaction.started": {
      const boundary = once(state, `${event.id}:compaction`, out, systemFields({ kind: "context_compaction", trigger: event.data.reason }));
      if (boundary)
        state.boundaryEventId = boundary.eventId;
      break;
    }
    case "session.compaction.ended": {
      const payload = {
        kind: "context_compaction_summary",
        boundaryEventId: state.boundaryEventId,
        summary: event.data.text
      };
      once(state, `${event.id}:summary`, out, systemFields(payload));
      break;
    }
  }
  return out;
}
function once(state, key, out, fields) {
  if (state.done.has(key))
    return;
  state.done.add(key);
  const event = fitEvent({
    eventId: eventUuid(`${state.sessionId}:${key}`),
    codingAgentSessionId: state.sessionId,
    codingAgentEventId: key,
    ...fields
  });
  if (state.parentToolUseId)
    event.parentToolUseId = state.parentToolUseId;
  out.events.push(event);
  return event;
}
function contentText(content) {
  return (content ?? []).map((c) => c.type === "text" ? c.text : `[${c.name ?? "file"}]`).join(`
`);
}
function resultText(name, input, text2, meta) {
  const exit = meta?.exit ?? meta?.exitCode;
  if ((name === "bash" || name === "shell") && typeof exit === "number" && exit !== 0)
    return `Exit code ${exit}
${text2}`;
  if (name === "write" && meta?.exists === false && typeof input?.file_path === "string")
    return `File created successfully at: ${input.file_path}
${text2}`;
  if (isSubagentTool(name))
    return /^<subagent\b[^>]*>\n?([\s\S]*?)\n?<\/subagent>\s*$/.exec(text2)?.[1] ?? text2;
  return text2;
}
function isSubagentTool(name) {
  return name === "subagent" || name === "task";
}
function subagentSession(state, event) {
  if (event.type !== "session.tool.progress" && event.type !== "session.tool.success" && event.type !== "session.tool.failed")
    return;
  const { id, metadata } = event.data;
  const sessionId = metadata?.sessionID ?? metadata?.sessionId;
  if (typeof sessionId !== "string" || !isSubagentTool(state.tools.get(id)?.name))
    return;
  return { toolCallId: id, sessionId };
}
function usageOf(key, t, cost, state) {
  const [input, output, cacheRead, cacheWrite] = [
    int(t.input),
    int(t.output + t.reasoning),
    int(t.cache.read),
    int(t.cache.write)
  ];
  if (input + output + cacheRead + cacheWrite === 0)
    return;
  return {
    usageReportKey: key,
    contextWindowTokens: input + cacheRead + cacheWrite + output,
    totalCostUsd: cost || 0,
    modelUsage: {
      [state.model ?? "unknown"]: {
        input_tokens: input,
        output_tokens: output,
        cache_read_input_tokens: cacheRead,
        cache_creation_input_tokens: cacheWrite
      }
    }
  };
}
var KEYS = new Map([
  ["filePath", "file_path"],
  ["oldString", "old_string"],
  ["newString", "new_string"],
  ["replaceAll", "replace_all"]
]);
var NAMES = new Map([
  ["glob", "Glob"],
  ["grep", "Grep"],
  ["list", "LS"],
  ["webfetch", "WebFetch"],
  ["todowrite", "TodoWrite"],
  ["task", "Task"],
  ...HUMANLAYER_TOOLS.map((name) => [name, `mcp__humanlayer__${name}`])
]);
function shapeToolCall(name, args, cwd) {
  const input = {};
  for (const [key, value] of Object.entries(args))
    input[KEYS.get(key) ?? key] = value;
  if ((name === "read" || name === "write" || name === "edit") && input.file_path === undefined && typeof input.path === "string")
    input.file_path = input.path;
  if (typeof input.file_path === "string" && !isAbsolute(input.file_path))
    input.file_path = resolve(cwd, input.file_path);
  if ((name === "bash" || name === "shell") && typeof input.timeout === "number") {
    input.timeout_ms = input.timeout;
    delete input.timeout;
  }
  if (name === "grep" && input.include !== undefined) {
    input.glob = input.include;
    delete input.include;
  }
  if (name === "shell")
    return ["bash", input];
  if (isSubagentTool(name)) {
    if (input.subagent_type === undefined && input.agent !== undefined)
      input.subagent_type = input.agent;
    delete input.agent;
    return ["Task", input];
  }
  return [NAMES.get(name) ?? name, input];
}
function int(n) {
  return Math.max(0, Math.round(n) || 0);
}

// apps/riptide-opencode-plugin/src/mirror.ts
import { readdir as readdir4, rm as rm3 } from "fs/promises";
import { basename as basename4, join as join7 } from "path";

// packages/session-sdk-artifacts/src/frontmatter.ts
var UNSURE = Symbol("unsure");
var NON_PRINTABLE = /[\x00-\x08\x0B-\x1F\x7F-\x9F\u2028\u2029\uFFFE\uFFFF\p{Cs}]/u;
var OTHER_NUMBER = /^(?:[-+]?(?:0b[01_]+|0x[\da-fA-F_]+|0[0-7_]+|(?:0|[1-9][\d_]*)(?::[0-5]?\d)*(?:\.[\d_]*)?(?:[eE][-+]?\d+)?|\.(?:inf|Inf|INF))|\.[\d_]+(?:[eE][-+]?\d+)?|\.(?:nan|NaN|NAN))$/;
var DATE = /^(\d{4})-(\d\d)-(\d\d)$/;
var DATETIME = /^(\d{4})-(\d\d?)-(\d\d?)(?:[Tt]|[ \t]+)(\d\d?):(\d\d):(\d\d)(?:\.(\d*))?(?:[ \t]*(Z|([-+])(\d\d?)(?::(\d\d))?))?$/;
var ESCAPES = new Map([
  ["\\", "\\"],
  ['"', '"'],
  ["/", "/"],
  ["n", `
`],
  ["t", "\t"],
  ["r", "\r"]
]);
function frontmatter(content) {
  const text2 = content.replace(/^\uFEFF/, "");
  const open2 = /^---[ \t]*(?:ya?ml[ \t]*)?\r?\n/i.exec(text2);
  if (!open2)
    return {};
  const rest = text2.slice(open2[0].length);
  const close = rest.startsWith("---") ? 0 : rest.indexOf(`
---`);
  const data = new Map;
  let key;
  let list;
  for (const raw of (close < 0 ? rest : rest.slice(0, close)).split(`
`)) {
    const line = raw.replace(/\r$/, "");
    if (NON_PRINTABLE.test(line))
      return {};
    if (/^[ \t]*(?:#.*)?$/.test(line))
      continue;
    const item = /^( *)-(?:[ \t]+(.*))?$/.exec(line);
    if (item) {
      const indent = item[1]?.length ?? 0;
      const value2 = scalar(item[2] ?? "");
      if (key === undefined || list && list.indent !== indent || value2 === UNSURE || Array.isArray(value2))
        return {};
      if (!list) {
        list = { indent, items: [] };
        data.set(key, list.items);
      }
      list.items.push(value2);
      continue;
    }
    const pair = /^([A-Za-z_][\w.-]*):(?:[ \t]+(.*))?$/.exec(line);
    const name = pair?.[1];
    if (!pair || !name || data.has(name) || /^(?:true|false|null)$/i.test(name))
      return {};
    const valueText = (pair[2] ?? "").trim();
    const value = scalar(valueText);
    if (value === UNSURE)
      return {};
    data.set(name, value);
    key = valueText === "" || valueText.startsWith("#") ? name : undefined;
    list = undefined;
  }
  return Object.fromEntries(data);
}
function scalar(raw) {
  const s = raw.trim();
  if (s === "" || s.startsWith("#"))
    return null;
  if (s.startsWith('"')) {
    const m = /^"((?:[^"\\]|\\.)*)"(?:[ \t]+#.*)?$/.exec(s);
    return m ? unescapeDouble(m[1] ?? "") : UNSURE;
  }
  if (s.startsWith("'")) {
    const m = /^'((?:[^']|'')*)'(?:[ \t]+#.*)?$/.exec(s);
    return m ? (m[1] ?? "").replaceAll("''", "'") : UNSURE;
  }
  if (s.startsWith("["))
    return flowList(s);
  const plain = s.replace(/[ \t]+#.*$/, "");
  if (/^[-?:](?:[ \t]|$)|^[,\]{}&*!|>%@`]|:(?:[ \t]|$)/.test(plain))
    return UNSURE;
  return plainScalar(plain);
}
function unescapeDouble(body) {
  let sure = true;
  const out = body.replace(/\\(u[\da-fA-F]{4}|.)/g, (_, e) => {
    if (e.length === 5)
      return String.fromCharCode(Number.parseInt(e.slice(1), 16));
    const c = ESCAPES.get(e);
    if (c === undefined)
      sure = false;
    return c ?? "";
  });
  return sure ? out : UNSURE;
}
function flowList(s) {
  const inner = /^\[(.*)\](?:[ \t]+#.*)?$/.exec(s)?.[1];
  if (inner === undefined)
    return UNSURE;
  if (inner.trim() === "")
    return [];
  const items = [];
  const next = /[ \t]*("(?:[^"\\]|\\.)*"|'(?:[^']|'')*'|[^,"'[\]{}#]+)[ \t]*(,|$)/y;
  while (next.lastIndex < inner.length) {
    const m = next.exec(inner);
    const value = m ? scalar(m[1] ?? "") : UNSURE;
    if (!m || value === UNSURE || Array.isArray(value))
      return UNSURE;
    if (m[2] === "," && next.lastIndex === inner.length)
      return UNSURE;
    items.push(value);
  }
  return items;
}
function plainScalar(s) {
  if (/^(?:~|null|Null|NULL)$/.test(s))
    return null;
  if (/^(?:true|True|TRUE)$/.test(s))
    return true;
  if (/^(?:false|False|FALSE)$/.test(s))
    return false;
  if (/^-?(?:0|[1-9]\d*)(?:\.\d+)?$/.test(s))
    return Number(s);
  if (OTHER_NUMBER.test(s))
    return UNSURE;
  return timestamp(s) ?? s;
}
function timestamp(s) {
  const m = DATE.exec(s) ?? DATETIME.exec(s);
  if (!m)
    return;
  const n = (i) => Number(m[i] ?? 0);
  const ms = Number((m[7] ?? "").slice(0, 3).padEnd(3, "0"));
  let t = Date.UTC(n(1), n(2) - 1, n(3), n(4), n(5), n(6), ms);
  if (m[9])
    t -= (m[9] === "-" ? -1 : 1) * (n(10) * 60 + n(11)) * 60000;
  return new Date(t).toISOString();
}
// packages/session-sdk-artifacts/src/rules.ts
var TEXT_EXTENSIONS = /\.(md|mdx|txt|json|jsonl)$/i;
var MIME_TYPES = new Map([
  ["png", "image/png"],
  ["jpg", "image/jpeg"],
  ["jpeg", "image/jpeg"],
  ["gif", "image/gif"],
  ["webp", "image/webp"],
  ["svg", "image/svg+xml"],
  ["pdf", "application/pdf"],
  ["html", "text/html"],
  ["htm", "text/html"],
  ["json", "application/json"],
  ["css", "text/css"],
  ["js", "text/javascript"],
  ["mjs", "text/javascript"]
]);
var MAX_ARTIFACT_TEXT_BYTES = 10 * 1024 * 1024;
function isTextFile(fileName) {
  return TEXT_EXTENSIONS.test(fileName);
}
function getMimeType(fileName) {
  const ext = fileName.split(".").pop()?.toLowerCase() ?? "";
  return MIME_TYPES.get(ext) ?? "application/octet-stream";
}
var ARTIFACT_TRASH_DIRECTORY = ".trash";
function isReservedArtifactTrashSubpath(subpath) {
  return subpath.replaceAll("\\", "/").split("/")[0] === ARTIFACT_TRASH_DIRECTORY;
}
function decoded(name) {
  try {
    return decodeURIComponent(name);
  } catch {
    return name;
  }
}
function isRefusedSubpath(name) {
  const risky = (s) => s.includes("\\") || s.includes("..") || s.includes("\x00") || s.startsWith("/") || s.endsWith("/") || s.split("/").some((part) => part === "" || part === ".");
  const d = decoded(name);
  return name.length < 1 || name.length > 1024 || risky(name) || risky(d) || d.split("/").length !== name.split("/").length;
}
function isSyncableSubpath(subpath) {
  return !isReservedArtifactTrashSubpath(subpath) && !isRefusedSubpath(subpath);
}
// packages/session-sdk-artifacts/src/sync.ts
import { createHash as createHash4 } from "crypto";
import { appendFile as appendFile3, lstat, mkdir as mkdir4, readdir as readdir3, readFile as readFile3, readlink as readlink2, realpath as realpath2, symlink, unlink as unlink3 } from "fs/promises";
import { basename as basename3, dirname as dirname4, isAbsolute as isAbsolute2, join as join5, relative, resolve as resolve2, sep } from "path";
function ohash(s) {
  const wrapped = `'${s.replaceAll("\x00", "")}'`;
  return createHash4("sha256").update(wrapped, "utf8").digest("base64url");
}
var HINT_SECTION = "artifacts_directory_information";
var TRIES = 5;
function safeSlug(slug) {
  return slug && /^[a-z0-9][\w.-]*$/i.test(slug) ? slug : undefined;
}
function taskFolder(cwd, slug) {
  return join5(cwd, ".humanlayer", "tasks", slug);
}
function artifactsHint(cwd, taskSlug, sessionUrl) {
  const lines = [];
  const slug = safeSlug(taskSlug);
  if (slug) {
    const folder = taskFolder(cwd, slug);
    lines.push(`Your task artifacts directory is: ${folder}`, "", "Files you write there sync to the user's HumanLayer task, where the user can read them.", "If the user asks you to continue work on a task, design discussion or plan and doesn't mention a file, check here first.", "This directory may be a symlink: list it with `ls -La`, and read and write files through this path, never through the link's target.", "Use the write and edit tools for artifacts.", "", "To show the user an HTML page or an image inline, put its path in a fenced block:", "```task-artifact", `${folder}/page.html`, "```");
  }
  if (sessionUrl) {
    if (lines.length > 0)
      lines.push("");
    lines.push(`The user can follow this session in HumanLayer at ${sessionUrl}`, "If they ask to see or open the session, open that URL (for example with `open` on macOS or `xdg-open` on Linux).");
  }
  return lines.length > 0 ? lines.join(`
`) : undefined;
}
async function openFolder(host, folder) {
  const { store, cwd, log: log2 } = host;
  await mkdir4(store, { recursive: true });
  await mkdir4(dirname4(folder), { recursive: true });
  const st = await lstat(folder).catch(() => {
    return;
  });
  if (st?.isSymbolicLink()) {
    if (basename3(await realpath2(folder).catch(() => "")) !== basename3(store)) {
      log2(`relinking ${folder} to ${store}`);
      const mine = basename3(await readlink2(folder)) !== basename3(store);
      await unlink3(folder);
      await linkFolder(host, folder, mine);
    }
  } else if (!st) {
    await linkFolder(host, folder, true);
  } else if (!st.isDirectory()) {
    log2(`${folder} is not a folder; task files will not sync`);
    return;
  }
  await excludeFromGit(cwd).catch((err) => log2(`info/exclude: ${errorMessage(err)}`));
  return realpath2(folder);
}
async function linkFolder(host, folder, mine) {
  if (mine)
    await host.recordLink?.(folder, basename3(host.store));
  try {
    await symlink(host.store, folder, "dir");
  } catch (err) {
    if (err.code !== "EEXIST")
      throw err;
  }
}
async function excludeFromGit(cwd) {
  if (await git(cwd, "check-ignore", "-q", "--no-index", "--", ".humanlayer/tasks") !== undefined)
    return;
  const [prefix, path] = await Promise.all([
    git(cwd, "rev-parse", "--show-prefix"),
    git(cwd, "rev-parse", "--git-path", "info/exclude")
  ]);
  if (prefix === undefined || !path)
    return;
  const file = resolve2(cwd, path);
  await mkdir4(dirname4(file), { recursive: true });
  const text2 = await readFile3(file, "utf8").catch(() => "");
  const line = `/${prefix.replace(/[*?[\\]/g, "\\$&")}.humanlayer/tasks/`;
  if (text2.split(/\r?\n/).includes(line))
    return;
  await appendFile3(file, `${text2 && !text2.endsWith(`
`) ? `
` : ""}${line}
`);
}
async function subpathOf(path, roots) {
  const under = (p) => {
    for (const root of roots) {
      const rel = relative(root, p);
      if (rel && !rel.startsWith("..") && !isAbsolute2(rel))
        return rel.split(sep).join("/");
    }
    return;
  };
  const real = await realpath2(path).catch(() => {
    return;
  });
  return under(resolve2(path)) ?? (real ? under(real) : undefined);
}
async function listFolder(folder) {
  const entries = await readdir3(folder, { recursive: true, withFileTypes: true });
  return entries.filter((e) => e.isFile()).map((e) => relative(folder, join5(e.parentPath, e.name)).split(sep).join("/")).filter(isSyncableSubpath);
}

class ArtifactSync {
  host;
  folder;
  outbox;
  ready;
  queued = new Set;
  refused = new Map;
  seq = 0;
  closed = false;
  constructor(host, folder) {
    this.host = host;
    const log2 = host.log;
    this.folder = folder;
    this.outbox = new Outbox({
      send: async (item) => {
        const changed = await this.send(item);
        this.queued.delete(item.name);
        if (changed)
          this.push(item.name);
      },
      classify: (err, plane) => {
        const action = classifyRpcError(err, plane);
        return isRetry(action) ? { kind: "retry-limited", maxAttempts: TRIES - 1 } : action;
      },
      onStopBinding: (reason) => host.warn(`artifacts:${reason}`, `HumanLayer: task files stopped syncing: ${reason}`),
      onSkip: (item, err) => {
        this.queued.delete(item.name);
        if (!isRetry(classifyRpcError(err, "daemon")) && item.read)
          this.refused.set(item.name, item.read);
        log2(`task file ${item.name} not synced: ${errorMessage(err)}`);
      },
      onDrop: (item) => this.queued.delete(item.name),
      log: log2,
      ...host.backoff
    });
    this.ready = openFolder(host, folder).then((real) => real ? [folder, real] : undefined, (err) => {
      log2(`task folder ${folder}: ${errorMessage(err)}`);
      return;
    });
    this.touched({});
  }
  touched(t) {
    if (this.closed || this.outbox.isStopped)
      return;
    const run = t.path ? this.syncPath(t.path) : this.scan();
    run.catch((err) => this.host.log(`task files: ${errorMessage(err)}`));
  }
  async flush(ms) {
    const end = Date.now() + ms;
    const run = this.scan().then(() => this.outbox.drain(Math.max(0, end - Date.now())));
    await Promise.race([run.catch(() => {
      return;
    }), sleepUnref(ms)]);
  }
  close() {
    this.closed = true;
    this.outbox.close();
  }
  async syncPath(path) {
    const roots = await this.ready;
    const name = roots && await subpathOf(path, roots);
    if (!name)
      return;
    if (isSyncableSubpath(name))
      this.push(name);
    else
      this.host.log(`task file ${name} skipped: .trash, or a name the server refuses`);
  }
  async scan() {
    if (!await this.ready || this.closed)
      return;
    const names = await listFolder(this.folder).catch((err) => {
      this.host.log(`task folder scan: ${errorMessage(err)}`);
      return [];
    });
    const stats = await Promise.all(names.map((name) => lstat(this.pathOf(name)).catch(() => {
      return;
    })));
    const ledger = this.host.ledger();
    names.forEach((name, i) => {
      const st = stats[i];
      const now = st && `${st.mtimeMs}:${st.size}`;
      if (now && ledger[name]?.mtimeSize !== now && this.refused.get(name) !== now)
        this.push(name);
    });
  }
  push(name) {
    if (this.closed || this.queued.has(name))
      return;
    this.queued.add(name);
    this.outbox.push({ id: String(++this.seq), plane: "daemon", sizeBytes: name.length, name });
  }
  pathOf(name) {
    return join5(this.folder, ...name.split("/"));
  }
  async send(item) {
    const path = this.pathOf(item.name);
    const st = await lstat(path).catch(() => {
      return;
    });
    if (!st?.isFile())
      return false;
    const read = `${st.mtimeMs}:${st.size}`;
    item.read = read;
    const bytes = await readFile3(path);
    const text2 = isTextFile(item.name);
    const hash = text2 ? ohash(bytes.toString("utf8")) : sha256Hex(bytes);
    const ledger = this.host.ledger();
    const last = ledger[item.name];
    if (hash !== last?.hash) {
      if (text2 ? bytes.length > MAX_ARTIFACT_TEXT_BYTES : bytes.length === 0) {
        this.refused.set(item.name, read);
        this.host.log(`task file ${item.name} not synced: ${text2 ? "over 10 MiB" : "empty"}`);
        return false;
      }
      if (text2)
        await this.upsert(item.name, path, bytes.toString("utf8"), last ? "Edit" : "Write");
      else
        await this.upload(item.name, bytes, hash);
      this.host.note(item.name);
    }
    ledger[item.name] = { hash, mtimeSize: read };
    this.host.save();
    const after = await lstat(path).catch(() => {
      return;
    });
    return after !== undefined && `${after.mtimeMs}:${after.size}` !== read;
  }
  async upsert(name, path, content, operationType) {
    await this.host.daemonCall(this.host.channel, "artifacts/upsert", {
      taskId: this.host.taskId,
      fileName: name,
      content,
      sessionId: this.host.sessionId,
      operationType,
      operationContents: { path, file_path: path },
      frontmatter: frontmatter(content)
    }, this.host.signal);
  }
  async upload(name, bytes, hash) {
    const contentType = getMimeType(name);
    const { uploadUrl } = await this.host.daemonCall(this.host.channel, "artifacts/createUpload", {
      taskId: this.host.taskId,
      fileName: name,
      contentType,
      contentHash: hash,
      fileSizeBytes: bytes.length
    }, this.host.signal);
    const res = await fetch(uploadUrl, {
      method: "PUT",
      headers: new Headers({ "content-type": contentType }),
      body: bytes,
      signal: timeout(30000 + Math.ceil(bytes.length / 100), this.host.signal)
    });
    await res.arrayBuffer().catch(() => {
      return;
    });
    if (!res.ok)
      throw new Error(`upload of ${name}: HTTP ${res.status}`);
  }
}
// packages/session-sdk-diffs/src/git-output.ts
function parseNameStatus(out) {
  const tokens = out.split("\x00");
  const changes = [];
  for (let i = 0;i < tokens.length; ) {
    const status = tokens[i] ?? "";
    const pair = status.startsWith("R") || status.startsWith("C");
    const path = tokens[i + (pair ? 2 : 1)];
    if (!status || !path)
      break;
    const prevPath = tokens[i + 1];
    if (status.startsWith("R") && prevPath)
      changes.push({ changeType: "renamed", path, prevPath });
    else if (status.startsWith("A") || status.startsWith("C"))
      changes.push({ changeType: "added", path });
    else
      changes.push({ changeType: status.startsWith("D") ? "deleted" : "modified", path });
    i += pair ? 3 : 2;
  }
  return changes;
}
function parseNumstat(out) {
  const stats = new Map;
  const tokens = out.split("\x00");
  for (let i = 0;i < tokens.length; i++) {
    const [added, deleted, ...rest] = (tokens[i] ?? "").split("\t");
    if (added === undefined || deleted === undefined || rest.length === 0)
      continue;
    let path = rest.join("\t");
    if (path === "") {
      path = tokens[i + 2] ?? "";
      i += 2;
    }
    const binary = added === "-" || deleted === "-";
    stats.set(path, { additions: binary ? 0 : Number(added), deletions: binary ? 0 : Number(deleted), binary });
  }
  return stats;
}
// packages/session-sdk-diffs/src/paths.ts
function isEnvironmentPath(filePath) {
  const segments = filePath.split(/[\\/]/);
  const filename = segments.at(-1);
  return segments.some((segment) => segment.startsWith(".env")) || filename?.endsWith(".env") === true;
}
function isPrivateDiffPath(filePath) {
  return isEnvironmentPath(filePath) || filePath.split(/[\\/]/).slice(0, -1).includes(".humanlayer");
}
// packages/streams/src/config.ts
var MAX_TASK_DIFF_PATCH_BYTES = 8 * 1024 * 1024;

// packages/session-sdk-diffs/src/rows.ts
function diffFileRowId(taskId, repoId, path) {
  return `${taskId}:${repoId}:${path}`;
}
function patchFits(patch, maxBytes = MAX_TASK_DIFF_PATCH_BYTES) {
  return Buffer.byteLength(JSON.stringify(patch)) <= maxBytes;
}
// packages/session-sdk-diffs/src/stream-messages.ts
function diffStreamMessage(type, key, operation, timestamp2, from, value) {
  const message = {
    type,
    key,
    headers: { operation, timestamp: timestamp2, from }
  };
  if (value !== undefined)
    message.value = value;
  return JSON.stringify(message);
}
function messageBatches(messages, maxBytes) {
  const bodies = [];
  let batch = [];
  let bytes = 1;
  for (const msg of messages) {
    const size = Buffer.byteLength(msg) + 1;
    if (batch.length > 0 && bytes + size > maxBytes) {
      bodies.push(`[${batch.join(",")}]`);
      batch = [];
      bytes = 1;
    }
    batch.push(msg);
    bytes += size;
  }
  if (batch.length > 0)
    bodies.push(`[${batch.join(",")}]`);
  return bodies;
}
// packages/session-sdk-diffs/src/sync.ts
import { execFile as execFile2 } from "child_process";
import { cp, mkdir as mkdir5, mkdtemp, rm as rm2 } from "fs/promises";
import { tmpdir } from "os";
import { delimiter, join as join6, resolve as resolve3 } from "path";
class DiffTargetError extends Error {
  constructor(message) {
    super(message);
    this.name = "DiffTargetError";
  }
}

class GitError extends Error {
  code;
  constructor(message, code) {
    super(message);
    this.name = "GitError";
    this.code = code ?? undefined;
  }
}
var MAX_POST_BYTES = 1e7;
var LIST_MAX_BUFFER = 256 * 1024 * 1024;
var GIT_TIMEOUT_MS = 120000;
var REQUEST_TIMEOUT_MS = 15000;
var MAX_ATTEMPTS = 4;
var PATCH_CONCURRENCY = 4;
var SHA = /^[0-9a-f]{40}([0-9a-f]{24})?$/;
var ENV_GLOBS = ["**/.env*", "**/.env*/**", "**/*.env"];
var WRITE = ["-c", "core.splitIndex=false"];
var FROM = "pi-humanlayer";
var DIFF = ["diff", "--cached", "-M", "--no-color", "--no-ext-diff", "--no-textconv"];
function git2(cwd, args, opts = {}) {
  const { env, signal, maxBuffer = LIST_MAX_BUFFER } = opts;
  return new Promise((done, fail) => {
    execFile2("git", args, { cwd, env, signal, maxBuffer, timeout: GIT_TIMEOUT_MS, encoding: "buffer" }, (err, stdout, stderr) => {
      if (!err)
        return done(stdout);
      const name = args.find((arg, i) => !arg.startsWith("-") && args[i - 1] !== "-c");
      const detail = stderr.toString("utf8").trim().split(`
`).at(-1) || err.message;
      fail(new GitError(`git ${name}: ${detail}`, err.code));
    });
  });
}
async function isCommit(cwd, sha, signal) {
  if (!SHA.test(sha))
    return false;
  try {
    await git2(cwd, ["rev-parse", "--verify", "-q", `${sha}^{commit}`], { signal });
    return true;
  } catch (err) {
    if (signal?.aborted)
      throw err;
    return false;
  }
}
var NO_STATS = { additions: 0, deletions: 0, binary: false };
async function mapLimit(items, limit, fn) {
  const out = [];
  const queue = items.entries();
  const worker = async () => {
    for (const [i, item] of queue)
      out[i] = await fn(item);
  };
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
  return out;
}
function compact(row) {
  return Object.fromEntries(Object.entries(row).filter(([, value]) => value !== undefined));
}
async function buildDiff(target, opts = {}) {
  const { gitRoot, baseSha, taskId, repoId, sessionId } = target;
  const { signal, maxPatchBytes = MAX_TASK_DIFF_PATCH_BYTES } = opts;
  if (!repoId || repoId.includes(":"))
    throw new DiffTargetError(`repo id ${JSON.stringify(repoId)} is empty or has a ":"`);
  if (!await isCommit(gitRoot, baseSha, signal)) {
    throw new DiffTargetError(`diff base ${JSON.stringify(baseSha)} is not a commit in ${gitRoot}`);
  }
  const paths = await git2(gitRoot, ["rev-parse", "--git-path", "index", "--git-path", "objects"], { signal });
  const [indexFile = "", objectsDir = ""] = paths.toString("utf8").trim().split(`
`).map((line) => resolve3(gitRoot, line));
  const tmp = await mkdtemp(join6(tmpdir(), "pi-hl-diff-"));
  try {
    const env = {
      ...process.env,
      GIT_INDEX_FILE: join6(tmp, "index"),
      GIT_OBJECT_DIRECTORY: join6(tmp, "objects"),
      GIT_ALTERNATE_OBJECT_DIRECTORIES: objectsDir.includes(delimiter) ? JSON.stringify(objectsDir) : objectsDir,
      GIT_TERMINAL_PROMPT: "0"
    };
    await mkdir5(env.GIT_OBJECT_DIRECTORY);
    const run = (args, maxBuffer) => git2(gitRoot, args, { env, signal, maxBuffer });
    const withSparse = (args) => run(args).catch((err) => {
      if (!(err instanceof GitError) || err.code !== 129)
        throw err;
      return run(args.filter((arg) => arg !== "--sparse"));
    });
    const copied = await cp(indexFile, env.GIT_INDEX_FILE, { preserveTimestamps: true }).then(() => true, (err) => {
      if (err.code !== "ENOENT")
        throw err;
      return false;
    });
    if (!copied)
      await run([...WRITE, "read-tree", baseSha]);
    const excludes = [...ENV_GLOBS, "**/.humanlayer/**"].map((glob) => `:(exclude,glob)${glob}`);
    await withSparse([...WRITE, "add", "--all", "--sparse", "--", ".", ...excludes]);
    const envSpecs = ENV_GLOBS.map((glob) => `:(glob)${glob}`);
    await withSparse([
      ...WRITE,
      "rm",
      "-r",
      "-f",
      "-q",
      "--cached",
      "--ignore-unmatch",
      "--sparse",
      "--",
      ...envSpecs
    ]);
    const [nameStatus, numstat] = await Promise.all([
      run([...DIFF, "-z", "--name-status", baseSha]),
      run([...DIFF, "-z", "--numstat", baseSha])
    ]);
    const stats = parseNumstat(numstat.toString("utf8"));
    const changes = parseNameStatus(nameStatus.toString("utf8")).filter((c) => !isPrivateDiffPath(c.path) && (c.prevPath === undefined || !isPrivateDiffPath(c.prevPath)));
    const updatedAt = new Date().toISOString();
    const rows = await mapLimit(changes, PATCH_CONCURRENCY, async (change) => {
      const specs = change.prevPath === undefined ? [change.path] : [change.prevPath, change.path];
      const args = [
        "--literal-pathspecs",
        ...DIFF,
        "--binary",
        "--full-index",
        "--src-prefix=a/",
        "--dst-prefix=b/"
      ];
      const patch = await run([...args, baseSha, "--", ...specs], maxPatchBytes).then((out) => out.toString("utf8"), (err) => {
        if (err instanceof GitError && err.code === "ERR_CHILD_PROCESS_STDIO_MAXBUFFER")
          return;
        throw err;
      });
      const byteLength = patch === undefined ? undefined : Buffer.byteLength(patch);
      let patchRow;
      if (patch !== undefined && patchFits(patch, maxPatchBytes)) {
        const patchHash = sha256Hex(patch);
        patchRow = {
          id: patchHash,
          taskId,
          repoId,
          sessionId,
          patchHash,
          patch,
          byteLength: Buffer.byteLength(patch),
          updatedAt
        };
      }
      const { additions, deletions, binary } = stats.get(change.path) ?? NO_STATS;
      const file = compact({
        id: `${taskId}:${repoId}:${change.path}`,
        taskId,
        repoId,
        repoDisplayName: repoId,
        sessionId,
        path: change.path,
        prevPath: change.prevPath,
        changeType: change.changeType,
        additions,
        deletions,
        binary,
        generated: false,
        patchHash: patchRow?.patchHash,
        patchByteLength: byteLength,
        patchOmittedReason: patchRow ? undefined : "too_large",
        updatedAt
      });
      return { file, patchRow };
    });
    return { files: rows.map((r) => r.file), patches: rows.flatMap((r) => r.patchRow ? [r.patchRow] : []) };
  } finally {
    await rm2(tmp, { recursive: true, force: true }).catch(() => {});
  }
}
function errorKind(err) {
  if (err instanceof LoginRequiredError || err instanceof RpcError && err.status === 401)
    return "login-required";
  if (err instanceof DiffTargetError || err instanceof RpcError && !isTransient(err))
    return "stopped";
  return "transient";
}

class DiffSync {
  target;
  opts;
  abort = new AbortController;
  ready = new Set;
  empty = new Set;
  published;
  timer;
  current = Promise.resolve();
  running = false;
  pending = false;
  closed = false;
  stopped = false;
  token;
  reminted = false;
  reported;
  constructor(target, opts) {
    this.target = target;
    this.opts = opts;
    this.published = new Map(Object.entries(target.published));
  }
  touched() {
    if (this.closed || this.stopped || this.timer)
      return;
    this.timer = setTimeout(() => {
      this.timer = undefined;
      this.kick();
    }, this.opts.debounceMs ?? 1500);
    this.timer.unref();
  }
  async flush(ms) {
    this.clearTimer();
    if (this.closed || this.stopped)
      return;
    await Promise.race([this.kick(), sleepUnref(ms)]);
  }
  close() {
    this.closed = true;
    this.clearTimer();
    this.abort.abort();
  }
  clearTimer() {
    clearTimeout(this.timer);
    this.timer = undefined;
  }
  kick() {
    this.pending = true;
    if (!this.running)
      this.current = this.loop().catch((err) => this.log(`diffs: ${errorMessage(err)}`));
    return this.current;
  }
  async loop() {
    this.running = true;
    try {
      while (this.pending && !this.closed && !this.stopped) {
        this.pending = false;
        await this.run();
      }
    } finally {
      this.running = false;
    }
  }
  async run() {
    this.token = undefined;
    this.reminted = false;
    try {
      const built = await buildDiff(this.target, {
        maxPatchBytes: this.opts.maxPatchBytes,
        signal: this.abort.signal
      });
      await this.ensure("diff-patches");
      await this.ensure("diff-files");
      const todo = this.plan(built);
      if (todo.patches.length + todo.files.length + todo.deletes.length > 0)
        await this.publish(todo);
      this.reported = undefined;
    } catch (err) {
      if (this.closed)
        return;
      const kind = errorKind(err);
      if (kind === "stopped") {
        this.stopped = true;
        this.clearTimer();
      }
      this.report(kind, errorMessage(err));
    }
  }
  plan(built) {
    const freshPatches = this.empty.has("diff-patches");
    const freshFiles = this.empty.has("diff-files");
    const patchRows = new Map(built.patches.map((row) => [row.patchHash, row]));
    const patches = new Map;
    const files2 = [];
    const next = new Map;
    for (const file of built.files) {
      const before = this.published.get(file.path);
      const rowHash = sha256Hex(JSON.stringify({ ...file, updatedAt: undefined }));
      next.set(file.path, file.patchHash === undefined ? { rowHash } : { patchHash: file.patchHash, rowHash });
      const patch = file.patchHash === undefined ? undefined : patchRows.get(file.patchHash);
      if (patch && (freshPatches || before?.patchHash !== patch.patchHash))
        patches.set(patch.patchHash, patch);
      if (freshFiles || before?.rowHash !== rowHash)
        files2.push(file);
    }
    const deletes = freshFiles ? [] : [...this.published.keys()].filter((path) => !next.has(path));
    return { patches: [...patches.values()], files: files2, deletes, next };
  }
  async ensure(stream) {
    if (this.ready.has(stream))
      return;
    if (await this.send("HEAD", stream) === 404 && await this.send("PUT", stream) === 201)
      this.empty.add(stream);
    this.ready.add(stream);
  }
  async publish(todo) {
    const { taskId, repoId } = this.target;
    const at = new Date().toISOString();
    const max = this.opts.maxPostBytes ?? MAX_POST_BYTES;
    const patches = todo.patches.map((row) => diffStreamMessage("task-diff-patch", row.id, "upsert", at, FROM, row));
    const files2 = [
      ...todo.files.map((row) => diffStreamMessage("task-diff-file", row.id, "upsert", at, FROM, row)),
      ...todo.deletes.map((path) => diffStreamMessage("task-diff-file", diffFileRowId(taskId, repoId, path), "delete", at, FROM))
    ];
    for (const body of messageBatches(patches, max))
      await this.send("POST", "diff-patches", body);
    for (const body of messageBatches(files2, max))
      await this.send("POST", "diff-files", body);
    this.published = todo.next;
    this.empty.clear();
    this.log(`diffs: published ${todo.files.length} files, ${todo.patches.length} patches, ${todo.deletes.length} deletes`);
    try {
      this.opts.onPublished(Object.fromEntries(todo.next));
    } catch (err) {
      this.log(`diffs: onPublished: ${errorMessage(err)}`);
    }
  }
  async send(method, stream, body) {
    let backoffMs2 = this.opts.retryBaseMs ?? 500;
    for (let attempt = 1;; attempt++) {
      try {
        return await this.request(method, stream, body);
      } catch (err) {
        if (this.closed)
          throw err;
        const status = err instanceof RpcError ? err.status : 0;
        if ((status === 401 || status === 403) && !this.reminted) {
          this.reminted = true;
          this.token = await this.opts.remintDaemonToken().catch((e) => {
            throw new LoginRequiredError(`daemon token re-mint failed: ${errorMessage(e)}`);
          });
          continue;
        }
        if (!isTransient(err) || attempt >= MAX_ATTEMPTS)
          throw err;
        this.log(`diffs: ${errorMessage(err)}; retry in ${backoffMs2} ms`);
        await sleepUnref(backoffMs2);
        backoffMs2 *= 2;
      }
    }
  }
  async request(method, stream, body) {
    this.token ??= await this.opts.daemonToken();
    const { syncUrl, orgId, taskId } = this.target;
    const url = `${syncUrl.replace(/\/+$/, "")}/v2/streams/organizations/${encodeURIComponent(orgId)}/tasks/${encodeURIComponent(taskId)}/${stream}`;
    const headers = new Headers({ "x-daemon-authorization": this.token });
    if (method !== "HEAD")
      headers.set("content-type", "application/json");
    const res = await fetch(url, {
      method,
      headers,
      body,
      signal: AbortSignal.any([this.abort.signal, AbortSignal.timeout(REQUEST_TIMEOUT_MS)])
    });
    const text2 = await res.text();
    if (res.ok || method === "HEAD" && res.status === 404 || method === "PUT" && res.status === 409)
      return res.status;
    throw new RpcError(`${method} ${stream}: HTTP ${res.status}${text2 ? ` ${text2.slice(0, 200)}` : ""}`, res.status, undefined);
  }
  report(kind, message) {
    this.log(`diffs: ${kind}: ${message}`);
    if (this.reported === kind)
      return;
    this.reported = kind;
    try {
      this.opts.onError?.(kind, message);
    } catch {}
  }
  log(line) {
    try {
      this.opts.log?.(line);
    } catch {}
  }
}
// apps/riptide-opencode-plugin/src/mirror.ts
var SETTLE_MS = 2000;
function bindingPath(channel, sessionId) {
  return join7(bindingsDir(channel), `${sessionId}.json`);
}
async function loadBinding(channel, sessionId, who) {
  const b = await readJsonFile(bindingPath(channel, sessionId));
  if (b?.version !== 1 || !b.cloudSessionId || b.channel !== channel || b.hostId !== await client2.hostId(channel))
    return;
  if (who.source === "device" && (b.userId !== who.userId || b.orgId !== who.orgId))
    return;
  b.taskMode ??= "ensure";
  return b;
}
function alive(pid) {
  if (pid === process.pid)
    return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    return err.code === "EPERM";
  }
}
function sessionTitle(prompt) {
  const line = prompt.split(`
`).find((l) => l.trim()) ?? "";
  return Array.from(line.trim()).slice(0, 80).join("").trim() || "opencode session";
}

class Mirror {
  host;
  binding;
  state;
  outbox;
  abort = new AbortController;
  seq = 0;
  model;
  contextWindow;
  lastStatus;
  halted;
  closing;
  artifacts;
  diff;
  synced;
  saveTimer;
  approvals = new Map;
  children = new Map;
  early = new Map;
  settling;
  constructor(host, binding, model) {
    this.host = host;
    this.binding = binding;
    this.model = model;
    this.state = createMapperState(binding.opencodeSessionId, binding.cwd);
    this.state.model = model?.id;
    this.outbox = this.newOutbox();
  }
  get id() {
    return this.binding.opencodeSessionId;
  }
  static async find(channel, cloudSessionId, cwd) {
    const files2 = await readdir4(bindingsDir(channel)).catch(() => []);
    for (const file of files2) {
      if (!file.endsWith(".json"))
        continue;
      const b = await readJsonFile(join7(bindingsDir(channel), file));
      if (b?.cloudSessionId === cloudSessionId && b.cwd === cwd)
        return b.opencodeSessionId;
    }
    return;
  }
  static async lostRuns(channel, cwd) {
    const files2 = await readdir4(bindingsDir(channel)).catch(() => []);
    const lost = [];
    for (const file of files2) {
      if (!file.endsWith(".json"))
        continue;
      const b = await readJsonFile(join7(bindingsDir(channel), file));
      if (b?.cwd === cwd && b.running && !alive(b.running.pid))
        lost.push(b.opencodeSessionId);
    }
    return lost;
  }
  static async resume(host, sessionId, who, model) {
    const saved = await loadBinding(who.channel, sessionId, who);
    if (!saved)
      return;
    log(`resuming ${saved.cloudSessionId} for ${sessionId}`);
    const m = new Mirror(host, saved, model);
    await m.modelChanged(model);
    m.startLanes();
    if (saved.running && !alive(saved.running.pid))
      m.endLostRun(saved.running.approvalIds);
    return m;
  }
  endLostRun(approvalIds) {
    log(`${this.id}: the last run ended with opencode; reporting it interrupted`);
    for (const approvalId of approvalIds) {
      const requestID = `lost-${approvalId}`;
      this.approvals.set(requestID, { approvalId, sessionID: this.id, answeredBy: "opencode" });
      this.push({ kind: "resolve", requestID, decision: "deny" }, "api", true);
    }
    this.pushStatus("interrupted");
  }
  static async bind(host, o) {
    const channel = o.who.channel;
    const [hostId, git3] = await Promise.all([client2.hostId(channel), gitInfo(o.cwd)]);
    const binding = {
      version: 1,
      channel,
      opencodeSessionId: o.sessionId,
      cwd: o.cwd,
      createdAt: new Date().toISOString(),
      hostId,
      taskMode: o.pick.taskMode
    };
    const slug = o.pick.taskMode === "ensure" ? o.pick.slug : o.pick.taskSlug;
    if (slug)
      binding.taskSlug = slug;
    else if (o.pick.taskMode === "use")
      binding.taskId = o.pick.taskIdOrSlug;
    if (git3)
      binding.git = git3;
    const m = new Mirror(host, binding, o.model);
    if (o.model)
      m.contextWindow = await host.contextLimit(o.model);
    const facts = {
      codingAgent: "opencode",
      hostId,
      title: sessionTitle(o.prompt),
      prompt: promptText(o.prompt, 0),
      cwd: o.cwd,
      model: o.model && { provider: o.model.providerID, id: o.model.id },
      git: git3
    };
    const fallback = o.pick.taskMode === "use" && o.pick.auto ? facts : undefined;
    m.push({ kind: "prepare", body: prepareBody({ ...facts, pick: o.pick }), fallback }, "api", true);
    m.pushStatus("running", { ...m.modelFields(), codingAgentSessionId: o.sessionId });
    m.report(git3);
    m.changed();
    return m;
  }
  prompt(messageID, content) {
    this.queue(mapPrompt(messageID, content, this.state));
  }
  hint() {
    if (!this.active)
      return;
    const text2 = artifactsHint(this.binding.cwd, this.binding.taskSlug, this.binding.sessionUrl);
    return text2 && `<${HINT_SECTION}>
${text2}
</${HINT_SECTION}>`;
  }
  toolTarget() {
    if (this.halted)
      return { reason: `HumanLayer mirroring stopped: ${this.halted}` };
    if (this.binding.off)
      return { reason: "HumanLayer mirroring is off for this session. Run /humanlayer on." };
    const taskId = this.binding.taskId;
    if (!taskId || !this.binding.cloudSessionId)
      return { reason: "HumanLayer is still linking this session to its task. Try again shortly.", pending: true };
    return { channel: this.binding.channel, taskId };
  }
  get cloudSessionId() {
    return this.binding.cloudSessionId;
  }
  get channel() {
    return this.binding.channel;
  }
  get loginRequired() {
    return this.outbox.isPaused;
  }
  info() {
    const b = this.binding;
    const info = { state: this.mirrorState(), queued: this.outbox.length };
    const task = b.taskSlug ?? b.taskId?.slice(0, 8);
    if (task)
      info.task = task;
    if (b.sessionUrl && b.cloudSessionId)
      info.url = b.sessionUrl;
    if (this.synced)
      info.synced = this.synced;
    if (this.halted)
      info.problem = this.halted;
    return info;
  }
  mirrorState() {
    if (this.halted)
      return "stopped";
    if (this.binding.off)
      return "off";
    return this.binding.cloudSessionId ? "mirroring" : "binding";
  }
  get active() {
    return !this.halted && !this.binding.off && !this.closing;
  }
  event(event) {
    switch (event.type) {
      case "session.execution.started":
        this.cancelSettle();
        if (this.lastStatus !== "running")
          this.pushStatus("running");
        return;
      case "session.execution.succeeded":
        return this.settle("ready_for_input");
      case "session.execution.failed":
        return this.settle("failed", { errorMessage: event.data.error.message });
      case "session.execution.interrupted":
        return this.settle("interrupted");
      case "permission.asked":
        return this.asked(event.data);
      case "permission.replied":
        return this.replied(event.data.requestID, event.data.reply !== "reject");
      case "session.renamed":
        return this.renamed(event.data.title);
      case "session.model.selected":
        this.modelChanged(event.data.model).catch((err) => log(`model: ${errorMessage(err)}`));
        return;
      case "session.compaction.started":
        this.pushUpdate({ isCompacting: true });
        break;
      case "session.compaction.ended":
      case "session.compaction.failed":
        this.pushUpdate({ isCompacting: false });
        break;
    }
    this.map(event, this.state);
    if (this.settling && this.state.openTools.size === 0)
      this.finishSettle();
  }
  childEvent(sessionID, event) {
    const state = this.children.get(sessionID);
    if (!state) {
      const early = this.early.get(sessionID) ?? [];
      if (early.length < 500)
        early.push(event);
      this.early.set(sessionID, early);
      return;
    }
    switch (event.type) {
      case "permission.asked":
        return this.asked(event.data, state);
      case "permission.replied":
        return this.replied(event.data.requestID, event.data.reply !== "reject");
      case "session.step.ended":
        return this.map(event, state, false);
    }
    if (event.type.startsWith("session.") && !event.type.startsWith("session.execution."))
      this.map(event, state);
  }
  map(event, state, usage = true) {
    const mapped = mapEvent(event, state);
    if (!usage)
      delete mapped.usage;
    this.queue(mapped);
    for (const path of mapped.touched ?? [])
      this.touched({ path });
    if (mapped.scan)
      this.touched({});
    const child = subagentSession(state, event);
    if (child && !this.children.has(child.sessionId)) {
      const childState = createMapperState(child.sessionId, this.binding.cwd);
      childState.parentToolUseId = child.toolCallId;
      this.children.set(child.sessionId, childState);
      const early = this.early.get(child.sessionId) ?? [];
      this.early.delete(child.sessionId);
      for (const e of early)
        this.childEvent(child.sessionId, e);
    }
  }
  asked(p, state = this.state) {
    if (!this.active || !this.binding.cloudSessionId || this.approvals.has(p.id))
      return;
    const toolUseId = p.source?.type === "tool" ? p.source.id : undefined;
    const call = toolUseId ? state.tools.get(toolUseId) : undefined;
    const toolName = call ? shapeToolCall(call.name, {}, this.binding.cwd)[0] : p.action;
    const toolInput = call?.input ?? { resources: p.resources, ...p.metadata };
    this.approvals.set(p.id, { sessionID: p.sessionID });
    const body = { toolName, toolInput };
    if (toolUseId)
      body.toolUseId = toolUseId;
    this.push({ kind: "approval", requestID: p.id, body }, "daemon", true);
  }
  renamed(title) {
    const name = title.trim();
    if (name)
      this.push({ kind: "rename", title: name }, "api");
  }
  replied(requestID, approved) {
    const approval = this.approvals.get(requestID);
    if (!approval || approval.answeredBy)
      return;
    approval.answeredBy = "opencode";
    const decision = approved ? "approve" : "deny";
    if (approval.approvalId)
      this.push({ kind: "resolve", requestID, decision }, "api", true);
    else
      approval.decision = decision;
  }
  answered(approvalId, approved, comment) {
    for (const [requestID, approval] of this.approvals) {
      if (approval.approvalId !== approvalId)
        continue;
      if (approval.answeredBy)
        return true;
      approval.answeredBy = "web";
      log(`web ${approved ? "approval" : "denial"} for ${this.binding.cloudSessionId}`);
      this.host.reply(approval.sessionID, requestID, approved, comment).catch((err) => {
        log(`permission reply: ${errorMessage(err)}`);
      });
      return true;
    }
    return false;
  }
  web(text2) {
    if (!this.active)
      return;
    log(`web message for ${this.binding.cloudSessionId} (${text2.length} chars)`);
    this.pushStatus("running");
    this.host.notify(this.id, "Message from the web app", "info");
    this.host.deliver(this.id, text2).catch((err) => {
      log(`web message: ${errorMessage(err)}`);
      this.host.notify(this.id, `Could not start the web message: ${errorMessage(err)}`, "error");
      this.pushStatus("ready_for_input");
    });
  }
  stop() {
    if (!this.active)
      return;
    log(`web interrupt for ${this.binding.cloudSessionId}`);
    if (this.lastStatus === "running" && !this.settling) {
      (async () => {
        for (const [requestID, approval] of this.approvals) {
          if (approval.answeredBy)
            continue;
          approval.answeredBy = "web";
          if (!approval.approvalId)
            approval.decision = "deny";
          await this.host.reply(approval.sessionID, requestID, false, undefined).catch((err) => {
            log(`permission reply: ${errorMessage(err)}`);
          });
        }
        await this.host.interrupt(this.id);
      })().catch((err) => {
        log(`web interrupt: ${errorMessage(err)}`);
        this.pushStatus("interrupted");
      });
    } else {
      this.cancelSettle();
      this.pushStatus("interrupted");
    }
  }
  setOff(off) {
    const b = this.binding;
    if (!!b.off === off && (off || !this.halted))
      return;
    if (off) {
      this.pushStatus("ready_for_input");
      b.off = true;
      this.closeLanes();
    } else {
      b.off = false;
      if (this.halted || this.outbox.isStopped) {
        this.halted = undefined;
        this.outbox = this.newOutbox();
      }
      this.startLanes();
    }
    this.saveNow();
    this.changed();
  }
  async detach() {
    if (this.binding.cloudSessionId && this.active)
      this.pushStatus("ready_for_input");
    await this.shutdown();
    await rm3(bindingPath(this.binding.channel, this.id), { force: true });
  }
  touched(t) {
    this.artifacts?.touched(t);
    this.diff?.touched();
  }
  settle(status, extra = {}) {
    this.cancelSettle();
    this.touched({});
    const timer = setTimeout(() => this.finishSettle(), SETTLE_MS);
    timer.unref();
    this.settling = { status, extra, timer };
    if (this.state.openTools.size === 0)
      this.finishSettle();
  }
  finishSettle() {
    const s = this.settling;
    if (!s)
      return;
    this.cancelSettle();
    this.state.openTools.clear();
    this.pushStatus(s.status, s.extra);
  }
  cancelSettle() {
    if (this.settling)
      clearTimeout(this.settling.timer);
    this.settling = undefined;
  }
  async modelChanged(model) {
    if (!model || this.model?.id === model.id && this.model.providerID === model.providerID && this.contextWindow)
      return;
    this.model = model;
    this.state.model = model.id;
    this.contextWindow = await this.host.contextLimit(model);
    this.pushUpdate(this.modelFields());
  }
  modelFields() {
    const id = this.model?.id;
    return { model: id, resolvedModel: id, contextWindowLimit: this.contextWindow };
  }
  report(git3) {
    if (git3)
      this.push({ kind: "rpc", path: "sessions/repositories/report", body: repositoriesReport(git3) });
  }
  queue(mapped) {
    for (const event of mapped.events)
      this.push({ kind: "rpc", path: "sessions/events/create", body: event });
    if (mapped.usage)
      this.pushUpdate({ ...mapped.usage, contextWindowLimit: this.contextWindow }, false);
  }
  pushUpdate(fields, keep = true) {
    this.push({ kind: "rpc", path: "sessions/update", body: fields }, "daemon", keep);
  }
  pushStatus(status, extra = {}) {
    this.lastStatus = status;
    this.pushUpdate({ ...extra, status });
    this.noteRun();
  }
  noteRun() {
    const b = this.binding;
    const approvalIds = [...this.approvals.values()].filter((a) => a.approvalId && !a.answeredBy).map((a) => a.approvalId ?? "");
    const running = this.lastStatus === "running" ? { pid: process.pid, approvalIds } : undefined;
    if (JSON.stringify(running) === JSON.stringify(b.running))
      return;
    b.running = running;
    this.saveNow();
  }
  push(job, plane = "daemon", keep = false) {
    if (this.halted || this.binding.off || this.closing)
      return;
    const sizeBytes = Buffer.byteLength(JSON.stringify("body" in job ? job.body : job));
    this.outbox.push({ ...job, id: String(++this.seq), plane, sizeBytes, keep });
    this.changed();
  }
  newOutbox() {
    return new Outbox({
      send: (item) => this.send(item).finally(() => this.changed()),
      log,
      onPause: () => this.changed(),
      onResume: () => this.changed(),
      onStopAll: (reason) => this.halt(reason),
      onStopBinding: (reason) => this.halt(reason),
      onSkip: (item, err) => {
        if (item.kind === "prepare")
          this.halt(`could not start the cloud session: ${errorMessage(err)}`);
      },
      onDrop: (item) => log(`queue full: dropped ${item.kind === "rpc" ? item.path : item.kind}`)
    });
  }
  async send(item) {
    const b = this.binding;
    if (item.kind === "prepare")
      return this.sendPrepare(item);
    if (!b.cloudSessionId)
      return;
    if (item.kind === "approval")
      return this.sendApproval(b.cloudSessionId, item);
    if (item.kind === "resolve")
      return this.sendResolve(item);
    if (item.kind === "rename")
      return this.sendRename(b.cloudSessionId, item.title);
    const body = { ...item.body, sessionId: b.cloudSessionId };
    const signal = this.abort.signal;
    await client2.withDaemonToken(b.channel, signal, (token) => client2.rpc(b.channel, "daemon", item.path, body, token, signal));
  }
  async sendApproval(sessionId, item) {
    const approval = this.approvals.get(item.requestID);
    if (!approval)
      return;
    const out = await client2.daemonCall(this.binding.channel, "approvals/create", { ...item.body, sessionId }, this.abort.signal);
    approval.approvalId = out.approvalId;
    if (approval.decision)
      this.push({ kind: "resolve", requestID: item.requestID, decision: approval.decision }, "api", true);
    this.noteRun();
  }
  async sendRename(sessionId, title) {
    const b = this.binding;
    const signal = this.abort.signal;
    await client2.apiRpc(b.channel, "sessions/update", { sessionId, title }, signal);
    if (b.taskMode === "ensure" && b.taskId)
      await client2.apiRpc(b.channel, "tasks/update", { taskId: b.taskId, name: title }, signal);
  }
  async sendResolve(item) {
    const approvalId = this.approvals.get(item.requestID)?.approvalId;
    if (!approvalId)
      return;
    const body = { approvalId, decision: item.decision };
    await client2.apiRpc(this.binding.channel, "approvals/resolve", body, this.abort.signal);
    this.approvals.delete(item.requestID);
  }
  async sendPrepare(item) {
    const b = this.binding;
    const signal = this.abort.signal;
    const org = await client2.daemonOrgId(b.channel, signal);
    let out;
    try {
      out = await client2.prepare(b.channel, item.body, signal);
    } catch (err) {
      if (!(err instanceof RpcError) || err.status !== 403 && err.status !== 404)
        throw err;
      const task = b.taskSlug ?? b.taskId;
      if (item.fallback) {
        log(`task link ${task} failed (${err.status}); creating a task`);
        const pick = newTask("opencode", b.opencodeSessionId);
        item.body = prepareBody({ ...item.fallback, pick });
        item.fallback = undefined;
        b.taskMode = "ensure";
        b.taskSlug = pick.slug;
        b.taskId = undefined;
        return this.sendPrepare(item);
      }
      if (err.status === 404)
        return this.halt(`task ${task} not found. Use /humanlayer attach <task> or /humanlayer attach new.`);
      return this.halt(`no access to task ${task}`);
    }
    const who = await client2.identity(b.channel);
    b.cloudSessionId = out.sessionId;
    b.taskId = out.taskId;
    b.userId = out.userId;
    b.orgId = org ?? who?.orgId;
    b.sessionUrl = `${getChannelConfig(b.channel).app}/sessions/${out.sessionId}`;
    await this.saveNow();
    log(`bound ${b.opencodeSessionId} to ${b.cloudSessionId}`);
    this.host.notify(this.id, `Mirroring to ${b.sessionUrl}`, "info");
    this.startLanes();
    this.changed();
  }
  startLanes() {
    const b = this.binding;
    if (!b.taskId || !b.cloudSessionId || !this.active)
      return;
    this.host.bound(b);
    this.startArtifacts(b.taskId, b.cloudSessionId);
    this.startDiff(b.taskId, b.cloudSessionId);
  }
  startArtifacts(taskId, sessionId) {
    const b = this.binding;
    const slug = safeSlug(b.taskSlug);
    if (this.artifacts || !slug)
      return;
    this.artifacts = new ArtifactSync({
      channel: b.channel,
      taskId,
      sessionId,
      cwd: b.cwd,
      store: artifactsDir(taskId),
      signal: this.abort.signal,
      ledger: () => b.artifactLedger ??= {},
      save: () => this.saveSoon(),
      note: (file) => {
        log(`synced task file ${file}`);
        this.synced = basename4(file);
        this.changed();
      },
      warn: (_key, message) => this.host.notify(this.id, message, "warning"),
      log,
      daemonCall: client2.daemonCall,
      recordLink: (path, id) => recordLink(linksPath(), path, id)
    }, taskFolder(b.cwd, slug));
  }
  startDiff(taskId, sessionId) {
    const b = this.binding;
    if (this.diff || b.taskMode !== "ensure" || !b.git?.headSha || !b.orgId)
      return;
    const signal = this.abort.signal;
    this.diff = new DiffSync({
      syncUrl: getChannelConfig(b.channel).sync,
      orgId: b.orgId,
      taskId,
      sessionId,
      gitRoot: b.git.root,
      baseSha: b.git.headSha,
      repoId: basename4(b.git.root),
      published: b.diffPublished ?? {}
    }, {
      daemonToken: () => client2.daemonToken(b.channel, signal),
      remintDaemonToken: () => client2.remintDaemonToken(b.channel, signal),
      onPublished: (published) => {
        b.diffPublished = published;
        this.saveSoon();
      },
      onError: (kind, message) => {
        if (kind !== "transient")
          this.host.notify(this.id, `Task diff ${kind}: ${message}`, "warning");
      },
      log
    });
    this.diff.touched();
  }
  closeLanes() {
    this.artifacts?.close();
    this.diff?.close();
    this.artifacts = undefined;
    this.diff = undefined;
  }
  changed() {
    this.host.changed(this.id);
  }
  saveSoon() {
    if (this.saveTimer)
      return;
    this.saveTimer = setTimeout(() => void this.saveNow(), 2000);
    this.saveTimer.unref();
  }
  async saveNow() {
    clearTimeout(this.saveTimer);
    this.saveTimer = undefined;
    const b = this.binding;
    if (!b.cloudSessionId)
      return;
    await writeJsonFileAtomic(bindingPath(b.channel, b.opencodeSessionId), b).catch((err) => log(`save: ${errorMessage(err)}`));
  }
  halt(reason) {
    if (this.halted)
      return;
    this.halted = reason;
    log(`stopped ${this.id}: ${reason}`);
    this.closeLanes();
    this.host.notify(this.id, `Mirroring stopped: ${reason}`, "error");
    this.changed();
  }
  shutdown() {
    this.closing ??= (async () => {
      for (const [requestID, approval] of this.approvals) {
        if (approval.approvalId && !approval.answeredBy)
          this.push({ kind: "resolve", requestID, decision: "deny" }, "api", true);
      }
      if (this.settling)
        this.finishSettle();
      else if (this.lastStatus === "running")
        this.pushStatus("interrupted");
      const end = Date.now() + FLUSH_MS;
      await Promise.all([
        this.outbox.drain(FLUSH_MS),
        this.artifacts?.flush(FLUSH_MS),
        this.diff?.flush(FLUSH_MS)
      ]);
      await this.saveNow();
      const left = this.outbox.length;
      if (left > 0)
        log(`${this.id}: ${left} updates not sent`);
      if (Date.now() > end)
        log(`${this.id}: flush ran out of time`);
      this.outbox.close();
      this.closeLanes();
      this.abort.abort();
    })();
    return this.closing;
  }
}

// apps/riptide-opencode-plugin/src/rpc.ts
var object = { type: "object" };
var HumanLayerRpc = {
  id: "humanlayer",
  methods: {
    status: { input: object, output: object },
    login: { input: object, output: object },
    chooseOrg: { input: object, output: object },
    logout: { input: object, output: object },
    attach: { input: object, output: object },
    off: { input: object, output: object },
    on: { input: object, output: object }
  },
  events: {
    changed: { schema: object },
    notice: { schema: object },
    login: { schema: object },
    compact: { schema: object }
  }
};

// apps/riptide-opencode-plugin/src/skills.ts
import { readdir as readdir5, readFile as readFile4 } from "fs/promises";
import { homedir as homedir2 } from "os";
import { join as join8 } from "path";
async function loadSkills(dir) {
  const names = await readdir5(dir).catch(() => []);
  const skills = await Promise.all(names.map(async (folder) => {
    const path = join8(dir, folder, "SKILL.md");
    const content = await readFile4(path, "utf8").catch(() => {
      return;
    });
    if (!content)
      return [];
    const meta = frontmatter(content);
    const name = typeof meta.name === "string" && meta.name ? meta.name : folder;
    const skill = { id: name, name, path, content };
    if (typeof meta.description === "string")
      skill.description = meta.description;
    return [skill];
  }));
  return skills.flat().sort((a, b) => a.name.localeCompare(b.name));
}
function scopeOf(path, workspace) {
  if (path.startsWith(`${workspace}/`))
    return "project";
  if (path.startsWith(`${homedir2()}/`) && !path.includes("/node_modules/"))
    return "user";
  return "plugin";
}
function skillsReport(o) {
  const skills = o.skills.map((s) => {
    const skill = {
      name: s.name,
      scope: o.bundled.has(s.path) ? "plugin" : scopeOf(s.path, o.workspace)
    };
    if (s.description)
      skill.description = s.description;
    return skill;
  });
  const commands2 = o.commands.map((c) => {
    const command = { name: c.name };
    if (c.description)
      command.description = c.description;
    return command;
  });
  return { hostId: o.hostId, agent: "opencode", workspacePath: o.workspace, commands: commands2, skills };
}
function slashCommand(text2) {
  const m = /^\/([^\s/]+)(?:\s+([\s\S]*))?$/.exec(text2.trim());
  return m?.[1] ? { name: m[1], args: m[2]?.trim() ?? "" } : undefined;
}

// apps/riptide-opencode-plugin/src/index.ts
var WEB_PROMPT = "humanlayer";
var CHANGED_MS = 250;
var src_default = Plugin.define({
  id: "humanlayer",
  async setup(ctx) {
    client2.getPat();
    const directory = ctx.location.directory;
    const disabled = isDisabled();
    log(`loaded for ${directory}${disabled ? " (disabled)" : ""}`);
    const mirrors = new Map;
    const offBeforeBind = new Set;
    const attachTargets = new Map;
    let nextAttach;
    const channels2 = new Map;
    const limits = new Map;
    const stop = new AbortController;
    let warnedSignedOut = false;
    const rpc2 = await ctx.rpc.register(HumanLayerRpc, {
      status: async (input) => status(input),
      login: async (input) => startLogin(input.channel),
      chooseOrg: async (input) => {
        chooseOrg?.(input.id);
        return {};
      },
      logout: async (input) => logout(input.channel),
      attach: async (input) => attach(input),
      off: async (input) => setOff(input.sessionID, true),
      on: async (input) => setOff(input.sessionID, false)
    });
    const emit = (name, data) => void rpc2.events.emit(name, { ...data }).catch((err) => log(`emit ${name}: ${errorMessage(err)}`));
    const pendingChanged = new Map;
    const changed = (sessionID) => {
      if (pendingChanged.has(sessionID))
        return;
      const timer = setTimeout(() => {
        pendingChanged.delete(sessionID);
        emit("changed", { sessionID });
      }, CHANGED_MS);
      timer.unref();
      pendingChanged.set(sessionID, timer);
    };
    const notify = (sessionID, message, variant) => {
      log(`notice: ${message}`);
      emit("notice", { sessionID, message, variant });
    };
    const bundled = await loadSkills(bundledSkillsDir());
    const bundledPaths = new Set(bundled.map((s) => s.path));
    if (bundled.length > 0)
      await ctx.skill.transform((editor) => {
        for (const s of bundled)
          if (!editor.get(s.id))
            editor.add(s);
      });
    const reportSkills = async (channel, hostId) => {
      const [skills, commands2] = await Promise.all([ctx.skill.list(), ctx.command.list()]);
      const body = skillsReport({
        hostId,
        workspace: directory,
        skills: skills.data,
        commands: commands2.data,
        bundled: bundledPaths
      });
      await client2.daemonCall(channel, "agentCommands/report", body, stop.signal);
      log(`reported ${body.skills.length} skills and ${body.commands.length} commands`);
    };
    const deliver = async (sessionID, text2) => {
      const slash = slashCommand(text2);
      if (slash?.name === "compact") {
        emit("compact", { sessionID });
        return;
      }
      if (slash) {
        const skill = (await ctx.skill.list()).data.find((s) => s.id === slash.name || s.name === slash.name);
        if (skill) {
          const body = slash.args || `Use the ${skill.name} skill.`;
          await ctx.session.prompt({
            sessionID,
            text: body,
            skills: [{ id: skill.id }],
            delivery: "queue",
            metadata: { [WEB_PROMPT]: "web" }
          });
          return;
        }
        const command = (await ctx.command.list()).data.find((c) => c.name === slash.name);
        if (command) {
          webEchoes.add(sessionID);
          await ctx.session.command({ sessionID, name: command.name, text: slash.args, delivery: "queue" });
          return;
        }
      }
      await ctx.session.prompt({ sessionID, text: text2, delivery: "queue", metadata: { [WEB_PROMPT]: "web" } });
    };
    const webEchoes = new Set;
    const loadLimit = async (model) => {
      const { data } = await ctx.model.list();
      const info = data.find((m) => m.providerID === model.providerID && (m.modelID === model.id || m.id === model.id));
      return info?.limit.context || undefined;
    };
    const host = {
      notify,
      changed,
      contextLimit: (model) => {
        const key = `${model.providerID}/${model.id}`;
        let limit = limits.get(key);
        if (!limit) {
          limit = loadLimit(model).catch(() => {
            return;
          });
          limits.set(key, limit);
        }
        return limit;
      },
      bound: (b) => startChannel(b.channel, b.hostId),
      deliver,
      interrupt: async (sessionID) => {
        await ctx.session.interrupt({ sessionID });
      },
      reply: async (sessionID, requestID, approved, message) => {
        const decision = approved ? "once" : "reject";
        if (message)
          await ctx.permission.reply({ sessionID, requestID, decision, message });
        else
          await ctx.permission.reply({ sessionID, requestID, decision });
      }
    };
    function startChannel(channel, hostId) {
      if (disabled || stop.signal.aborted)
        return;
      const running = channels2.get(channel);
      if (running) {
        if (running.inbox.isStopped)
          running.inbox = followInbox(channel, hostId);
        if (running.approvals.isStopped)
          running.approvals = followAnswers(channel);
        return;
      }
      const beat = {
        hostId,
        hostName: hostname(),
        capabilities: ["attachedSessionsOnly", "agent:opencode"],
        canSelfUpdate: false
      };
      const once2 = () => guard("heartbeat", () => client2.daemonCall(channel, "hosts/heartbeat", beat));
      once2();
      const timer = setInterval(once2, HEARTBEAT_MS);
      timer.unref();
      channels2.set(channel, {
        beat: timer,
        inbox: followInbox(channel, hostId),
        approvals: followAnswers(channel)
      });
      guard("skills report", () => reportSkills(channel, hostId));
    }
    function followInbox(channel, hostId) {
      return followSessions({
        client: client2,
        channel,
        hostId,
        target: (cloudSessionId) => webTarget(channel, cloudSessionId),
        claim: (key) => claim(claimsDir(), key),
        log,
        signal: stop.signal
      });
    }
    function followAnswers(channel) {
      return followApprovals({
        client: client2,
        channel,
        log,
        signal: stop.signal,
        answered: (id, approved, comment) => {
          for (const m of mirrors.values())
            if (m?.answered(id, approved, comment))
              return;
        }
      });
    }
    const startSignedIn = () => guard("start", async () => {
      const channel = await client2.resolveChannel();
      const who = await client2.identity(channel);
      if (!who)
        return;
      startChannel(channel, await client2.hostId(channel));
      for (const sessionID of await Mirror.lostRuns(channel, directory))
        await resumed(sessionID, who);
    });
    async function webTarget(channel, cloudSessionId) {
      for (const m of mirrors.values())
        if (m?.cloudSessionId === cloudSessionId)
          return m;
      const sessionID = await Mirror.find(channel, cloudSessionId, directory);
      if (!sessionID || mirrors.get(sessionID) === null)
        return;
      const who = await client2.identity(channel);
      if (!who)
        return;
      return resumed(sessionID, who);
    }
    const session = (sessionID) => ctx.session.get({ sessionID });
    const unlinked = new Set;
    const resuming = new Map;
    const resumed = (sessionID, who) => {
      const known = mirrors.get(sessionID);
      if (known)
        return Promise.resolve(known);
      let p = resuming.get(sessionID);
      if (!p) {
        p = (async () => {
          const info = await session(sessionID);
          const model = info.model && { providerID: info.model.providerID, id: info.model.id };
          const m = await Mirror.resume(host, sessionID, who, model);
          if (m) {
            mirrors.set(sessionID, m);
            changed(sessionID);
          }
          return m;
        })().finally(() => resuming.delete(sessionID));
        resuming.set(sessionID, p);
      }
      return p;
    };
    const onPrompt = async (sessionID, messageID, content, fromWeb) => {
      const known = mirrors.get(sessionID);
      if (known)
        return fromWeb ? undefined : known.prompt(messageID, content);
      if (known === null || offBeforeBind.has(sessionID))
        return;
      const info = await session(sessionID);
      if (info.parentID)
        return void mirrors.set(sessionID, null);
      const who = await client2.identity(await client2.resolveChannel());
      if (!who) {
        if (!warnedSignedOut)
          notify(sessionID, `Not signed in to HumanLayer. Run /humanlayer login.`, "warning");
        warnedSignedOut = true;
        return;
      }
      const model = info.model && { providerID: info.model.providerID, id: info.model.id };
      let attach2 = attachTargets.get(sessionID);
      const m = attach2 ? undefined : await resumed(sessionID, who);
      if (m)
        return fromWeb ? undefined : m.prompt(messageID, content);
      attachTargets.delete(sessionID);
      if (!attach2 && nextAttach) {
        attach2 = nextAttach;
        nextAttach = undefined;
      }
      const pick = await pickTask({
        prefix: "opencode",
        sessionId: sessionID,
        cwd: directory,
        linksPath: linksPath(),
        attach: attach2,
        chosen: taskFromEnv(),
        freshId: () => crypto.randomUUID()
      });
      mirrors.set(sessionID, await Mirror.bind(host, { sessionId: sessionID, cwd: directory, who, prompt: content, model, pick }));
    };
    if (!disabled) {
      await ctx.session.hook("prompt", (p) => {
        const fromWeb = p.metadata?.[WEB_PROMPT] === "web" || webEchoes.delete(p.sessionID);
        return guard("prompt", () => onPrompt(p.sessionID, p.messageID, promptText2(p.prompt), fromWeb));
      });
      await ctx.session.hook("context", (c) => {
        const hint = mirrors.get(c.sessionID)?.hint();
        if (hint)
          c.system.push({ type: "text", text: hint });
      });
    }
    const chains = new Map;
    const inOrder = (sessionID, fn) => {
      const run = (chains.get(sessionID) ?? Promise.resolve()).then(fn, fn);
      chains.set(sessionID, run.catch(() => {
        return;
      }));
      return run;
    };
    if (!disabled)
      await ctx.tool.transform((editor) => {
        for (const name of HUMANLAYER_TOOLS) {
          const { description, params } = TOOLS[name];
          editor.add({
            name,
            description,
            input: toJsonSchema(params),
            options: { codemode: false },
            execute: async (input, context) => {
              const target = () => mirrors.get(context.sessionID)?.toolTarget() ?? {
                reason: "This opencode session is not linked to a HumanLayer task."
              };
              const text2 = await inOrder(context.sessionID, () => runTool(name, input, target, client2.daemonCall, context.signal));
              return { content: text2 };
            }
          });
        }
      });
    const parents = new Map;
    const mirrorAbove = async (sessionID) => {
      let id = sessionID;
      for (let depth = 0;depth < 8; depth++) {
        let parent = parents.get(id);
        if (!parent) {
          parent = session(id).then((info) => info.parentID, () => {
            return;
          });
          parents.set(id, parent);
        }
        const up = await parent;
        if (!up)
          return;
        const m = mirrors.get(up);
        if (m)
          return m;
        id = up;
      }
      return;
    };
    if (!disabled)
      (async () => {
        for await (const event of ctx.event.subscribe({ signal: stop.signal })) {
          const sessionID = "data" in event && event.data && "sessionID" in event.data ? event.data.sessionID : undefined;
          if (typeof sessionID !== "string")
            continue;
          const m = mirrors.get(sessionID);
          if (m)
            await guard(event.type, () => m.event(event));
          else {
            const root = await guard("parent", () => mirrorAbove(sessionID));
            if (root)
              await guard(event.type, () => root.childEvent(sessionID, event));
          }
        }
      })().catch((err) => {
        if (!stop.signal.aborted)
          log(`events: ${errorMessage(err)}`);
      });
    const channelOf = async (channel) => {
      if (channel === undefined)
        return client2.resolveChannel();
      return isChannel(channel) ? channel : undefined;
    };
    const unknownChannel = {
      message: `Unknown channel. Use ${ALL_CHANNELS.join(", ")}.`,
      variant: "warning"
    };
    async function status(input) {
      const channel = await client2.resolveChannel();
      const out = { channel };
      if (disabled)
        out.disabled = true;
      const who = await client2.identity(channel).catch(() => null);
      if (who)
        out.signedIn = { email: who.email, org: who.orgName, source: who.source };
      const pending = login2.pendingLogin(channel);
      if (pending) {
        out.pending = {};
        if (pending.url)
          out.pending.url = pending.url;
        if (pending.code)
          out.pending.code = pending.code;
      }
      if ([...mirrors.values()].some((m2) => m2?.loginRequired))
        out.loginRequired = true;
      const id = input.sessionID;
      if (!id) {
        if (nextAttach)
          out.session = { state: "unbound", queued: 0, attach: nextAttach };
        return out;
      }
      if (who && !mirrors.has(id) && !unlinked.has(id) && !attachTargets.has(id)) {
        if (!await resumed(id, who))
          unlinked.add(id);
      }
      const m = mirrors.get(id);
      if (m)
        out.session = m.info();
      else if (m === null)
        out.session = { state: "off", queued: 0, problem: "a subagent session" };
      else
        out.session = { state: offBeforeBind.has(id) ? "off" : "unbound", queued: 0 };
      const attach2 = attachTargets.get(id);
      if (attach2)
        out.session.attach = attach2;
      return out;
    }
    let chooseOrg;
    async function startLogin(channel) {
      const ch = await channelOf(channel);
      if (!ch)
        return unknownChannel;
      const waiting = login2.pendingLogin(ch);
      if (waiting?.url && waiting.code) {
        emit("login", { kind: "code", channel: ch, url: waiting.url, code: waiting.code });
        return { message: `A login is waiting: open ${waiting.url} and check the code is ${waiting.code}.` };
      }
      login2.startDeviceLogin(ch, {
        hasUI: true,
        showCode: (url, code) => emit("login", { kind: "code", channel: ch, url, code }),
        pickOrg: (orgs, current) => new Promise((resolve4) => {
          chooseOrg = (id) => {
            chooseOrg = undefined;
            resolve4(id ? orgs.find((o) => o.organizationId === id) : undefined);
          };
          const options = orgs.map((o) => ({ id: o.organizationId, name: o.organizationName }));
          emit("login", {
            kind: "orgs",
            channel: ch,
            orgs: options,
            current: current?.organizationId
          });
        })
      }).then((creds) => {
        if (!creds)
          return emit("login", { kind: "done", channel: ch, ok: false, message: "Login cancelled." });
        warnedSignedOut = false;
        resumeAll();
        startSignedIn();
        emit("login", {
          kind: "done",
          channel: ch,
          ok: true,
          message: `Signed in to HumanLayer (${ch}) as ${creds.email}, org ${creds.orgName}.`
        });
      }).catch((err) => emit("login", {
        kind: "done",
        channel: ch,
        ok: false,
        message: `Login failed: ${errorMessage(err)}`
      })).finally(() => {
        for (const id of mirrors.keys())
          changed(id);
      });
      return { message: `Signing in to HumanLayer (${ch})\u2026` };
    }
    async function logout(channel) {
      const ch = await channelOf(channel);
      if (!ch)
        return unknownChannel;
      await login2.logout(ch);
      for (const id of mirrors.keys())
        changed(id);
      return { message: `Signed out of HumanLayer (${ch}).` };
    }
    async function attach({ sessionID, target }) {
      if (disabled)
        return { message: "HumanLayer is turned off (HUMANLAYER_OPENCODE_DISABLE).", variant: "warning" };
      const what = target === "new" ? "a new task" : `task ${target}`;
      if (!sessionID) {
        nextAttach = target;
        emit("changed", {});
        return { message: `Your next new session starts in ${what}.` };
      }
      const m = mirrors.get(sessionID);
      if (m === null)
        return { message: "Subagent sessions show in their parent session.", variant: "warning" };
      if (m)
        await m.detach();
      mirrors.delete(sessionID);
      offBeforeBind.delete(sessionID);
      attachTargets.set(sessionID, target);
      changed(sessionID);
      return { message: `The next prompt starts a HumanLayer session in ${what}.` };
    }
    async function setOff(sessionID, off) {
      if (disabled)
        return { message: "HumanLayer is turned off (HUMANLAYER_OPENCODE_DISABLE).", variant: "warning" };
      const m = mirrors.get(sessionID);
      if (m === null)
        return { message: "Subagent sessions show in their parent session.", variant: "warning" };
      if (m)
        m.setOff(off);
      else if (off)
        offBeforeBind.add(sessionID);
      else
        offBeforeBind.delete(sessionID);
      changed(sessionID);
      return { message: off ? "Mirroring is off for this session." : "Mirroring is on for this session." };
    }
    if (!disabled) {
      pruneClaims(claimsDir());
      startSignedIn();
    }
    return async () => {
      stop.abort();
      for (const { beat, inbox: inbox2, approvals } of channels2.values()) {
        clearInterval(beat);
        inbox2.close();
        approvals.close();
      }
      channels2.clear();
      for (const timer of pendingChanged.values())
        clearTimeout(timer);
      await Promise.all([...mirrors.values()].flatMap((m) => m ? [m.shutdown()] : []));
      await rpc2.dispose();
    };
  }
});
export {
  src_default as default
};
