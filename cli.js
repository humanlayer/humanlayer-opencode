#!/usr/bin/env bun
// @bun

// apps/riptide-opencode-plugin/src/cli.ts
import { createInterface } from "readline/promises";

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
// apps/riptide-opencode-plugin/src/config.ts
import { existsSync } from "fs";
import { homedir } from "os";
import { join as join2 } from "path";
import { fileURLToPath } from "url";

// packages/session-sdk-auth/src/client.ts
import { randomUUID as randomUUID2 } from "crypto";
import { join } from "path";
function authPaths(dir) {
  return {
    config: join(dir, "config.json"),
    host: (channel) => join(dir, `host-${channel}.json`),
    session: (channel) => join(dir, `session-${channel}.json`),
    lock: (channel) => join(dir, `session-${channel}.json.lock`)
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
    const claims = decodeJwtPayload(creds.accessToken);
    const exp = typeof claims.exp === "number" ? claims.exp : 0;
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
  function prepare(channel, body, signal) {
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
    prepare
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
    const claim = (key) => {
      const value = jwtClaim(tokens.accessToken, key);
      if (!value)
        throw new Error(`the token is missing claim "${key}"`);
      return value;
    };
    const userId = claim("sub");
    const orgId = claim("internal_org_id");
    const workosOrgId = claim("org_id");
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
  return process.env.HUMANLAYER_RIPTIDE_HOME || join2(homedir(), ".humanlayer", "riptide");
}
function opencodeDir() {
  return join2(riptideHome(), "opencode");
}
function artifactsDir(taskId) {
  return join2(riptideHome(), "artifacts", taskId);
}
function bindingsDir(channel) {
  return join2(opencodeDir(), "bindings", channel);
}
function claimsDir() {
  return join2(opencodeDir(), "claims");
}
function linksPath() {
  return join2(opencodeDir(), "task-links.jsonl");
}
function bundledSkillsDir() {
  const built = fileURLToPath(new URL("./skills/", import.meta.url));
  return existsSync(built) ? built : fileURLToPath(new URL("../../riptide-pi-extension/skills/", import.meta.url));
}
function logFilePath() {
  return join2(opencodeDir(), "logs", "opencode-humanlayer.log");
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

// apps/riptide-opencode-plugin/src/cli.ts
async function pickOrg(orgs, current) {
  orgs.forEach((org, i) => {
    console.log(`  ${i + 1}. ${org.organizationName}${org === current ? " (current)" : ""}`);
  });
  const rl = createInterface({ input: process.stdin, output: process.stdout });
  try {
    const answer = (await rl.question("Organization number: ")).trim();
    if (!answer)
      return current ?? orgs[0];
    return orgs[Number.parseInt(answer, 10) - 1];
  } finally {
    rl.close();
  }
}
async function main() {
  const [command = "status", arg] = process.argv.slice(2);
  if (arg !== undefined && !isChannel(arg))
    throw new Error(`unknown channel "${arg}": use ${ALL_CHANNELS.join(", ")}`);
  const channel = arg ?? await client2.resolveChannel();
  if (command === "login") {
    const hold = setInterval(() => {}, 60000);
    const creds = await login2.startDeviceLogin(channel, {
      hasUI: process.stdin.isTTY === true,
      showCode: (url, code) => console.log(`Open ${url}
and check the code is ${code}.`),
      pickOrg
    }).finally(() => clearInterval(hold));
    if (!creds)
      return console.log("Login cancelled.");
    console.log(`Signed in to HumanLayer (${channel}) as ${creds.email}, org ${creds.orgName}.`);
    return;
  }
  if (command === "logout") {
    await login2.logout(channel);
    console.log(`Signed out of HumanLayer (${channel}).`);
    return;
  }
  if (command === "status") {
    const who = await client2.identity(channel);
    if (!who)
      return console.log(`Not signed in to HumanLayer (${channel}).`);
    if (who.source === "pat")
      return console.log(`Using HUMANLAYER_PAT (${channel}).`);
    console.log(`Signed in to HumanLayer (${channel}) as ${who.email}, org ${who.orgName}.`);
    return;
  }
  throw new Error(`unknown command "${command}": use login, logout or status`);
}
main().catch((err) => {
  console.error(`humanlayer-opencode: ${errorMessage(err)}`);
  process.exit(1);
});
