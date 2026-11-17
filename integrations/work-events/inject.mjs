#!/usr/bin/env node

import { createHash } from "node:crypto";
import { isIP } from "node:net";

export class InjectorError extends Error {}

export function parseEnvelope(raw) {
  let value;
  try {
    value = JSON.parse(raw);
  } catch {
    throw new InjectorError("invalid Garden wake envelope JSON");
  }
  if (
    value?.version !== 1 ||
    value?.type !== "garden_wake" ||
    typeof value.reason !== "string" ||
    !value.reason.trim() ||
    typeof value.message !== "string" ||
    !value.message.trim() ||
    value.reason.length > 200 ||
    value.message.length > 4000
  ) {
    throw new InjectorError("invalid Garden wake envelope");
  }
  return {
    version: 1,
    type: "garden_wake",
    reason: value.reason,
    message: value.message,
  };
}

export function parseTarget(raw) {
  const target = new URL(
    raw ?? "https://xlc-general-bridge-lyj.onrender.com/work/events",
  );
  const loopback = target.hostname === "localhost" ||
    target.hostname === "127.0.0.1" || target.hostname === "::1";
  if (
    (target.protocol !== "https:" && !(target.protocol === "http:" && loopback)) ||
    target.username || target.password || target.hash ||
    (isIP(target.hostname) && !loopback)
  ) {
    throw new InjectorError("WORK_EVENTS_URL must be HTTPS (HTTP is loopback-only)");
  }
  return target;
}

export function eventId(envelope, now = Date.now()) {
  // The Bridge performs at most one quick local retry. A minute bucket makes
  // that retry idempotent without turning later, legitimate Garden wakes into
  // permanent duplicates.
  const slot = Math.floor(now / 60_000);
  const digest = createHash("sha256")
    .update(JSON.stringify([envelope.reason, envelope.message, slot]))
    .digest("hex")
    .slice(0, 32);
  return `garden_${slot}_${digest}`;
}

export async function inject(envelope, options = {}) {
  const token = options.token ?? process.env.WORK_EVENTS_INGEST_TOKEN;
  if (typeof token !== "string" || token.length < 24) {
    throw new InjectorError("WORK_EVENTS_INGEST_TOKEN is missing or too short");
  }
  const target = parseTarget(options.url ?? process.env.WORK_EVENTS_URL);
  const id = eventId(envelope, options.now ?? Date.now());
  const response = await fetch(target, {
    method: "POST",
    headers: {
      authorization: `Bearer ${token}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      eventId: id,
      source: "galatea.garden.bridge",
      text: envelope.message,
    }),
    signal: AbortSignal.timeout(options.timeoutMs ?? 10_000),
    redirect: "error",
  });
  const body = await response.json().catch(() => null);
  if (!response.ok || body?.accepted !== true) {
    throw new InjectorError(`Work event ingress rejected the wake (${response.status})`);
  }
  if (!Number.isInteger(body.subscriptions) || body.subscriptions < 1) {
    throw new InjectorError("Work event ingress has no active subscriber");
  }
  return { accepted: true, eventId: id, subscriptions: body.subscriptions };
}

async function main() {
  const chunks = [];
  for await (const chunk of process.stdin) chunks.push(chunk);
  const envelope = parseEnvelope(Buffer.concat(chunks).toString("utf8"));
  const result = await inject(envelope);
  process.stdout.write(`${JSON.stringify(result)}\n`);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((error) => {
    const message = error instanceof InjectorError ? error.message : "Work event injection failed";
    process.stderr.write(`${message}\n`);
    process.exitCode = 1;
  });
}
