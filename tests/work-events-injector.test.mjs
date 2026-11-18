import assert from "node:assert/strict";
import { generateKeyPairSync, verify as verifyDetached } from "node:crypto";
import { once } from "node:events";
import { createServer } from "node:http";
import test from "node:test";
import {
  InjectorError,
  eventId,
  inject,
  parseEnvelope,
  parseTarget,
} from "../integrations/work-events/inject.mjs";

const envelope = {
  version: 1,
  type: "garden_wake",
  reason: "game_turn_required",
  message: "游戏轮到你了。请调用 Garden MCP 的 get_my_status 查看当前局面。",
};

const { publicKey, privateKey } = generateKeyPairSync("ed25519");
const deliveryId = "ee2d1438-b2e5-4cfb-bd77-016b8af97604";

test("posts a privacy-safe Garden wake to the Work event ingress", async () => {
  let received;
  const server = createServer(async (request, response) => {
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    received = {
      authorization: request.headers.authorization,
      body: JSON.parse(Buffer.concat(chunks).toString("utf8")),
    };
    response.writeHead(202, { "content-type": "application/json" });
    response.end(JSON.stringify({ accepted: true, subscriptions: 1, delivered: 1 }));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  try {
    const result = await inject(envelope, {
      privateKey,
      url: `http://127.0.0.1:${address.port}/work/garden-events`,
      now: 1_800_000,
      deliveryId,
    });
    assert.equal(result.accepted, true);
    assert.equal(received.authorization, undefined);
    assert.equal(received.body.eventId, `garden_${deliveryId}`);
    assert.equal(received.body.timestamp, 1800);
    assert.equal(received.body.text, envelope.message);
    assert.equal(typeof received.body.signature, "string");
    assert.equal(verifyDetached(
      null,
      Buffer.from(JSON.stringify([
        received.body.eventId,
        received.body.timestamp,
        received.body.text,
      ])),
      publicKey,
      Buffer.from(received.body.signature, "base64url"),
    ), true);
  } finally {
    server.close();
    await once(server, "close");
  }
});

test("uses a stable id for bounded retries in the same minute", () => {
  assert.equal(eventId(envelope, 61_000), eventId(envelope, 119_999));
  assert.notEqual(eventId(envelope, 119_999), eventId(envelope, 120_000));
});

test("rejects malformed envelopes and unsafe targets", () => {
  assert.deepEqual(parseEnvelope(JSON.stringify(envelope)), envelope);
  assert.throws(() => parseEnvelope("{}"), InjectorError);
  assert.throws(() => parseTarget("http://example.com/work/events"), InjectorError);
  assert.throws(() => parseTarget("https://user:pass@example.com/work/events"), InjectorError);
});

test("does not acknowledge an ingress without an active subscriber", async () => {
  const server = createServer((_request, response) => {
    response.writeHead(202, { "content-type": "application/json" });
    response.end(JSON.stringify({ accepted: true, subscriptions: 0 }));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  try {
    await assert.rejects(
      inject(envelope, {
        privateKey,
        url: `http://127.0.0.1:${address.port}/work/garden-events`,
      }),
      /no active subscriber/,
    );
  } finally {
    server.close();
    await once(server, "close");
  }
});
