#!/usr/bin/env python3
"""Route Garden Cloud v1.0 notifications to Work using its existing SSE sidecar."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time
from urllib.request import urlopen
import uuid

INGRESS = "https://xlc-general-bridge-lyj.onrender.com/work/garden-events"
INJECTOR_URL = (
    "https://raw.githubusercontent.com/liyanjun921/galatea-garden-wake-bridge/"
    "555cf53e2b89579bad12d0dc875e1fdce87d28f2/integrations/work-events/inject.mjs"
)
INJECTOR_SHA256 = "32d1e8d29b9a97c8ee1e95d294873f11117c46c90746940d46d67f4e06c69e17"

WRAPPER = r"""
'use strict';
async function main() {
  const fs = require('node:fs/promises');
  const { parseEnvelope, inject } = await import('./work-events-inject.mjs');
  let size = 0;
  const chunks = [];
  for await (const chunk of process.stdin) {
    size += chunk.length;
    if (size > 20000) throw new Error('Garden envelope too large');
    chunks.push(chunk);
  }
  const envelope = parseEnvelope(Buffer.concat(chunks).toString('utf8'));
  const deliveryId = process.env.XLC_GARDEN_DELIVERY_ID;
  if (!deliveryId) throw new Error('Missing Garden delivery ID');
  const receipt = await inject(envelope, { deliveryId, timeoutMs: 85_000 });
  const path = '/data/garden/last-work-delivery.json';
  const temporary = path + '.tmp';
  await fs.writeFile(temporary, JSON.stringify({
    ...receipt, acceptedAt: new Date().toISOString(),
  }) + '\n', {mode: 0o600});
  await fs.rename(temporary, path);
}
main().catch(error => {
  const message = error?.constructor?.name === 'InjectorError'
    ? error.message : 'Garden Work delivery failed';
  process.stderr.write(message + '\n');
  process.exitCode = 2;
});
"""

# Runs in a short-lived container with the prospective mounts. No Garden SSE
# connection is opened, and the invalid signature cannot enqueue an event.
PREFLIGHT = r"""
const fs = require('node:fs');
const crypto = require('node:crypto');
async function main() {
  const key = crypto.createPrivateKey(fs.readFileSync(process.env.WORK_EVENTS_GARDEN_PRIVATE_KEY_FILE));
  const actual = crypto.createPublicKey(key).export({format: 'jwk'});
  if (actual.crv !== 'Ed25519' || actual.x !== process.argv[1]) throw Error('SIGNING_KEY_MISMATCH');
  await import('file:///opt/garden-work/work-events-inject.mjs');
  const response = await fetch(process.argv[2], {
    method: 'POST', redirect: 'error', signal: AbortSignal.timeout(75_000),
    headers: {'content-type': 'application/json'},
    body: JSON.stringify({eventId:'garden_config_probe', timestamp:0, text:'config probe', signature:'invalid'}),
  });
  const result = await response.json().catch(() => ({}));
  if (response.status !== 401 || result.error !== 'invalid_signature') {
    throw Error('WORK_INGRESS_NOT_READY_HTTP_' + response.status);
  }
  console.log('WORK_INGRESS_READY');
}
main().catch(error => { console.error(error.message); process.exitCode = 1; });
"""


def write(path, data):
    fd, tmp = tempfile.mkstemp(prefix="." + path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
        os.replace(tmp, path)
    finally:
        Path(tmp).unlink(missing_ok=True)


def save(path, value):
    write(path, (json.dumps(value, indent=2) + "\n").encode())


def load(path):
    return json.loads(path.read_text())


def mount(source, target):
    return {"type": "bind", "source": str(source), "target": target, "read_only": True}


def stage_work(root, definition):
    key = root / "work-signing/private.pem"
    if key.is_symlink() or not key.is_file() or key.stat().st_mode & 0o077:
        raise RuntimeError("Expected a private.pem file with mode 600")

    files = root / "work-adapter"
    files.mkdir(mode=0o700, exist_ok=True)
    with urlopen(INJECTOR_URL, timeout=20) as response:
        module = response.read(64 * 1024)
    if hashlib.sha256(module).hexdigest() != INJECTOR_SHA256:
        raise RuntimeError("Work injector checksum mismatch")

    # Keep the stock installer bundle byte-for-byte intact. The timeout is
    # longer on the HTTPS path so Render has time to start after inactivity.
    original = (root / "code/run-bridge.mjs").read_text()
    line = "await runBridge(config,adapter,logger,shutdown.signal);"
    if original.count(line) != 1:
        raise RuntimeError("Unexpected Garden Cloud runner version")
    runner = original.replace("from './", "from '/opt/garden/").replace(
        "/opt/garden/injector.cjs", "/opt/garden-work/injector.cjs",
    ).replace(
        line,
        "const workConfig={...config,timeouts:{...config.timeouts,runtimeDeliveryMs:95_000}};\n"
        "  await runBridge(workConfig,adapter,logger,shutdown.signal);",
    )
    write(files / "work-events-inject.mjs", module)
    write(files / "injector.cjs", WRAPPER.encode())
    write(files / "run-work-bridge.mjs", runner.encode())

    service = definition["services"]["garden"]
    service["command"] = ["/opt/garden-work/run-work-bridge.mjs"]
    service["environment"]["WORK_EVENTS_GARDEN_PRIVATE_KEY_FILE"] = "/run/garden-work-signing-key"
    service["volumes"].extend([
        mount(files, "/opt/garden-work"),
        mount(key, "/run/garden-work-signing-key"),
    ])


def run(args, timeout=100, capture=False):
    return subprocess.run(args, check=True, timeout=timeout, text=True, capture_output=capture)


def await_connection(state):
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        status = load(state)
        phase = status.get("phase")
        if phase == "connected":
            print("GARDEN_SSE_CONNECTED", flush=True)
            return
        if phase in ("failed", "stopped"):
            raise RuntimeError("GARDEN_SSE_" + str(status.get("failureKind", phase)).upper())
        time.sleep(1)
    raise RuntimeError("GARDEN_SSE_CONNECTION_UNCONFIRMED")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--public-key")
    parser.add_argument("--rollback", action="store_true", help="restore Garden delivery to the original WeChat thread")
    args = parser.parse_args()
    if os.getuid() == 0:
        parser.error("run as the Ubuntu user, without sudo")
    if not args.rollback and not re.fullmatch(r"[A-Za-z0-9_-]{43}", args.public_key or ""):
        parser.error("--public-key requires the public key registered in Work")
    os.umask(0o077)
    root = Path.home() / "xlc-cloud/garden-cloud-v1.0"
    base = root / "compose.json"
    with (root / "worker.lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        definition = load(base)
        if definition.get("name") != "xlc-garden-cloud-v1" or set(definition.get("services", {})) != {"garden"}:
            raise RuntimeError("Unexpected Garden Compose project")
        service = definition["services"]["garden"]
        if service.get("command") != ["/opt/garden/run-bridge.mjs"] or service.get("restart") != "no":
            raise RuntimeError("Expected the original one-attempt Garden Cloud v1.0 setup")

        attempt = str(uuid.uuid4())
        service["environment"]["XLC_GARDEN_START_ATTEMPT"] = attempt
        candidate = root / ("compose.wechat-rollback.json" if args.rollback else "compose.work.json")
        if not args.rollback:
            stage_work(root, definition)
        save(candidate, definition)

        docker = ["sudo", "docker", "compose"]
        selected = docker + ["-f", str(candidate)]
        if not args.rollback:
            run(selected + ["run", "--rm", "--no-deps", "--pull", "never", "--entrypoint", "node",
                            "garden", "-e", PREFLIGHT, args.public_key, INGRESS])

        print("SWITCHING_GARDEN_TARGET=" + ("wechat" if args.rollback else "work"), flush=True)
        # Both files use the same Compose project and garden service.
        run(docker + ["-f", str(base), "stop", "garden"], timeout=45)
        state = root / "sse-state/status.json"
        save(state, {"schema": "xlc.garden.cloud.v1", "phase": "starting", "automaticReconnect": False})
        save(root / "sse-state/attempt.json", {"id": attempt, "used": False})
        run(selected + ["up", "-d", "--no-deps", "--no-build", "--pull", "never", "--force-recreate", "garden"])
        await_connection(state)
        print("GARDEN_TARGET=" + ("wechat" if args.rollback else "work"), flush=True)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        # A subprocess exception may contain its entire argument list.
        detail = str(error) if isinstance(error, RuntimeError) else type(error).__name__
        raise SystemExit("GARDEN_SWITCH_FAILED " + detail)
