// submit.js — persistent Node submitter.
//
// Stdin:  NDJSON, one line per block directive:
//   {"type":"block","block_id":123,"txs":[{"id":"tx-1","functionName":"send_payment","arguments":["50","acc0007","acc0134"]}, ...]}
//   {"type":"stop"}
//
// Stdout: NDJSON events (one per line):
//   {"type":"submitter_ready","peer":"peer0.org1.example.com","channel":"mychannel","chaincode":"smallbank","t":<ms>}
//   {"type":"tx_submit","block_id":123,"tx_id":"tx-1","fabric_txid":"...","t_submit_start":<ms>}
//   {"type":"tx_endorse_done","tx_id":"tx-1","fabric_txid":"...","t_endorse_done":<ms>,"err":null}
//   {"type":"tx_broadcast_done","tx_id":"tx-1","fabric_txid":"...","t_broadcast_done":<ms>,"err":null}
//   {"type":"block_done","block_id":123,"t":<ms>,"endorsed":N,"broadcast":M,"failed":F}
//   {"type":"submitter_stopped","t":<ms>}
//
// Errors go to stderr (not on the NDJSON stream). Errors on individual tx go
// as {"err": "..."} in the corresponding event.

const readline = require("readline");
const { connectGateway } = require("./gateway");

const NOW = () => Date.now();

function emit(obj) {
  process.stdout.write(JSON.stringify(obj) + "\n");
}

function parseArgs() {
  const argv = process.argv.slice(2);
  const out = {
    ccp: process.env.CCP_PATH,
    mspId: process.env.MSP_ID || "Org1MSP",
    userMspDir: process.env.USER_MSP_DIR,
    peer: process.env.PEER_NAME || null,
    channel: process.env.CHANNEL || "mychannel",
    chaincode: process.env.CHAINCODE || "smallbank",
    gateMode: process.env.GATE_MODE || "commit",  // "commit" | "broadcast"
    interBlockPauseMs: parseInt(process.env.INTER_BLOCK_PAUSE_MS || "0"),
    // Rate control (P2.6). 0 = unlimited (backward-compat).
    targetTps: parseInt(process.env.TARGET_TPS || "0"),
    maxConcurrency: parseInt(process.env.MAX_CONCURRENCY || "0"),
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--ccp") out.ccp = argv[++i];
    else if (a === "--msp-id") out.mspId = argv[++i];
    else if (a === "--user-msp-dir") out.userMspDir = argv[++i];
    else if (a === "--peer") out.peer = argv[++i];
    else if (a === "--channel") out.channel = argv[++i];
    else if (a === "--chaincode") out.chaincode = argv[++i];
    else if (a === "--gate-mode") out.gateMode = argv[++i];
    else if (a === "--inter-block-pause-ms") out.interBlockPauseMs = parseInt(argv[++i]);
    else if (a === "--target-tps") out.targetTps = parseInt(argv[++i]);
    else if (a === "--max-concurrency") out.maxConcurrency = parseInt(argv[++i]);
  }
  if (!out.ccp) throw new Error("--ccp <path> or CCP_PATH env is required");
  if (!out.userMspDir)
    throw new Error("--user-msp-dir <path> or USER_MSP_DIR env is required");
  if (out.gateMode !== "commit" && out.gateMode !== "broadcast") {
    throw new Error(`--gate-mode must be commit|broadcast, got ${out.gateMode}`);
  }
  // Phase 2.7: fire-and-forget dispatch means "no rate control AND no
  // concurrency cap" would fire every tx of every buffered block into the
  // peer at once, tripping the gateway's server-side concurrency limit
  // (default 2000 on our deploy). Old processBlock accidentally bounded
  // in-flight to one block's worth via Promise.all; the rewrite removes
  // that. Restore the safety by defaulting maxConcurrency to 1000 (matches
  // the historical per-block bound). Pass `--max-concurrency -1` to
  // opt into truly-unbounded dispatch.
  if (out.targetTps <= 0 && (out.maxConcurrency === 0 || Number.isNaN(out.maxConcurrency))) {
    out.maxConcurrency = 1000;
    out.maxConcurrencyDefaulted = true;
  }
  if (out.maxConcurrency < 0) {
    out.maxConcurrency = 0;    // -1 → truly unlimited (opt-in)
  }
  return out;
}

// Background commit-tracker set: promises resolved by tx_commit_done handlers.
// In broadcast gate mode, we push commit-status promises here so main() can
// await them all at shutdown before exiting (metric fidelity).
const _pendingCommits = new Set();

// Phase 2.7 — global fire-and-forget state, shared across all blocks.
//   _pendingTx: every in-flight submitOne(...) promise, added on dispatch,
//               removed in its .finally. Drained once at shutdown.
//   _nextT:     global token-bucket "next dispatch time" in ms since epoch.
//               Preserved across blocks so target TPS is enforced globally,
//               not reset per block (that was the P2.6 bug).
const _pendingTx = new Set();
let _nextT = 0;

function _trackCommit(fabricTxid, blockId, txId, submittedTx) {
  const p = submittedTx.getStatus()
    .then((status) => {
      emit({
        type: "tx_commit_done",
        block_id: blockId,
        tx_id: txId,
        fabric_txid: fabricTxid,
        t_commit_done: NOW(),
        commit_block_num: Number(status.blockNumber),
        commit_code: status.code,
        commit_success: status.successful === true,
        err: null,
      });
    })
    .catch((e) => {
      emit({
        type: "tx_commit_done",
        block_id: blockId,
        tx_id: txId,
        fabric_txid: fabricTxid,
        t_commit_done: NOW(),
        commit_block_num: null,
        commit_code: null,
        commit_success: false,
        err: String(e && e.message ? e.message : e),
      });
    })
    .finally(() => {
      _pendingCommits.delete(p);
    });
  _pendingCommits.add(p);
  return p;
}

async function submitOne(contract, blockId, tx, gateMode) {
  // Timing: t_submit_start captured just before endorsement request.
  const t_submit_start = NOW();
  emit({
    type: "tx_submit",
    block_id: blockId,
    tx_id: tx.id,
    t_submit_start,
  });

  let proposal, transaction, fabric_txid;
  try {
    proposal = contract.newProposal(tx.functionName, {
      arguments: tx.arguments || [],
    });
    fabric_txid = proposal.getTransactionId();
    transaction = await proposal.endorse();
    const t_endorse_done = NOW();
    emit({
      type: "tx_endorse_done",
      block_id: blockId,
      tx_id: tx.id,
      fabric_txid,
      t_endorse_done,
      err: null,
    });
  } catch (e) {
    emit({
      type: "tx_endorse_done",
      block_id: blockId,
      tx_id: tx.id,
      fabric_txid: fabric_txid || null,
      t_endorse_done: NOW(),
      err: String(e && e.message ? e.message : e),
    });
    return { fabric_txid, endorsed: false };
  }

  let submitted;
  try {
    submitted = await transaction.submit();
    const t_broadcast_done = NOW();
    emit({
      type: "tx_broadcast_done",
      block_id: blockId,
      tx_id: tx.id,
      fabric_txid,
      t_broadcast_done,
      err: null,
    });
  } catch (e) {
    emit({
      type: "tx_broadcast_done",
      block_id: blockId,
      tx_id: tx.id,
      fabric_txid,
      t_broadcast_done: NOW(),
      err: String(e && e.message ? e.message : e),
    });
    return { fabric_txid, endorsed: true, broadcast: false };
  }

  // Two gate modes:
  //   "commit"    — await SubmittedTransaction.getStatus() before returning.
  //                 The Promise.all in processBlock then resolves only when
  //                 every tx has committed to world state on the endorsing
  //                 peer. Guarantees the DSatur read-set order. Slow.
  //   "broadcast" — return as soon as the envelope reached the orderer
  //                 (submit() resolved). Fire getStatus in the background
  //                 so tx_commit_done events still land in the metric
  //                 stream, but do NOT gate the pipeline on them. Fast.
  //                 Small MVCC risk on adjacent-block conflicts.
  if (gateMode === "commit") {
    try {
      const status = await submitted.getStatus();
      emit({
        type: "tx_commit_done",
        block_id: blockId,
        tx_id: tx.id,
        fabric_txid,
        t_commit_done: NOW(),
        commit_block_num: Number(status.blockNumber),
        commit_code: status.code,
        commit_success: status.successful === true,
        err: null,
      });
      return { fabric_txid, endorsed: true, broadcast: true, committed: true, submitted };
    } catch (e) {
      emit({
        type: "tx_commit_done",
        block_id: blockId,
        tx_id: tx.id,
        fabric_txid,
        t_commit_done: NOW(),
        commit_block_num: null,
        commit_code: null,
        commit_success: false,
        err: String(e && e.message ? e.message : e),
      });
      return { fabric_txid, endorsed: true, broadcast: true, committed: false, submitted };
    }
  } else {
    // broadcast gate: track commit in background but don't await it here.
    _trackCommit(fabric_txid, blockId, tx.id, submitted);
    return { fabric_txid, endorsed: true, broadcast: true, submitted };
  }
}

async function enqueueBlock(contract, blockId, txs, gateMode, opts) {
  // Phase 2.7 — Caliper-style fire-and-forget dispatch.
  //
  // Unlike the P2.6 `processBlock`, this function never awaits an individual
  // tx's completion. It dispatches all txs of a block into the module-level
  // `_pendingTx` set (which lives across blocks) and returns as soon as the
  // last tx has been fired. That means block B can start being enqueued
  // while block A's txs are still endorsing / broadcasting / committing —
  // no block-boundary drain.
  //
  // Ordering: within a block, `for (tx of txs)` dispatches serially (one tx
  // per event-loop tick modulo rate-control sleeps). Because the main loop
  // still `await`s enqueueBlock, block A's last tx is dispatched before
  // block B's first tx. The peer's mempool sees A's envelopes, then B's,
  // preserving the DSatur ordering that the scheduler produced.
  //
  // Rate control:
  //   - `targetTps > 0`: global token bucket via module-level `_nextT`.
  //     If we've fallen behind, `_nextT` sticks to NOW() (no burst) so
  //     the bucket never lets us fire faster than target.
  //   - `targetTps == 0`: skip rate control entirely — bounded only by
  //     `maxConcurrency`. This is the regression path (matches P2.5).
  //   - `maxConcurrency > 0`: waits with `Promise.race(_pendingTx)` when
  //     the in-flight set is full. Prevents runaway event-loop pressure
  //     if the peer stalls.
  const { targetTps = 0, maxConcurrency = 0 } = opts || {};
  const minGapMs = targetTps > 0 ? 1000 / targetTps : 0;
  let dispatched = 0;

  for (let i = 0; i < txs.length; i++) {
    // 1. GLOBAL token bucket (only if targetTps > 0)
    if (minGapMs > 0) {
      if (_nextT === 0) _nextT = NOW();
      const wait = _nextT - NOW();
      if (wait > 0) {
        await new Promise((r) => setTimeout(r, wait));
      }
      // Do NOT let _nextT run ahead of NOW() — that would build up a
      // burst credit and defeat rate control on the next fast block.
      _nextT = Math.max(NOW(), _nextT + minGapMs);
    }
    // 2. concurrency bound — wait if the GLOBAL in-flight set is full
    while (maxConcurrency > 0 && _pendingTx.size >= maxConcurrency) {
      await Promise.race(_pendingTx);
    }
    // 3. FIRE-AND-FORGET — the tx promise lives in _pendingTx until it
    //    settles; we don't await it. Errors are handled inside submitOne
    //    (per-tx events already emitted), so the .catch here just
    //    prevents an unhandled rejection.
    const p = submitOne(contract, blockId, txs[i], gateMode)
      .catch(() => {})
      .finally(() => {
        _pendingTx.delete(p);
      });
    _pendingTx.add(p);
    dispatched++;
  }

  // "block_done" now means "block fully DISPATCHED" (not "fully committed").
  // Downstream (collector.py) uses this only for a debug log line; per-tx
  // event accounting is unchanged.
  emit({
    type: "block_done",
    block_id: blockId,
    t: NOW(),
    n: txs.length,
    dispatched,
    in_flight: _pendingTx.size,
    gate_mode: gateMode,
    target_tps: targetTps,
    max_concurrency: maxConcurrency,
  });
}

async function main() {
  const opts = parseArgs();
  const gw = await connectGateway({
    ccpPath: opts.ccp,
    mspId: opts.mspId,
    userMspDir: opts.userMspDir,
    peerName: opts.peer,
    channel: opts.channel,
    chaincode: opts.chaincode,
  });
  emit({
    type: "submitter_ready",
    peer: gw.peerName,
    channel: opts.channel,
    chaincode: opts.chaincode,
    gate_mode: opts.gateMode,
    inter_block_pause_ms: opts.interBlockPauseMs,
    target_tps: opts.targetTps,
    max_concurrency: opts.maxConcurrency,
    max_concurrency_defaulted: !!opts.maxConcurrencyDefaulted,
    t: NOW(),
  });

  // Serial per-block loop reads stdin line by line.
  const rl = readline.createInterface({ input: process.stdin, terminal: false });
  const inbox = [];
  let closed = false;
  let waiter = null;

  rl.on("line", (line) => {
    line = line.trim();
    if (!line) return;
    let msg;
    try {
      msg = JSON.parse(line);
    } catch (e) {
      process.stderr.write("submit.js: bad JSON on stdin: " + e.message + "\n");
      return;
    }
    inbox.push(msg);
    if (waiter) {
      const w = waiter;
      waiter = null;
      w();
    }
  });

  rl.on("close", () => {
    closed = true;
    if (waiter) {
      const w = waiter;
      waiter = null;
      w();
    }
  });

  async function nextMsg() {
    if (inbox.length > 0) return inbox.shift();
    if (closed) return null;
    await new Promise((resolve) => (waiter = resolve));
    if (inbox.length > 0) return inbox.shift();
    return null;
  }

  while (true) {
    const msg = await nextMsg();
    if (msg === null) break;
    if (msg.type === "stop") break;
    if (msg.type !== "block") {
      process.stderr.write(
        "submit.js: unknown msg type " + JSON.stringify(msg.type) + "\n"
      );
      continue;
    }
    try {
      await enqueueBlock(gw.contract, msg.block_id, msg.txs || [], opts.gateMode, {
        targetTps: opts.targetTps,
        maxConcurrency: opts.maxConcurrency,
      });
    } catch (e) {
      process.stderr.write(
        "submit.js: enqueueBlock threw: " + (e.message || e) + "\n"
      );
    }
    // Optional pacing between blocks. Rarely useful now that enqueueBlock
    // is fire-and-forget (there is no bursty end-of-block drain to smooth
    // over), but kept for parity with older experiment scripts.
    if (opts.interBlockPauseMs > 0) {
      await new Promise((r) => setTimeout(r, opts.interBlockPauseMs));
    }
  }

  // Phase 2.7 shutdown drain — all in-flight submitOne promises must settle
  // before we can claim we're stopped. Without this, the submitter would
  // exit while dozens/hundreds of txs are still endorsing → their per-tx
  // events would be lost. Bounded via Promise.allSettled (never throws)
  // and a 120s hard cap so a stuck peer doesn't hang exit indefinitely.
  if (_pendingTx.size > 0) {
    process.stderr.write(
      `submit.js: draining ${_pendingTx.size} in-flight tx submissions...\n`
    );
    const drain = Promise.allSettled(Array.from(_pendingTx));
    const timeout = new Promise((r) => setTimeout(r, 120000));
    await Promise.race([drain, timeout]);
    process.stderr.write(
      `submit.js: tx drain done, ${_pendingTx.size} still pending\n`
    );
  }

  // On shutdown in broadcast-gate mode, wait for any in-flight commit
  // trackers so tx_commit_done events land before we stop. Bound this at
  // 60s so a stuck peer doesn't hang exit.
  if (opts.gateMode === "broadcast" && _pendingCommits.size > 0) {
    process.stderr.write(
      `submit.js: draining ${_pendingCommits.size} pending commit trackers...\n`
    );
    const drain = Promise.allSettled(Array.from(_pendingCommits));
    const timeout = new Promise((r) => setTimeout(r, 60000));
    await Promise.race([drain, timeout]);
    process.stderr.write(
      `submit.js: drain done, ${_pendingCommits.size} still pending\n`
    );
  }

  gw.close();
  emit({ type: "submitter_stopped", t: NOW() });
}

main().catch((e) => {
  process.stderr.write("submit.js: fatal: " + (e && e.stack ? e.stack : e) + "\n");
  process.exit(1);
});
