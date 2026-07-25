// block_listener.js — subscribes to Network.getBlockEvents() and emits per-tx
// commit events on stdout as NDJSON:
//
//   {"type":"listener_ready","peer":"...","channel":"...","t":<ms>}
//   {"type":"block_committed","block_num":N,"t_seen":<ms>,"num_txs":M,
//    "txs":[{"tx_id":"<fabric_txid>","validation_code":<int>,"valid":true|false}, ...]}
//   {"type":"listener_stopped","t":<ms>}
//
// The tx_id here is Fabric's transaction id (envelope's channel header).
// Correlate with submit.js's fabric_txid to build the full timeline.

const { connectGateway } = require("./gateway");
const gateway = require("@hyperledger/fabric-gateway");

const NOW = () => Date.now();

// TX_VALIDATION_CODE mapping (from common/txvalidator/vscc.go).
// 0 == VALID; anything else is a failure with the given code.
const VALIDATION_CODE_NAMES = {
  0: "VALID",
  1: "NIL_ENVELOPE",
  2: "BAD_PAYLOAD",
  3: "BAD_COMMON_HEADER",
  4: "BAD_CREATOR_SIGNATURE",
  5: "INVALID_ENDORSER_TRANSACTION",
  6: "INVALID_CONFIG_TRANSACTION",
  7: "UNSUPPORTED_TX_PAYLOAD",
  8: "BAD_PROPOSAL_TXID",
  9: "DUPLICATE_TXID",
  10: "ENDORSEMENT_POLICY_FAILURE",
  11: "MVCC_READ_CONFLICT",
  12: "PHANTOM_READ_CONFLICT",
  13: "UNKNOWN_TX_TYPE",
  14: "TARGET_CHAIN_NOT_FOUND",
  15: "MARSHAL_TX_ERROR",
  16: "NIL_TXACTION",
  17: "EXPIRED_CHAINCODE",
  18: "CHAINCODE_VERSION_CONFLICT",
  19: "BAD_HEADER_EXTENSION",
  20: "BAD_CHANNEL_HEADER",
  21: "BAD_RESPONSE_PAYLOAD",
  22: "BAD_RWSET",
  23: "ILLEGAL_WRITESET",
  24: "INVALID_WRITESET",
  25: "INVALID_CHAINCODE",
  254: "NOT_VALIDATED",
  255: "INVALID_OTHER_REASON",
};

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
    startBlock: process.env.START_BLOCK ? BigInt(process.env.START_BLOCK) : null,
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--ccp") out.ccp = argv[++i];
    else if (a === "--msp-id") out.mspId = argv[++i];
    else if (a === "--user-msp-dir") out.userMspDir = argv[++i];
    else if (a === "--peer") out.peer = argv[++i];
    else if (a === "--channel") out.channel = argv[++i];
    else if (a === "--chaincode") out.chaincode = argv[++i];
    else if (a === "--start-block") out.startBlock = BigInt(argv[++i]);
  }
  if (!out.ccp) throw new Error("--ccp <path> or CCP_PATH env is required");
  if (!out.userMspDir)
    throw new Error("--user-msp-dir <path> or USER_MSP_DIR env is required");
  return out;
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
    type: "listener_ready",
    peer: gw.peerName,
    channel: opts.channel,
    t: NOW(),
  });

  // Handle SIGINT/SIGTERM cleanly.
  const stop = () => {
    try {
      gw.close();
    } catch (e) {}
    emit({ type: "listener_stopped", t: NOW() });
    process.exit(0);
  };
  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);

  // Use "getChaincodeEvents" is chaincode-specific — we want block events for
  // ALL txs including commit-side failures. Use getBlockEvents on Network.
  const startOpts = opts.startBlock !== null
    ? { startBlock: opts.startBlock }
    : {};
  const events = await gw.network.getBlockEvents(startOpts);

  for await (const block of events) {
    const t_seen = NOW();
    const header = block.getHeader();
    const blockNum = Number(header.getNumber());
    const data = block.getData();
    const dataList = data ? data.getDataList_asU8() : [];
    const metadata = block.getMetadata();
    // Metadata index 2 (BlockMetadataIndex.TRANSACTIONS_FILTER) is a byte
    // array with one entry per tx in the block: validation code per tx.
    const filterBytes = metadata
      ? metadata.getMetadataList_asU8()[2] || new Uint8Array()
      : new Uint8Array();

    // Parse each envelope to extract the channel header (tx id).
    // Envelope wrapping is common/common.proto:
    //   Envelope { payload: Payload }
    //   Payload  { header: Header { channel_header: ChannelHeader { tx_id, ... } } }
    // We don't want to pull in google-protobuf here; fabric-gateway ships
    // the decoded structures via its `blocks` helpers. But we can also
    // deserialize manually using google-protobuf, which is a transitive
    // dep — pull it in.
    const protoCommon = require("@hyperledger/fabric-protos/lib/common");
    const txs = [];
    for (let i = 0; i < dataList.length; i++) {
      let txId = null;
      try {
        const env = protoCommon.Envelope.deserializeBinary(dataList[i]);
        const payload = protoCommon.Payload.deserializeBinary(
          env.getPayload_asU8()
        );
        const header = payload.getHeader();
        const chHdr = protoCommon.ChannelHeader.deserializeBinary(
          header.getChannelHeader_asU8()
        );
        txId = chHdr.getTxId();
      } catch (e) {
        // Config txs or malformed envelopes — record with empty tx_id.
      }
      const vcode = filterBytes[i] !== undefined ? filterBytes[i] : 255;
      txs.push({
        tx_id: txId,
        validation_code: vcode,
        validation_name: VALIDATION_CODE_NAMES[vcode] || `CODE_${vcode}`,
        valid: vcode === 0,
      });
    }

    emit({
      type: "block_committed",
      block_num: blockNum,
      t_seen,
      num_txs: dataList.length,
      valid_count: txs.filter((t) => t.valid).length,
      txs,
    });
  }
}

main().catch((e) => {
  process.stderr.write(
    "block_listener.js: fatal: " + (e && e.stack ? e.stack : e) + "\n"
  );
  process.exit(1);
});
