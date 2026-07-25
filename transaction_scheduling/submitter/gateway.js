// gateway.js — load CCP + identity, return a connected fabric-gateway Gateway.
//
// Usage:
//   const gw = await connectGateway({
//     ccpPath: "/path/to/connection-org1.yaml",
//     mspId: "Org1MSP",
//     userMspDir: "/path/to/User1@org1.example.com/msp",
//     peerName: "peer0.org1.example.com",   // which peer to open the gRPC channel to
//     channel: "mychannel",
//     chaincode: "smallbank",
//   });
//   gw.contract  → fabric-gateway Contract
//   gw.network   → fabric-gateway Network (for block events)
//   gw.close()   → tears down grpc client + gateway
//
// This is deliberately minimal: one peer connection per Gateway. If the
// submitter wants multi-peer parallelism, spawn multiple Gateways.

const fs = require("fs");
const path = require("path");
const yaml = require("js-yaml");
const grpc = require("@grpc/grpc-js");
const crypto = require("crypto");
const gateway = require("@hyperledger/fabric-gateway");
const { connect, signers } = gateway;

function loadCcp(ccpPath) {
  const text = fs.readFileSync(ccpPath, "utf-8");
  return yaml.load(text);
}

function firstFileIn(dir) {
  const entries = fs.readdirSync(dir);
  if (entries.length === 0) {
    throw new Error(`no files in ${dir}`);
  }
  return path.join(dir, entries[0]);
}

function loadIdentity(mspId, userMspDir) {
  const signCertPath = firstFileIn(path.join(userMspDir, "signcerts"));
  const keyPath = firstFileIn(path.join(userMspDir, "keystore"));
  const cert = fs.readFileSync(signCertPath);
  const key = crypto.createPrivateKey(fs.readFileSync(keyPath));
  return {
    identity: { mspId, credentials: cert },
    signer: signers.newPrivateKeySigner(key),
  };
}

async function newGrpcClient(ccp, peerName) {
  const peerEntry = ccp.peers && ccp.peers[peerName];
  if (!peerEntry) {
    throw new Error(`peer ${peerName} not present in CCP`);
  }
  const url = peerEntry.url;
  const tlsCaPem = peerEntry.tlsCACerts && peerEntry.tlsCACerts.pem;
  if (!tlsCaPem) {
    throw new Error(`peer ${peerName} has no tlsCACerts.pem in CCP`);
  }
  const hostnameOverride =
    (peerEntry.grpcOptions && peerEntry.grpcOptions["ssl-target-name-override"]) ||
    peerName;

  // Strip leading "grpcs://" or "grpc://"
  const target = url.replace(/^grpcs?:\/\//, "");
  const credentials = grpc.credentials.createSsl(Buffer.from(tlsCaPem));
  const client = new grpc.Client(target, credentials, {
    "grpc.ssl_target_name_override": hostnameOverride,
    "grpc.default_authority": hostnameOverride,
    // Fabric block size can exceed the gRPC 4 MB default when
    // MaxMessageCount=1000 and PreferredMaxBytes=8 MB — a single block
    // event carries the whole block payload. Bump both directions to
    // Fabric's AbsoluteMaxBytes (20 MB) plus headroom.
    "grpc.max_receive_message_length": 100 * 1024 * 1024,
    "grpc.max_send_message_length":    100 * 1024 * 1024,
  });
  return client;
}

async function connectGateway({
  ccpPath,
  mspId,
  userMspDir,
  peerName,
  channel,
  chaincode,
}) {
  const ccp = loadCcp(ccpPath);
  if (!peerName) {
    // pick the first peer in the CCP
    peerName = Object.keys(ccp.peers || {})[0];
    if (!peerName) throw new Error("no peers in CCP");
  }
  const client = await newGrpcClient(ccp, peerName);
  const { identity, signer } = loadIdentity(mspId, userMspDir);

  const g = connect({
    client,
    identity,
    signer,
    // Reasonable defaults for local network; callers can tune later.
    evaluateOptions: () => ({ deadline: Date.now() + 5000 }),
    endorseOptions: () => ({ deadline: Date.now() + 15000 }),
    submitOptions: () => ({ deadline: Date.now() + 5000 }),
    commitStatusOptions: () => ({ deadline: Date.now() + 60000 }),
  });
  const network = g.getNetwork(channel);
  const contract = network.getContract(chaincode);
  return {
    gateway: g,
    network,
    contract,
    peerName,
    close: () => {
      try {
        g.close();
      } catch (e) {}
      try {
        client.close();
      } catch (e) {}
    },
  };
}

module.exports = { connectGateway, loadCcp, loadIdentity };
