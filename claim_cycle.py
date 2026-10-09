#!/usr/bin/env python3
"""Add-on demonstration for an existing MultiChain IoV chain.
Does not delete/recreate chain data. Commands: cycle, claim VEHICLE_ID, action VEHICLE_ID, status.
"""
import base64, json, os, sys, time, urllib.request, urllib.error
from pathlib import Path

CHAIN = "iovchain"
HOME = Path.home()
MULTICHAIN_HOME = Path(os.environ.get("APPDATA", str(HOME / "AppData" / "Roaming"))) / "MultiChain"
NODES = {
    "TA":   {"datadir": MULTICHAIN_HOME, "rpcport": 7446},
    "RSU1": {"datadir": HOME / "iov" / "rsu1", "rpcport": 7448},
    "RSU2": {"datadir": HOME / "iov" / "rsu2", "rpcport": 7450},
    "VEH":  {"datadir": HOME / "iov" / "veh",  "rpcport": 7452},
}
ASSET = "TrustCredit"
CLAIM_STREAM = "reward_claims"
STATE_FILE = Path(__file__).with_name("iov_roles.json")


def rpc(node, method, *params):
    conf = NODES[node]["datadir"] / CHAIN / "multichain.conf"
    if not conf.exists():
        raise RuntimeError(f"Missing RPC config: {conf}. Is {node} daemon running?")
    vals = {}
    for line in conf.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1); vals[k.strip()] = v.strip()
    if not vals.get("rpcuser") or not vals.get("rpcpassword"):
        raise RuntimeError(f"rpcuser/rpcpassword missing in {conf}")
    payload = json.dumps({"method": method, "params": list(params), "id": "iov-claim-cycle"}).encode()
    req = urllib.request.Request(f"http://127.0.0.1:{NODES[node]['rpcport']}", data=payload,
                                 headers={"Content-Type": "application/json"})
    token = base64.b64encode(f"{vals['rpcuser']}:{vals['rpcpassword']}".encode()).decode()
    req.add_header("Authorization", "Basic " + token)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            result = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{node}.{method} HTTP {e.code}: {e.read().decode(errors='replace')}") from e
    if result.get("error"):
        raise RuntimeError(f"{node}.{method}: {result['error'].get('message', result['error'])}")
    return result.get("result")


def state():
    if not STATE_FILE.exists():
        raise RuntimeError(f"Missing {STATE_FILE}. Run your existing iov_trust.py setup first.")
    s = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    if len(s.get("vehicles", [])) != 6 or not s.get("addresses", {}).get("TA"):
        raise RuntimeError("iov_roles.json does not contain the six registered vehicles and TA address.")
    return s


def ensure_claim_stream(s):
    names = {x.get("name") for x in (rpc("TA", "liststreams", "*") or [])}
    if CLAIM_STREAM not in names:
        try:
            rpc("TA", "create", "stream", CLAIM_STREAM, False)
            print(f"Created stream {CLAIM_STREAM} on existing chain.")
        except Exception as e:
            # Recheck in case the stream exists but liststreams indexing was delayed.
            names = {x.get("name") for x in (rpc("TA", "liststreams", "*") or [])}
            if CLAIM_STREAM not in names:
                raise RuntimeError(f"Could not create {CLAIM_STREAM}: {e}")
    ta_addr = s["addresses"]["TA"]
    for perm in (f"{CLAIM_STREAM}.read", f"{CLAIM_STREAM}.write"):
        try: rpc("TA", "grant", ta_addr, perm)
        except Exception as e:
            if not any(w in str(e).lower() for w in ("already", "duplicate", "permission already")):
                print(f"WARNING grant {perm}: {e}")
    try: rpc("TA", "subscribe", CLAIM_STREAM)
    except Exception as e:
        if "already" not in str(e).lower(): print(f"WARNING subscribe: {e}")


def publish(stream, key, obj, address):
    data_hex = json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode().hex()
    return rpc("TA", "publishfrom", address, stream, key, data_hex)


def records(stream):
    try: items = rpc("TA", "liststreamitems", stream, True, 999999)
    except Exception: items = rpc("TA", "liststreamitems", stream)
    out = []
    for it in items or []:
        raw = it.get("data", "")
        try: obj = json.loads(bytes.fromhex(raw).decode()) if raw else {}
        except Exception: obj = {}
        obj["_key"] = (it.get("keys") or [""])[0]
        obj["_txid"] = it.get("txid", "")
        obj["_confirmations"] = it.get("confirmations", 0)
        out.append(obj)
    return out


def latest_claim_for(vehicle):
    matches = [r for r in records(CLAIM_STREAM) if r.get("vehicle_id") == vehicle]
    if not matches: return None
    # Stream listing is in blockchain order; the last matching record is the latest state.
    return matches[-1]


def vehicle(s, vid):
    v = next((x for x in s["vehicles"] if x.get("id", "").upper() == vid.upper()), None)
    if not v: raise RuntimeError(f"Unknown vehicle {vid}; valid IDs are V01 through V06.")
    return v


def cycle():
    s = state(); ensure_claim_stream(s)
    # A fresh cycle id prevents collisions with earlier demonstration runs.
    now = int(time.time())
    cycle_id = f"CYCLE-V03-{now}"
    v03 = vehicle(s, "V03")
    # Record the deduction as a new on-chain trust-score update.
    trust = {"event_id": cycle_id, "vehicle_id": "V03", "address": v03["address"],
             "previous_trust_score": 60.0, "trust_score": 48.0, "report_count": 4,
             "majority_count": 4, "majority_type": "packet_drop_or_false_message",
             "plausibility_passed": False, "status": "active", "analysed_by": "RSU1",
             "analysis_type": "reference_style", "updated_at": now}
    tx_trust = publish("trust_scores", "V03", trust, s["addresses"]["TA"])
    print("\n[1] TRUST DEDUCTION PUBLISHED")
    print("Vehicle V03: 60.0 -> 48.0; reports=4; majority=4; plausibility_passed=false")
    print("Trust-score TXID:", tx_trust)

    # Reward recipient is V01 for this demo cycle; claim is pending until user runs claim V01.
    v01 = vehicle(s, "V01")
    claim_id = f"CLAIM-V01-{now}"
    pending = {"claim_id": claim_id, "cycle_id": cycle_id, "vehicle_id": "V01",
               "recipient_address": v01["address"], "asset": ASSET, "amount": 5,
               "status": "pending", "created_at": now,
               "rule": "claim required before protected action"}
    tx_claim = publish(CLAIM_STREAM, claim_id, pending, s["addresses"]["TA"])
    print("\n[2] REWARD CLAIM CREATED")
    print(f"Claim {claim_id}: 5 {ASSET}; status=pending")
    print("Claim-record TXID:", tx_claim)
    print("\n[3] TRY PROTECTED ACTION BEFORE CLAIM")
    protected_action("V01")
    print("\nCycle prepared. Next run: py -3 claim_cycle.py claim V01")


def token_source(amount):
    # Look across TA wallet addresses for one that actually holds enough of this asset.
    addrs = rpc("TA", "getaddresses") or []
    for addr in addrs:
        try:
            bals = rpc("TA", "getaddressbalances", addr) or []
            for b in bals:
                if b.get("name") == ASSET and float(b.get("qty", 0)) >= amount:
                    return addr, float(b["qty"])
        except Exception:
            continue
    raise RuntimeError(f"No TA-wallet address has at least {amount} {ASSET}. Check TrustCredit balances; no claim was marked claimed.")


def claim(vid):
    s = state(); ensure_claim_stream(s); v = vehicle(s, vid)
    current = latest_claim_for(v["id"])
    if not current:
        raise RuntimeError(f"No reward claim found for {v['id']}. Run `py -3 claim_cycle.py cycle` first.")
    if current.get("status") == "claimed":
        print(f"Claim already completed for {v['id']}; no duplicate transfer sent.")
        print("Claim TXID:", current.get("transfer_txid", "not recorded")); return
    if current.get("status") != "pending":
        raise RuntimeError(f"Latest claim status is {current.get('status')}; cannot claim it.")
    amount = float(current.get("amount", 5))
    src, bal = token_source(amount)
    tx_transfer = rpc("TA", "sendassetfrom", src, v["address"], ASSET, amount)
    print(f"Reward transfer sent: {amount:g} {ASSET} to {v['id']} ({v['address']})")
    print("Transfer TXID:", tx_transfer)
    # Only mark claimed after the transfer RPC succeeds.
    done = dict(current)
    done.update({"status": "claimed", "claimed_at": int(time.time()), "transfer_txid": tx_transfer,
                 "claimed_by": v["id"], "source_address": src})
    tx_record = publish(CLAIM_STREAM, current.get("claim_id", current["_key"]), done, s["addresses"]["TA"])
    print("Claim status transaction TXID:", tx_record)
    print("Claim status: claimed")


def protected_action(vid):
    s = state(); v = vehicle(s, vid)
    try: ensure_claim_stream(s)
    except Exception as e:
        print(f"BLOCKED: claim stream unavailable ({e})"); return False
    latest = latest_claim_for(v["id"])
    if not latest or latest.get("status") != "claimed":
        status = latest.get("status", "no_claim") if latest else "no_claim"
        print(f"BLOCKED: {v['id']} cannot perform protected action; reward claim status={status}.")
        print("Required: submit claim and wait for successful token transfer + claimed record.")
        return False
    # Gate on actual confirmation, not merely on a successful RPC response.
    transfer_txid = latest.get("transfer_txid")
    try:
        transfer_info = rpc("TA", "getrawtransaction", transfer_txid, 1) if transfer_txid else None
    except Exception:
        transfer_info = None
    if int(latest.get("_confirmations", 0)) < 1 or not transfer_info or int(transfer_info.get("confirmations", 0)) < 1:
        print(f"BLOCKED: {v['id']} claim/transfer is not confirmed yet.")
        print(f"Claim-record confirmations={latest.get('_confirmations', 0)}; transfer confirmations={(transfer_info or {}).get('confirmations', 0)}")
        print("Run this action command again after both transactions are confirmed.")
        return False
    tx = publish("session_results", f"PROTECTED-{v['id']}-{int(time.time())}",
                 {"vehicle_id": v["id"], "action": "protected_action", "result": "allowed",
                  "claim_id": latest.get("claim_id"), "claim_status": "claimed", "timestamp": int(time.time())},
                 s["addresses"]["TA"])
    print(f"ALLOWED: {v['id']} protected action recorded after claim.")
    print("Action-record TXID:", tx)
    return True


def status():
    s = state(); ensure_claim_stream(s)
    print("Recent reward claim records:")
    for r in sorted(records(CLAIM_STREAM), key=lambda x: int(x.get("created_at", 0)), reverse=True)[:10]:
        print(f"{r.get('vehicle_id')} | claim={r.get('claim_id')} | status={r.get('status')} | amount={r.get('amount')} {r.get('asset')} | txid={r.get('_txid')} | confirmations={r.get('_confirmations')}")
    print("\nLatest V03 trust records:")
    for r in [x for x in records("trust_scores") if x.get("vehicle_id") == "V03"][-3:]:
        print(f"score={r.get('trust_score')} previous={r.get('previous_trust_score', 'n/a')} txid={r.get('_txid')} confirmations={r.get('_confirmations')}")


def usage():
    print("Usage: py -3 claim_cycle.py cycle | claim V01 | action V01 | status")

if __name__ == "__main__":
    try:
        if len(sys.argv) < 2: usage(); sys.exit(2)
        cmd = sys.argv[1].lower()
        if cmd == "cycle": cycle()
        elif cmd == "claim" and len(sys.argv) > 2: claim(sys.argv[2].upper())
        elif cmd == "action" and len(sys.argv) > 2: protected_action(sys.argv[2].upper())
        elif cmd == "status": status()
        else: usage(); sys.exit(2)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr); sys.exit(1)
