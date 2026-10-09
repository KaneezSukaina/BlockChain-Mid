import csv, json, os, sys, time, urllib.request, urllib.error, base64, random
from pathlib import Path

# IoV Trust Management for an EXISTING MultiChain chain. Never resets chain data.
CHAIN = "iovchain"
HOME = Path.home()
MULTICHAIN_HOME = Path(os.environ.get("APPDATA", str(HOME / "AppData" / "Roaming"))) / "MultiChain"
NODES = {
    "TA": {"datadir": MULTICHAIN_HOME, "rpcport": 7446, "p2pport": 7447},
    "RSU1": {"datadir": HOME / "iov" / "rsu1", "rpcport": 7448, "p2pport": 7449},
    "RSU2": {"datadir": HOME / "iov" / "rsu2", "rpcport": 7450, "p2pport": 7451},
    "VEH": {"datadir": HOME / "iov" / "veh", "rpcport": 7452, "p2pport": 7453},
}
STREAMS = ["vehicle_registry", "misbehavior_reports", "trust_scores", "session_results", "revocations", "reward_claims"]
ASSET = "TrustCredit"
STATE_FILE = Path(__file__).with_name("iov_roles.json")
BENCH_FILE = Path(__file__).with_name("bench_results.csv")
VEHICLE_COUNT = 6
TRUST_THRESHOLD = 40.0
MIN_REPORTERS = 4
REWARD_CREDIT = 5
ALARMER_BONUS = 2
SOFT_BLOCK_SECONDS = 60
MAX_SPEED, SPEED_TOLERANCE, MAX_HEADING_RATE, MIN_BEACON_GAP, EVENT_RANGE = 70.0, 15.0, 90.0, 0.05, 300.0
RECOVERY_POINTS = 20.0


def conf_values(node):
    conf = NODES[node]["datadir"] / CHAIN / "multichain.conf"
    if not conf.exists():
        raise RuntimeError(f"Missing RPC config: {conf}. Is {node} daemon initialized/running?")
    vals = {}
    for line in conf.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1); vals[k.strip()] = v.strip()
    if not vals.get("rpcuser") or not vals.get("rpcpassword"):
        raise RuntimeError(f"rpcuser/rpcpassword missing in {conf}")
    return vals


def rpc(node, method, *params):
    cfg = conf_values(node)
    url = f"http://127.0.0.1:{NODES[node]['rpcport']}"
    payload = json.dumps({"method": method, "params": list(params), "id": "iov-trust"}).encode()
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    token = base64.b64encode(f"{cfg['rpcuser']}:{cfg['rpcpassword']}".encode()).decode()
    req.add_header("Authorization", "Basic " + token)
    try:
        with urllib.request.urlopen(req, timeout=25) as response:
            result = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"{node}.{method} HTTP {exc.code}: {exc.read().decode(errors='replace')}") from exc
    except Exception as exc:
        raise RuntimeError(f"{node}.{method} failed ({url}): {exc}") from exc
    if result.get("error"):
        err = result["error"]
        raise RuntimeError(f"{node}.{method}: {err.get('message', err)}")
    return result.get("result")


def try_rpc(node, method, *params):
    try: return rpc(node, method, *params)
    except Exception as exc:
        if any(s in str(exc).lower() for s in ("already exists", "already have", "permission already", "already granted", "duplicate")): return None
        raise


def node_address(node):
    addresses = rpc(node, "getaddresses")
    return addresses[0] if isinstance(addresses, list) and addresses else rpc(node, "getnewaddress")


def all_stream_names():
    # MultiChain 2.3.3 accepts liststreams with no "all" parameter.
    return {x.get("name") for x in (rpc("TA", "liststreams") or [])}


def all_asset_names():
    return {x.get("name") for x in (rpc("TA", "listassets") or [])}


def grant_safe(address, permission):
    try:
        rpc("TA", "grant", address, permission); print(f"  granted {permission} -> {address}")
    except Exception as exc:
        if any(s in str(exc).lower() for s in ("already", "duplicate", "permission already")):
            print(f"  already has {permission}: {address}")
        else: print(f"  WARNING grant {permission} to {address}: {exc}")


def load_state():
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            if isinstance(state.get("vehicles"), list) and len(state["vehicles"]) == VEHICLE_COUNT and isinstance(state.get("addresses"), dict): return state
        except Exception: pass
    return {"vehicles": [], "addresses": {}}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def ensure_streams():
    existing = all_stream_names()
    for stream in STREAMS:
        if stream not in existing:
            try_rpc("TA", "create", "stream", stream, False)
            print(f"Created stream if absent: {stream}")
        else: print(f"Reusing stream: {stream}")
    addresses = {n: node_address(n) for n in NODES}
    for addr in addresses.values():
        for stream in STREAMS: grant_safe(addr, f"{stream}.read")
    for node in NODES:
        for stream in STREAMS:
            try: rpc(node, "subscribe", stream)
            except Exception as exc:
                if "already" not in str(exc).lower(): print(f"WARNING subscribe {node}/{stream}: {exc}")
    return addresses


def publish(node, stream, key, obj, from_address=None):
    data_hex = json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8").hex()
    if from_address: return rpc(node, "publishfrom", from_address, stream, key, data_hex)
    return rpc(node, "publish", stream, key, data_hex)


def stream_records(stream):
    # Read the stream from TA, with a conservative count first for compatibility.
    try:
        items = rpc("TA", "liststreamitems", stream, True, 10000)
    except Exception:
        try:
            items = rpc("TA", "liststreamitems", stream)
        except Exception as exc:
            print(f"WARNING could not read {stream}: {exc}")
            return []
    records = []
    for item in items or []:
        raw = item.get("data", "")
        obj = {}
        if isinstance(raw, dict):
            # MultiChain builds may return decoded JSON under a nested "json" key.
            nested = raw.get("json")
            if isinstance(nested, dict):
                obj = nested.copy()
            elif isinstance(nested, str):
                try: obj = json.loads(nested)
                except Exception: obj = {"raw_data": raw}
            else:
                obj = raw.copy()
        elif isinstance(raw, (list, tuple)):
            obj = {"raw_data": raw}
        elif isinstance(raw, str) and raw:
            # MultiChain normally returns hex data; also tolerate plain JSON strings.
            try:
                obj = json.loads(bytes.fromhex(raw).decode("utf-8"))
            except Exception:
                try:
                    obj = json.loads(raw)
                except Exception:
                    obj = {"raw_data": raw}
        if not isinstance(obj, dict):
            obj = {"raw_data": obj}
        obj.setdefault("_key", (item.get("keys") or [""])[0])
        obj.setdefault("_txid", item.get("txid", ""))
        obj.setdefault("_blocktime", item.get("blocktime", 0))
        records.append(obj)
    return records


def latest_by(records, field, timestamp_fields=("updated_at", "timestamp", "revoked_at")):
    out = {}
    for rec in records:
        key = rec.get(field)
        if not key: continue
        ts = max((int(rec.get(f, 0) or 0) for f in timestamp_fields), default=0)
        if key not in out or ts >= out[key][0]: out[key] = (ts, rec)
    return {k: v[1] for k, v in out.items()}


def ensure_reward_pool(addresses):
    """Top up existing RSU reward wallets from TA only when needed."""
    try:
        if ASSET not in all_asset_names():
            print(f"Reward asset {ASSET} is not available yet; no pool funding attempted.")
            return
        for node in ("RSU1", "RSU2"):
            address = addresses[node]
            balances = rpc(node, "getaddressbalances", address)
            current = next((float(x.get("qty", 0)) for x in (balances or []) if x.get("name") == ASSET), 0.0)
            if current >= REWARD_CREDIT:
                print(f"{node} reward pool balance: {current} {ASSET}")
                continue
            amount = 20000
            print(f"{node}: attempting to top up reward pool by {amount} {ASSET} from TA...")
            txid = rpc("TA", "sendassetfrom", addresses["TA"], address, ASSET, amount)
            print(f"{node} pool top-up TXID={txid}")
    except Exception as exc:
        print(f"WARNING reward pool initialization: {exc}")


def setup():
    print("\n=== IoV setup: reusing existing chain; no reset/delete ===")
    for node in NODES:
        info = rpc(node, "getinfo")
        print(f"{node}: chain={info.get('chainname', CHAIN)}, blocks={info.get('blocks', '?')}")
    addresses = ensure_streams()
    assets = all_asset_names()
    if ASSET not in assets:
        try:
            rpc("TA", "issuefrom", addresses["TA"], addresses["TA"], {"name": ASSET, "open": True}, 100000)
            print(f"Issued {ASSET} (100000 units).")
        except Exception as exc: print(f"WARNING issuing {ASSET}: {exc}")
    else: print(f"Reusing existing asset {ASSET}.")
    ensure_reward_pool(addresses)
    state = load_state()
    # Keep existing vehicle addresses whenever there are six valid entries.
    if len(state.get("vehicles", [])) != VEHICLE_COUNT:
        vehicles = []
        for i in range(VEHICLE_COUNT):
            vehicles.append({"id": f"V{i+1:02d}", "address": rpc("VEH", "getnewaddress"), "trust": 100.0, "status": "active"})
        state["vehicles"] = vehicles
    state["addresses"] = addresses
    for node in ("RSU1", "RSU2"):
        grant_safe(addresses[node], "connect,mine")
        for stream in STREAMS: grant_safe(addresses[node], f"{stream}.write")
    grant_safe(addresses["VEH"], "connect")
    for v in state["vehicles"]:
        grant_safe(v["address"], "connect")
        grant_safe(v["address"], "misbehavior_reports.write")
    for stream in STREAMS: grant_safe(addresses["TA"], f"{stream}.write")
    registered = {str(x.get("vehicle_id")) for x in stream_records("vehicle_registry")}
    for v in state["vehicles"]:
        if v["id"] not in registered:
            txid = publish("TA", "vehicle_registry", v["id"], {"vehicle_id": v["id"], "address": v["address"], "initial_trust": 100.0, "status": "active", "registered_at": int(time.time())}, addresses["TA"])
            print(f"Registered {v['id']}: {txid}")
    save_state(state)
    print("Setup complete. Existing chain history was not deleted.")
    for v in state["vehicles"]: print(f"  {v['id']}: {v['address']}")


def report(vehicle_id, session_id, report_type="packet_drop_or_false_message", severity=5, details="Abnormal forwarding behavior observed"):
    state = load_state(); v = next((x for x in state["vehicles"] if x["id"] == vehicle_id), None)
    if not v: raise RuntimeError(f"Unknown vehicle {vehicle_id}; run setup first")
    record = {"report_id": f"R-{session_id}-{vehicle_id}-{int(time.time()*1000)}", "vehicle_id": vehicle_id, "vehicle_address": v["address"], "session_id": session_id, "type": report_type, "severity": int(severity), "details": details, "reporter": vehicle_id, "reporter_id": vehicle_id, "reporter_address": v["address"], "timestamp": int(time.time())}
    txid = publish("VEH", "misbehavior_reports", record["report_id"], record, v["address"])
    print(f"Report published from vehicle address {v['address']}; TXID={txid}")
    return record


def run_session(session_id, participants, outcome="normal", malicious_vehicle=None):
    state = load_state(); print(f"\n--- Session {session_id}: {outcome} ---")
    for vid in participants:
        if not any(v["id"] == vid for v in state["vehicles"]): raise RuntimeError(f"Unknown vehicle {vid}")
        txid = publish("RSU1", "session_results", f"SESSION-{session_id}-{vid}", {"event_type": "session_participation", "session_id": session_id, "vehicle_id": vid, "result": "participated", "outcome": outcome, "timestamp": int(time.time())}, state["addresses"]["RSU1"])
        print(f"Session record {vid}: {txid}")
    if malicious_vehicle: report(malicious_vehicle, session_id)


def publish_multi_reports(suspect, session_id, rsus=("RSU1", "RSU2")):
    state = load_state(); by_id = {v["id"]: v for v in state["vehicles"]}
    if suspect not in by_id: raise RuntimeError(f"Unknown vehicle {suspect}")
    # Select four registered vehicles other than the suspect, so every target can be tested.
    # Each report is published from that reporter's own address via the VEH wallet.
    reporters = [v["id"] for v in state["vehicles"] if v["id"] != suspect][:MIN_REPORTERS]
    if len(reporters) < MIN_REPORTERS:
        raise RuntimeError(f"Need at least {MIN_REPORTERS + 1} registered vehicles to create {MIN_REPORTERS} independent reports")
    for reporter_id in reporters:
        v = by_id[reporter_id]
        record = {"report_id": f"R-{session_id}-{reporter_id}", "vehicle_id": suspect, "vehicle_address": by_id[suspect]["address"], "session_id": session_id, "type": "packet_drop_or_false_message", "severity": 5, "details": "Simulated abnormal forwarding behavior for academic demonstration", "reporter": reporter_id, "reporter_id": reporter_id, "reporter_address": v["address"], "timestamp": int(time.time())}
        txid = publish("VEH", "misbehavior_reports", record["report_id"], record, v["address"])
        print(f"Report reporter={reporter_id}, address={v['address']}, TXID={txid}")
        time.sleep(0.25)
    # Allow the RSU/TA view of the shared chain to catch up before analysis.
    expected_ids = {f"R-{session_id}-{rid}" for rid in reporters}
    deadline = time.time() + 12
    while time.time() < deadline:
        visible = {r.get("report_id") for r in stream_records("misbehavior_reports") if r.get("session_id") == session_id}
        if expected_ids.issubset(visible):
            print(f"Confirmed {len(expected_ids)} session reports visible to TA/RSU analysis.")
            break
        time.sleep(0.5)
    else:
        visible = {r.get("report_id") for r in stream_records("misbehavior_reports") if r.get("session_id") == session_id}
        print(f"WARNING: only {len(visible)}/{len(expected_ids)} session reports are visible to analysis after waiting; inspect stream permissions/subscriptions/data decoding.")


def detect_duplicate_reports(reports):
    seen, duplicates = set(), set()
    for r in reports:
        reporter = r.get("reporter_id") or r.get("reporter"); suspect = r.get("vehicle_id"); session = r.get("session_id")
        key = (session, reporter, suspect)
        if key in seen: duplicates.add(key)
        seen.add(key)
    return duplicates


def make_beacons(kind="normal", vehicle_id="V03"):
    now = time.time()
    if kind == "normal": return [{"vehicle_id": vehicle_id, "speed": s, "heading": h, "x": x, "y": y, "timestamp": now+i} for i,(s,h,x,y) in enumerate([(45,90,100,100),(47,92,102,100),(46,94,104,101)])]
    if kind == "teleport": return [{"vehicle_id": vehicle_id,"speed":40,"heading":90,"x":100,"y":100,"timestamp":now},{"vehicle_id":vehicle_id,"speed":300,"heading":90,"x":10000,"y":10000,"timestamp":now+1}]
    if kind == "erratic": return [{"vehicle_id":vehicle_id,"speed":45,"heading":h,"x":100+i,"y":100,"timestamp":now+i} for i,h in enumerate((0,180,0))]
    raise RuntimeError("Beacon type must be normal, teleport, or erratic")


def plausibility(beacons, claim=None, own_view=None):
    if not beacons: return False
    prev = None
    for b in beacons:
        if float(b.get("speed", 0)) > MAX_SPEED + SPEED_TOLERANCE: return False
        if prev is not None:
            dt = float(b.get("timestamp",0))-float(prev.get("timestamp",0))
            if dt > 0:
                dh = abs(float(b.get("heading",0))-float(prev.get("heading",0)))
                if dh > 180: dh = 360-dh
                if dh/dt > MAX_HEADING_RATE or dt < MIN_BEACON_GAP: return False
        prev = b
    if claim and own_view:
        d = ((float(claim.get("x",0))-float(own_view.get("x",0)))**2 + (float(claim.get("y",0))-float(own_view.get("y",0)))**2)**0.5
        if d > EVENT_RANGE: return False
    return True


def analyse_reference(rsu="RSU1", session_id=None):
    if rsu not in ("RSU1", "RSU2"): raise RuntimeError("RSU must be RSU1 or RSU2")
    state = load_state(); reports = stream_records("misbehavior_reports")
    if session_id:
        reports = [r for r in reports if r.get("session_id") == session_id]
    print(f"\n=== Reference-style analysis by {rsu}; session={session_id or 'ALL'} ===")
    for vehicle in state["vehicles"]:
        vid = vehicle["id"]
        relevant = [r for r in reports if r.get("vehicle_id") == vid]
        # Only one report per reporter per session; preserve session scope for idempotency.
        unique = {}
        for r in relevant:
            reporter = r.get("reporter_id") or r.get("reporter")
            if reporter: unique[(r.get("session_id"), reporter)] = r
        if session_id: unique = {k:v for k,v in unique.items() if k[0] == session_id}
        unique_reports = list(unique.values())
        reporter_ids = {r.get("reporter_id") or r.get("reporter") for r in unique_reports}
        if len(reporter_ids) < MIN_REPORTERS:
            print(f"{vid}: only {len(reporter_ids)} unique reporters; no deduction")
            continue
        counts = {}
        for r in unique_reports: counts[r.get("type", "unknown")] = counts.get(r.get("type", "unknown"), 0) + 1
        majority_type, majority_count = max(counts.items(), key=lambda x:x[1])
        if majority_count <= len(unique_reports)/2:
            print(f"{vid}: no strict majority; no deduction"); continue
        sid = session_id or "ALL-HISTORY"
        analysis_id = f"{vid}|{sid}"
        if any(r.get("analysis_id") == analysis_id for r in stream_records("trust_scores")):
            print(f"{vid}: {sid} already analysed; skipping duplicate deduction"); continue
        previous_record = latest_by([r for r in stream_records("trust_scores") if r.get("vehicle_id")==vid], "vehicle_id").get(vid)
        current = float(previous_record.get("trust_score",100.0)) if previous_record else 100.0
        plausible = plausibility(make_beacons("teleport" if any(int(r.get("severity",0))>=4 for r in unique_reports) else "normal", vid))
        new = max(0.0, current-12.0)
        record = {"vehicle_id":vid,"address":vehicle["address"],"trust_score":new,"previous_trust_score":current,"deduction":current-new,"report_count":len(unique_reports),"majority_count":majority_count,"majority_type":majority_type,"plausibility_passed":plausible,"evidence_reporters":sorted(str(x) for x in reporter_ids),"evidence_report_txids":[r.get("_txid","") for r in unique_reports],"analysis_id":analysis_id,"analysis_session_id":sid,"analysis_type":"reference_style","status":"active" if new>TRUST_THRESHOLD else "flagged","analysed_by":rsu,"updated_at":int(time.time())}
        txid = publish(rsu,"trust_scores",f"TRUST-{vid}-{sid}",record,state["addresses"][rsu])
        print(f"{vid}: {current:.1f} -> {new:.1f}; reporters={len(reporter_ids)}; score TXID={txid}")
        if not plausible: print(f"  Plausibility test also flagged abnormal beacon sequence for {vid}.")
        for rep in sorted(reporter_ids): create_reward_claim(rep, vid, sid, rsu=rsu)


def create_reward_claim(reporter_id, suspect, session_id, amount=REWARD_CREDIT, rsu="RSU1"):
    state = load_state(); by_id = {v["id"]:v for v in state["vehicles"]}
    if reporter_id not in by_id: print(f"Cannot create reward claim for unknown reporter {reporter_id}"); return None
    claim_id = f"CLAIM-{session_id}-{suspect}-{reporter_id}"
    existing = [r for r in stream_records("reward_claims") if r.get("claim_id")==claim_id]
    if existing:
        print(f"Reward claim already exists for {reporter_id}: {existing[-1].get('_txid','')}"); return existing[-1]
    rec = {"event_type":"reporter_reward_claim","claim_id":claim_id,"vehicle_id":reporter_id,"vehicle_address":by_id[reporter_id]["address"],"suspect":suspect,"session_id":session_id,"reward_asset":ASSET,"reward_amount":amount,"status":"pending","created_by":rsu,"timestamp":int(time.time())}
    txid = publish(rsu,"reward_claims",claim_id,rec,state["addresses"][rsu])
    print(f"Pending reward claim for {reporter_id}: {amount} {ASSET}; TXID={txid}")
    return rec


def claim_reward(reporter_id, claim_id=None, rsu="RSU1"):
    state = load_state(); by_id = {v["id"]:v for v in state["vehicles"]}
    if reporter_id not in by_id: raise RuntimeError(f"Unknown reporter {reporter_id}")
    records = [r for r in stream_records("reward_claims") if r.get("vehicle_id")==reporter_id and (not claim_id or r.get("claim_id")==claim_id)]
    # Claims are immutable events; identify pending claims that have not already been claimed.
    claimed_ids = {r.get("claim_id") for r in records if r.get("status")=="claimed"}
    pending = [r for r in records if r.get("status")=="pending" and r.get("claim_id") not in claimed_ids]
    if not pending: raise RuntimeError("No pending reward claim found for this vehicle.")
    claim = max(pending,key=lambda r:int(r.get("timestamp",0)))
    if claim.get("vehicle_address") != by_id[reporter_id]["address"]: raise RuntimeError("Claim address does not match registered reporter address")
    txid = rpc(rsu,"sendassetfrom",state["addresses"][rsu],claim["vehicle_address"],ASSET,claim["reward_amount"])
    print(f"Actual {ASSET} transfer TXID={txid}")
    confirmed = dict(claim); confirmed.update({"status":"claimed","claimed_by":reporter_id,"transfer_txid":txid,"timestamp":int(time.time())})
    status_txid = publish(rsu,"reward_claims",f"{claim['claim_id']}-CLAIMED",confirmed,state["addresses"][rsu])
    print(f"Claim marked claimed on-chain; TXID={status_txid}")
    return confirmed


def reward_status():
    state=load_state(); print("\n=== TrustCredit balances ===")
    for node in ("TA","RSU1","RSU2"):
        try:
            addr=state["addresses"][node]; balances=rpc(node,"getaddressbalances",addr)
            qty=next((b.get("qty",0) for b in balances or [] if b.get("name")==ASSET),0)
            print(f"{node}: {qty} {ASSET}")
        except Exception as exc: print(f"{node}: ERROR {exc}")
    print("\n=== Reward claims ===")
    for r in stream_records("reward_claims"):
        if r.get("event_type")=="reporter_reward_claim": print(f"{r.get('claim_id')} | {r.get('vehicle_id')} | {r.get('status')} | transfer={r.get('transfer_txid','-')} | TXID={r.get('_txid','')}")


def access_control(vehicle_id, operation="protected_operation", rsu="RSU1", require_claim=False):
    if rsu not in ("RSU1","RSU2"): raise RuntimeError("RSU must be RSU1 or RSU2")
    state=load_state(); vehicle=next((v for v in state.get("vehicles",[]) if v["id"]==vehicle_id),None)
    allowed=False; reason=""
    if vehicle is None: reason="vehicle_not_registered"
    else:
        if require_claim:
            claims=[r for r in stream_records("reward_claims") if r.get("vehicle_id")==vehicle_id and r.get("event_type")=="reporter_reward_claim"]
            claimed={r.get("claim_id") for r in claims if r.get("status")=="claimed"}
            pending=[r for r in claims if r.get("status")=="pending" and r.get("claim_id") not in claimed]
            if pending: reason="reward_claim_required"
            elif not claimed: reason="reward_not_claimed" if claims else "reward_claim_required"
        if not reason:
            revs=[r for r in stream_records("revocations") if r.get("vehicle_id")==vehicle_id]
            rev=max(revs,key=lambda r:int(r.get("revoked_at",0)),default=None)
            reinstates=[r for r in stream_records("session_results") if r.get("event_type")=="vehicle_reinstatement" and r.get("vehicle_id")==vehicle_id]
            reinst=max(reinstates,key=lambda r:int(r.get("timestamp",0)),default=None)
            active=False
            if rev and not (reinst and int(reinst.get("timestamp",0))>int(rev.get("revoked_at",0))):
                active = rev.get("level")=="hard" or (rev.get("level")=="soft" and int(time.time())<int(rev.get("until",0)))
            if active: reason="active_revocation"
            else:
                scores=[r for r in stream_records("trust_scores") if r.get("vehicle_id")==vehicle_id]
                latest=max(scores,key=lambda r:int(r.get("updated_at",r.get("timestamp",0))),default=None)
                if latest is None: reason="trust_score_not_available"
                elif float(latest.get("trust_score",0))<=TRUST_THRESHOLD: reason="trust_score_at_or_below_threshold"
                else: allowed=True; reason="trusted_vehicle"
    decision="GRANTED" if allowed else "DENIED"
    print(f"Access {decision}: vehicle={vehicle_id}, operation={operation}, reason={reason}")
    audit={"event_type":"access_control","vehicle_id":vehicle_id,"operation":operation,"decision":decision,"reason":reason,"checked_by":rsu,"timestamp":int(time.time())}
    try:
        txid=publish(rsu,"session_results",f"ACCESS-{vehicle_id}-{int(time.time()*1000)}",audit,state["addresses"][rsu]); print(f"Access audit TXID={txid}")
    except Exception as exc: print(f"WARNING access audit failed: {exc}; protected operation must not proceed"); return False
    return allowed


def revoke_permission_safe(address, permission):
    try: rpc("TA","revokefrom",node_address("TA"),address,permission); print(f"Revoked {permission} from {address}"); return True
    except Exception as exc:
        if any(x in str(exc).lower() for x in ("does not have","not found","permission")): print(f"Permission absent: {permission}"); return True
        print(f"WARNING revoke {permission}: {exc}"); return False


def enforce():
    state=load_state(); scores=latest_by(stream_records("trust_scores"),"vehicle_id",("updated_at","timestamp")); revs=latest_by(stream_records("revocations"),"vehicle_id",("revoked_at","timestamp"))
    print("\n=== TA enforcement ===")
    for v in state.get("vehicles",[]):
        vid=v["id"]; rec=scores.get(vid)
        if not rec: print(f"{vid}: no trust score; skipped"); continue
        score=float(rec.get("trust_score",100))
        if score>TRUST_THRESHOLD: print(f"{vid}: score={score:.1f}; no new restriction"); continue
        level="hard" if score<20 else "soft"; perms=["send","receive","misbehavior_reports.write"] if level=="hard" else ["send","misbehavior_reports.write"]
        old=revs.get(vid)
        if old and old.get("level")=="hard": print(f"{vid}: hard revocation already recorded"); continue
        until=None if level=="hard" else int(time.time()+SOFT_BLOCK_SECONDS)
        for p in perms: revoke_permission_safe(v["address"],p)
        rev={"vehicle_id":vid,"address":v["address"],"level":level,"trust_score":score,"reason":"trust score at or below configured threshold","revoked_at":int(time.time())}
        if until: rev["until"]=until
        txid=publish("TA","revocations",f"{level.upper()}-{vid}-{int(time.time()*1000)}",rev,state["addresses"]["TA"])
        v["status"]="revoked" if level=="hard" else "restricted"; print(f"{vid}: {level} restriction TXID={txid}")
    save_state(state)


def recover_vehicle(vehicle_id, rsu="RSU1"):
    if rsu not in ("RSU1","RSU2"): raise RuntimeError("RSU must be RSU1 or RSU2")
    state=load_state(); v=next((x for x in state["vehicles"] if x["id"]==vehicle_id),None)
    if not v: raise RuntimeError(f"Unknown vehicle {vehicle_id}")
    scores=[r for r in stream_records("trust_scores") if r.get("vehicle_id")==vehicle_id]
    if not scores: print("Recovery denied: no trust score"); return False
    latest=max(scores,key=lambda r:int(r.get("updated_at",r.get("timestamp",0))))
    old=float(latest.get("trust_score",0)); new=min(100.0,old+RECOVERY_POINTS)
    txid=publish(rsu,"trust_scores",f"RECOVERY-{vehicle_id}-{int(time.time()*1000)}",{"vehicle_id":vehicle_id,"address":v["address"],"trust_score":new,"previous_score":old,"event":"trust_recovery","recovery_points":RECOVERY_POINTS,"timestamp":int(time.time()),"updated_at":int(time.time())},state["addresses"][rsu])
    print(f"{vehicle_id}: trust {old:.1f} -> {new:.1f}; TXID={txid}")
    if new>TRUST_THRESHOLD:
        txid2=publish(rsu,"session_results",f"REINSTATE-{vehicle_id}-{int(time.time()*1000)}",{"event_type":"vehicle_reinstatement","vehicle_id":vehicle_id,"new_trust_score":new,"reason":"trust_score_recovered_above_threshold","approved_by":rsu,"timestamp":int(time.time())},state["addresses"][rsu])
        for p in ("send","receive","misbehavior_reports.write"): grant_safe(v["address"],p)
        v["trust"]=new; v["status"]="active"; save_state(state); print(f"Reinstatement TXID={txid2}"); return True
    print(f"Still restricted: score must exceed {TRUST_THRESHOLD}"); return False


def status():
    state=load_state(); scores=latest_by(stream_records("trust_scores"),"vehicle_id",("updated_at","timestamp")); revs=latest_by(stream_records("revocations"),"vehicle_id",("revoked_at","timestamp")); reinst=latest_by([r for r in stream_records("session_results") if r.get("event_type")=="vehicle_reinstatement"],"vehicle_id",("timestamp",))
    print("Vehicle  Trust  Status        Address\n-------  -----  ------------  -------")
    for v in state.get("vehicles",[]):
        vid=v["id"]; score=float(scores.get(vid,{}).get("trust_score",v.get("trust",100))); st=v.get("status","active"); rev=revs.get(vid)
        if rev:
            if reinst.get(vid) and int(reinst[vid].get("timestamp",0))>int(rev.get("revoked_at",0)): st="active"
            elif rev.get("level")=="hard": st="hard-revoked"
            elif rev.get("level")=="soft" and int(time.time())<int(rev.get("until",0)): st="soft-revoked"
            else: st="active"
        print(f"{vid:<7}  {score:>5.1f}  {st:<12}  {v['address']}")
    print("\nNode connectivity:")
    for n in NODES:
        try: print(f"{n}: blocks={rpc(n,'getblockcount')}, peers={len(rpc(n,'getpeerinfo') or [])}")
        except Exception as exc: print(f"{n}: ERROR {exc}")


def transaction_cycle(suspect=None, session_id=None, rsu="RSU1"):
    """Run one simulated incident against any selected vehicle.

    If suspect is omitted or "RANDOM", choose a target dynamically. Four other
    vehicles report it. Each reporter must claim its reward before the protected
    next-session action is allowed.
    """
    state = load_state()
    vehicles = state.get("vehicles", [])
    ids = [v.get("id") for v in vehicles]
    if not ids:
        raise RuntimeError("No registered vehicles found; run setup first")
    if suspect is None or str(suspect).upper() in ("RANDOM", "AUTO"):
        suspect = random.choice(ids)
    else:
        suspect = str(suspect).upper()
    if suspect not in ids:
        raise RuntimeError(f"Unknown target {suspect}. Choose one of: {', '.join(ids)}, or RANDOM")
    reporters = [vid for vid in ids if vid != suspect][:MIN_REPORTERS]
    if len(reporters) < MIN_REPORTERS:
        raise RuntimeError(f"Need at least {MIN_REPORTERS + 1} registered vehicles for this cycle")

    sid = session_id or f"CYCLE-{int(time.time())}-{random.randint(100,999)}"
    print(f"\n{'='*16} IoV FULL TRANSACTION CYCLE: {sid} {'='*16}")
    print(f"Target/simulated malicious vehicle={suspect}")
    print(f"Four independent reporters={', '.join(reporters)}")

    # 1. Four distinct vehicles publish evidence against the selected target.
    publish_multi_reports(suspect, sid)

    # 2. RSU reads this session's reports, checks majority, and writes a score decision.
    analyse_reference(rsu, sid)

    # 3. TA applies configured score/revocation rules.
    enforce()

    # 4. Each reporter's pending reward claim gates access to the next IoV session.
    #    A missing/unclaimed claim must result in DENIED; failed transfer stays blocked.
    print("\n=== Reporter reward-claim and next-session access gate ===")
    for reporter in reporters:
        claim_id = f"CLAIM-{sid}-{suspect}-{reporter}"
        print(f"\n--- {reporter}: access attempt BEFORE claiming reward ---")
        access_control(reporter, "join_next_iov_session", rsu, require_claim=True)

        try:
            claim_reward(reporter, claim_id, rsu)
        except Exception as exc:
            print(f"{reporter} reward claim NOT completed: {exc}")

        print(f"--- {reporter}: access attempt AFTER claim attempt ---")
        access_control(reporter, "join_next_iov_session", rsu, require_claim=True)

    # The suspected vehicle is not automatically allowed to enter; it still must
    # satisfy claim, trust-score, and revocation checks.
    print(f"\n--- Target {suspect}: protected next-session access check ---")
    access_control(suspect, "join_next_iov_session", rsu, require_claim=True)

    print("\n--- Final balances and vehicle status ---")
    reward_status()
    status()


def demo():
    state=load_state()
    if len(state.get("vehicles",[]))!=VEHICLE_COUNT: raise RuntimeError("Run setup first")
    ids=[v["id"] for v in state["vehicles"]]
    run_session("DEMO-NORMAL",ids[:4],"normal")
    transaction_cycle("RANDOM",f"DEMO-{int(time.time())}")


def simulate(session_id):
    sid=session_id.upper(); scenarios={"S1":("normal",["V01","V02","V03","V04"],None),"S2":("misbehavior",["V02","V03","V04","V05"],"V03"),"S3":("post-mitigation",["V01","V02","V04","V05","V06"],None)}
    if sid not in scenarios: raise RuntimeError("Scenario must be S1, S2, or S3")
    outcome,participants,bad=scenarios[sid]; run_session(sid,participants,outcome,bad)


def beacon_test(vehicle_id="V03", beacon_type="normal"):
    result=plausibility(make_beacons(beacon_type.lower(),vehicle_id.upper()))
    print(f"Vehicle={vehicle_id.upper()} | Beacon={beacon_type.lower()} | Plausibility={'PASS' if result else 'FAIL'}")


def bench(count):
    count=int(count)
    if count<1: raise RuntimeError("Transaction count must be >= 1")
    state=load_state()
    if not state.get("addresses"): raise RuntimeError("Run setup first")
    start=time.perf_counter()
    for i in range(count): publish("RSU1","session_results",f"BENCH-{int(time.time())}-{i}",{"benchmark":True,"sequence":i+1,"count":count,"timestamp":int(time.time())},state["addresses"]["RSU1"])
    elapsed=time.perf_counter()-start; tps=count/elapsed if elapsed else 0; exists=BENCH_FILE.exists()
    with BENCH_FILE.open("a",newline="",encoding="utf-8") as f:
        w=csv.writer(f)
        if not exists: w.writerow(["transaction_count","execution_time_seconds","throughput_transactions_per_second"])
        w.writerow([count,f"{elapsed:.6f}",f"{tps:.6f}"])
    print(f"Transactions={count}; time={elapsed:.3f}s; throughput={tps:.2f} tx/s; saved {BENCH_FILE}")


def usage():
    print("""Usage:
  py -3 iov_trust.py setup
  py -3 iov_trust.py demo
  py -3 iov_trust.py transaction-cycle [V01-V06|RANDOM] [optional_session_id]
  py -3 iov_trust.py status
  py -3 iov_trust.py simulate S1|S2|S3
  py -3 iov_trust.py analyse RSU1|RSU2
  py -3 iov_trust.py analyse-reference [RSU1|RSU2] [optional_session_id]
  py -3 iov_trust.py enforce
  py -3 iov_trust.py access V01 [operation]
  py -3 iov_trust.py access-claim V01 [operation]
  py -3 iov_trust.py claim V01 [claim_id]
  py -3 iov_trust.py recover V03
  py -3 iov_trust.py rewards
  py -3 iov_trust.py beacon-test V03 normal|teleport|erratic
  py -3 iov_trust.py bench 1|100|500|1000
""")


def main(argv):
    if len(argv)<2: usage(); return 2
    cmd=argv[1].lower()
    try:
        if cmd=="setup": setup()
        elif cmd=="demo": demo()
        elif cmd=="status": status()
        elif cmd=="transaction-cycle": transaction_cycle(argv[2].upper() if len(argv)>2 else "RANDOM",argv[3] if len(argv)>3 else None)
        elif cmd=="simulate" and len(argv)>=3: simulate(argv[2])
        elif cmd=="analyse": analyse_reference(argv[2].upper() if len(argv)>2 else "RSU1")
        elif cmd=="analyse-reference": analyse_reference(argv[2].upper() if len(argv)>2 else "RSU1",argv[3] if len(argv)>3 else None)
        elif cmd=="enforce": enforce()
        elif cmd in ("access","access-claim") and len(argv)>=3:
            allowed=access_control(argv[2].upper(),argv[3] if len(argv)>3 else "protected_operation",require_claim=(cmd=="access-claim"))
            if not allowed: print("Protected operation BLOCKED.")
        elif cmd=="claim" and len(argv)>=3: claim_reward(argv[2].upper(),argv[3] if len(argv)>3 else None)
        elif cmd=="recover" and len(argv)>=3: recover_vehicle(argv[2].upper())
        elif cmd=="rewards": reward_status()
        elif cmd=="beacon-test": beacon_test(argv[2] if len(argv)>2 else "V03",argv[3] if len(argv)>3 else "normal")
        elif cmd=="bench" and len(argv)>=3: bench(argv[2])
        else: usage(); return 2
    except KeyboardInterrupt: print("\nStopped by user."); return 130
    except Exception as exc:
        print(f"\nERROR: {exc}",file=sys.stderr)
        print("Check that all four existing MultiChain daemons are running and RPC ports/config files match this script.",file=sys.stderr)
        return 1
    return 0

if __name__=="__main__": raise SystemExit(main(sys.argv))
