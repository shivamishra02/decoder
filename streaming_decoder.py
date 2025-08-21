#!/usr/bin/env python3
"""
Streaming multi-chain event decoder (heuristic).
Usage: python3 streaming_decoder.py input1.json [input2.json ...]
Writes NDJSON decoded events to stdout or to a file when redirected.
This is a self-contained script using simple heuristics for demo purposes.
"""

import sys, json, datetime

# Basic helpers (same heuristics as used earlier)
def normalize_addr(addr):
    return addr.lower() if addr else addr

def hex_to_int(h):
    if not h or h == "0x": return 0
    return int(h,16)

def split_32byte_chunks(data_hex):
    d = data_hex[2:] if data_hex.startswith("0x") else data_hex
    if d == "": return []
    if len(d) % 64 != 0:
        d = d.rjust(((len(d)//64)+1)*64, '0')
    return [d[i:i+64] for i in range(0, len(d), 64)]

def int256_from_hexslot(slot_hex):
    val = int(slot_hex, 16)
    if val >= 2**255:
        val = val - 2**256
    return val

# Known mapping can be expanded by reading a config file or registry
KNOWN = {
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": {"name":"USDC","type":"token","decimals":6},
    "0xdac17f958d2ee523a2206206994597c13d831ec7": {"name":"USDT","type":"token","decimals":6},
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": {"name":"WBTC","type":"token","decimals":8},
    "0xc36442b4a4522e871399cd717abdd847ab11fe88": {"name":"UniswapV3:NFTManager","type":"dex","subtype":"uniswap_v3"},
    "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2": {"name":"AaveV3:LendingPool","type":"lending","subtype":"aave_v3"}
}

EVENT_SIG_MAP = {
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef":"Transfer",
    "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925":"Approval"
}

def decode_erc20_transfer(log, token_info):
    t = log.get("topics", [])
    from_addr = "0x" + t[1][-40:] if len(t) > 1 and t[1] else None
    to_addr = "0x" + t[2][-40:] if len(t) > 2 and t[2] else None
    value = hex_to_int(log.get("data","0x0"))
    decimals = token_info.get("decimals",18) if token_info else 18
    human_amount = value / (10 ** decimals) if decimals is not None else value
    return {"from": from_addr, "to": to_addr, "value_raw": str(value), "value": human_amount, "decimals": decimals}

def decode_uniswap_like(log):
    data = log.get("data","0x")
    slots = split_32byte_chunks(data)
    if len(slots) < 5:
        return None
    amount0 = int256_from_hexslot(slots[0])
    amount1 = int256_from_hexslot(slots[1])
    sqrtPriceX96 = int(slots[2],16)
    liquidity = int(slots[3],16)
    tick = int256_from_hexslot(slots[4])
    return {"amount0_raw":str(amount0),"amount1_raw":str(amount1),"sqrtPriceX96":str(sqrtPriceX96),"liquidity":str(liquidity),"tick":tick}

def decode_aave_like(log):
    data = log.get("data","0x")
    slots = split_32byte_chunks(data)
    if not slots: return None
    return {"amount_raw": str(int(slots[0],16))}

# Streaming generator: yields decoded events one by one
def stream_decode_from_file(path):
    with open(path,"r") as f:
        payload = json.load(f)
    chain = payload.get("chain", payload.get("metadata", {}).get("chain", "unknown"))
    logs = payload.get("logs", [])
    for log in logs:
        addr = normalize_addr(log.get("address",""))
        proto = KNOWN.get(addr)
        topic0 = None
        if log.get("topics") and len(log["topics"])>0 and log["topics"][0]:
            topic0 = log["topics"][0].lower()
        event_name = EVENT_SIG_MAP.get(topic0, None)
        decoded = None
        human = None
        if event_name == "Transfer" and proto and proto.get("type")=="token":
            decoded = decode_erc20_transfer(log, proto)
            human = f"Transfer {decoded['value']} {proto['name']}"
            event_type = "Transfer"
        elif event_name == "Approval" and proto and proto.get("type")=="token":
            decoded = decode_erc20_transfer(log, proto)
            human = f"Approval {decoded['value']} {proto['name']}"
            event_type = "Approval"
        elif proto and proto.get("subtype")=="uniswap_v3":
            enriched = decode_uniswap_like(log)
            if enriched:
                decoded = enriched
                human = "Uniswap-like swap/mint/burn (raw values)"
                event_type = "Uniswap:SwapLike"
        elif proto and proto.get("subtype")=="aave_v3":
            enriched = decode_aave_like(log)
            if enriched:
                decoded = enriched
                human = "Aave-like event (raw amount)"
                event_type = "Aave:Event"
        else:
            # generic
            data_int = hex_to_int(log.get("data","0x0"))
            topics_parsed = []
            for t in log.get("topics",[]):
                if not t:
                    topics_parsed.append(None)
                else:
                    tv = t.lower()
                    if tv.startswith("0x") and len(tv)>=66:
                        topics_parsed.append("0x"+tv[-40:])
                    else:
                        topics_parsed.append(tv)
            decoded = {"topics":topics_parsed,"data_int":str(data_int)}
            human = "Generic event (topics+data)"
            event_type = "Generic"
        out = {
            "decodedAt": datetime.datetime.utcnow().isoformat()+"Z",
            "chain": chain,
            "transactionHash": log.get("transactionHash"),
            "blockNumber": log.get("blockNumber"),
            "logIndex": log.get("logIndex"),
            "contractAddress": addr,
            "protocol": proto.get("name") if proto else None,
            "protocolType": proto.get("type") if proto else None,
            "eventType": event_type,
            "eventName": event_name or event_type,
            "decoded": decoded,
            "humanReadable": human,
            "raw": {"topics": log.get("topics"), "data": log.get("data")}
        }
        yield out

def main(args):
    out_file = None
    if len(args) >= 2:
        out_file = args[1]
    input_files = args[2:] if len(args) > 2 else ["sample_zeru.json"]
    # If out_file provided, write NDJSON to it
    writer = None
    if out_file:
        writer = open(out_file, "w")
    for inp in input_files:
        for decoded in stream_decode_from_file(inp):
            line = json.dumps(decoded)
            if writer:
                writer.write(line + "\n")
            else:
                print(line)

if __name__ == "__main__":
    # default behavior: write to streamed_decoded.ndjson in same dir
    import os, sys
    args = sys.argv
    if len(args) == 1:
        # invoked without args; default to sample file and default out
        args = [args[0], "streamed_decoded.ndjson", "sample_zeru.json"]
    main(args)
