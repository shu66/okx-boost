#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OKX Boost 实时排行 · 服务端 v2（自动更新，不用再重新上传页面）
================================================================
服务端负责：
  * 提供看板页面（okx-boost-live-rank.html，以后基本不用再换）
  * 后台自动扫描链上 OKX DEX 订单，持续写入 data/<活动>.json
  * 定时拉取 OKX 官方排行，写入 data/<活动>_off.json
  * 在页面上粘贴活动链接即可新增活动（/api/add），可隐藏 / 显示活动（/api/hide）
  * /okx/priapi/... 只读代理（浏览器通过它读取官方接口）

运行：  python3 okx_boost_server.py              （默认 127.0.0.1:8787）
        HOST=0.0.0.0 PORT=8787 python3 okx_boost_server.py
可选环境变量：AUTH_USER / AUTH_PASS（访问密码）、ADMIN_PASS（新增/隐藏活动的管理密码，默认与 AUTH_PASS 相同，
             都不设置则不校验）、CACHE_TTL、NO_BROWSER=1
只需要 Python 3.6+，无第三方依赖。服务器需要能访问 web3.okx.com、www.okx.com 和各链公共 RPC。
"""
import base64, gzip, hmac, hashlib, secrets, http.server, json, os, re, socketserver, sys, threading, time, traceback, urllib.error, urllib.request, webbrowser
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor

HOST = os.environ.get("HOST", "127.0.0.1"); PORT = int(os.environ.get("PORT", "8787"))
AUTH_USER, AUTH_PASS = os.environ.get("AUTH_USER", ""), os.environ.get("AUTH_PASS", "")
ADMIN_PASS = os.environ.get("ADMIN_PASS", AUTH_PASS)
CACHE_TTL = float(os.environ.get("CACHE_TTL", "5"))
HERE = os.path.dirname(os.path.abspath(__file__))
PAGE = os.environ.get("PAGE", "okx-boost-live-rank.html")
DATA = os.path.join(HERE, "data"); os.makedirs(DATA, exist_ok=True)
ACT_FILE = os.path.join(HERE, "activities.json")
OKX = "https://web3.okx.com"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128 Safari/537.36"
ORDER = "0x1bb43f2da90e35f7b0cf38521ca95a49e68eb42fac49924930a5bd73cdf7576c"
CF = "0xcd5eae9d9d0b96532bd1b7dbf6628ce436b2af735829087a03c548439f8bf850"
CT = "0x3cfb523a4c38d88561dd3bf04805a31715c8b5fc468a03b8d684356f360dea99"
# OKX DEX 代付 Gas（免 Gas / 交易终端）合约：由中继地址代发交易，用户在代币里付「Gas 费」
RL_SWAP = "0xd65443291bb59863c3ddfe64892356e9f0c3888a4c99efd97df5c028cb5db267"   # topics: 用户, 卖出币, 买入币; data: 卖出数量(含费), 到账数量(扣费后)
RL_FEE = "0x02b4603e6be0c4002f11c4445116c51a20adeecb33e1434306ec55378f75a099"    # 平台手续费 topics: 币, 收款; data[2]: 数量
RL_GAS = "0xc6d99ee7574d9d88cc700f01e1ecbcc9cfed97d50310b48abc3a6dfde3ae2dc0"    # 代付 Gas 费，同上
EEE = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
DETAIL_SEC = 2 * 3600          # 最近 2 小时保留逐笔明细，更早的按小时合并
STABLE = {"USDT", "USDC", "BUSD", "USDBC", "USDG", "DAI", "FDUSD", "USD₮0", "USDT0", "PYUSD", "USD1"}

CHAINS = {
    56: dict(name="BNB Chain", native="bnb", bt=0.45, range=1000, explorer="https://web3.okx.com/zh-hans/explorer/bsc",
             rpcs=["https://rpc-bsc.48.club", "https://0.48.club", "https://bsc.rpc.blxrbdn.com"],
             router="0x5994814f2c4040b863a0125a45de152a8c2a4dec", relayer="0xc75d2ce9d0a2c67b606a7b325c16e8a1b7c15a4a", wrapped={"WBNB", "BNB"},
             quotes={"USDT": [("0x55d398326f99059ff775485246999027b3197955", 18)], "USDC": [("0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d", 18)],
                     "BUSD": [("0xe9e7cea3dedca5984780bafc599bd69add087d56", 18)], "WBNB": [("0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c", 18)], "BNB": [(EEE, 18)]}),
    196: dict(name="X Layer", native="okb", bt=1, range=100, explorer="https://web3.okx.com/zh-hans/explorer/x-layer",
              rpcs=["https://rpc.xlayer.tech", "https://xlayerrpc.okx.com"], router="0x7c5bee2a8091c3ef39072f64f18fac913060aeaf", relayer="0xc75d2ce9d0a2c67b606a7b325c16e8a1b7c15a4a",
              lev="0x18a6704775570afa8db0d3fc0242515d90d120c0", wrapped={"WOKB", "OKB"},
              quotes={"USDT": [("0x1e4a5963abfd975d8c9021ce480b42188849d41d", 6), ("0x779ded0c9e1022225f8e0630b35a9b54be713736", 6)],
                      "USDC": [("0x74b7f16337b8972027f6196a17a631ac6de26d22", 6), ("0xb6ceceab302e2e4948951ee7843fc24e92933061", 6)],
                      "USDG": [("0x4ae46a509f6b1d9056937ba4500cb143933d2dc8", 6)], "WOKB": [("0xe538905cf8410324e03a5a23c1c177a474d59b2b", 18)], "OKB": [(EEE, 18)]}),
    8453: dict(name="Base", native="eth", bt=2, range=500, explorer="https://web3.okx.com/zh-hans/explorer/base",
               rpcs=["https://mainnet.base.org"], router="0x67d03631fe51b741c0c00c4e16eb662ac84381df", wrapped={"WETH", "ETH"},
               quotes={"USDC": [("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913", 6)], "USDBC": [("0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca", 6)],
                       "USDT": [("0xfde4c96c8593536e31f229ea8f37b2ada2699bb2", 6)], "WETH": [("0x4200000000000000000000000000000000000006", 18)], "ETH": [(EEE, 18)]}),
    5042: dict(name="Arc", native="usd", bt=0.5, range=5000, explorer="https://explorer.arc.io",
               rpcs=["https://rpc.mainnet.arc.io", "https://rpc.blockdaemon.mainnet.arc.io", "https://rpc.quicknode.mainnet.arc.io"],
               router="0x4e3bcce28caf98a143fd8bd9e4875ccab3e7bbe0", wrapped=set(),
               quotes={"USDC": [("0x3600000000000000000000000000000000000000", 6), (EEE, 18)]}),
    501: dict(name="Solana", offOnly=True, explorer="https://web3.okx.com/zh-hans/explorer/solana"),
}

LOCK = threading.RLock()
STATUS = {}            # key -> 同步状态文字
TRACK = defaultdict(set)

LOGS = deque(maxlen=400); START_TS = time.time()
def log(*a):
    line = time.strftime("[%m-%d %H:%M:%S] ") + " ".join(str(x) for x in a)
    LOGS.append(line); sys.stderr.write(line + "\n"); sys.stderr.flush()

# ---------------- 访问统计 ----------------
STATS_FILE = os.path.join(HERE, "data", "stats.json")
STATS = None; SEEN = {}          # ip -> (最近访问时间, 设备, 当前活动)
def stats():
    global STATS
    if STATS is None:
        STATS = jload(STATS_FILE, None) or {"pv": 0, "days": {}, "ips": {}}
    return STATS
def hit(ip, ua, kind, act=None):
    st = stats(); day = time.strftime("%Y-%m-%d"); d = st["days"].setdefault(day, {"pv": 0, "api": 0, "ips": []})
    if kind == "page":
        st["pv"] += 1; d["pv"] += 1
        if ip not in d["ips"] and len(d["ips"]) < 20000: d["ips"].append(ip)
        x = st["ips"].setdefault(ip, {"first": int(time.time()), "pv": 0}); x["pv"] += 1; x["last"] = int(time.time())
        x["ua"] = "手机" if re.search(r"Mobile|Android|iPhone", ua or "") else "电脑"
    else: d["api"] += 1
    if len(SEEN) > 5000:
        now = time.time()
        for k in [k for k, v in SEEN.items() if now - v[0] > 600]: SEEN.pop(k, None)
    old = SEEN.get(ip); SEEN[ip] = (time.time(), "手机" if re.search(r"Mobile|Android|iPhone", ua or "") else "电脑", act or (old[2] if old else None))
def save_stats():
    if STATS is None: return
    days = sorted(STATS["days"])
    for k in days[:-60]: STATS["days"].pop(k, None)
    if len(STATS["ips"]) > 50000:
        for k in sorted(STATS["ips"], key=lambda k: STATS["ips"][k].get("last", 0))[:10000]: STATS["ips"].pop(k, None)
    jsave(STATS_FILE, STATS)

# ---------------- 文件 ----------------
def jload(p, d=None):
    try:
        with open(p, encoding="utf-8") as f: return json.load(f)
    except Exception: return d
# 性能：已解析的数据文件按修改时间缓存，避免反复解析几 MB 的 JSON
_JC = {}
def jload_c(p, d=None):
    try: m = os.path.getmtime(p)
    except OSError: return d
    c = _JC.get(p)
    if c and c[0] == m: return c[1]
    v = jload(p, d); _JC[p] = (m, v)
    if len(_JC) > 60: _JC.pop(next(iter(_JC)))
    return v
NET = threading.BoundedSemaphore(int(os.environ.get("NET_MAX", "12")))   # 后台对外请求总并发上限
def jsave(p, obj):
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f: json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, p)
    try: _JC[p] = (os.path.getmtime(p), obj)
    except OSError: pass
def acts():
    with LOCK: return jload(ACT_FILE, [])
def save_acts(lst):
    with LOCK: jsave(ACT_FILE, lst)
def update_act(key, **kw):
    with LOCK:
        lst = jload(ACT_FILE, [])
        for a in lst:
            if a["key"] == key: a.update(kw)
        jsave(ACT_FILE, lst)

# ---------------- 网络 ----------------
def http_get(url, timeout=30, as_json=True, tries=3):
    err = None
    for k in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json,text/html", "Referer": "https://web3.okx.com/"})
            with NET, urllib.request.urlopen(req, timeout=timeout) as r:
                b = r.read()
                return json.loads(b.decode("utf-8")) if as_json else (b.decode("utf-8", "ignore"), r.geturl())
        except Exception as e:
            err = e; time.sleep(1 + k)
    raise err

def diag():
    out = {}
    def t(name, fn):
        t0 = time.time()
        try: fn(); out[name] = "OK %dms" % ((time.time() - t0) * 1000)
        except Exception as e: out[name] = "失败：" + str(e)[:60]
    t("OKX 官方接口 web3.okx.com", lambda: http_get(OKX + "/priapi/v1/dapp/boost/launchpool/leaderboard?launchpoolId=793", timeout=10, tries=1))
    t("OKX 行情 www.okx.com", lambda: http_get("https://www.okx.com/api/v5/market/ticker?instId=BTC-USDT", timeout=10, tries=1))
    for cid, ch in CHAINS.items():
        if ch.get("rpcs"): t("%s RPC" % ch["name"], lambda ch=ch: Rpc(ch["rpcs"][:1]).call("eth_blockNumber", [], tries=1))
    return out

class Rpc:
    def __init__(self, urls): self.urls, self.i = urls, 0
    def call(self, method, params, tries=8):
        err = None
        for k in range(tries):
            url = self.urls[self.i % len(self.urls)]; self.i += 1
            try:
                req = urllib.request.Request(url, data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
                                             headers={"content-type": "application/json", "User-Agent": UA})
                with NET, urllib.request.urlopen(req, timeout=60) as r: j = json.loads(r.read().decode())
                if "error" in j: raise Exception(j["error"])
                return j["result"]
            except Exception as e:
                err = e; time.sleep(min(10, 1 + k))
        raise err
    def head(self): return int(self.call("eth_blockNumber", []), 16)
    def ts(self, b):
        for _ in range(4):
            r = self.call("eth_getBlockByNumber", [hex(b), False])
            if r: return int(r["timestamp"], 16)
            time.sleep(1)
        return 0
    def block_at(self, t, hi=None):
        hi = hi or self.head(); lo = 1
        hts = self.ts(hi)
        if t >= hts: return hi
        bt = max(0.2, (hts - self.ts(max(1, hi - 10000))) / 10000.0)
        lo = max(1, hi - int((hts - t) / bt * 1.3) - 1000)
        while lo < hi:
            m = (lo + hi) // 2
            if self.ts(m) < t: lo = m + 1
            else: hi = m
        return lo
    def logs(self, a, b, address, topics=None):
        q = {"fromBlock": hex(a), "toBlock": hex(b), "address": address}
        if topics: q["topics"] = topics
        try: return self.call("eth_getLogs", [q], tries=3)
        except Exception as e:
            if "429" in str(e) or "Too Many" in str(e):   # 限流：等一会儿原范围重试，不拆分
                time.sleep(15); return self.call("eth_getLogs", [q], tries=6)
            if b - a < 10: return self.call("eth_getLogs", [q])
            m = (a + b) // 2
            return self.logs(a, m, address, topics) + self.logs(m + 1, b, address, topics)

# ---------------- 链上订单解析（与页面一致） ----------------
def w(d, i): return d[2 + 64 * i: 2 + 64 * (i + 1)]
def decode_tx(A, ls, addrs, ai):
    tok = [t["a"] for t in A["tokens"]]; tdec = [t["d"] for t in A["tokens"]]; Q = A["quotes"]; out = []
    for l in ls:
        if l["topics"][0] != ORDER: continue
        d = l["data"]; li = int(l["logIndex"], 16)
        ft = "0x" + w(d, 0)[-40:]; tt = "0x" + w(d, 1)[-40:]; snd = "0x" + w(d, 2)[-40:]
        fa = int(w(d, 3), 16); ra = int(w(d, 4), 16); cfa = cta = 0
        prev = max([int(m["logIndex"], 16) for m in ls if m["topics"][0] == ORDER and int(m["logIndex"], 16) < li] or [-1])
        nxt = min([int(m["logIndex"], 16) for m in ls if m["topics"][0] == ORDER and int(m["logIndex"], 16) > li] or [10 ** 9])
        for m in ls:  # 一笔订单可能有多条手续费事件（OKX 手续费 + 推荐/渠道手续费），全部累加
            lj = int(m["logIndex"], 16); tk = "0x" + w(m["data"], 0)[-40:]
            own = prev < lj < li or (li < lj < nxt and nxt == 10 ** 9 and m["topics"][0] == CT)
            if not own: continue
            if m["topics"][0] == CF and tk == ft: cfa += int(w(m["data"], 1), 16)
            if m["topics"][0] == CT and tk == tt: cta += int(w(m["data"], 1), 16)
        if ft in tok and tt in Q:
            ti = tok.index(ft); kind, qd = Q[tt]; buy = 0
            ta = (fa + cfa) / 10 ** tdec[ti]; q = ra / 10 ** qd
            fee = (cfa / 10 ** tdec[ti]) * (q / ta if ta else 0) + cta / 10 ** qd
        elif tt in tok and ft in Q:
            ti = tok.index(tt); kind, qd = Q[ft]; buy = 1
            q = (fa + cfa) / 10 ** qd; ta = ra / 10 ** tdec[ti]
            fee = cfa / 10 ** qd + ((cta / 10 ** tdec[ti]) * ((fa / 10 ** qd) / ((ra + cta) / 10 ** tdec[ti])) if ra + cta else 0)
        else: continue
        if snd not in ai: ai[snd] = len(addrs); addrs.append(snd)
        out.append([ai[snd], int(l["blockNumber"], 16), 0 if kind == "usd" else 1, float("%.8g" % q), ti, float("%.8g" % ta), buy, float("%.6g" % fee), 1])
    return out

def decode_relayer(A, ls, addrs, ai):
    tok = [t["a"] for t in A["tokens"]]; tdec = [t["d"] for t in A["tokens"]]; Q = A["quotes"]; out = []
    fee = defaultdict(int); gas = defaultdict(int)
    for l in ls:
        if l["topics"][0] in (RL_FEE, RL_GAS):
            t = "0x" + l["topics"][1][-40:]; amt = int(w(l["data"], 2), 16)
            (fee if l["topics"][0] == RL_FEE else gas)[t] += amt
    for l in ls:
        if l["topics"][0] != RL_SWAP or len(l["topics"]) < 4: continue
        user = "0x" + l["topics"][1][-40:]; ft = "0x" + l["topics"][2][-40:]; tt = "0x" + l["topics"][3][-40:]
        fa = int(w(l["data"], 0), 16); ra = int(w(l["data"], 1), 16)
        if ft in tok and tt in Q:   # 卖出：手续费从到账的计价币里扣
            ti = tok.index(ft); kind, qd = Q[tt]; buy = 0; ta = fa / 10 ** tdec[ti]; q = ra / 10 ** qd; f = fee[tt] / 10 ** qd; g = gas[tt] / 10 ** qd
        elif tt in tok and ft in Q:  # 买入：卖出数量已含手续费
            ti = tok.index(tt); kind, qd = Q[ft]; buy = 1; q = fa / 10 ** qd; ta = ra / 10 ** tdec[ti]; f = fee[ft] / 10 ** qd; g = gas[ft] / 10 ** qd
        else: continue
        if user not in ai: ai[user] = len(addrs); addrs.append(user)
        out.append([ai[user], int(l["blockNumber"], 16), 0 if kind == "usd" else 1, float("%.8g" % q), ti, float("%.8g" % ta), buy, float("%.6g" % f), 1, float("%.6g" % g)])
    return out

def compact(S, bt):
    """把 DETAIL_SEC 之前的明细按 地址×代币×方向×计价类型×小时 合并"""
    end, endTs, base = S["end"], S["endTs"], S["base"]
    keep_from = end - int(DETAIL_SEC / bt) - base
    old = [r for r in S["o"] if r[1] < keep_from and r[8] == 1]
    if len(old) < 5000: return
    rest = [r for r in S["o"] if not (r[1] < keep_from and r[8] == 1)]
    agg = {}
    for r in old:
        h = int((endTs - (end - (base + r[1])) * bt) // 3600)
        k = (r[0], r[4], r[6], r[2], h); x = agg.get(k)
        if not x: x = agg[k] = [r[0], r[1], r[2], 0.0, r[4], 0.0, r[6], 0.0, 0, 0.0]
        x[1] = max(x[1], r[1]); x[3] += r[3]; x[5] += r[5]; x[7] += r[7]; x[8] += 1; x[9] += r[9] if len(r) > 9 else 0
    merged = [[x[0], x[1], x[2], float("%.8g" % x[3]), x[4], float("%.8g" % x[5]), x[6], float("%.6g" % x[7]), max(2, x[8]), float("%.6g" % x[9])] for x in agg.values()]
    S["o"] = sorted(merged + rest, key=lambda r: r[1])

# ---------------- 后台：链上扫描 ----------------
SAVE_SEC = int(os.environ.get("SAVE_SEC", "90"))           # 链上数据写盘间隔
def scan_activity(A):
    if A.get("offOnly") or A.get("done"): return
    key = A["key"]; ch = CHAINS.get(A["chainId"], {}); rpc = Rpc(A.get("rpcs") or ch["rpcs"]); bt = A["blockTime"]
    p = os.path.join(DATA, key + ".json")
    S = jload_c(p)
    if not S:
        start = A.get("startBlock") or rpc.block_at(A["startTs"])
        update_act(key, startBlock=start)
        S = {"base": start, "end": start - 1, "endTs": A["startTs"], "addrs": [], "o": []}
    addrs = S["addrs"]; ai = {a: i for i, a in enumerate(addrs)}
    head = rpc.head(); head_ts = rpc.ts(head)
    stop = head
    if head_ts > A["endTs"]:
        eb = A.get("endBlock") or rpc.block_at(A["endTs"], head)
        update_act(key, endBlock=eb); stop = min(head, eb)
    frm = S["end"] + 1
    if frm > stop:
        if head_ts > A["endTs"] + 600: update_act(key, done=True); STATUS[key] = "已结束（数据完整）"
        return
    total = stop - frm + 1; step = A.get("range") or ch.get("range", 1000)
    lev = A.get("lev"); last_save = time.time()
    def job(a):
        b = min(a + step - 1, stop)
        if lev:
            L = rpc.logs(a, b, [A["router"], lev])
            lv = {l["transactionHash"] for l in L if l["address"].lower() == lev}
            L = [l for l in L if l["transactionHash"] in lv and l["address"].lower() == A["router"] and l["topics"][0] in (ORDER, CF, CT)]
        else:
            L = rpc.logs(a, b, A["router"], [[ORDER, CF, CT]])
        g = defaultdict(list)
        for l in L: g[l["transactionHash"]].append(l)
        tk = [t["a"][2:] for t in A["tokens"]]
        txs = [ls for ls in g.values() if any(l["topics"][0] == ORDER and any(t in l["data"] for t in tk) for l in ls)]
        rel = A.get("relayer") or CHAINS.get(A.get("chainId"), {}).get("relayer"); rtx = []
        if rel:
            g2 = defaultdict(list)
            for l in rpc.logs(a, b, rel, [[RL_SWAP, RL_FEE, RL_GAS]]): g2[l["transactionHash"]].append(l)
            rtx = [ls for ls in g2.values() if any(l["topics"][0] == RL_SWAP and len(l["topics"]) >= 4 and any(t in (l["topics"][2] + l["topics"][3]) for t in tk) for l in ls)]
        return b, txs, rtx
    hashes = []
    starts = list(range(frm, stop + 1, step))
    with ThreadPoolExecutor(int(os.environ.get("SCAN_THREADS", "3"))) as ex:
        for i in range(0, len(starts), 40):
            res = list(ex.map(job, starts[i:i + 40]))
            for b, txs, rtx in res:
                hashes.extend(ls[0]["transactionHash"] for ls in txs[:3])
                for ls in txs:
                    for r in decode_tx(A, ls, addrs, ai):
                        r[1] -= S["base"]; S["o"].append(r)
                for ls in rtx:
                    for r in decode_relayer(A, ls, addrs, ai):
                        r[1] -= S["base"]; S["o"].append(r)
            S["end"] = res[-1][0]
            S["endTs"] = head_ts - int((head - S["end"]) * bt)
            STATUS[key] = "同步中 %d%%" % int((S["end"] - frm + 1) * 100 / total) if total > 200 else "实时"
            if time.time() - last_save > SAVE_SEC or S["end"] >= stop:
                compact(S, bt); jsave(p, S); last_save = time.time()
    if hashes and not S.get("gasMed"):
        sample_gas(rpc, S, hashes); jsave(p, S)
    if S["end"] >= stop:
        STATUS[key] = "实时" if head_ts <= A["endTs"] else "已结束（数据完整）"
        if head_ts > A["endTs"] + 600: update_act(key, done=True)

def sample_gas(rpc, S, hashes):
    """抽样计算每笔订单交易的 Gas（原生币），供非收藏地址估算 Gas"""
    import random
    hs = random.sample(hashes, min(40, len(hashes))); g = []
    for h in hs:
        try:
            r = rpc.call("eth_getTransactionReceipt", [h], tries=2)
            g.append(int(r["gasUsed"], 16) * int(r.get("effectiveGasPrice") or "0x0", 16) / 1e18)
        except Exception: pass
    if g: g.sort(); S["gasMed"] = g[len(g) // 2]

# ---------------- 后台：X Launch 领奖记录 → 官方精确排名 ----------------
TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
def pad32(a): return "0x" + "0" * 24 + a[2:].lower()
def reward_token(A):
    ch = CHAINS[A["chainId"]]; unit = (A["pool"].get("unit") or "USDT").upper()
    for t in A["tokens"]:
        if t["s"].upper() == unit: return t["a"], t["d"]
    for a, dec in ch["quotes"].get(unit, []):
        if a != EEE: return a, dec
    return None, 18
def claims_xlaunch(A):
    """领奖开始后，奖励由分发合约逐个转给获奖地址：金额 = 官方最终奖励。
    按金额排序即官方名次；交易量 = (奖励 − 均分额) × 获奖总交易量 ÷ 交易量奖池。"""
    cs = A.get("claimStart") or 0
    if not cs or time.time() < cs or A.get("chainId") not in CHAINS or CHAINS[A["chainId"]].get("offOnly"): return
    key = A["key"]; p = os.path.join(DATA, key + "_claims.json"); C = jload(p, {})
    if C.get("done"): return
    O = jload(os.path.join(DATA, key + "_off.json"), {}); L = O.get("list") or []
    if not L or not O.get("total"): return
    tok, dec = reward_token(A)
    if not tok: return
    rpc = Rpc(A.get("rpcs") or CHAINS[A["chainId"]]["rpcs"]); step = min(5000, A.get("range") or 1000)
    head = rpc.head(); ce = A.get("claimEnd") or (cs + 14 * 86400)
    if not C.get("from"): C["from"] = rpc.block_at(cs, head)
    if not C.get("dist"):   # 用官方前几名的到账记录定位分发合约
        tops = L[:5]; hit = {}
        for b0 in range(C["from"], min(head, C["from"] + int(3 * 86400 / A["blockTime"])), step):
            b1 = min(b0 + step - 1, head)
            for addr, r, v, est in tops:
                for l in rpc.logs(b0, b1, tok, [TRANSFER, None, pad32(addr)]):
                    amt = int(l["data"], 16) / 10 ** dec; frm = "0x" + l["topics"][1][-40:]
                    if est and abs(amt - est) <= max(0.02, est * 0.002): hit[frm] = hit.get(frm, 0) + 1
            if hit: break
        if not hit: jsave(p, C); return
        C["dist"] = max(hit, key=hit.get); C["to"] = C["from"] - 1; C["got"] = {}
    stop = min(head, rpc.block_at(ce, head) if time.time() > ce else head)
    starts = list(range(C["to"] + 1, stop + 1, step))
    def job(b0): return rpc.logs(b0, min(b0 + step - 1, stop), tok, [TRANSFER, pad32(C["dist"])])
    with ThreadPoolExecutor(1 if A["chainId"] == 8453 else 8) as ex:
        for i in range(0, len(starts), 60):
            for logs in ex.map(job, starts[i:i + 60]):
                for l in logs:
                    a = "0x" + l["topics"][2][-40:]
                    if a not in C["got"]: C["got"][a] = int(l["data"], 16) / 10 ** dec   # 每个地址只取本期第一笔（同一分发合约可能还发其他活动）
            C["to"] = min(stop, starts[min(i + 59, len(starts) - 1)] + step - 1); jsave(p, C)
    # 用官方前 100 名的（交易量, 实际领奖金额）做线性回归：奖励 = 均分额 + 交易量 × 系数
    P = [(v, C["got"][addr]) for addr, r, v, est in L if addr in C["got"] and v > 0]
    if len(P) >= 5:
        mx = sum(x for x, y in P) / len(P); my = sum(y for x, y in P) / len(P)
        k = sum((x - mx) * (y - my) for x, y in P) / (sum((x - mx) ** 2 for x, y in P) or 1)
        C["base"] = my - k * mx; C["T"] = A["pool"]["bonus"] / k if k > 0 else O["total"]
        C["N"] = round(A["pool"]["base"] / C["base"]) if C["base"] > 0 else None
    if time.time() > ce: C["done"] = True; build_prior()
    jsave(p, C); STATUS[key] = "官方精确排名（领奖记录 %d 个）" % len(C["got"])

# ---------------- 报名概率模型（用已结束活动的领奖结果训练） ----------------
import math
def _bucket(v): return min(6, max(0, int(math.log10(max(v, 1)) * 2) - 2))
def addr_feats(S, px):
    F = {}
    for r in S.get("o", []):
        u = r[3] * (1 if r[2] == 0 else px); fee = (r[7] if len(r) > 7 else 0) * (1 if r[2] == 0 else px)
        n = r[8] if len(r) > 8 else 1
        f = F.setdefault(S["addrs"][r[0]], [0.0, 0.0, 0, 0.0])
        if r[6]: f[0] += u - fee
        else: f[1] += u
        f[2] += n; f[3] = max(f[3], u / n)
    return F
def feat_key(prior, a, f):
    b, s_, n, mt = f; rt = min(b, s_) / max(b, s_) if max(b, s_) else 0
    turn = (b + s_) / mt if mt else 0      # 资金周转倍数：交易量 ÷ 最大单笔，同一笔资金反复买卖时很高
    return ("P" if a in prior else "N") + ("R" if rt > .9 and turn >= 6 else "X") + "|%d" % _bucket(b + s_)
def build_prior():
    """已结束 X Launch 的领奖地址 = 已报名地址。统计「历史获奖者 / 来回刷量 / 交易量级」各组的报名比例，供进行中活动估算名次。"""
    done = []
    for A in acts():
        if not A.get("poolId"): continue
        C = jload(os.path.join(DATA, A["key"] + "_claims.json"), {})
        if C.get("got") and len(C["got"]) > 50: done.append((A["startTs"], A, set(C["got"])))
    done.sort(key=lambda x: x[0]); prior = set(); cnt = {}
    for ts, A, W in done:
        S = jload_c(os.path.join(DATA, A["key"] + ".json"), {})
        if S:
            for a, f in addr_feats(S, S.get("px") or 1).items():
                if f[0] + f[1] < 1: continue
                k = feat_key(prior, a, f); c = cnt.setdefault(k, [0, 0]); c[0] += 1; c[1] += a in W
        prior |= W
    table = {k: (c[1] + 1) / (c[0] + 2) for k, c in cnt.items()}
    jsave(os.path.join(DATA, "prior.json"), {"ut": int(time.time()), "acts": [A["key"] for _, A, _ in done], "addrs": sorted(prior), "table": table})
    return len(prior), len(table)

# ---------------- 后台：官方数据 ----------------
def okx_api(path):
    j = http_get(OKX + path)
    if j.get("code") != 0: raise Exception(j)
    return j["data"]
def official_xlaunch(A):
    d = okx_api("/priapi/v1/dapp/boost/launchpool/leaderboard?launchpoolId=%s&t=%d" % (A["poolId"], time.time() * 1000))
    jsave(os.path.join(DATA, A["key"] + "_off.json"), {"ut": int(time.time()), "participants": d.get("participants"), "total": d.get("totalBoostVolume"),
          "list": [[x["walletAddress"], x["rank"], x["boostVolume"], x["estimatedReward"]] for x in d.get("list") or []]})
LAST_UT = {}
OFF_FULL_SEC = int(os.environ.get("OFF_FULL_SEC", "1800"))  # 全量核对官方名次的最短间隔
def official_tiers(A, force=False):
    key = A["key"]; p = os.path.join(DATA, key + "_off.json"); O = jload(p, {"m": {}, "unreg": [], "ut": 0})
    path = lambda a: "/priapi/v1/dapp/competition/queryPnlRank?walletAddress=%s&activityId=%s&sortType=5&t=%d" % (a, A["actId"], time.time() * 1000)
    probe = "11111111111111111111111111111111" if A.get("offOnly") else "0x0000000000000000000000000000000000000000"
    d = okx_api(path(probe)); ut = int(d["rankUpdateTime"] // 1000)
    for x in d.get("pnlRankInfos") or []: O["m"][x["walletAddress"]] = [x["currentRank"], round(float(x["volume"] or 0), 4), ut]
    O["ut"] = max(O.get("ut", 0), ut)
    final = time.time() > A["endTs"] + 300 and ut >= A["endTs"] - 60
    if final and O.get("final"):
        jsave(p, O); return
    if ut == LAST_UT.get(key) and not force and not final:
        jsave(p, O); return
    LAST_UT[key] = ut
    S = jload_c(os.path.join(DATA, key + ".json"), {"addrs": []})
    full = final or force or time.time() - O.get("fullAt", 0) > OFF_FULL_SEC
    known = set(O["m"]) | set(O.get("unreg", []))
    if full: cand = set(S.get("addrs", [])) | TRACK[key] | set(O["m"].keys()); O["fullAt"] = int(time.time())
    else: cand = (set(S.get("addrs", [])) - known) | TRACK[key]          # 平时只查新出现的地址和收藏地址
    unreg = set(O.get("unreg", []))
    def q(a):
        try: return a, okx_api(path(a))
        except Exception: return a, None
    with ThreadPoolExecutor(int(os.environ.get("OFF_THREADS", "4"))) as ex:
        for a, dd in ex.map(q, sorted(cand)):
            if dd is None: continue
            me = dd.get("myPnlRankInfo"); u2 = int(dd["rankUpdateTime"] // 1000)
            if me: O["m"][a] = [me["currentRank"], round(float(me["volume"] or 0), 4), u2]; unreg.discard(a)
            else: unreg.add(a); O["m"].pop(a, None)
    O["unreg"] = sorted(unreg); O["checkedAt"] = int(time.time())
    if final: O["final"] = True   # 活动结束后的官方最终排名，页面直接采用官方名次
    jsave(p, O)

def official_daily(A):
    """日榜：当前期 + 刚结束的上一期（3 小时内）。前 100 名 + 逐个地址查询精确名次。"""
    D = A.get("daily") or {}; now = time.time(); key = A["key"]
    p = os.path.join(DATA, key + "_off.json"); O = jload(p, {"m": {}, "unreg": [], "ut": 0}); O.setdefault("daily", {})
    S = jload_c(os.path.join(DATA, key + ".json"), {"addrs": [], "o": []}); unreg = set(O.get("unreg", []))
    for ps, pe in D.get("periods", []):
        if not (ps <= now < pe + 3 * 3600): continue
        P = O["daily"].setdefault(str(ps), {"m": {}, "ut": 0})
        url = lambda a: "/priapi/v1/dapp/competition/queryDailyRank?activityId=%s&periodStart=%d%s&t=%d" % (A["actId"], ps, ("&walletAddress=" + a) if a else "", time.time() * 1000)
        d = okx_api(url(None)); ut = int((d.get("rankUpdateTime") or 0) // 1000)
        for x in d.get("rankInfos") or []: P["m"][x["walletAddress"]] = [x["currentRank"], round(float(x["volume"] or 0), 4), ut]
        if ut and ut != P.get("ut"):
            P["ut"] = ut
            if S.get("o") and S.get("endTs"):    # 本期有成交、且未确认未报名的地址
                bt = A["blockTime"]; b0 = S["end"] - int((S["endTs"] - ps) / bt) - S["base"]; b1 = S["end"] - int((S["endTs"] - pe) / bt) - S["base"]
                cand = {S["addrs"][r[0]] for r in S["o"] if b0 <= r[1] <= b1} - unreg
                if time.time() - P.get("fullAt", 0) < OFF_FULL_SEC: cand -= set(P["m"])     # 已在日榜名单里的不用每次重查
                else: P["fullAt"] = int(time.time())
                cand |= TRACK[key]
                def q(a):
                    try: return a, okx_api(url(a)).get("myRankInfo")
                    except Exception: return a, "ERR"
                with ThreadPoolExecutor(int(os.environ.get("OFF_THREADS", "4"))) as ex:
                    for a, me in ex.map(q, sorted(cand)):
                        if me == "ERR": continue
                        if me and float(me.get("volume") or 0) > 0: P["m"][a] = [me["currentRank"], round(float(me["volume"] or 0), 4), ut]
                        else: P["m"].pop(a, None)
        if now >= pe + 300: P["final"] = True
    jsave(p, O)

def worker_chain():
    while True:
        for A in acts():
            if A.get("hidden"): continue                       # 隐藏的活动不再同步
            try: scan_activity(A)
            except Exception as e: STATUS[A["key"]] = "链上同步出错，稍后重试"; log("scan", A["key"], repr(e)[:200])
        time.sleep(int(os.environ.get("CHAIN_SLEEP", "30")))
def worker_official():
    n = 0
    while True:
        for A in acts():
            now = time.time()
            if A.get("hidden"): continue
            try:
                if A.get("poolId") and A.get("claimStart") and now > A["claimStart"]: claims_xlaunch(A)
            except Exception as e: log("claims", A["key"], repr(e)[:200])
            if now < A["startTs"] - 3600 or now > A["endTs"] + 3 * 3600: continue
            try:
                if A.get("poolId"): official_xlaunch(A)
                elif A.get("actId"):
                    official_tiers(A)
                    if A.get("daily"): official_daily(A)
            except Exception as e: log("official", A["key"], repr(e)[:200])
        n += 1; time.sleep(120)

# ---------------- 新增活动（粘贴链接） ----------------
def slug_of(url):
    m = re.search(r"/boost/(x-launch|trading-competition)/([A-Za-z0-9_-]+)", url)
    return (m.group(1), m.group(2)) if m else (None, None)
def decimals(rpc, a):
    if a == EEE: return 18
    return int(rpc.call("eth_call", [{"to": a, "data": "0x313ce567"}, "latest"]), 16)
def default_bands(mx):
    b = [x for x in [[1, 1], [2, 3], [4, 10], [11, 50], [51, 100], [101, 500], [501, 1000], [1001, 3000], [3001, mx]] if x[0] <= mx]
    b[-1] = [b[-1][0], mx]; return b
def add_activity(url, state=None):
    url = url.strip()
    kind, slug = slug_of(url)
    if not kind and state: kind = "x-launch" if "LaunchPoolDetail" in state else "trading-competition"; slug = re.sub(r"[^a-z0-9-]", "", (url.rstrip("/").split("/")[-1].split("?")[0] or "act").lower())
    if not kind:
        html, final = http_get(url, as_json=False); kind, slug = slug_of(final)
        if not kind: raise Exception("无法识别这个链接，请打开活动页后复制地址栏里的完整网址（含 /boost/x-launch/ 或 /boost/trading-competition/）")
    for A in acts():
        if A.get("slug") == slug: update_act(A["key"], hidden=False); return A["key"]
    if state: ip = state
    else:
        try: html, _ = http_get("%s/zh-hans/boost/%s/%s" % (OKX, kind, slug), as_json=False)
        except Exception as e: raise Exception("NEED_STATE:服务器访问 OKX 超时（%s）" % str(e)[:60])
        m = re.search(r'id="appState">(.*?)</script>', html, re.S)
        if not m: raise Exception("NEED_STATE:读取活动页失败")
        ip = json.loads(m.group(1))["appContext"]["initialProps"]
    key = re.sub(r"[^a-z0-9]", "", slug.lower())[:16] or "act%d" % int(time.time())
    if kind == "x-launch":
        d = ip["LaunchPoolDetail"]; cid = int(d["chainId"]); ch = CHAINS.get(cid)
        if not ch or ch.get("offOnly"): raise Exception("暂不支持该链：%s" % d.get("chainName"))
        rpc = Rpc(ch["rpcs"]); r = d["reward"]; t = d["times"]
        tokens = [{"a": x["tokenAddress"].lower(), "s": x["tokenName"], "d": decimals(rpc, x["tokenAddress"].lower())} for x in d["tradingTokens"]]
        quotes = {}
        for x in d["eligibleQuoteTokens"]:
            a = x["tokenAddress"].lower(); s = x["tokenName"].upper()
            if s in STABLE: quotes[a] = ["usd", decimals(rpc, a)]
            elif s in ch["wrapped"] or a == EEE: quotes[a] = [ch["native"], decimals(rpc, a)]
        unit = r.get("token") or "USDT"; mx = int(r.get("maxRewardWinners") or 5000)
        A = dict(key=key, slug=slug, type="xlaunch", name=d["name"], short="%s X Launch" % tokens[0]["s"], chainId=cid, chain=ch["name"],
                 link="%s/zh-hans/boost/x-launch/%s" % (OKX, slug), router=ch["router"], rpcs=ch["rpcs"], range=ch["range"], blockTime=ch["bt"],
                 startTs=int(t["joinStartTime"] // 1000), endTs=int(t["joinEndTime"] // 1000), endLbl="报名截止", poolId=d["id"], gasFull=True,
                 claimStart=int((t.get("claimStartTime") or 0) // 1000), claimEnd=int((t.get("claimEndTime") or 0) // 1000),
                 tokens=tokens, quotes=quotes, native=ch["native"], explorer=ch["explorer"], reward="xlaunch",
                 pool={"base": r.get("basePoolAmount"), "bonus": r.get("bonusPoolAmount"), "max": mx, "unit": unit},
                 rewTok=unit.upper() not in STABLE, minTrade=0, minVol=0, bands=default_bands(mx),
                 desc="报名后通过 OKX DEX 聚合器交易 %s。前 %d 名均分 %s %s，另按交易量占比瓜分 %s %s。" % (
                     tokens[0]["s"], mx, format(r.get("basePoolAmount") or 0, ","), unit, format(r.get("bonusPoolAmount") or 0, ","), unit))
    else:
        d = ip["PnlCompetition"]["detailData"]; cid = int(d["chainId"]); ch = CHAINS.get(cid)
        if not ch: raise Exception("暂不支持该链：%s" % d.get("chainName"))
        tab = d["tabConfigs"][0]; pool = tab["prizePoolDistribution"][0]
        tiers = []
        for x in pool["rules"]:
            iv = x["interval"].split("-"); tiers.append([int(iv[0]), int(iv[-1]), float(x["reward"])])
        text = " ".join(x.get("desc", "") for x in tab.get("tabDetails", []))
        mv = re.search(r"超\s*\$\s*([\d,]+)", text) or re.search(r"达到\s*\$?\s*([\d,]+)\s*(?:USD|美元|U\b)", text)
        minVol = float(mv.group(1).replace(",", "")) if mv else 0
        unit = pool.get("rewardUnit") or d.get("rewardTokenSymbol") or "USDT"
        A = dict(key=key, slug=slug, type="tiers", name=d["name"] + " 交易赛", short=d["name"], chainId=cid, chain=ch["name"],
                 link="%s/zh-hans/boost/trading-competition/%s" % (OKX, slug), startTs=int(d["startTime"]), endTs=int(d["endTime"]), endLbl="结束",
                 actId=d["id"], explorer=ch["explorer"], reward="tiers", tiers=tiers, bands=[[a, b, v] for a, b, v in tiers], minTrade=1, minVol=minVol,
                 unit=unit, rewInst=(None if unit.upper() in STABLE else unit.upper() + "-USDT"),
                 tokens=[{"a": x["tokenAddress"], "s": x["tokenName"], "d": 18} for x in d["tradingTokens"]],
                 desc="报名后通过 OKX DEX 交易 %s。交易量 ≥ $%s 上榜，前 %d 名按名次阶梯领取，共 %s %s。" % (
                     "/".join(x["tokenName"] for x in d["tradingTokens"]), int(minVol), tiers[-1][1], format(pool.get("totalReward"), ""), unit))
        if int(d.get("leaderboardMode") or 0) == 1 and d.get("dailyPeriods"):   # 日榜 + 总榜
            dp = d.get("dailyRewardPools") or []; dt = []
            if dp:
                for x in dp[0]["rankConfig"]["prizePoolDistribution"][0]["rules"]:
                    iv = x["interval"].split("-"); dt.append([int(iv[0]), int(iv[-1]), float(x["reward"])])
            A["daily"] = {"periods": [[int(x["periodStart"]), int(x["periodEnd"])] for x in d["dailyPeriods"]], "tiers": dt,
                          "pool": float(dp[0]["rankConfig"]["prizePoolDistribution"][0]["totalReward"]) if dp else 0}
            A["desc"] = "报名后通过 OKX DEX 交易 %s。每 24 小时一期日榜（每期前 %d 名，%s %s）+ 总榜（前 %d 名，%s %s），可同时获得多期日榜和总榜奖励；每个榜单交易量 ≥ $%d 才能上榜。" % (
                "/".join(x["tokenName"] for x in d["tradingTokens"]), dt[-1][1] if dt else 0, format(int(A["daily"]["pool"]), ","), unit,
                tiers[-1][1], format(int(float(pool.get("totalReward") or 0)), ","), unit, int(minVol))
        if ch.get("offOnly"):
            A.update(offOnly=True, blockTime=1, native="usd", quotes={})
        else:
            rpc = Rpc(ch["rpcs"]); m2 = re.search(r"仅计算.*?链上\s*(.+?)\s*与本期", text)
            syms = [s.strip().upper() for s in re.split(r"[、,，\s]+", m2.group(1)) if s.strip()] if m2 else list(ch["quotes"].keys())
            quotes = {}
            for s in syms:
                for a, dec in ch["quotes"].get(s, []):
                    quotes[a] = ["usd" if s in STABLE else ch["native"], dec]
            A.update(router=ch["router"], rpcs=ch["rpcs"], range=ch["range"], blockTime=ch["bt"], native=ch["native"], quotes=quotes)
            for t in A["tokens"]: t["a"] = t["a"].lower(); t["d"] = decimals(rpc, t["a"])
            if int(d.get("tradeScene") or 0) == 1 and ch.get("lev"): A["lev"] = ch["lev"]
    if CHAINS.get(A.get("chainId"), {}).get("relayer"): A["relayer"] = CHAINS[A["chainId"]]["relayer"]
    keys = {x["key"] for x in acts()}
    while A["key"] in keys: A["key"] += "x"
    lst = acts(); lst.append(A); save_acts(lst); STATUS[A["key"]] = "准备同步"
    log("新增活动", A["key"], A["name"])
    return A["key"]

# ---------------- HTTP ----------------
# ---------------- 管理后台 ----------------
VERSION = "4.6"
ADMIN_FILES = {"okx-boost-live-rank.html", "admin.html", "okx_boost_server.py"}
def admin_token(): return hmac.new((ADMIN_PASS or "").encode(), b"okx-boost-admin", hashlib.sha256).hexdigest()[:40]
DIAG_CACHE = [0, None]
def diag_cached():
    if time.time() - DIAG_CACHE[0] > 60: DIAG_CACHE[1] = diag(); DIAG_CACHE[0] = time.time()
    return DIAG_CACHE[1]
def fsize(p):
    try: return os.path.getsize(p)
    except OSError: return 0
def overview():
    st = stats(); now = time.time(); day = time.strftime("%Y-%m-%d")
    days = sorted(st["days"])[-14:]
    series = [{"d": k[5:], "pv": st["days"][k]["pv"], "uv": len(st["days"][k]["ips"]), "api": st["days"][k].get("api", 0)} for k in days]
    online = [(ip, v) for ip, v in SEEN.items() if now - v[0] < 300]
    by_act = defaultdict(int); dev = defaultdict(int)
    for ip, v in online: by_act[v[2] or "-"] += 1; dev[v[1]] += 1
    recent = sorted(st["ips"].items(), key=lambda kv: -kv[1].get("last", 0))[:40]
    mask = lambda ip: re.sub(r"(\d+)\.(\d+)$", r"*.*", ip) if "." in ip else ip[:12] + "…"
    A = []
    for a in acts():
        k = a["key"]; S = jload_c(os.path.join(DATA, k + ".json"), None) or {}; O = jload_c(os.path.join(DATA, k + "_off.json"), None) or {}
        A.append({"key": k, "name": a.get("name"), "short": a.get("short"), "chain": a.get("chain"), "type": a.get("type") or a.get("reward"),
                  "hidden": bool(a.get("hidden")), "done": bool(a.get("done")), "startTs": a.get("startTs"), "endTs": a.get("endTs"),
                  "status": STATUS.get(k, ""), "chainTs": S.get("endTs"), "addrs": len(S.get("addrs", [])), "orders": len(S.get("o", [])),
                  "offUt": O.get("ut"), "reg": len(O.get("m", {})) or len(O.get("list", [])), "unreg": len(O.get("unreg", [])),
                  "daily": bool(a.get("daily")), "link": a.get("link"),
                  "size": fsize(os.path.join(DATA, k + ".json")) + fsize(os.path.join(DATA, k + "_off.json"))})
    return {"version": VERSION, "uptime": int(now - START_TS), "now": int(now),
            "pv": st["pv"], "today": {"pv": st["days"].get(day, {}).get("pv", 0), "uv": len(st["days"].get(day, {}).get("ips", []))},
            "series": series, "online": len(online), "onlineDev": dev, "onlineAct": by_act, "totalIps": len(st["ips"]),
            "recent": [{"ip": mask(ip), "pv": v["pv"], "first": v["first"], "last": v.get("last"), "ua": v.get("ua")} for ip, v in recent],
            "acts": A, "diag": diag_cached(), "logs": list(LOGS)[-200:], "notices": notices(), "pageVersion": page_version(), "changelog": merged_changelog(),
            "files": {f: {"size": fsize(os.path.join(HERE, f)), "mtime": int(os.path.getmtime(os.path.join(HERE, f))) if os.path.exists(os.path.join(HERE, f)) else 0} for f in sorted(ADMIN_FILES)},
            "dataSize": sum(fsize(os.path.join(DATA, f)) for f in os.listdir(DATA))}
def restart_later():
    def go():
        time.sleep(1.0); save_stats(); log("管理后台：重启服务")
        os.execv(sys.executable, [sys.executable] + sys.argv)
    threading.Thread(target=go, daemon=True).start()
def bg(fn, *a):
    def run():
        try: fn(*a)
        except Exception as e: log("后台任务出错", fn.__name__, repr(e)[:200])
    threading.Thread(target=run, daemon=True).start()
def admin_act(body):
    op = body.get("op"); key = body.get("key")
    if op == "add": return {"ok": True, "key": add_activity(body.get("url", ""), body.get("state"))}
    A = next((a for a in acts() if a["key"] == key), None)
    if not A: return {"ok": False, "msg": "活动不存在"}
    if op in ("hide", "show"): update_act(key, hidden=(op == "hide")); return {"ok": True}
    if op == "only":       # 只显示这个活动
        lst = acts(); [a.update(hidden=(a["key"] != key)) for a in lst]; save_acts(lst); return {"ok": True}
    if op == "remove": save_acts([a for a in acts() if a["key"] != key]); return {"ok": True}
    if op == "rename":
        kw = {k: str(body[k])[:60] for k in ("name", "short") if body.get(k)}; update_act(key, **kw); return {"ok": True}
    if op == "rescan":
        for f in (key + ".json",):
            try: os.remove(os.path.join(DATA, f))
            except OSError: pass
        update_act(key, done=False, startBlock=None, endBlock=None); STATUS[key] = "等待重新扫描"; return {"ok": True}
    if op == "cluster": CL_KICK.set(); return {"ok": True, "msg": "已触发关联分析，结果会分段保存，页面刷新即可看到进度"}
    if op == "recheck":
        def run():
            if A.get("poolId"): official_xlaunch(A)
            elif A.get("actId"):
                official_tiers(A, force=True)
                if A.get("daily"): official_daily(A)
            log("管理后台：官方数据已刷新", key)
        bg(run); return {"ok": True, "msg": "已在后台刷新官方数据"}
    return {"ok": False, "msg": "未知操作"}
def admin_upload(body):
    name = body.get("name"); data = base64.b64decode(body.get("b64") or "")
    if name not in ADMIN_FILES: return {"ok": False, "msg": "只能上传：" + "、".join(sorted(ADMIN_FILES))}
    if not data: return {"ok": False, "msg": "文件为空"}
    if name.endswith(".py"):
        try: compile(data.decode("utf-8"), name, "exec")
        except Exception as e: return {"ok": False, "msg": "Python 语法错误，未替换：%s" % e}
    if name.endswith(".html") and b"<html" not in data[:2000].lower(): return {"ok": False, "msg": "不是有效的 HTML 文件"}
    dst = os.path.join(HERE, name)
    if os.path.exists(dst):
        with open(dst, "rb") as f, open(dst + ".bak", "wb") as g: g.write(f.read())
    with open(dst + ".tmp", "wb") as f: f.write(data)
    os.replace(dst + ".tmp", dst); GZ.pop(dst, None); log("管理后台：已更新文件", name, len(data), "字节")
    if name.endswith(".py"): restart_later(); return {"ok": True, "msg": "程序已更新，服务正在重启（约 3 秒）"}
    return {"ok": True, "msg": "已更新，用户刷新页面即可生效"}
NOTICE_FILE = os.path.join(DATA, "notices.json")
def notices(): return jload(NOTICE_FILE, []) or []
def page_version():
    try:
        with open(os.path.join(HERE, PAGE), encoding="utf-8") as f: m = re.search(r'APP_VERSION\s*=\s*"([^"]+)"', f.read(200000))
        return m.group(1) if m else None
    except Exception: return None
def page_changelog():
    """读取页面文件里内置的版本更新记录，供后台查看"""
    try:
        with open(os.path.join(HERE, PAGE), encoding="utf-8") as f: t = f.read(400000)
        out = []
        for m in re.finditer(r'\{v:"([^"]+)",d:"([^"]+)",items:\[(.*?)\]\}', t):
            out.append({"v": m.group(1), "d": m.group(2), "items": [re.sub(r"<[^>]+>", "", x) for x in re.findall(r'"((?:[^"\\]|\\.)*)"', m.group(3))]})
        return out
    except Exception: return []
CL_FILE = os.path.join(DATA, "changelog.json")
def cl_over(): return jload(CL_FILE, None) or {"items": [], "deleted": []}
def merged_changelog():
    o = cl_over(); m = {c["v"]: dict(c, builtin=True) for c in page_changelog()}
    for c in o["items"]: m[c["v"]] = dict(c, edited=True, builtin=c["v"] in m)
    def vk(v):
        try: return [int(x) for x in str(v).split(".")]
        except Exception: return [0]
    return [dict(c, deleted=c["v"] in o["deleted"]) for c in sorted(m.values(), key=lambda c: vk(c["v"]), reverse=True)]
def admin_changelog(body):
    o = cl_over(); op = body.get("op"); v = str(body.get("v") or "").strip()[:12]
    if not v: return {"ok": False, "msg": "版本号不能为空"}
    if op == "save":
        old = str(body.get("oldv") or v)
        items = [str(x).strip()[:300] for x in (body.get("items") or []) if str(x).strip()][:20]
        if not items: return {"ok": False, "msg": "至少写一条更新内容"}
        o["items"] = [c for c in o["items"] if c["v"] not in (v, old)]
        if old != v and old not in o["deleted"] and any(c["v"] == old for c in page_changelog()): o["deleted"].append(old)
        o["items"].append({"v": v, "d": str(body.get("d") or time.strftime("%Y-%m-%d"))[:10], "items": items})
        o["deleted"] = [x for x in o["deleted"] if x != v]
    elif op == "delete":
        o["items"] = [c for c in o["items"] if c["v"] != v]
        if any(c["v"] == v for c in page_changelog()) and v not in o["deleted"]: o["deleted"].append(v)
    elif op == "restore":
        o["items"] = [c for c in o["items"] if c["v"] != v]; o["deleted"] = [x for x in o["deleted"] if x != v]
    else: return {"ok": False, "msg": "未知操作"}
    jsave(CL_FILE, o); log("管理后台：版本记录", op, v); return {"ok": True}
def admin_notice(body):
    lst = notices(); op = body.get("op")
    if op == "add":
        t = str(body.get("title") or "").strip()[:80]; b = str(body.get("body") or "").strip()[:3000]
        if not t: return {"ok": False, "msg": "标题不能为空"}
        lst.insert(0, {"id": "n%d" % int(time.time() * 1000), "ts": int(time.time()), "title": t, "body": b, "pin": bool(body.get("pin"))})
    elif op == "remove": lst = [x for x in lst if x.get("id") != body.get("id")]
    elif op == "edit":
        x = next((x for x in lst if x.get("id") == body.get("id")), None)
        if not x: return {"ok": False, "msg": "公告不存在"}
        t = str(body.get("title") or "").strip()[:80]
        if not t: return {"ok": False, "msg": "标题不能为空"}
        x.update(title=t, body=str(body.get("body") or "").strip()[:3000], pin=bool(body.get("pin")), edited=int(time.time()))
        if body.get("renotify"): x["id"] = "n%d" % int(time.time() * 1000)     # 换新 ID，用户会再次收到提醒
    else: return {"ok": False, "msg": "未知操作"}
    jsave(NOTICE_FILE, lst[:100]); log("管理后台：公告", op, body.get("title") or body.get("id")); return {"ok": True}

# ---------------- 疑似关联地址（同一人操作） ----------------
CLUSTER_TOP = int(os.environ.get("CLUSTER_TOP", "1500"))     # 只分析交易量前 N 的地址（获奖区附近）
NONCE_HUB = int(os.environ.get("NONCE_HUB", "3000"))   # 发款地址交易笔数超过这个数视为交易所 / 公共地址
KNOWN_HUBS = {"0x7797c4a7f786f77dbce13d71d2c710310cb92aa3"}   # OKX 手续费收款等公共地址
MIN_LINK_USD = 10                                            # 资金来源 / 归集：单个地址累计至少 $10 才算
HUB_MAX = 150                                                # 同一资金来源 / 归集地址关联的地址数上限（超过视为交易所、池子等公共地址）
def _addr_stats(A, S):
    """每个地址：交易量、笔数、平均单笔、首/末成交区块、主要代币"""
    px = {"bnb": 600, "okb": 120, "eth": 2600}.get(A.get("native"), 1) if not S.get("px") else S["px"]
    st = {}
    for r in S.get("o", []):
        u = r[3] * (1 if r[2] == 0 else px); n = r[8] if len(r) > 8 else 1; a = S["addrs"][r[0]]
        x = st.setdefault(a, {"v": 0.0, "n": 0, "b0": 1 << 62, "b1": 0, "tv": defaultdict(float), "bs": set(), "mx": 0.0})
        x["v"] += u; x["n"] += n; x["b0"] = min(x["b0"], r[1]); x["b1"] = max(x["b1"], r[1]); x["tv"][r[4]] += u; x["mx"] = max(x["mx"], u / n)
        if n == 1: x["bs"].add(r[1])
    return st
CL_STATE = {}                       # key -> {running, pct, startedAt, lastRun, nextRun, msg}
CL_KICK = threading.Event()
def cl_state(key): return CL_STATE.setdefault(key, {"running": False, "pct": 0, "lastRun": 0, "nextRun": 0, "msg": ""})
def scan_links(A):
    """增量、分段扫描候选地址的计价币 / 活动代币转账。每段扫完立即保存并重建分组，
    用户随时都能看到最新的已保存结果，不用等整轮分析结束。"""
    ch = CHAINS.get(A.get("chainId"), {})
    if A.get("offOnly") or not ch.get("rpcs"): return None
    key = A["key"]; p = os.path.join(DATA, key + "_links.json"); CS = cl_state(key)
    L = jload(p, None) or {"to": 0, "scanned": [], "in": {}, "out": {}, "code": {}}
    L.setdefault("nonce", {}); L.setdefault("pend", None)
    S = jload_c(os.path.join(DATA, key + ".json"), None)
    if not S or not S.get("o"): return L
    st = _addr_stats(A, S); minv = max(A.get("minVol") or 0, 10)
    cand = [a for a, x in sorted(st.items(), key=lambda kv: -kv[1]["v"]) if x["v"] >= minv][:CLUSTER_TOP]
    candset = set(cand); rpc = Rpc(A.get("rpcs") or ch["rpcs"]); bt = A["blockTime"]
    toks = [t["a"] for t in A["tokens"]] + [q for q in A.get("quotes", {}) if q != EEE]
    acttok = {t["a"] for t in A["tokens"]}
    w0 = S["base"] - int(6 * 3600 / bt); end = min(rpc.head(), S["end"] + 30)
    step = {56: 5000, 8453: 2000}.get(A["chainId"], A.get("range") or 100)
    seg = step * 20                                           # 每段区块数：扫完一段就保存一次
    def fetch(addrs, b0, b1):
        out = []
        for i in range(0, len(addrs), 300):
            pads = [pad32(x) for x in addrs[i:i + 300]]
            for tp in ([TRANSFER, pads], [TRANSFER, None, pads]):
                def job(x): return rpc.logs(x, min(x + step - 1, b1), toks, tp)
                with ThreadPoolExecutor(int(os.environ.get("CLUSTER_THREADS", "3"))) as ex:
                    for lg in ex.map(job, list(range(b0, b1 + 1, step))): out += lg
        return out
    def absorb(logs, cs):
        bytx = defaultdict(list); seen = set()
        for l in logs:
            k = (l["transactionHash"], l["logIndex"])
            if k not in seen: seen.add(k); bytx[l["transactionHash"]].append(l)
        for h, ls in bytx.items():
            swap = any(l["address"].lower() in acttok for l in ls) and any(l["address"].lower() not in acttok for l in ls)
            for l in ls:
                if len(l["topics"]) < 3: continue
                f = "0x" + l["topics"][1][-40:]; t = "0x" + l["topics"][2][-40:]; tok = l["address"].lower()
                if swap and not (f in candset and t in candset): continue
                dec = 6 if A.get("quotes", {}).get(tok, [None, 18])[1] == 6 else 18
                amt = int(l["data"], 16) / 10 ** dec if l["data"] not in ("0x", "") else 0
                usd = amt if tok in A.get("quotes", {}) and A["quotes"][tok][0] == "usd" else 0
                if t in cs: L["in"].setdefault(t, {}); L["in"][t][f] = round(L["in"][t].get(f, 0) + usd, 2) if usd else L["in"][t].get(f, 0) or 0.0001
                if f in cs: L["out"].setdefault(f, {}); L["out"][f][t] = round(L["out"][f].get(t, 0) + usd, 2) if usd else L["out"][f].get(t, 0) or 0.0001
    last_cp = [0]
    def checkpoint(force=False):
        if not force and time.time() - last_cp[0] < 180: return       # 分组重建最多 3 分钟一次
        last_cp[0] = time.time(); public_check(); jsave(p, L)
        try: build_clusters(A)
        except Exception as e: log("关联分组", key, repr(e)[:120])
    def public_check():
        cnt = defaultdict(int)
        for d in list(L["in"].values()) + list(L["out"].values()):
            for x in d: cnt[x] += 1
        chk = [x for x, c in cnt.items() if (c >= 2 or x in candset) and x not in L["code"]]
        def code(x):
            try: return x, len(rpc.call("eth_getCode", [x, "latest"], tries=3) or "0x") > 2
            except Exception: return x, None
        with ThreadPoolExecutor(6) as ex:
            for x, c in ex.map(code, chk):
                if c is not None: L["code"][x] = c
        srcs = defaultdict(int)
        for d in L["in"].values():
            for x in d: srcs[x] += 1
        chk = [x for x, c in srcs.items() if c >= 2 and x not in candset and not L["code"].get(x) and x not in L["nonce"]]
        def nonce(x):
            try: return x, int(rpc.call("eth_getTransactionCount", [x, "latest"], tries=3), 16)
            except Exception: return x, None
        with ThreadPoolExecutor(6) as ex:
            for x, n in ex.map(nonce, chk):
                if n is not None: L["nonce"][x] = n
    # 任务 1：新进入前 N 名的地址，补扫历史区间（断点续扫：pend 记录补扫到哪个区块）
    scanned = set(L["scanned"]); new = [x for x in cand if x not in scanned]
    if L["to"] and (new or L["pend"]):
        P = L["pend"] or {"addrs": new, "at": w0, "upto": L["to"]}
        if not L["pend"]: L["scanned"] = sorted(scanned | set(new)); L["pend"] = P
        ps = set(P["addrs"]); tot = max(1, P["upto"] - w0)
        while P["at"] <= P["upto"]:
            b1 = min(P["at"] + seg - 1, P["upto"]); absorb(fetch(P["addrs"], P["at"], b1), ps); P["at"] = b1 + 1
            CS["pct"] = int((P["at"] - w0) * 100 / tot); CS["msg"] = "补扫新上榜地址 %d 个" % len(P["addrs"]); checkpoint()
        L["pend"] = None; checkpoint(True)
    elif not L["to"]: L["scanned"] = sorted(candset)
    # 任务 2：所有候选地址，扫新区块
    frm = (L["to"] + 1) if L["to"] else w0; tot = max(1, end - frm + 1)
    while frm <= end:
        b1 = min(frm + seg - 1, end); absorb(fetch(cand, frm, b1), candset); L["to"] = b1
        CS["pct"] = 100 - int((end - b1) * 100 / tot); CS["msg"] = "扫描新区块"; frm = b1 + 1
        L["scanned"] = sorted(set(L["scanned"]) | candset); checkpoint()
    checkpoint(True)
    return L
def _jacc(a, b): return len(a & b) / max(1, len(a | b))
def build_clusters(A):
    """疑似同一人操作的地址分组（v2）
    证据（边）及权重：
      互相转账（累计 ≥$20 或 ≥2 次）      3
      同一资金来源（普通钱包、≥$10）        2
      资金归集到同一地址（≥$10）            2
      同步交易（同一分钟内反复一起成交：共同活跃分钟 ≥6 且重合度 ≥40%）  2
      手法相同（同主币、平均单笔差 ≤5%、开始时间差 ≤10 分钟、笔数差 ≤2 倍） 1
    只有权重 ≥2 的边直接合并；手法相同的边需要 ≥3 个地址互相相似才成组。组号按成员重合度沿用上一版，名称可由管理员自定义。"""
    key = A["key"]; S = jload_c(os.path.join(DATA, key + ".json"), None); L = jload_c(os.path.join(DATA, key + "_links.json"), None)
    if not S: return
    st = _addr_stats(A, S); O = jload_c(os.path.join(DATA, key + "_off.json"), {}) or {}
    unreg = set(O.get("unreg", [])); code = dict((L or {}).get("code", {}))
    for x, n in ((L or {}).get("nonce") or {}).items():
        if n > NONCE_HUB: code[x] = True
    code["0x0000000000000000000000000000000000000000"] = True
    for x in KNOWN_HUBS: code[x] = True
    minv = max(A.get("minVol") or 0, 10); bt = A["blockTime"]
    cand = [a for a, x in sorted(st.items(), key=lambda kv: -kv[1]["v"]) if x["v"] >= minv][:CLUSTER_TOP]
    candset = set(cand); par = {a: a for a in cand}
    def find(a):
        while par[a] != a: par[a] = par[par[a]]; a = par[a]
        return a
    def union(a, b): par[find(a)] = find(b)
    edges = []                                    # (a, b, 类型, 权重, 附加信息)
    pair = defaultdict(lambda: [0, 0.0]); src = defaultdict(dict); dst = defaultdict(dict)
    if L:
        for a, d in L["in"].items():
            if a not in candset: continue
            for f, usd in d.items():
                if code.get(f): continue
                if f in candset: k = tuple(sorted((a, f))); pair[k][0] += 1; pair[k][1] += usd if usd > 0.001 else 0
                elif usd >= MIN_LINK_USD: src[f][a] = usd
        for a, d in L["out"].items():
            if a not in candset: continue
            for t, usd in d.items():
                if code.get(t) or t in candset: continue
                if usd >= MIN_LINK_USD: dst[t][a] = usd
    for (a, b), (n, usd) in pair.items():
        if usd >= 20 or n >= 2: edges.append((a, b, "transfer", 3, usd))
    for kind, hub in (("source", src), ("sink", dst)):
        for h, ms in hub.items():
            if 2 <= len(ms) <= HUB_MAX:
                ms = sorted(ms)
                for m in ms[1:]: edges.append((ms[0], m, kind, 2, h))
    # 同步交易：15 秒内一起成交的次数，远高于随机巧合（≥5 倍）才算；人太多的时间段说明是行情高峰，不计
    W = 15; mins = {a: {int(b * bt // W) for b in st[a]["bs"]} for a in cand if len(st[a]["bs"]) >= 8}
    idx = defaultdict(list)
    for a, ms in mins.items():
        for m in ms: idx[m].append(a)
    nb = max(1, len(idx)); co = defaultdict(int)
    for m, ms in idx.items():
        if 2 <= len(ms) <= 6:
            for i in range(len(ms)):
                for j in range(i + 1, len(ms)): co[tuple(sorted((ms[i], ms[j])))] += 1
    for (a, b), c in co.items():
        na, nb2 = len(mins[a]), len(mins[b]); exp = na * nb2 / nb
        if c >= 8 and c / min(na, nb2) >= 0.5 and c >= 5 * exp: edges.append((a, b, "sync", 2, c))
    for a, b, t, w, _ in edges:
        if w >= 2: union(a, b)
    # 手法相同：按主币分组，平均单笔接近、开始时间接近、笔数量级接近
    fps = defaultdict(list)
    for a in cand:
        x = st[a]
        if x["n"] < 4: continue
        fps[max(x["tv"], key=x["tv"].get)].append((x["v"] / x["n"], x["b0"], x["n"], a))
    for tok, arr in fps.items():
        arr.sort(key=lambda z: z[1])
        for i, (avg, b0, n, a) in enumerate(arr):
            sim = []
            for avg2, b2, n2, a2 in arr[i + 1:]:
                if (b2 - b0) * bt > 600: break
                if abs(avg2 - avg) / max(avg, avg2) <= 0.05 and max(n, n2) / min(n, n2) <= 2: sim.append(a2)
            if len(sim) >= 2:
                for a2 in sim: edges.append((a, a2, "behavior", 1, None))
    # 手法相同是弱证据：只把「还没有强证据分组」的地址合成手法组，不再把已有的资金组串成大组
    strong = defaultdict(int)
    for a in cand: strong[find(a)] += 1
    for a, b, t, w, _ in edges:
        if t == "behavior" and strong[find(a)] == 1 and strong[find(b)] == 1: union(a, b)
    groups = defaultdict(list)
    for a in cand: groups[find(a)].append(a)
    TN = {"transfer": "地址之间互相转账", "source": "同一资金来源", "sink": "资金归集到同一地址", "sync": "15 秒内同步交易", "behavior": "操作手法相同"}
    out = []
    for g, ms in groups.items():
        if len(ms) < 2: continue
        mset = set(ms); ge = [e for e in edges if e[0] in mset]
        kinds = defaultdict(list)
        for e in ge: kinds[e[2]].append(e)
        ev = []
        if kinds["transfer"]: ev.append({"t": TN["transfer"], "n": len(kinds["transfer"]), "usd": round(sum(e[4] for e in kinds["transfer"]), 2)})
        for kd in ("source", "sink"):
            hubs = defaultdict(set)
            for e in kinds[kd]: hubs[e[4]] |= {e[0], e[1]}
            for h, mm in sorted(hubs.items(), key=lambda kv: -len(kv[1]))[:3]:
                usd = sum((src if kd == "source" else dst)[h].get(m, 0) for m in mm)
                ev.append({"t": TN[kd], "a": h, "n": len(mm), "usd": round(usd, 2)})
        if kinds["sync"]: ev.append({"t": TN["sync"], "n": len(kinds["sync"]), "m": max(e[4] for e in kinds["sync"])})
        if kinds["behavior"]:
            x0 = st[ms[0]]; ev.append({"t": TN["behavior"], "tok": A["tokens"][max(x0["tv"], key=x0["tv"].get)]["s"],
                "avg": round(sum(st[a]["v"] for a in ms) / max(1, sum(st[a]["n"] for a in ms)), 2), "n": round(sum(st[a]["n"] for a in ms) / len(ms)),
                "start": int(S["endTs"] - (S["end"] - S["base"] - min(st[a]["b0"] for a in ms)) * bt)})
        score = sum(e[3] for e in ge) / len(ms)
        fund = any(kinds[k] for k in ("transfer", "source", "sink"))
        lv = "h" if fund and (score >= 2.5 or len(kinds["transfer"]) >= 2) else "m" if fund or kinds["sync"] else "l"
        out.append({"kind": "fund" if fund else "behavior", "m": sorted(ms, key=lambda a: -st[a]["v"]), "ev": ev[:6], "score": round(score, 2), "lv": lv,
                    "vol": round(sum(st[a]["v"] for a in ms), 2), "size": len(ms), "unreg": sum(1 for a in ms if a in unreg)})
    out.sort(key=lambda g: (-g["size"], -g["vol"]))
    # 组号沿用：与上一版成员重合度最高（≥30%）的组继承组号；名称按成员重合度匹配
    prev = (jload(os.path.join(DATA, key + "_clusters.json"), None) or {}).get("groups") or []
    names = jload(os.path.join(DATA, key + "_clnames.json"), []) or []
    used = set(); nxt = max([g.get("id", 0) for g in prev] + [0]) + 1
    for g in out:
        ms = set(g["m"]); best = None; bj = 0.3
        for pg in prev:
            if pg["id"] in used: continue
            j = _jacc(ms, set(pg["m"]))
            if j >= bj: best, bj = pg, j
        if best: g["id"] = best["id"]; used.add(best["id"])
        else: g["id"] = nxt; nxt += 1
        bn = None; bj = 0.3
        for nm in names:
            j = _jacc(ms, set(nm["m"]))
            if j >= bj: bn, bj = nm, j
        if bn: g["name"] = bn["name"]
    lt = (L or {}).get("to"); ltTs = int(S["endTs"] - (S["end"] - lt) * bt) if lt else None
    jsave(os.path.join(DATA, key + "_clusters.json"), {"ut": int(time.time()), "top": len(cand), "linksTo": lt, "linksTs": ltTs,
          "scanned": len((L or {}).get("scanned", [])), "algo": 2, "groups": out})
    return out
def cluster_rename(key, gid, name):
    C = jload(os.path.join(DATA, key + "_clusters.json"), None)
    g = next((x for x in (C or {}).get("groups", []) if x["id"] == gid), None)
    if not g: return {"ok": False, "msg": "关联组不存在"}
    name = str(name or "").strip()[:30]; p = os.path.join(DATA, key + "_clnames.json"); names = jload(p, []) or []
    names = [n for n in names if _jacc(set(n["m"]), set(g["m"])) < 0.3]
    if name: names.append({"name": name, "m": g["m"], "ts": int(time.time())})
    jsave(p, names); g["name"] = name or None; jsave(os.path.join(DATA, key + "_clusters.json"), C)
    log("管理后台：关联组改名", key, gid, name); return {"ok": True}

CL_INTERVAL = int(os.environ.get("CLUSTER_INTERVAL", "600"))
def run_cluster(A):
    CS = cl_state(A["key"]); CS.update(running=True, pct=0, startedAt=int(time.time()), msg="开始分析")
    try: scan_links(A); build_clusters(A); CS["msg"] = ""
    except Exception as e: CS["msg"] = "分析出错，稍后重试"; log("关联分析", A["key"], repr(e)[:200])
    finally: CS.update(running=False, pct=100, lastRun=int(time.time()), nextRun=int(time.time()) + CL_INTERVAL)
def worker_cluster():
    if os.environ.get("CLUSTER_OFF"): log("关联分析已关闭（CLUSTER_OFF）"); return
    for A in acts():                   # 启动时先用已保存的链上关联记录立即生成分组，用户不用等
        if A.get("offOnly"): continue
        try:
            if os.path.exists(os.path.join(DATA, A["key"] + "_links.json")): build_clusters(A)
        except Exception as e: log("关联分组", A["key"], repr(e)[:120])
    time.sleep(15)
    while True:
        for A in acts():
            if A.get("hidden") or A.get("offOnly"): continue
            if time.time() > A["endTs"] + 6 * 3600 and os.path.exists(os.path.join(DATA, A["key"] + "_clusters.json")): continue
            run_cluster(A)
        CL_KICK.wait(CL_INTERVAL); CL_KICK.clear()
def cluster_payload(key):
    C = jload_c(os.path.join(DATA, key + "_clusters.json"), None); CS = cl_state(key)
    return {"ok": True, "data": C, "state": {k: CS.get(k) for k in ("running", "pct", "lastRun", "nextRun", "msg", "startedAt")}, "now": int(time.time())}

def worker_stats():
    while True:
        time.sleep(60)
        try: save_stats()
        except Exception as e: log("stats", repr(e)[:120])

CACHE = {}; GZ = {}
def file_bytes(p, gz):
    try: m = os.path.getmtime(p)
    except OSError: return None
    c = GZ.get(p)
    if not c or c[0] != m:
        raw = open(p, "rb").read(); c = GZ[p] = (m, raw, gzip.compress(raw, 3))
    return c[2] if gz else c[1]

class H(http.server.BaseHTTPRequestHandler):
    server_version = "OKXBoost/2.0"
    protocol_version = "HTTP/1.1"
    def log_message(self, *a): pass
    def ip(self): return (self.headers.get("X-Forwarded-For") or self.client_address[0]).split(",")[0].strip()
    def send(self, code, body, ctype="application/json; charset=utf-8", gz=False):
        if isinstance(body, (dict, list)): body = json.dumps(body, ensure_ascii=False).encode()
        if isinstance(body, str): body = body.encode()
        self.send_response(code); self.send_header("Content-Type", ctype); self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        if gz: self.send_header("Content-Encoding", "gzip")
        self.send_header("Content-Length", str(len(body))); self.end_headers()
        if self.command != "HEAD": self.wfile.write(body)
    def authed(self):
        if not (AUTH_USER and AUTH_PASS): return True
        h = self.headers.get("Authorization", "")
        if h.startswith("Basic ") and base64.b64decode(h[6:]).decode("utf8", "ignore") == "%s:%s" % (AUTH_USER, AUTH_PASS): return True
        self.send_response(401); self.send_header("WWW-Authenticate", 'Basic realm="OKX Boost"'); self.send_header("Content-Length", "0"); self.end_headers()
        return False
    def admin_ok(self, body):
        return not ADMIN_PASS or body.get("pass") == ADMIN_PASS or self.is_admin()
    def is_admin(self):
        if not ADMIN_PASS: return False
        c = self.headers.get("Cookie", ""); m = re.search(r"okxadm=([0-9a-f]+)", c)
        return bool(m) and hmac.compare_digest(m.group(1), admin_token())
    def do_OPTIONS(self):
        self.send_response(204); self.send_header("Access-Control-Allow-Origin", "*"); self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*"); self.send_header("Content-Length", "0"); self.end_headers()
    def do_HEAD(self): self.do_GET()
    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/healthz": return self.send(200, "ok", "text/plain")
        if not self.authed(): return
        gz = "gzip" in self.headers.get("Accept-Encoding", "")
        if path in ("/admin", "/admin/", "/admin.html"):
            b = file_bytes(os.path.join(HERE, "admin.html"), gz)
            return self.send(200, b, "text/html; charset=utf-8", gz) if b else self.send(404, "admin.html missing", "text/plain")
        if path == "/api/admin/overview":
            if not self.is_admin(): return self.send(401, {"ok": False, "msg": "请先登录"})
            return self.send(200, overview())
        if path in ("/", "/index.html", "/" + PAGE):
            hit(self.ip(), self.headers.get("User-Agent"), "page")
            b = file_bytes(os.path.join(HERE, PAGE), gz)
            return self.send(200, b, "text/html; charset=utf-8", gz) if b else self.send(404, "page missing", "text/plain")
        if path == "/okx-boost-helper.user.js":
            b = file_bytes(os.path.join(HERE, "okx-boost-helper.user.js"), False)
            return self.send(200, b, "text/javascript; charset=utf-8") if b else self.send(404, "", "text/plain")
        if path == "/api/diag": return self.send(200, diag())
        if path == "/api/whoami": return self.send(200, {"admin": self.is_admin()})
        m2 = re.match(r"^/api/clusters/([a-z0-9]+)$", path)
        if m2: return self.send(200, cluster_payload(m2.group(1)))
        if path == "/api/notices":
            return self.send(200, {"notices": notices()[:30], "server": VERSION, "page": page_version(), "changelog": cl_over()})
        if path == "/api/activities":
            hit(self.ip(), self.headers.get("User-Agent"), "api")
            return self.send(200, {"acts": acts(), "status": STATUS, "admin": bool(ADMIN_PASS), "now": int(time.time())})
        m = re.match(r"^/data/([a-z0-9]+(?:_off|_claims|_clusters)?|prior)\.json$", path)
        if m:
            b = file_bytes(os.path.join(DATA, m.group(1) + ".json"), gz)
            return self.send(200, b, "application/json; charset=utf-8", gz) if b else self.send(404, {"code": -1, "msg": "no data yet"})
        if path.startswith("/okx/priapi/"): return self.proxy(self.path[4:])
        return self.send(404, {"code": -1, "msg": "not found"})
    def do_POST(self):
        if not self.authed(): return
        n = int(self.headers.get("Content-Length") or 0)
        if n > 40 * 1024 * 1024: return self.send(413, {"ok": False, "msg": "文件太大"})
        try: body = json.loads(self.rfile.read(n).decode() or "{}")
        except Exception: body = {}
        path = self.path.split("?")[0]
        if path == "/api/ping":
            hit(self.ip(), self.headers.get("User-Agent"), "api", str(body.get("act") or "")[:20]); return self.send(200, {"ok": True})
        if path == "/api/admin/login":
            if not ADMIN_PASS: return self.send(200, {"ok": False, "msg": "服务器未设置 ADMIN_PASS，启动命令里加上 ADMIN_PASS=你的密码 后重启"})
            if body.get("pass") != ADMIN_PASS: time.sleep(1); return self.send(200, {"ok": False, "msg": "密码错误"})
            b = json.dumps({"ok": True}).encode(); self.send_response(200)
            self.send_header("Set-Cookie", "okxadm=%s; Path=/; HttpOnly; SameSite=Lax; Max-Age=2592000" % admin_token())
            self.send_header("Content-Type", "application/json"); self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b); return
        if path.startswith("/api/admin/"):
            if not self.is_admin(): return self.send(401, {"ok": False, "msg": "请先登录"})
            try:
                if path == "/api/admin/act": return self.send(200, admin_act(body))
                if path == "/api/admin/upload": return self.send(200, admin_upload(body))
                if path == "/api/admin/notice": return self.send(200, admin_notice(body))
                if path == "/api/admin/changelog": return self.send(200, admin_changelog(body))
                if path == "/api/admin/clname": return self.send(200, cluster_rename(str(body.get("key")), int(body.get("id") or 0), body.get("name")))
                if path == "/api/admin/restart": restart_later(); return self.send(200, {"ok": True, "msg": "服务正在重启"})
                if path == "/api/admin/logout":
                    b = b'{"ok":true}'; self.send_response(200); self.send_header("Set-Cookie", "okxadm=; Path=/; Max-Age=0")
                    self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b); return
            except Exception as e:
                log("admin", path, repr(e)[:300]); return self.send(200, {"ok": False, "msg": str(e)[:300]})
        if path == "/api/track":
            k = body.get("key"); addrs = body.get("addrs") or []
            if k: TRACK[k].update(a for a in addrs[:200] if isinstance(a, str) and len(a) < 64)
            return self.send(200, {"ok": True})
        if path in ("/api/add", "/api/hide", "/api/remove"):
            if not self.admin_ok(body): return self.send(403, {"ok": False, "msg": "管理密码错误"})
            try:
                if path == "/api/add":
                    k = add_activity(body.get("url", ""), body.get("state")); return self.send(200, {"ok": True, "key": k})
                if path == "/api/hide":
                    update_act(body["key"], hidden=bool(body.get("hidden"))); return self.send(200, {"ok": True})
                if path == "/api/remove":
                    save_acts([a for a in acts() if a["key"] != body["key"]]); return self.send(200, {"ok": True})
            except Exception as e:
                log("admin", path, repr(e)[:300]); return self.send(200, {"ok": False, "msg": str(e)[:300]})
        return self.send(404, {"ok": False})
    def proxy(self, path):
        key = path.split("&t=")[0]
        hit = CACHE.get(key)
        if hit and time.time() - hit[0] < CACHE_TTL: return self.send(hit[1], hit[2])
        req = urllib.request.Request(OKX + path, headers={"User-Agent": UA, "Accept": "application/json", "Referer": "https://web3.okx.com/"})
        try:
            with urllib.request.urlopen(req, timeout=20) as r: code, b = r.status, r.read()
        except urllib.error.HTTPError as e: code, b = e.code, e.read()
        except Exception as e: code, b = 502, json.dumps({"code": -1, "msg": str(e)[:80]}).encode()
        if code == 200:
            CACHE[key] = (time.time(), code, b)
            if len(CACHE) > 20000: CACHE.clear()
        self.send(code, b)

class S(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True; allow_reuse_address = True; request_queue_size = 128
    _slots = threading.BoundedSemaphore(int(os.environ.get("HTTP_MAX", "48")))   # 同时处理的请求数上限
    def process_request_thread(self, request, client_address):
        with self._slots: super().process_request_thread(request, client_address)

if __name__ == "__main__":
    if not os.path.exists(os.path.join(HERE, PAGE)): print("找不到 %s，请把它和本脚本放在同一个文件夹" % PAGE); sys.exit(1)
    if not os.path.exists(ACT_FILE): save_acts([])
    try: build_prior()
    except Exception as e: log("prior", repr(e)[:200])
    try: os.nice(int(os.environ.get("NICE", "5")))   # 降低后台计算优先级，不抢网站和宝塔的 CPU
    except Exception: pass
    for f in (worker_chain, worker_official, worker_stats, worker_cluster): threading.Thread(target=f, daemon=True).start()
    srv = S((HOST, PORT), H)
    print("OKX Boost 看板 v%s 已启动：http://%s:%d   管理后台：/admin   数据目录：%s" % (VERSION, "127.0.0.1" if HOST in ("0.0.0.0", "::") else HOST, PORT, DATA))
    if HOST in ("127.0.0.1", "localhost") and not os.environ.get("NO_BROWSER"):
        threading.Timer(0.8, lambda: webbrowser.open("http://127.0.0.1:%d" % PORT)).start()
    try: srv.serve_forever()
    except KeyboardInterrupt: print("已退出")
