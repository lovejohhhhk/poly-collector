#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Polymarket 二元事件（{coin}-updown-15m-{ts}）盘口采集器 —— Step 0「可用性/成本实测」

== 网络通路 ==
gamma-api.polymarket.com / clob.polymarket.com 在墙内直连被 GFW 封（DNS 污染 + SNI 层双重），
真实 IP + 正确 SNI 也是 000。两条可用通路（实测均通）：
  A. 本机代理隧道：--proxy http://127.0.0.1:7890（clash.meta mixed-port），实测 err_rate 0%
  B. 墙外服务器直连：宝塔/VPS 在香港或海外时，不加 --proxy 即可，最省事
必须走 ccxt 原生 https_proxy（requests 的 CONNECT 隧道），不要用 proxyUrl —— 那是浏览器端
CORS 中转前缀（会拼 URL + 补 Origin 头），且在 ccxt 4.5.76 里必抛 InvalidProxySettings。

== 做什么（纯只读，无策略）==
  1. 本地按 ts=(now_utc_sec//900)*900 推导 slug: {coin}-updown-15m-{ts}
     （gamma 索引有延迟，不能靠列表接口翻找，只能按时间戳推导后 /markets?slug= 取）
  2. 每个窗口取一次市场元数据（clobTokenIds / tick_size / min_order_size / base_fee）
  3. 窗口内周期性采 UP / DOWN 两个 token 的 CLOB 全深度盘口，落盘时压成「累计深度曲线」
     -> 直接回答 go/no-go 的核心问题：<=q_max 一侧到底有没有 50 股可以吃
  4. 窗口结束后拉结算结果（outcomePrices 判 Up/Down）

== 用法（本机）==
  <freqtrade venv python> collect_poly_15m.py                # 常驻采集（Ctrl+C 停止）
  <freqtrade venv python> collect_poly_15m.py --run-for 600  # 跑 10 分钟自检
  <freqtrade venv python> collect_poly_15m.py --coins BTC,ETH,SOL
  <freqtrade venv python> collect_poly_15m.py --proxy ""      # 显式直连
  <freqtrade venv python> collect_poly_15m.py --report        # 只读已落盘数据出 go/no-go 报告，不联网

  必须用 freqtrade 虚拟环境解释器（Python 3.10 + ccxt 4.5.76）：
    g:\\pytest\\factormining\\freqtrade\\.venv\\Scripts\\python.exe collect_poly_15m.py

== 部署到宝塔（Linux VPS）==
  0) 装依赖（public 端点无需 API key，只要 ccxt）：
       pip3 install "ccxt>=4.5.76"
     自检：python3 -c "from ccxt.prediction import polymarket"（不报错即可）
  1) 上传本文件（单文件，无本地依赖）到例如 /www/wwwroot/polymarket/
  2) 代理：
       - VPS 在香港/海外 -> 直连，什么都不用传（Linux 默认直连）
       - VPS 在墙内      -> export POLY_PROXY=http://127.0.0.1:7890（或 --proxy 同值）
     优先级：--proxy 显式 > $POLY_PROXY > $HTTPS_PROXY/$https_proxy > 平台默认 > 直连
  3) 启动（三种任选）：
       A. nohup 常驻：
            cd /www/wwwroot/polymarket
            nohup python3 -u collect_poly_15m.py > poly.log 2>&1 &
       B. 宝塔「进程守护管理器」新建守护：
            启动命令：python3 -u /www/wwwroot/polymarket/collect_poly_15m.py
            运行目录：/www/wwwroot/polymarket
            （守护器停服发 SIGTERM，脚本收尾当前窗口后退出，不丢数据）
       C. 宝塔「计划任务」-> Shell 脚本，周期 1 小时（挂了自动拉起）：
            pgrep -f collect_poly_15m.py >/dev/null || (cd /www/wwwroot/polymarket && \
              nohup python3 -u collect_poly_15m.py >> poly.log 2>&1 &)
  4) 看进度：tail -f poly.log（首屏一次性 selfcheck：gamma/clob/fee-rate 的 RTT、点差、手续费）
  5) 出报告：python3 collect_poly_15m.py --report（读 poly_data/ 下全部 jsonl）
  6) 可选环境变量（等价于对应参数）：POLY_PROXY / POLY_COINS / POLY_OUT
  注意：数据目录默认取脚本同级的 poly_data/，需可写。

== 落盘 ==
  poly_data/poly_{COIN}_{YYYYMMDD}.jsonl   每行一个 JSON（UTC 日期分片），kind 区分：
    info      进程事件（启动/停止/状态）
    market    一个 15min 窗口的市场元数据（token ids / 手续费 / 结算源），每窗一条
    sample    盘口快照（UP + DOWN 的 best / 累计深度曲线 / 服务端时间戳 / RTT）
    summary   窗口收尾汇总（采样数、中点极值）
    settle    窗口结算结果（up_won）
    err       单次请求失败（统计中转可用率，决定要不要自建反代）
"""


import argparse
import asyncio
import collections
import datetime
import glob
import json
import os
import signal
import statistics
import sys
import time

# ============================== 配置区 ==============================
PROXY_DEFAULT_WIN = "http://127.0.0.1:7890"
PROXY_ENV_KEYS = ("POLY_PROXY", "HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy")
COINS_DEFAULT = [c.strip().upper() for c in os.environ.get("POLY_COINS", "BTC,ETH").split(",")
                 if c.strip()]
WINDOW_SECONDS = 900
POLL_SECONDS = 20
POLL_SECONDS_BURST = 10
BURST_SECONDS = 90
MIN_REQUEST_GAP_S = 1.0
TIMEOUT_MS = 25000
MAX_RETRIES = 4
BACKOFF_BASE_S = 1.5
SETTLE_RETRY_S = 30                   # 退避基数（第 0 次尝试后等 30s）
MAX_SETTLE_ATTEMPTS = 100             # 提到 100，配合退避覆盖 1~2 小时
SETTLE_BACKOFF_MAX_S = 600            # 退避上限 10 分钟
MARKET_RESOLVE_RETRY_S = 30
Q_MAX_LIST = [(0.5214, "main"), (0.5314, "sleeve"), (0.5308, "long"), (0.5329, "short")]
DEPTH_THRESHOLDS = sorted(set([0.40, 0.45, 0.47, 0.50, 0.52, 0.53, 0.55, 0.60, 0.70, 0.80, 0.90]
                              + [q for q, _ in Q_MAX_LIST]))
MIN_FILL_SHARES = 50
FEE_RATE_MAX = 0.25
STATUS_PRINT_S = 300
TAIL_SETTLE_WAIT_S = 60               # 停止时，等尾窗结束 + 60s 再退出

OUT_DIR = os.environ.get("POLY_OUT") or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "poly_data")
SLUG_PREFIX = {"BTC": "btc", "ETH": "eth", "SOL": "sol", "XRP": "xrp",
               "DOGE": "doge", "BNB": "bnb", "ADA": "ada", "APT": "apt",
               "ARB": "arb", "AVAX": "avax"}

STOP = False


def now_ms():
    return int(time.time() * 1000)


def resolve_proxy(cli_proxy):
    if cli_proxy is not None:
        return cli_proxy.strip() or None
    for k in PROXY_ENV_KEYS:
        v = (os.environ.get(k) or "").strip()
        if v:
            return v
    return PROXY_DEFAULT_WIN if os.name == "nt" else None


def install_signal_handlers():
    def _handler(signum, _frame):
        global STOP
        if not STOP:
            STOP = True
            print(f"\n[collector] 收到信号 {signum}，收尾当前窗口后退出（最多等一个采样间隔）",
                  flush=True)
    for name in ("SIGTERM", "SIGINT", "SIGHUP"):
        s = getattr(signal, name, None)
        if s is None:
            continue
        try:
            signal.signal(s, _handler)
        except Exception:
            pass


def slug_for(coin, ts):
    return f"{SLUG_PREFIX.get(coin, coin.lower())}-updown-15m-{ts}"


def out_file(coin, ts):
    day = datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).strftime("%Y%m%d")
    return os.path.join(OUT_DIR, f"poly_{coin}_{day}.jsonl")


def log_line(fh, obj):
    fh.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n")
    fh.flush()


def write_row(coin, ts, obj):
    fp = out_file(coin, ts)
    with open(fp, "a", encoding="utf-8") as fh:
        log_line(fh, obj)


def parse_arr(v):
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        try:
            r = json.loads(v)
            return r if isinstance(r, list) else []
        except Exception:
            return []
    return []


def fnum(v, d=None):
    try:
        return float(v)
    except Exception:
        return d


def depth_curve(levels, thresholds, mode):
    if mode == "ask":
        lv = sorted(levels, key=lambda x: x[0])
        thr = sorted(thresholds)
        key_fn = lambda p, t: p <= t + 1e-12
    else:
        lv = sorted(levels, key=lambda x: -x[0])
        thr = sorted(thresholds, reverse=True)
        key_fn = lambda p, t: p >= t - 1e-12
    out, cum, i = {}, 0.0, 0
    for t in thr:
        while i < len(lv) and key_fn(lv[i][0], t):
            cum += lv[i][1]
            i += 1
        out[str(t)] = round(cum, 2)
    return out


def _levels(raw, side):
    lv = []
    for x in (raw.get(side) or []):
        p, s = fnum(x.get("price")), fnum(x.get("size"))
        if p is not None and s is not None and s > 0:
            lv.append((p, s))
    lv.sort(key=lambda x: -x[0] if side == "bids" else x[0])
    return lv


def pct(sorted_vals, p):
    if not sorted_vals:
        return None
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    k = max(0, min(len(sorted_vals) - 1, int(round(p / 100.0 * (len(sorted_vals) - 1)))))
    return sorted_vals[k]


def med(vals):
    vals = [v for v in vals if v is not None]
    return statistics.median(vals) if vals else None


class Collector:
    def __init__(self, ex, args):
        self.ex = ex
        self.args = args
        self.coins = args.coins
        self.windows = {}
        self.pending = {}
        self.last_req = 0.0
        self.recent = collections.deque(maxlen=20)
        self.slow = 1.0
        self.last_settle_check = 0.0
        self.last_print = 0.0
        self.n_ok = self.n_err = 0
        self.t_start = time.time()

    async def _call(self, method, params, where, coin=None, ts=None):
        fn = getattr(self.ex, method)
        last = None
        for attempt in range(1, MAX_RETRIES + 1):
            gap = MIN_REQUEST_GAP_S - (time.time() - self.last_req)
            if gap > 0:
                await asyncio.sleep(gap)
            self.last_req = time.time()
            t0 = time.time()
            try:
                res = await fn(params)
                if res is None:
                    raise RuntimeError("empty response")
                self.n_ok += 1
                self.recent.append(True)
                self._adapt()
                return res
            except Exception as e:
                self.n_err += 1
                self.recent.append(False)
                self._adapt()
                last = e
                el = int((time.time() - t0) * 1000)
                if coin:
                    write_row(coin, ts or int(time.time()) // WINDOW_SECONDS * WINDOW_SECONDS,
                              {"kind": "err", "ts_ms": now_ms(), "coin": coin, "where": where,
                               "attempt": attempt, "elapsed_ms": el,
                               "err": f"{type(e).__name__}: {e}"[:240]})
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(BACKOFF_BASE_S * attempt)
        raise last

    def _adapt(self):
        if len(self.recent) < 20:
            return
        rate = sum(1 for x in self.recent if not x) / len(self.recent)
        new = 1.0 if rate < 0.4 else (2.0 if rate < 0.7 else 3.0)
        if new != self.slow:
            print(f"[collector] 请求失败率 {rate:.0%} -> 采样间隔 ×{new:.0f}", flush=True)
            self.slow = new

    async def _gamma_market(self, slug, coin, ts, closed=None):
        params = {"slug": slug, "limit": 2}
        if closed is not None:
            params["closed"] = "true" if closed else "false"
        if self.args.bust_cache:
            params["_nocache"] = now_ms()
        res = await self._call("gammaPublicGetMarkets", params, "gamma/markets", coin, ts)
        rows = res if isinstance(res, list) else (res.get("data") if isinstance(res, dict) else None) or []
        for m in rows:
            if m.get("slug") == slug:
                return m
        return rows[0] if rows else None

    async def resolve(self, w):
        coin, ts = w["coin"], w["ts"]
        ts_ms = now_ms()
        try:
            m = await self._gamma_market(w["slug"], coin, ts)
        except Exception:
            w["resolve_fails"] += 1
            return
        if not m:
            w["resolve_fails"] += 1
            if w["resolve_fails"] in (1, 5, 15):
                write_row(coin, ts, {"kind": "info", "ts_ms": ts_ms, "coin": coin,
                                     "msg": f"market not found: {w['slug']} (try #{w['resolve_fails']})"})
            return
        labels = parse_arr(m.get("outcomes"))
        toks = parse_arr(m.get("clobTokenIds"))
        ids = {}
        for i, lab in enumerate(labels):
            if i >= len(toks) or not toks[i]:
                continue
            ids["UP" if str(lab).strip().lower() in ("up", "yes") else "DOWN"] = toks[i]
        if "UP" not in ids or "DOWN" not in ids:
            write_row(coin, ts, {"kind": "info", "ts_ms": ts_ms, "coin": coin,
                                 "msg": f"token ids unresolvable outcomes={labels} n_toks={len(toks)}"})
            w["resolve_fails"] += 1
            return
        w["token_ids"] = ids
        w["resolved"] = True
        w["tick_size"] = fnum(m.get("orderPriceMinTickSize"), 0.01)
        w["min_size"] = fnum(m.get("orderMinSize"), 1.0)
        fee_bps, fee_raw = None, None
        try:
            fee_raw = await self._call("clobPublicGetFeeRate", {"token_id": ids["UP"]}, "clob/fee-rate", coin, ts)
            fee_bps = fnum((fee_raw or {}).get("base_fee"))
        except Exception:
            pass
        write_row(coin, ts, {
            "kind": "market", "ts_ms": ts_ms, "coin": coin, "slug": w["slug"],
            "window_ts": ts, "exp_ms": (ts + WINDOW_SECONDS) * 1000,
            "market_id": m.get("id"), "condition_id": m.get("conditionId"),
            "question": m.get("question"), "outcomes": labels, "token_ids": ids,
            "tick_size": w["tick_size"], "min_size": w["min_size"],
            "neg_risk": bool(m.get("negRisk")), "accepting_orders": bool(m.get("acceptingOrders")),
            "fees_enabled": bool(m.get("feesEnabled")), "fee_type": m.get("feeType"),
            "base_fee_bps": fee_bps, "fee_raw": fee_raw,
            "end_date": m.get("endDate"), "liquidity": fnum(m.get("liquidity")),
            "volume": fnum(m.get("volume")),
            "gamma_best_bid": fnum(m.get("bestBid")), "gamma_best_ask": fnum(m.get("bestAsk")),
            "gamma_spread": fnum(m.get("spread")), "uma": m.get("umaResolutionStatus"),
        })

    async def _book(self, w, tag):
        params = {"token_id": w["token_ids"][tag]}
        if self.args.bust_cache:
            params["_nocache"] = now_ms()
        t0 = time.time()
        try:
            raw = await self._call("clobPublicGetBook", params, "clob/book", w["coin"], w["ts"])
        except Exception as e:
            return {"err": f"{type(e).__name__}: {e}"[:160], "rtt_ms": int((time.time() - t0) * 1000)}
        bids, asks = _levels(raw, "bids"), _levels(raw, "asks")
        bb = bids[0][0] if bids else None
        ba = asks[0][0] if asks else None
        return {
            "bid": bb, "bid_sz": bids[0][1] if bids else None,
            "ask": ba, "ask_sz": asks[0][1] if asks else None,
            "mid": round((bb + ba) / 2, 4) if (bb is not None and ba is not None) else None,
            "n_bid": len(bids), "n_ask": len(asks),
            "bid_depth": depth_curve(bids, DEPTH_THRESHOLDS, "bid"),
            "ask_depth": depth_curve(asks, DEPTH_THRESHOLDS, "ask"),
            "last": fnum(raw.get("last_trade_price")), "tick": fnum(raw.get("tick_size")),
            "min_sz": fnum(raw.get("min_order_size")),
            "srv_ts": fnum(raw.get("timestamp")), "book_hash": raw.get("hash"),
            "rtt_ms": int((time.time() - t0) * 1000),
        }

    async def sample(self, w):
        ts_ms = now_ms()
        up = await self._book(w, "UP")
        dn = await self._book(w, "DOWN")
        if "err" in up and "err" in dn:
            return
        row = {"kind": "sample", "ts_ms": ts_ms, "coin": w["coin"], "slug": w["slug"],
               "window_ts": w["ts"], "exp_remain_s": w["ts"] + WINDOW_SECONDS - ts_ms / 1000.0,
               "up": up, "down": dn}
        write_row(w["coin"], w["ts"], row)
        w["n_samples"] += 1
        w["last_ms"] = ts_ms
        for tag, b in (("up", up), ("down", dn)):
            if "err" in b or b.get("mid") is None:
                continue
            lo, hi = w.setdefault(f"{tag}_mid_range", (b["mid"], b["mid"]))
            w[f"{tag}_mid_range"] = (min(lo, b["mid"]), max(hi, b["mid"]))

    def finalize(self, w):
        self.windows.pop((w["coin"], w["ts"]), None)
        if w.get("n_samples"):
            write_row(w["coin"], w["ts"], {
                "kind": "summary", "ts_ms": now_ms(), "coin": w["coin"], "slug": w["slug"],
                "window_ts": w["ts"], "n_samples": w["n_samples"],
                "last_sample_ms": w.get("last_ms"),
                "up_mid_range": w.get("up_mid_range"), "down_mid_range": w.get("down_mid_range"),
                "resolve_fails": w.get("resolve_fails")})
        # 改动 4：加 last_try，配合指数退避
        self.pending[(w["coin"], w["ts"])] = {"slug": w["slug"], "attempts": 0, "last_try": 0.0}

    async def try_settle(self, coin, ts, st):
        try:
            m = await self._gamma_market(st["slug"], coin, ts, closed=True)
        except Exception:
            return False
        if not m:
            return False
        labels = [str(x).strip().lower() for x in parse_arr(m.get("outcomes"))]
        prices = [fnum(x) for x in parse_arr(m.get("outcomePrices"))]
        closed = bool(m.get("closed"))
        decisive = None
        for i, p in enumerate(prices):
            if p is not None and p >= 0.99:
                decisive = i
        if decisive is None and not closed:
            return False
        if decisive is None:
            return False
        win_label = labels[decisive] if decisive < len(labels) else str(decisive)
        write_row(coin, ts, {
            "kind": "settle", "ts_ms": now_ms(), "coin": coin, "slug": st["slug"],
            "window_ts": ts, "closed": closed, "uma": m.get("umaResolutionStatus"),
            "outcomes": labels, "outcome_prices": prices,
            "win_index": decisive, "win_label": win_label,
            "up_won": win_label in ("up", "yes")})
        return True

    async def loop(self):
        print(f"[collector] proxy={'direct' if not self.args.proxy else self.args.proxy} "
              f"coins={self.coins} poll={POLL_SECONDS}s out={OUT_DIR}", flush=True)
        for c in self.coins:
            write_row(c, int(time.time()) // WINDOW_SECONDS * WINDOW_SECONDS,
                      {"kind": "info", "ts_ms": now_ms(), "coin": c,
                       "msg": f"collector started proxy={self.args.proxy or 'direct'} "
                              f"poll={POLL_SECONDS}s burst={POLL_SECONDS_BURST}s"})
        stop_at = self.t_start + self.args.run_for if self.args.run_for else None

        while not STOP:
            t0 = time.time()
            ts = int(time.time()) // WINDOW_SECONDS * WINDOW_SECONDS
            try:
                for w in [x for x in self.windows.values() if x["ts"] + WINDOW_SECONDS <= time.time()]:
                    self.finalize(w)
                for coin in self.coins:
                    key = (coin, ts)
                    w = self.windows.get(key)
                    if w is None:
                        w = self.windows[key] = {"coin": coin, "ts": ts, "slug": slug_for(coin, ts),
                                                 "resolved": False, "resolve_fails": 0,
                                                 "n_samples": 0, "last_ms": None,
                                                 "last_resolve_ms": 0}
                    if (not w["resolved"]
                            and ts + WINDOW_SECONDS - time.time() > 60
                            and now_ms() - w["last_resolve_ms"] > MARKET_RESOLVE_RETRY_S * 1000):
                        w["last_resolve_ms"] = now_ms()
                        await self.resolve(w)
                for coin in self.coins:
                    w = self.windows.get((coin, ts))
                    if w and w["resolved"]:
                        await self.sample(w)

                # 改动 2：结算指数退避
                now_t = time.time()
                for (coin, wts), st in list(self.pending.items()):
                    n = st.get("attempts", 0)
                    backoff = min(SETTLE_RETRY_S * (2 ** n), SETTLE_BACKOFF_MAX_S)
                    if now_t - st.get("last_try", 0.0) < backoff:
                        continue
                    st["last_try"] = now_t
                    if await self.try_settle(coin, wts, st):
                        del self.pending[(coin, wts)]
                    else:
                        st["attempts"] = n + 1
                        if st["attempts"] > MAX_SETTLE_ATTEMPTS:
                            write_row(coin, wts, {"kind": "settle_miss", "ts_ms": now_ms(),
                                                  "coin": coin, "slug": st["slug"],
                                                  "window_ts": wts, "attempts": st["attempts"]})
                            del self.pending[(coin, wts)]

                if time.time() - self.last_print > STATUS_PRINT_S:
                    self.last_print = time.time()
                    run_s = time.time() - self.t_start
                    print(f"[collector] {datetime.datetime.now():%H:%M:%S} "
                          f"windows={len(self.windows)} pending_settle={len(self.pending)} "
                          f"req ok={self.n_ok} err={self.n_err} "
                          f"err_rate={self.n_err / max(1, self.n_ok + self.n_err):.0%} "
                          f"slow=×{self.slow:.0f} up={run_s / 3600:.2f}h", flush=True)
            except Exception as e:
                print(f"[collector] loop error: {type(e).__name__}: {e}", flush=True)

            if stop_at and time.time() >= stop_at:
                break
            if STOP:
                break
            poll = POLL_SECONDS_BURST if (time.time() - ts) < BURST_SECONDS else POLL_SECONDS
            remain = max(1.0, poll * self.slow - (time.time() - t0))
            while remain > 0 and not STOP:
                chunk = min(1.0, remain)
                await asyncio.sleep(chunk)
                remain -= chunk

        # ===== 收尾 =====
        # 改动 1：先 finalize 当前窗口，然后等尾窗结束 + 60s 再尝试结算
        for w in list(self.windows.values()):
            self.finalize(w)

        if self.pending:
            last_end = max(wts + WINDOW_SECONDS for (_, wts) in self.pending)
            wait_until = last_end + TAIL_SETTLE_WAIT_S
            now = time.time()
            if wait_until > now:
                wait_s = wait_until - now
                print(f"[collector] 收尾等待尾窗结算 {wait_s:.0f}s"
                      f"（到 {datetime.datetime.fromtimestamp(wait_until):%H:%M:%S}）", flush=True)
                while time.time() < wait_until and not STOP:
                    await asyncio.sleep(1.0)

        for (coin, wts), st in list(self.pending.items()):
            try:
                if await self.try_settle(coin, wts, st):
                    del self.pending[(coin, wts)]
            except Exception:
                pass
        for c in self.coins:
            write_row(c, int(time.time()) // WINDOW_SECONDS * WINDOW_SECONDS,
                      {"kind": "info", "ts_ms": now_ms(), "coin": c,
                       "msg": f"collector stopped signaled={STOP} "
                              f"ok={self.n_ok} err={self.n_err} pending_settle={len(self.pending)}"})
        print(f"[collector] stopped  运行 {(time.time()-self.t_start)/3600:.2f}h  "
              f"req ok={self.n_ok} err={self.n_err}  未结算={len(self.pending)}", flush=True)


def make_exchange(proxy):
    try:
        from ccxt.prediction import polymarket as P
        import ccxt
    except ImportError:
        sys.exit("缺少 ccxt（需 >= 4.5.76 的 prediction 模块），请先安装：\n"
                 "  pip3 install \"ccxt>=4.5.76\"")
    ex = P({"timeout": TIMEOUT_MS, "enableRateLimit": True})
    if proxy:
        ex.https_proxy = proxy
    print(f"[collector] python={sys.version.split()[0]} ccxt={getattr(ccxt, '__version__', '?')} "
          f"proxy={proxy or 'direct'}", flush=True)
    return ex


async def selfcheck(col):
    ts = int(time.time()) // WINDOW_SECONDS * WINDOW_SECONDS
    coin = col.coins[0]
    m = None
    for cand in (ts, ts - WINDOW_SECONDS):
        try:
            m = await col._gamma_market(slug_for(coin, cand), coin, cand)
        except Exception as e:
            print(f"[selfcheck] gamma 请求失败：{type(e).__name__}: {e}", flush=True)
            return False
        if m:
            ts = cand
            break
    if not m:
        print("[selfcheck] gamma 通但未取到市场（gamma 索引延迟或 slug 推导异常）", flush=True)
        return False
    print(f"[selfcheck] gamma OK  slug={m.get('slug')} outcomes={parse_arr(m.get('outcomes'))} "
          f"tick={m.get('orderPriceMinTickSize')} min_sz={m.get('orderMinSize')} "
          f"best_bid={m.get('bestBid')} best_ask={m.get('bestAsk')} spread={m.get('spread')}",
          flush=True)
    toks = parse_arr(m.get("clobTokenIds"))
    if not toks:
        print("[selfcheck] clobTokenIds 为空，无法继续", flush=True)
        return False
    tok = toks[0]
    t0 = time.time()
    try:
        book = await col._call("clobPublicGetBook", {"token_id": tok}, "clob/book")
    except Exception as e:
        print(f"[selfcheck] CLOB /book 失败：{type(e).__name__}: {e}", flush=True)
        return False
    rtt = int((time.time() - t0) * 1000)
    bids, asks = _levels(book, "bids"), _levels(book, "asks")
    lag = now_ms() - int(fnum(book.get("timestamp"), now_ms()))
    print(f"[selfcheck] clob OK  RTT={rtt}ms  bid={bids[0][0] if bids else None} "
          f"ask={asks[0][0] if asks else None} n_bid={len(bids)} n_ask={len(asks)} "
          f"srv_ts_lag={lag}ms", flush=True)
    try:
        fee = await col._call("clobPublicGetFeeRate", {"token_id": tok}, "clob/fee-rate")
        print(f"[selfcheck] fee-rate OK  {fee}", flush=True)
    except Exception as e:
        print(f"[selfcheck] fee-rate 失败（不致命）：{type(e).__name__}: {e}", flush=True)
    print("[selfcheck] 通过 —— 通路可用，进入采集", flush=True)
    return True


# ------------------------------ go/no-go 报告 ------------------------------
def report(out_dir):
    rows = []
    for fp in sorted(glob.glob(os.path.join(out_dir, "poly_*.jsonl"))):
        with open(fp, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except Exception:
                    pass
    if not rows:
        print(f"[report] {out_dir} 下没有数据")
        return
    by_coin = collections.defaultdict(list)
    for r in rows:
        by_coin[r.get("coin", "?")].append(r)

    print("=" * 78)
    print(f"Polymarket 15min 盘口 Step 0 报告   rows={len(rows)}")
    print("=" * 78)
    verdict = []

    for coin, rs in sorted(by_coin.items()):
        markets = [r for r in rs if r["kind"] == "market"]
        samples = [r for r in rs if r["kind"] == "sample"]
        settles = [r for r in rs if r["kind"] == "settle"]
        errs = [r for r in rs if r["kind"] == "err"]

        # 改动 3：剔窗——首采样偏移 > 60s 的窗口不纳入统计
        first_sample_ms = {}
        for r in samples:
            key = (r["coin"], r["window_ts"])
            if key not in first_sample_ms:
                first_sample_ms[key] = r["ts_ms"]
        bad_windows = set()
        for (c2, wts), first_ms in first_sample_ms.items():
            offset_s = first_ms / 1000.0 - wts
            if offset_s > 60:
                bad_windows.add((c2, wts))
        n_before = len(samples)
        samples = [r for r in samples if (r["coin"], r["window_ts"]) not in bad_windows]
        n_dropped = n_before - len(samples)

        wins = sum(1 for s in settles if s.get("up_won"))
        print(f"\n--- {coin} ---")
        print(f"窗口 {len(markets)}  采样 {len(samples)}"
              + (f"（剔窗 -{n_dropped}）" if n_dropped else "")
              + f"  结算 {len(settles)}(up {wins}/down {len(settles)-wins})  "
                f"失败请求 {len(errs)}")
        if errs:
            print(f"  失败率 {len(errs)/max(1,len(errs)+len(samples)*2):.0%}"
                  f"  首条: {errs[0]['err'][:90]}")
        if not samples:
            continue

        spreads, fill_med, fill_frac, srv_lag, gaps = [], {}, {}, [], []
        prev_ms = {}
        fee_bps_vals = [m.get("base_fee_bps") for m in markets if m.get("base_fee_bps") is not None]
        for r in samples:
            ts_ms = r["ts_ms"]
            p = prev_ms.get(r["coin"])
            if p:
                gaps.append((ts_ms - p) / 1000.0)
            prev_ms[r["coin"]] = ts_ms
            up = r.get("up") or {}
            if up.get("err"):
                continue
            if up.get("bid") is not None and up.get("ask") is not None:
                spreads.append(round((up["ask"] - up["bid"]) * 100, 3))
            if up.get("srv_ts"):
                srv_lag.append(ts_ms - up["srv_ts"])
            for tag in ("up", "down"):
                bk = r.get(tag) or {}
                if bk.get("err"):
                    continue
                for q, name in Q_MAX_LIST:
                    d = (bk.get("ask_depth") or {}).get(str(q))
                    if not d:
                        continue
                    fill_med.setdefault(name, []).append(d)
                    fill_frac.setdefault(name, []).append(1 if d >= MIN_FILL_SHARES else 0)

        ss = sorted(s for s in spreads if s is not None)
        print(f"  点差(UP token, ¢): 中位 {med(ss)}  p25 {pct(ss,25)}  p75 {pct(ss,75)}  p90 {pct(ss,90)}  n={len(ss)}")
        print(f"  采集节奏(s): 中位间隔 {round(med(gaps),1) if gaps else None}  实际采样 {len(samples)} 条")
        if srv_lag:
            print(f"  服务端时间戳滞后(ms): 中位 {med(srv_lag):.0f}")
        if fee_bps_vals:
            print(f"  base_fee(bps): 取值 {sorted(set(fee_bps_vals))}"
                  f"  feesEnabled={sorted(set(bool(m.get('fees_enabled')) for m in markets))}")
        else:
            print("  base_fee: 未取到（/fee-rate 请求全部失败）")
        fee_p50 = FEE_RATE_MAX * 0.5 * (0.5 * 0.5) ** 2
        print(f"  50¢ 处封顶有效费率: {fee_p50*100:.2f}% 每股多付 {fee_p50*100:.2f}¢")
        print(f"  <=q_max 一侧 50 股可得性（仅统计最优卖价<=q_max 的可触发机会；UP/DOWN 两侧合计）:")
        for q, name in Q_MAX_LIST:
            f = fill_frac.get(name, [])
            if f:
                print(f"    q_max={q} ({name}): 可触发 {len(f)} 机会 / {len(samples)} 条采样  中位深度 {med(fill_med.get(name, []))} 股  "
                      f"≥{MIN_FILL_SHARES}股占比 {sum(f)/len(f)*100:.0f}%")
            else:
                print(f"    q_max={q} ({name}): 两侧均未触及（0 机会 / {len(samples)} 条采样）")

        m_spread = med(ss)
        f_main = fill_frac.get("main", [])
        frac_main = sum(f_main) / len(f_main) if f_main else 0.0
        d_main = med(fill_med.get("main", []))
        if m_spread is None:
            verdict.append(f"{coin}: 数据不足")
        elif m_spread >= 4:
            verdict.append(f"{coin}: 终止 —— 中位点差 {m_spread}¢ ≥ 4¢")
        elif fee_p50 > 0.01 and fee_bps_vals:
            verdict.append(f"{coin}: 终止 —— 50¢ 处手续费 >1%")
        elif len(f_main) < 20:
            verdict.append(f"{coin}: 数据不足 —— 可触发机会仅 {len(f_main)} 个，需继续挂机")
        elif (d_main is None) or (frac_main < 0.3):
            verdict.append(f"{coin}: 终止 —— 可触发机会中 ≥{MIN_FILL_SHARES}股占比仅 {frac_main:.0%}")
        elif m_spread <= 2 and frac_main >= 0.5:
            verdict.append(f"{coin}: GO —— 中位点差 {m_spread}¢、可触发机会 ≥{MIN_FILL_SHARES}股占比 {frac_main:.0%}")
        else:
            verdict.append(f"{coin}: 灰色 —— 点差 {m_spread}¢ / ≥{MIN_FILL_SHARES}股占比 {frac_main:.0%}，需人工判断")

    print("\n" + "=" * 78)
    print("结论（Step 0 go/no-go）")
    for v in verdict:
        print(f"  {v}")
    ok = sum(1 for v in verdict if "GO" in v)
    print(f"\n可用币种 {ok}/{len(verdict)}；若全部为灰色/终止，优先自建 Cloudflare Pages 反代再复测。")
    print("=" * 78)


def main():
    global OUT_DIR
    ap = argparse.ArgumentParser(description="Polymarket 15min 盘口采集器（Step 0）")
    ap.add_argument("--coins", default=",".join(COINS_DEFAULT), help="币种，逗号分隔")
    ap.add_argument("--proxy", default=None, help="HTTP 代理；空串=显式直连；不传=按环境变量")
    ap.add_argument("--run-for", type=float, default=0, help="运行秒数，0=常驻")
    ap.add_argument("--report", action="store_true", help="只读已落盘数据出报告，不联网")
    ap.add_argument("--out", default=OUT_DIR, help="数据目录")
    ap.add_argument("--bust-cache", action="store_true", help="给目标 URL 加 _nocache 参数")
    ap.add_argument("--skip-selfcheck", action="store_true", help="跳过启动自检")
    args = ap.parse_args()
    args.coins = [c.strip().upper() for c in args.coins.split(",") if c.strip()]
    args.proxy = resolve_proxy(args.proxy)
    OUT_DIR = args.out

    if args.report:
        report(OUT_DIR)
        return
    os.makedirs(OUT_DIR, exist_ok=True)
    install_signal_handlers()
    ex = make_exchange(args.proxy)
    col = Collector(ex, args)
    loop = asyncio.new_event_loop()
    try:
        if not args.skip_selfcheck:
            loop.run_until_complete(selfcheck(col))
        loop.run_until_complete(col.loop())
    except KeyboardInterrupt:
        print("\n[collector] interrupted", flush=True)
    finally:
        try:
            loop.run_until_complete(ex.close())
        except Exception:
            pass
        loop.close()


if __name__ == "__main__":
    main()
