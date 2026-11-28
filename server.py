import gc, os, re, json, glob, time, errno, shutil, platform, zipfile, importlib.metadata, threading, queue, subprocess, uuid, urllib.request
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel

DATA = Path(os.environ.get("DATA_DIR", "data")); DATA.mkdir(exist_ok=True)
LOG = Path(os.environ.get("LOG_DIR", "logs")); LOG.mkdir(exist_ok=True)  # performance logs, kept outside data/ so cleanups never touch them
KEEP_HOURS, SPEED_EVERY, SPEED_BYTES = 48, 600, 2_000_000
SAMPLE_EVERY = float(os.environ.get("SAMPLE_EVERY", "5"))  # seconds between performance samples while a job runs
DISK_MARGIN = 500_000_000  # free space needed on top of 2x the video size (download + merged copy)
STEP = 16                  # lines per progress report; equals NLLB max_batch_size below, so same speed as 32
SEARCH_PER_MIN = 12        # global rate limit for /api/search
# code, display name, ISO-639-2 tag for the subtitle track. Arabic first = default.
TARGETS = [("ar", "Arabic", "ara"), ("en", "English", "eng"), ("tr", "Turkish", "tur"), ("es", "Spanish", "spa"),
           ("fr", "French", "fra"), ("de", "German", "deu"), ("ur", "Urdu", "urd"), ("fa", "Persian", "fas")]
INCLUDE_AUTO = True  # False = manual captions only
# NLLB-200 language codes (YouTube caption code -> NLLB). Captions in other languages are hidden from the list.
LANG = {"ar": "arb_Arab", "tr": "tur_Latn", "en": "eng_Latn", "es": "spa_Latn", "fr": "fra_Latn", "de": "deu_Latn",
        "ur": "urd_Arab", "fa": "pes_Arab", "ru": "rus_Cyrl", "pt": "por_Latn", "it": "ita_Latn", "nl": "nld_Latn",
        "pl": "pol_Latn", "uk": "ukr_Cyrl", "ro": "ron_Latn", "el": "ell_Grek", "he": "heb_Hebr", "iw": "heb_Hebr",
        "hi": "hin_Deva", "bn": "ben_Beng", "id": "ind_Latn", "ms": "zsm_Latn", "vi": "vie_Latn", "th": "tha_Thai",
        "ko": "kor_Hang", "ja": "jpn_Jpan", "zh": "zho_Hans", "zh-Hans": "zho_Hans", "zh-Hant": "zho_Hant",
        "sv": "swe_Latn", "da": "dan_Latn", "no": "nob_Latn", "nb": "nob_Latn", "fi": "fin_Latn", "cs": "ces_Latn",
        "hu": "hun_Latn", "bg": "bul_Cyrl", "sr": "srp_Cyrl", "hr": "hrv_Latn", "sk": "slk_Latn", "sl": "slv_Latn",
        "lt": "lit_Latn", "lv": "lvs_Latn", "et": "est_Latn", "az": "azj_Latn", "ka": "kat_Geor", "hy": "hye_Armn",
        "kk": "kaz_Cyrl", "uz": "uzn_Latn", "sw": "swh_Latn", "af": "afr_Latn", "sq": "als_Latn", "bs": "bos_Latn",
        "mk": "mkd_Cyrl", "ta": "tam_Taml", "te": "tel_Telu", "ml": "mal_Mlym", "mr": "mar_Deva", "gu": "guj_Gujr",
        "pa": "pan_Guru", "ne": "npi_Deva", "si": "sin_Sinh", "my": "mya_Mymr", "km": "khm_Khmr", "fil": "tgl_Latn",
        "tl": "tgl_Latn", "is": "isl_Latn", "ga": "gle_Latn", "cy": "cym_Latn", "ca": "cat_Latn", "gl": "glg_Latn",
        "be": "bel_Cyrl", "mn": "khk_Cyrl", "so": "som_Latn", "am": "amh_Ethi", "ha": "hau_Latn", "yo": "yor_Latn",
        "zu": "zul_Latn", "sd": "snd_Arab", "ps": "pbt_Arab", "tg": "tgk_Cyrl", "ky": "kir_Cyrl", "lo": "lao_Laoo",
        "jv": "jav_Latn", "su": "sun_Latn", "mt": "mlt_Latn", "lb": "ltz_Latn", "ceb": "ceb_Latn"}
NLLB = {"600M": ("JustFrederik/nllb-200-distilled-600M-ct2-int8", "facebook/nllb-200-distilled-600M"),
        "1.3B": ("JustFrederik/nllb-200-distilled-1.3B-ct2-int8", "facebook/nllb-200-distilled-1.3B")}
SIZE = os.environ.get("NLLB_MODEL", "600M")                 # "1.3B" = better but ~2x slower, ~2.1 GB RAM
BEAM = int(os.environ.get("NLLB_BEAM", "4"))                # 2 = faster, slightly less accurate
THREADS = int(os.environ.get("NLLB_THREADS", os.cpu_count() or 2))
ACTIVE = ("queued", "downloading", "translating", "muxing")

app = FastAPI()
jobs, meta, q, procs, cancels = {}, {}, queue.Queue(), {}, set()
speed = {"bps": 0}
cur = {"j": None}                       # job the worker thread is inside right now (even after Stop was pressed)
sc, shits, slock = {}, [], threading.Lock()  # search cache, search timestamps, one yt-dlp search at a time
pf = {}  # per-job performance notes (timeline + facts); written to logs/jobs.jsonl when the job ends

# Overall progress bar (job["pct"], never goes backwards, 100 only when done):
#   download 0-44 | finish download 44 | load model 44-48 | translate 48-90 | English retry 90 | merge 93-99 | done 100
def upd(j, **kw):
    if j in jobs:
        if "pct" in kw and kw.get("status", jobs[j]["status"]) != "done":
            kw["pct"] = min(99, max(kw["pct"], jobs[j]["pct"]))
        jobs[j].update(kw)

def chk(j):
    if j in cancels: raise RuntimeError("Stopped by user")

def busy(): return cur["j"] is not None or any(x["status"] in ACTIVE for x in jobs.values())

def free_bytes():
    s = os.statvfs(DATA); return s.f_bavail * s.f_frsize

def gb(n): return f"{n / 1e9:.1f} GB"

def dsize(d): return sum(f.stat().st_size for f in d.rglob("*") if f.is_file())

def need_for(m, h):  # peak disk use = downloaded streams + merged copy. No size known -> assume 1 MB per second of video
    return 2 * (m["sizes"].get(h) or m["dur"] * 1_000_000) + DISK_MARGIN

def make_room(need):
    """Make `need` bytes free by deleting orphaned folders, then the oldest finished files. Never touches the active job.
    Deletes nothing if even deleting everything would not be enough."""
    c = []
    for d in list(DATA.iterdir()):
        if not d.is_dir() or d.name == cur["j"]: continue
        x = jobs.get(d.name)
        if x is None: c.append((0, d.stat().st_mtime, d, None))        # leftover from a restart: no job, no link
        elif x["status"] == "done": c.append((1, x["t"], d, x))
    c.sort(key=lambda t: t[:2])
    if free_bytes() + sum(dsize(t[2]) for t in c) < need: return False
    for _, _, d, x in c:
        if free_bytes() >= need: break
        shutil.rmtree(d, ignore_errors=True)
        if x: x.update(status="deleted", file="", stage="", ind=False, msg="Deleted to free disk space")
    return free_bytes() >= need

def fmt_dur(dur):
    h, m, s = dur // 3600, (dur % 3600) // 60, dur % 60
    return f"{h}:{m:02d}:{s:02d}" if h else (f"{m}:{s:02d}" if dur else "")

# ---- Performance log in logs/ (open /logs.zip on the page and send it back for tuning):
#   specs.json (once per start) | speedtest.jsonl (every 10 min, idle) | fetch.jsonl (each lookup)
#   samples.jsonl (every SAMPLE_EVERY s while a job runs) | jobs.jsonl (one line per finished job)
def rd(path):
    try: return Path(path).read_text()
    except Exception: return ""

def stat(key, text):  # "MemAvailable:  123 kB" (meminfo) or "pswpin 12" (vmstat) -> int
    m = re.search(rf"^{key}:?\s+(\d+)", text, re.M); return int(m.group(1)) if m else 0

def grab(rx, text):
    m = re.search(rx, text, re.M); return m.group(1) if m else ""

def jl(name, obj):  # append one JSON line; logging must never break the app. One 20 MB rotation.
    try:
        f = LOG / name
        if f.exists() and f.stat().st_size > 20_000_000: f.replace(LOG / (name + ".1"))
        with open(f, "a") as fh: fh.write(json.dumps(obj, separators=(",", ":")) + "\n")
    except Exception: pass

def per(n, s): return round(n / s, 2) if n and s and s > 0 else None

def bps(s):  # yt-dlp speed text "5.20MiB/s" -> bytes per second
    m = re.match(r"([\d.]+)([KMG]i)?B/s", s)
    return float(m.group(1)) * {"Ki": 1024, "Mi": 1024 ** 2, "Gi": 1024 ** 3}.get(m.group(2), 1) if m else 0

def default_iface():
    for l in rd("/proc/net/route").split("\n")[1:]:
        f = l.split()
        if len(f) > 7 and f[1] == "00000000": return f[0]
    return ""

def disk_info():  # which physical disk holds data/ (follows LVM and partitions), and whether it spins
    try:
        st = os.stat(DATA).st_dev
        name = os.path.basename(os.path.realpath(f"/sys/dev/block/{os.major(st)}:{os.minor(st)}")); base = name
        for _ in range(4):
            sl = sorted(os.listdir(f"/sys/block/{base}/slaves")) if os.path.isdir(f"/sys/block/{base}/slaves") else []
            if sl: base = sl[0]; continue
            rp = os.path.realpath(f"/sys/class/block/{base}")
            if os.path.exists(rp + "/partition"): base = os.path.basename(os.path.dirname(rp)); continue
            break
        return {"fs_dev": name, "disk": base, "rotational": rd(f"/sys/block/{base}/queue/rotational").strip(),
                "disk_model": rd(f"/sys/block/{base}/device/model").strip(), "disk_gb": round(int(rd(f"/sys/block/{base}/size") or 0) * 512 / 1e9, 1)}
    except Exception: return {}

DISK = disk_info().get("disk", "")

def throttle(): return sum(int(x) for x in map(rd, glob.glob("/sys/devices/system/cpu/cpu[0-9]*/thermal_throttle/core_throttle_count")) if x.strip().isdigit())

def psi(k):  # share of the last 10 s some task was stalled on cpu / io / memory
    m = re.search(r"some avg10=([\d.]+)", rd(f"/proc/pressure/{k}")); return float(m.group(1)) if m else None

_wr = {"t": 0, "v": {}}
def wifi_rate():  # link rate and band from `iw`, refreshed every 30 s
    if time.time() - _wr["t"] > 30:
        _wr["t"] = time.time()
        try:
            o = subprocess.check_output(["iw", "dev", default_iface(), "link"], text=True, timeout=3, stderr=subprocess.DEVNULL)
            _wr["v"] = {"wifi_mhz": int(grab(r"freq:\s*(\d+)", o) or 0) or None, "wifi_rx_mbit": float(grab(r"rx bitrate:\s*([\d.]+)", o) or 0) or None,
                        "wifi_tx_mbit": float(grab(r"tx bitrate:\s*([\d.]+)", o) or 0) or None}
        except Exception: _wr["v"] = {}
    return _wr["v"]

def vitals():  # cheap snapshot of the usual suspects on an old laptop: CPU speed/heat/throttling, memory, Wi-Fi
    try:
        mi = rd("/proc/meminfo")
        fr = [int(x) for x in map(rd, glob.glob("/sys/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_cur_freq")) if x.strip().isdigit()]
        tm = [int(x) for x in map(rd, glob.glob("/sys/class/thermal/thermal_zone*/temp")) if x.strip().lstrip("-").isdigit()]
        w = next((l.split(":", 1)[1].split() for l in rd("/proc/net/wireless").split("\n") if l.strip().startswith(default_iface() + ":")), None)
        return {"mhz": round(sum(fr) / len(fr) / 1000) if fr else None, "temp_c": round(max(tm) / 1000, 1) if tm else None,
                "throttle": throttle(), "load1": round(os.getloadavg()[0], 2), "mem_avail_mb": stat("MemAvailable", mi) // 1024,
                "swap_used_mb": (stat("SwapTotal", mi) - stat("SwapFree", mi)) // 1024,
                "wifi_link": float(w[1].rstrip(".")) if w else None, "wifi_dbm": float(w[2].rstrip(".")) if w else None, **wifi_rate()}
    except Exception: return {}

def ticks():  # cpu ticks used so far by every process, by name
    out = {}
    for d in Path("/proc").iterdir():
        if d.name.isdigit():
            s = rd(d / "stat"); i = s.rfind(")")
            if i > 0:
                f = s[i + 2:].split()
                out[d.name] = (s[s.find("(") + 1:i], int(f[11]) + int(f[12]))
    return out

def snap():  # raw counters; two of these give rates
    try:
        nd = {l.split(":")[0].strip(): l.split(":", 1)[1].split() for l in rd("/proc/net/dev").split("\n")[2:] if ":" in l}.get(default_iface(), ["0"] * 9)
        ds = next((l.split() for l in rd("/proc/diskstats").split("\n") if l.split()[2:3] == [DISK]), None)
        return {"t": time.time(), "cpu": [int(x) for x in rd("/proc/stat").split("\n")[0].split()[1:]], "rx": int(nd[0]), "tx": int(nd[8]),
                "rs": int(ds[5]) if ds else 0, "ws": int(ds[9]) if ds else 0, "dio": int(ds[12]) if ds else 0,
                "sw": stat("pswpin", rd("/proc/vmstat")) + stat("pswpout", rd("/proc/vmstat")), "pt": ticks()}
    except Exception: return {"t": time.time(), "cpu": [0] * 8, "rx": 0, "tx": 0, "rs": 0, "ws": 0, "dio": 0, "sw": 0, "pt": {}}

def sample(j, a, b):  # one row of rates between two snaps
    dt = max(b["t"] - a["t"], 0.001); dc = [y - x for x, y in zip(a["cpu"], b["cpu"])]; tot = sum(dc) or 1
    top = {}
    for pid, (n, tk) in b["pt"].items():
        d = tk - a["pt"].get(pid, (n, tk))[1]
        if d > 0: top[n] = top.get(n, 0) + d
    top = sorted(top.items(), key=lambda x: -x[1])[:3]  # biggest CPU users; 100 = one full core
    J = jobs.get(j, {})
    return {"j": j, "t": int(b["t"]), "s": int(b["t"] - pf[j]["t0"]) if j in pf else None, "stage": J.get("stage") or J.get("status"), "pct": J.get("pct"),
            "cpu": round(100 * (tot - dc[3] - dc[4]) / tot, 1), "iowait": round(100 * dc[4] / tot, 1),
            "top": [[n, round(100 * d / os.sysconf("SC_CLK_TCK") / dt)] for n, d in top],
            "rx_MBps": round((b["rx"] - a["rx"]) / dt / 1e6, 2), "tx_MBps": round((b["tx"] - a["tx"]) / dt / 1e6, 2),
            "disk_r_MBps": round((b["rs"] - a["rs"]) * 512 / dt / 1e6, 1), "disk_w_MBps": round((b["ws"] - a["ws"]) * 512 / dt / 1e6, 1),
            "disk_util": min(100, round((b["dio"] - a["dio"]) / dt / 10)), "swap_pages": b["sw"] - a["sw"],
            "rss_mb": stat("VmRSS", rd("/proc/self/status")) // 1024, "psi_cpu": psi("cpu"), "psi_io": psi("io"), "psi_mem": psi("memory"),
            "free_gb": round(free_bytes() / 1e9, 1), **vitals()}

def sampler():
    last = None
    while True:
        time.sleep(min(1, SAMPLE_EVERY))
        try:
            j = cur["j"]
            if not j: last = None; continue
            now = snap()
            if last is None: last = now
            elif now["t"] - last["t"] >= SAMPLE_EVERY: jl("samples.jsonl", sample(j, last, now)); last = now
        except Exception: pass

def specs():  # the fixed facts about this machine and setup
    ci, mi = rd("/proc/cpuinfo"), rd("/proc/meminfo")
    def ver(*cmd):
        try: return subprocess.check_output(cmd, text=True, timeout=10, stderr=subprocess.STDOUT).splitlines()[0][:100]
        except Exception: return None
    def pkg(n):
        try: return importlib.metadata.version(n)
        except Exception: return None
    g = lambda x: rd(f"/sys/devices/system/cpu/{x}").strip() or None
    return {"cpu": grab(r"^model name\s*:\s*(.+)$", ci), "logical_cores": os.cpu_count(),
            "physical_cores": len(set(zip(re.findall(r"^physical id\s*:\s*(\d+)", ci, re.M), re.findall(r"^core id\s*:\s*(\d+)", ci, re.M)))) or None,
            "cpu_flags": sorted(set(grab(r"^flags\s*:\s*(.+)$", ci).split()) & {"sse4_2", "avx", "avx2", "fma", "avx512f", "avx512bw", "avx512_vnni", "avx_vnni", "amx_int8"}),
            "cpu_max_mhz": int(g("cpu0/cpufreq/cpuinfo_max_freq") or 0) // 1000 or None, "governor": g("cpu0/cpufreq/scaling_governor"),
            "cpu_driver": g("cpu0/cpufreq/scaling_driver"), "no_turbo": g("intel_pstate/no_turbo"),
            "ram_mb": stat("MemTotal", mi) // 1024, "swap_mb": stat("SwapTotal", mi) // 1024, **disk_info(),
            "fs_total_gb": round(os.statvfs(DATA).f_blocks * os.statvfs(DATA).f_frsize / 1e9, 1),
            "iface": default_iface(), "wifi": os.path.isdir(f"/sys/class/net/{default_iface()}/wireless"),
            "os": platform.platform(), "python": platform.python_version(), "yt_dlp": ver("yt-dlp", "--version"),
            "ffmpeg": ver("ffmpeg", "-version"), "node": ver("node", "--version"),
            "ctranslate2": pkg("ctranslate2"), "transformers": pkg("transformers"),
            "config": {"model": SIZE, "beam": BEAM, "threads": THREADS, "step": STEP, "sample_every": SAMPLE_EVERY}}

try: SPECS = specs()
except Exception as e: SPECS = {"error": str(e)[:100]}
try: (LOG / "specs.json").write_text(json.dumps(SPECS, indent=1))
except Exception: pass

def mk(j, label):  # timeline mark: seconds since the job started (first time only)
    if j in pf and all(x[0] != label for x in pf[j]["tl"]): pf[j]["tl"].append([label, round(time.time() - pf[j]["t0"], 1)])

def kv(j, **kw):
    if j in pf: pf[j]["kv"].update(kw)

def perf_log(j):  # one line per job, written when it ends (done, error or stopped)
    p, J = pf.pop(j, None), jobs.get(j)
    if not p or not J: return
    now, a, b = time.time(), p["s0"], snap()
    tl = p["tl"] + [["end", round(now - p["t0"], 1)]]
    t, k = dict(tl), p["kv"]
    sp = lambda x, y: t[y] - t[x] if x in t and y in t else None
    dc = [y - x for x, y in zip(a["cpu"], b["cpu"])]; tot = sum(dc) or 1
    jl("jobs.jsonl", {
        "id": j, "start": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(p["t0"])), "wall_s": round(now - p["t0"], 1),
        "status": J["status"], "msg": J["msg"], "url": J["url"], "title": J["name"], "src": J["src"], "tgt": J["tgt"],
        "height": J["height"], "duration_s": J["dur"], "est_bytes": J.get("est"), "speedtest_bps_at_start": J.get("spd"),
        "stages_s": {x[0]: round(y[1] - x[1], 1) for x, y in zip(tl, tl[1:])}, "timeline": tl,
        "download_MBps": per((k.get("video_bytes") or 0) / 1e6, sp("video_start", "ytdlp_done")),
        "translate_lines_per_s": per(k.get("cues"), sp("model_load_done", "translate_done")),
        "translate_chars_per_s": per(k.get("chars"), sp("model_load_done", "translate_done")),
        "mux_MBps": per((k.get("final_bytes") or 0) / 1e6, sp("mux_start", "mux_done")), **k,
        "net_rx_mb": round((b["rx"] - a["rx"]) / 1e6), "net_tx_mb": round((b["tx"] - a["tx"]) / 1e6),
        "disk_read_mb": round((b["rs"] - a["rs"]) * 512 / 1e6), "disk_write_mb": round((b["ws"] - a["ws"]) * 512 / 1e6),
        "cpu_avg": round(100 * (tot - dc[3] - dc[4]) / tot, 1), "iowait_avg": round(100 * dc[4] / tot, 1),
        "throttle_events": throttle() - p["thr0"], "free_gb_end": round(free_bytes() / 1e9, 1), "specs": SPECS})

def speed_loop():  # server's own download speed, refreshed every 10 min (skipped while a job runs); also logged
    while True:
        if not busy():
            row = {"t": int(time.time())}
            try:
                t0 = time.time()
                req = urllib.request.Request(f"https://speed.cloudflare.com/__down?bytes={SPEED_BYTES}",
                                             headers={"User-Agent": "Mozilla/5.0"})
                n = len(urllib.request.urlopen(req, timeout=20).read())
                speed["bps"] = int(n / max(time.time() - t0, 0.05)); row["bps"] = speed["bps"]
            except Exception as e: row["err"] = str(e)[:80]
            jl("speedtest.jsonl", {**row, **vitals()})
        time.sleep(SPEED_EVERY)

def parse_srt(path):
    cues, prev = [], []
    for block in re.split(r"\n\s*\n", Path(path).read_text("utf-8-sig").replace("\r", "")):
        lines = block.strip().split("\n")
        if len(lines) < 3 or "-->" not in lines[1]: continue
        txt = [re.sub(r"<[^>]+>", "", l).strip() for l in lines[2:]]
        txt = [l for l in txt if l]
        new = [l for l in txt if l not in prev]
        prev = txt
        if new: cues.append((lines[1], " ".join(new)))
    return cues

def nllb_code(c):
    c = c.replace("_", "-")
    return LANG.get(c) or LANG.get(c.split("-")[0])

def load_nllb():  # loaded per job and freed afterwards, so the RAM is free while downloading/muxing
    import ctranslate2, transformers, huggingface_hub
    repo, tokr = NLLB[SIZE]
    return (ctranslate2.Translator(huggingface_hub.snapshot_download(repo), device="cpu",
                                   compute_type="int8", intra_threads=THREADS),
            transformers.AutoTokenizer.from_pretrained(tokr))

def nllb_run(tr, tok, lines, src, tgt):
    tok.src_lang = src
    toks = [tok.convert_ids_to_tokens(tok.encode(t, truncation=True, max_length=200)) for t in lines]
    res = tr.translate_batch(toks, target_prefix=[[tgt]] * len(toks), beam_size=BEAM,
                             max_batch_size=16, max_decoding_length=256)
    return [tok.decode(tok.convert_tokens_to_ids(r.hypotheses[0][1:]), skip_special_tokens=True) for r in res]

def has_text(s): return any(c.isalpha() for c in s)  # skips music notes, sound symbols, blanks

def suspicious(s, o):  # empty, left untranslated, or stuck in a repeat loop
    w = o.split()
    return (not o.strip() and bool(s.strip())) or (o.strip() == s.strip() and sum(c.isalpha() for c in s) > 6) or \
        any(w[i] == w[i + 1] == w[i + 2] == w[i + 3] for i in range(len(w) - 3))

def translate(lines, j, src, tgt):
    if not lines: raise RuntimeError("Caption file had no text")
    s, t = nllb_code(src), nllb_code(tgt)
    upd(j, stage="Loading translation model", detail="", pct=44, ind=True)
    mk(j, "model_load_start")
    tr, tok = load_nllb()
    mk(j, "model_load_done")
    try:
        upd(j, stage="Translating", pct=48, ind=False)
        txt = [l.replace("\n", " ").strip() for l in lines]
        order = sorted(range(len(txt)), key=lambda i: -len(txt[i]))  # similar lengths batch faster; longest first so the ETA is not too optimistic
        out, t0 = [""] * len(txt), time.time()
        bs = []; kv(j, batch_secs=bs)  # seconds per batch, for the performance log
        for k in range(0, len(order), STEP):
            chk(j); tb = time.time()
            idx = [i for i in order[k:k + STEP] if has_text(txt[i])]
            if idx:
                for i, o in zip(idx, nllb_run(tr, tok, [txt[i] for i in idx], s, t)): out[i] = o
            done = min(k + STEP, len(order)); bs.append(round(time.time() - tb, 1))
            eta = int((time.time() - t0) * (len(order) - done) / done)
            upd(j, pct=48 + done * 42 // len(order),
                detail=f"{done}/{len(order)} lines" + (f" \u2022 ~{eta // 60}m left" if eta > 60 else ""))
        mk(j, "translate_done")
        bad = [i for i in range(len(txt)) if has_text(txt[i]) and suspicious(txt[i], out[i])]
        kv(j, weak_lines=len(bad))
        if bad and "eng_Latn" not in (s, t):  # rescue pass: retry the bad lines through English
            upd(j, stage="Retrying weak lines", detail=f"{len(bad)} lines via English", pct=90, ind=True)
            en = nllb_run(tr, tok, [txt[i] for i in bad], s, "eng_Latn")
            for i, o in zip(bad, nllb_run(tr, tok, en, "eng_Latn", t)): out[i] = o
            bad = [i for i in bad if suspicious(txt[i], out[i])]
            mk(j, "retry_done")
        return [o or x for o, x in zip(out, txt)], len(bad)
    finally:
        del tr; gc.collect()

def run(j):
    d = DATA / j; d.mkdir(exist_ok=True)
    try:
        pf[j] = {"t0": time.time(), "tl": [["start", 0.0]], "kv": {}, "s0": snap(), "thr0": throttle()}
        chk(j)
        J = jobs[j]
        kv(j, queue_wait_s=round(time.time() - J["t"], 1))
        upd(j, status="downloading", stage="Starting download", pct=0, detail="", ind=True)
        fmt = f"bv*[vcodec^=avc1][height<={J['height']}]+ba[ext=m4a]/b[ext=mp4][height<={J['height']}]"
        cmd = ["yt-dlp", "--no-playlist", "--newline", "--restrict-filenames", "--js-runtimes", "node",
               "-f", fmt, "--merge-output-format", "mp4", "--write-subs", "--write-auto-subs", "--sub-langs", J["src"],
               "--convert-subs", "srt", "-o", str(d / "%(title).80B.%(ext)s"), "--", J["url"]]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        procs[j] = p
        kind, nospace, spd = "", False, []
        for line in p.stdout:
            nospace = nospace or "No space left" in line
            m = re.match(r"\[download\] Destination:\s+(.+)", line)
            if m:  # captions download first; only the video and audio streams move the bar
                ext = Path(m.group(1).strip()).suffix.lower()
                kind = "audio" if ext in (".m4a", ".mp3", ".opus", ".aac") else "video" if ext in (".mp4", ".webm", ".mkv", ".m4v") else ""
                if kind: upd(j, stage=f"Downloading {kind}", ind=False); mk(j, kind + "_start")
                continue
            if line.startswith("[Merger]"):
                upd(j, stage="Finishing download", detail="merging video and audio", pct=44, ind=True); mk(j, "ytdlp_merge")
                continue
            m = re.match(r"\[download\]\s+(\d+(?:\.\d+)?)%", line)
            if m and kind:
                f, vf = float(m.group(1)) / 100, J["vf"]
                sp = re.search(r"at\s+(\S+)\s+ETA\s+(\S+)", line)
                if sp and "Unknown" not in sp.group(0) and bps(sp.group(1)) > 0: spd.append(bps(sp.group(1)))
                upd(j, pct=int(44 * (vf * f if kind == "video" else vf + (1 - vf) * f)),
                    detail=f"{f * 100:.0f}%" + (f" \u2022 {sp.group(1)} \u2022 ETA {sp.group(2)}" if sp and "Unknown" not in sp.group(0) else ""))
        rc = p.wait(); procs.pop(j, None); mk(j, "ytdlp_done"); chk(j)
        if spd: kv(j, ytdlp_speed={"avg_MBps": round(sum(spd) / len(spd) / 1e6, 2), "max_MBps": round(max(spd) / 1e6, 2), "min_MBps": round(min(spd) / 1e6, 2), "n": len(spd)})
        if nospace: raise RuntimeError("Disk full during download")
        if rc: raise RuntimeError("Download failed (try updating yt-dlp)")
        vids, subs = list(d.glob("*.mp4")), list(d.glob("*.srt"))
        if not vids: raise RuntimeError("No video produced")
        if not subs: raise RuntimeError("Caption download failed")
        v = vids[0]
        upd(j, name=v.stem)
        cues = parse_srt(subs[0])
        kv(j, video_bytes=v.stat().st_size, cues=len(cues), chars=sum(len(c[1]) for c in cues)); mk(j, "captions_parsed")
        chk(j)
        upd(j, status="translating", stage="Loading translation model", pct=44, detail="", ind=True)
        tr, bad = translate([c[1] for c in cues], j, J["src"], J["tgt"])
        srt = d / "tr.srt"
        srt.write_text("\n".join(f"{i}\n{c[0]}\n{t}\n" for i, (c, t) in enumerate(zip(cues, tr), 1)), "utf-8")
        chk(j)
        mk(j, "srt_written")
        need = int(v.stat().st_size * 1.02) + DISK_MARGIN  # the merged copy is about as big as the download
        if not make_room(need): raise RuntimeError(f"Not enough disk to merge: need {gb(need)}, free {gb(free_bytes())}")
        upd(j, status="muxing", stage="Merging subtitles", pct=93, detail="", ind=not J["dur"])
        mk(j, "mux_start")
        out = d / f"{v.stem}_{J['tgt'].upper()}.mp4"
        iso = next(t[2] for t in TARGETS if t[0] == J["tgt"])
        log = d / "ffmpeg.log"
        with open(log, "w") as lg:
            p = subprocess.Popen(["ffmpeg", "-y", "-nostats", "-loglevel", "error", "-progress", "pipe:1",
                                  "-i", str(v), "-i", str(srt), "-map", "0:v", "-map", "0:a", "-map", "1",
                                  "-c", "copy", "-c:s", "mov_text", "-metadata:s:s:0", f"language={iso}",
                                  "-disposition:s:0", "default", "-movflags", "+faststart", str(out)],
                                 stdout=subprocess.PIPE, stderr=lg, text=True)
            procs[j] = p
            for line in p.stdout:
                m = re.match(r"out_time_(?:us|ms)=(\d+)", line)  # both are microseconds
                if m and J["dur"]:
                    f = min(1.0, int(m.group(1)) / 1e6 / J["dur"])
                    if f >= 0.999: upd(j, stage="Optimizing for streaming", detail="", pct=99, ind=True); mk(j, "faststart_start")  # faststart pass is silent
                    else: upd(j, pct=93 + int(6 * f), detail=f"{int(f * 100)}%")
            rc = p.wait(); procs.pop(j, None)
        mk(j, "mux_done"); chk(j)
        if rc:
            err = log.read_text(errors="ignore")
            raise RuntimeError("Disk full while merging" if "No space left" in err
                               else "Merge failed: " + (err.strip().splitlines() or ["ffmpeg error"])[-1][:120])
        kv(j, final_bytes=out.stat().st_size)
        log.unlink(missing_ok=True)
        v.unlink()
        upd(j, status="done", pct=100, stage="", detail="", ind=False, file=out.name, note=f"({bad} lines may be inaccurate)" if bad else "")
    except Exception as e:
        procs.pop(j, None); shutil.rmtree(d, ignore_errors=True)
        full = (isinstance(e, OSError) and e.errno == errno.ENOSPC) or "No space left" in str(e)
        upd(j, status="stopped" if j in cancels else "error", stage="", ind=False, detail="",
            msg="" if j in cancels else ("Disk full - free some space and try again" if full else str(e)[:200]))
        cancels.discard(j)
    finally:
        try: perf_log(j)
        except Exception: pass
        meta.pop(jobs[j].get("mid"), None)  # free the video info as soon as the job ends

def worker():
    while True:
        j = q.get(); cur["j"] = j
        try: run(j)
        finally: cur["j"] = None

def sweep():  # delete finished files after KEEP_HOURS and mark their jobs expired so no dead Download link stays
    for d in list(DATA.iterdir()):
        x = jobs.get(d.name)
        if d.name == cur["j"] or (x and x["status"] in ACTIVE): continue
        if d.is_dir() and time.time() - d.stat().st_mtime > KEEP_HOURS * 3600:
            shutil.rmtree(d, ignore_errors=True)
            if x and x["status"] == "done": x.update(status="expired", file="", msg=f"Expired after {KEEP_HOURS} h")

def cleaner():
    while True:
        sweep()
        time.sleep(3600)

for fn in (worker, cleaner, speed_loop, sampler): threading.Thread(target=fn, daemon=True).start()

class Fetch(BaseModel):
    url: str

class Start(BaseModel):
    mid: str
    src: str
    tgt: str
    height: int

@app.post("/api/fetch")
def fetch(r: Fetch):
    if not re.match(r"https?://", r.url): raise HTTPException(400, "Bad URL")
    cmd = ["yt-dlp", "--no-playlist", "--no-warnings", "--js-runtimes", "node",
           "--extractor-args", "youtube:skip=translated_subs", "-J", "--", r.url]
    t0 = time.time()
    try: info = json.loads(subprocess.check_output(cmd, text=True, timeout=90, stderr=subprocess.DEVNULL))
    except Exception:
        jl("fetch.jsonl", {"t": int(t0), "url": r.url, "ok": False, "secs": round(time.time() - t0, 1)})
        raise HTTPException(400, "Could not read that video (check the URL, or update yt-dlp)")
    jl("fetch.jsonl", {"t": int(t0), "url": r.url, "ok": True, "secs": round(time.time() - t0, 1), "formats": len(info.get("formats") or [])})
    dur = int(info.get("duration") or 0)
    dstr = fmt_dur(dur)
    subs = [{"code": c, "name": (v[0].get("name") if v else None) or c}
            for c, v in (info.get("subtitles") or {}).items() if c != "live_chat"]
    if INCLUDE_AUTO:  # auto-generated tracks go after manual ones, labelled "(auto)"
        auto = info.get("automatic_captions") or {}
        auto = {c: v for c, v in auto.items() if c.endswith("-orig")} or auto  # prefer the spoken-language track
        have = {x["code"] for x in subs}
        subs += [{"code": c, "name": ((v[0].get("name") if v else None) or c) + " (auto)"}
                 for c, v in auto.items() if c not in have]
    subs = [x for x in subs if nllb_code(x["code"])]  # only languages the translation model supports
    fm = info.get("formats", [])
    size = lambda f: int(f.get("filesize") or f.get("filesize_approx") or (f.get("tbr") or 0) * 125 * dur)
    aud = max((f for f in fm if f.get("vcodec") == "none" and str(f.get("acodec", "")).startswith("mp4a")),
              key=lambda f: f.get("abr") or f.get("tbr") or 0, default=None)
    best = {}  # only H.264 (avc1): the only video codec iPhones play reliably
    for f in fm:
        ht = f.get("height")
        if ht and ht >= 360 and str(f.get("vcodec", "")).startswith("avc1") and \
           (ht not in best or (f.get("tbr") or 0) > (best[ht].get("tbr") or 0)):
            best[ht] = f
    heights = [{"h": k, "size": size(best[k]) + (size(aud) if aud else 0)} for k in sorted(best, reverse=True)]
    mid = uuid.uuid4().hex[:8]
    meta[mid] = {"url": r.url, "subs": [x["code"] for x in subs], "heights": [x["h"] for x in heights],
                 "sizes": {x["h"]: x["size"] for x in heights}, "dur": dur, "asz": size(aud) if aud else 0, "t": time.time()}
    while len(meta) > 3: meta.pop(min(meta, key=lambda k: meta[k]["t"]))  # never hoard abandoned lookups
    return {"mid": mid, "title": info.get("title", ""), "duration": dstr, "thumb": info.get("thumbnail", ""),
            "subs": subs, "heights": heights, "speed": speed["bps"],
            "targets": [{"code": t[0], "name": t[1]} for t in TARGETS]}

@app.get("/api/search")
def search(q: str = ""):
    q = " ".join(q.split())[:100]
    if len(q) < 2: return []
    key, now = q.lower(), time.time()
    if key in sc and now - sc[key][0] < 600: return sc[key][1]
    shits[:] = [t for t in shits if now - t < 60]
    if len(shits) >= SEARCH_PER_MIN: raise HTTPException(429, "Too many searches - wait a few seconds")
    shits.append(now)
    if not slock.acquire(timeout=20): raise HTTPException(429, "Search busy - try again")
    try:
        out = json.loads(subprocess.check_output(["yt-dlp", "--no-warnings", "--flat-playlist", "-J", "--", f"ytsearch10:{q}"],
                                                 text=True, timeout=25, stderr=subprocess.DEVNULL))
    except Exception: raise HTTPException(502, "Search failed (try again, or update yt-dlp)")
    finally: slock.release()
    res = [{"id": e["id"], "title": e.get("title") or "", "channel": e.get("channel") or e.get("uploader") or "",
            "dur": fmt_dur(int(e.get("duration") or 0)) or "live", "thumb": f"https://i.ytimg.com/vi/{e['id']}/mqdefault.jpg",
            "url": f"https://www.youtube.com/watch?v={e['id']}"}
           for e in out.get("entries") or [] if len(e.get("id") or "") == 11]
    sc[key] = (now, res)
    while len(sc) > 20: sc.pop(min(sc, key=lambda k: sc[k][0]))
    return res

@app.post("/api/jobs")
def add(r: Start):
    m = meta.get(r.mid)
    if not m: raise HTTPException(400, "Video info expired - press Submit again")
    if r.src not in m["subs"] or r.height not in m["heights"] or r.tgt not in [t[0] for t in TARGETS] or nllb_code(r.src) == nllb_code(r.tgt):
        raise HTTPException(400, "Invalid selection")
    if busy(): raise HTTPException(409, "A job is already running")
    need = need_for(m, r.height)
    if not make_room(need): raise HTTPException(507, f"Not enough disk space: need {gb(need)}, free {gb(free_bytes())}")
    sz = m["sizes"].get(r.height) or 0
    vf = min(.99, max(.5, 1 - m["asz"] / sz)) if sz and m["asz"] else .95  # video's share of the download, for the bar
    j = uuid.uuid4().hex[:8]
    jobs[j] = dict(id=j, mid=r.mid, url=m["url"], src=r.src, tgt=r.tgt, height=r.height, name="", status="queued",
                   pct=0, stage="", ind=False, detail="", msg="", file="", note="", dur=m["dur"], vf=vf, est=sz, spd=speed["bps"], t=time.time())
    q.put(j)
    return {"id": j}

@app.post("/api/jobs/{j}/stop")
def stop(j: str):
    cancels.add(j)
    p = procs.pop(j, None)
    if p:
        try: p.terminate()
        except Exception: pass
    if j in jobs and jobs[j]["status"] in ACTIVE:
        upd(j, status="stopped", stage="", ind=False, detail="", msg="")
    return {"ok": True}

@app.get("/api/jobs")
def lst(): return sorted(jobs.values(), key=lambda x: -x["t"])

@app.get("/logs.zip")
def logs():  # all performance logs in one zip: open on the phone, save to Files, send it over
    z = LOG / "export.zip"
    with zipfile.ZipFile(z, "w", zipfile.ZIP_DEFLATED) as f:
        for x in sorted(LOG.iterdir()):
            if x.is_file() and x.suffix != ".zip": f.write(x, x.name)
    return FileResponse(z, filename="yt-ar-logs.zip", media_type="application/zip")

@app.get("/files/{j}")
def get(j: str):
    f = jobs.get(j, {}).get("file")
    p = DATA / j / f if f else None
    if not p or not p.exists(): raise HTTPException(404, "File gone")
    return FileResponse(p, filename=f, media_type="video/mp4")

PAGE = r"""<!doctype html><meta name=viewport content="width=device-width,initial-scale=1">
<meta name=color-scheme content="light dark"><title>Video Subtitles</title>
<style>
body{font:16px system-ui;max-width:560px;margin:auto;padding:16px}
input,select,button{font:inherit;padding:12px;width:100%;box-sizing:border-box;margin:4px 0;border-radius:8px;border:1px solid #8886}
button{font-weight:600;cursor:pointer}
.urow{display:flex;gap:6px}.urow input{flex:1;min-width:0}.urow button{width:auto;white-space:nowrap}
#info,#cfg{display:none}#info.on{display:flex;gap:10px;align-items:center;margin:0 0 8px}#cfg.on{display:block}
#info img{width:120px;border-radius:8px}#info div{font-size:14px;word-break:break-word}
#go.off{opacity:.4}#sum{font-size:14px;margin:6px 2px}
#toast{position:fixed;left:50%;bottom:20px;transform:translateX(-50%);background:#222;color:#fff;padding:10px 16px;border-radius:99px;font-size:14px;opacity:0;pointer-events:none;transition:opacity .25s;max-width:90vw;text-align:center}
#toast.on{opacity:1}
.stop{background:#dc2626;color:#fff;border-color:#dc2626;margin-top:8px}
.j{border:1px solid #8886;border-radius:10px;padding:12px;margin:10px 0;word-break:break-word}
.j.old{opacity:.6}
.bar{background:#8883;border-radius:99px;height:14px;overflow:hidden;margin:8px 0;position:relative}
.fill{background:#2563eb;height:100%;transition:width 2s linear}
.fill.live{background:linear-gradient(90deg,#2563eb,#60a5fa,#2563eb,#60a5fa,#2563eb) 0 0/200% 100%;animation:sh 1.6s linear infinite}
.fill.ok{background:#16a34a}
.bar.ind::after{content:"";position:absolute;top:0;bottom:0;width:30%;background:linear-gradient(90deg,transparent,#fff6,transparent);animation:sw 1.2s ease-in-out infinite}
@keyframes sh{to{background-position:100% 0}}
@keyframes sw{from{left:-30%}to{left:100%}}
@media(prefers-reduced-motion:reduce){.fill.live,.bar.ind::after{animation:none}}
.row{display:flex;justify-content:space-between;font-size:14px;opacity:.85}
.sr{display:flex;gap:10px;align-items:center;padding:8px 4px;border-bottom:1px solid #8883;cursor:pointer}
.sr img{width:120px;aspect-ratio:16/9;object-fit:cover;border-radius:8px;flex:none;background:#8883}
.sr div{font-size:14px;min-width:0;word-break:break-word}.sr small{opacity:.7;display:block;margin-top:2px}
.none{font-size:14px;opacity:.7;padding:6px 2px}
</style>
<h2>Video subtitles</h2>
<input id=sq type=search placeholder="Search YouTube" autocomplete=off oninput=sdeb()>
<div id=sres></div>
<div id=info></div>
<div class=urow><input id=u type=url placeholder="YouTube URL" oninput=reset()><button id=sub type=button onclick=submit()>Submit</button></div>
<div id=cfg>
<select id=src onchange=update()></select>
<select id=tgt onchange=update()></select>
<select id=res onchange=update()></select>
<div id=sum></div>
<button id=go type=button class=off onclick=start()>Start</button>
</div>
<div id=l></div>
<p class=none><a href="/logs.zip">Performance logs (zip)</a></p>
<div id=toast></div>
<script>
const $=id=>document.getElementById(id);
const esc=s=>String(s==null?"":s).replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
const ACT=["queued","downloading","translating","muxing"];
let M=null,busy=false,tt,st,sa,SR=[];
function toast(t){const e=$("toast");e.textContent=t;e.classList.add("on");clearTimeout(tt);tt=setTimeout(()=>e.classList.remove("on"),3200)}
function reset(){M=null;$("info").classList.remove("on");$("cfg").classList.remove("on");$("sum").textContent=""}
const gb=b=>!b?"size n/a":b>=1e9?(b/1e9).toFixed(1)+" GB":Math.round(b/1e6)+" MB";
const dur=s=>s<60?"<1 min":s<3600?Math.round(s/60)+" min":Math.floor(s/3600)+" h "+Math.round(s%3600/60)+" min";
function sdeb(){clearTimeout(st);const q=$("sq").value.trim();if(q.length<3){clearSearch();return}st=setTimeout(()=>search(q),600)}
function clearSearch(){if(sa)sa.abort();SR=[];$("sres").innerHTML=""}
async function search(q){
  if(sa)sa.abort();sa=new AbortController();
  $("sres").innerHTML='<div class=none>Searching...</div>';
  try{
    const r=await fetch("/api/search?q="+encodeURIComponent(q),{signal:sa.signal});
    const d=await r.json();if(!r.ok)throw new Error(d.detail||"Search failed");
    SR=d;
    $("sres").innerHTML=d.length?d.map((v,i)=>`<div class=sr onclick="pick(${i})"><img loading=lazy src="${esc(v.thumb)}"><div><b>${esc(v.title)}</b><small>${esc(v.channel)} &bull; ${esc(v.dur)}</small></div></div>`).join(""):'<div class=none>No results</div>';
  }catch(e){if(e.name!=="AbortError"){$("sres").innerHTML="";toast(e.message)}}
}
function pick(i){const v=SR[i];if(!v)return;$("u").value=v.url;$("sq").value="";clearSearch();submit()}
async function submit(){
  const u=$("u").value.trim();if(!u)return;
  const b=$("sub");b.disabled=true;b.textContent="Checking...";reset();
  try{
    const r=await fetch("/api/fetch",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({url:u})});
    const d=await r.json();if(!r.ok)throw new Error(d.detail||"Could not read that URL");
    M=d;render();
  }catch(e){toast(e.message)}
  b.disabled=false;b.textContent="Submit";
}
function render(){
  $("info").innerHTML=`<img src="${esc(M.thumb)}"><div><b>${esc(M.title)}</b><br>${esc(M.duration)} &bull; ${M.subs.length?M.subs.length+" caption language(s)":"no captions"}</div>`;
  $("info").classList.add("on");
  $("src").innerHTML=M.subs.length?M.subs.map(s=>`<option value="${esc(s.code)}">${esc(s.name)}</option>`).join(""):'<option value="">No captions available</option>';
  $("src").disabled=!M.subs.length;
  $("tgt").innerHTML=M.targets.map(t=>`<option value="${t.code}">${esc(t.name)}</option>`).join("");
  $("res").innerHTML='<option value="">Select resolution</option>'+M.heights.map(h=>`<option value="${h.h}">${h.h}p (&asymp;${gb(h.size)})</option>`).join("");
  $("cfg").classList.add("on");update();
}
function same(){return $("src").value&&$("src").value.split("-")[0]===$("tgt").value}
function problem(){
  if(!M.subs.length)return "This video has no captions, so it can't be translated.";
  if(!M.heights.length)return "No iPhone-compatible (H.264) resolutions found.";
  if(!$("src").value||!$("tgt").value||!$("res").value)return "Select a source language, target language and resolution first.";
  if(same())return "Source and target language are the same.";
  if(busy)return "A job is already running.";
  return "";
}
function update(){
  if(!M)return;
  const p=problem();$("go").classList.toggle("off",!!p);
  const sn=$("src").selectedOptions[0],tn=$("tgt").selectedOptions[0];
  if(same()&&$("res").value){$("sum").textContent="Source and target language are the same.";return}
  if(p){$("sum").textContent=M.subs.length?"":p;return}
  const h=M.heights.find(x=>x.h==$("res").value);
  $("sum").textContent=`${sn.text} --> ${tn.text} \u2022 `+(M.speed&&h.size?"~"+dur(h.size/M.speed)+" to download":"download time n/a");
}
async function start(){
  const p=problem();if(p){toast(p);return}
  const r=await fetch("/api/jobs",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({mid:M.mid,src:$("src").value,tgt:$("tgt").value,height:+$("res").value})});
  const d=await r.json();if(!r.ok){toast(d.detail||"Could not start");return}
  reset();$("u").value="";busy=true;poll();
}
async function stopJob(id){await fetch("/api/jobs/"+id+"/stop",{method:"POST"});poll()}
const D={};  // job cards are kept between polls (not rebuilt) so the bar can glide instead of jumping
function paint(j){
  let e=D[j.id];
  if(!e){
    const c=document.createElement("div");c.className="j";
    c.innerHTML='<div><b class=nm></b></div><div class=row><span class=st></span><span class=pc></span></div><div class=bar><div class=fill></div></div><div class=ms></div><div class=ac></div>';
    e=D[j.id]={c,nm:c.querySelector(".nm"),st:c.querySelector(".st"),pc:c.querySelector(".pc"),bar:c.querySelector(".bar"),fill:c.querySelector(".fill"),ms:c.querySelector(".ms"),ac:c.querySelector(".ac")};
    e.fill.style.width=j.pct+"%";$("l").prepend(c);
  }
  const act=ACT.includes(j.status),gone=j.status==="deleted"||j.status==="expired";
  e.c.classList.toggle("old",gone);
  e.nm.textContent=j.name||j.url;
  e.st.textContent=gone?j.status:(act&&j.stage||j.status)+(act&&j.detail?" \u2022 "+j.detail:"");
  e.pc.textContent=gone?"":j.pct+"%";
  e.bar.style.display=gone?"none":"";
  e.bar.classList.toggle("ind",act&&!!j.ind);
  e.fill.style.width=j.pct+"%";
  e.fill.className="fill"+(act?" live":"")+(j.status==="done"?" ok":"");
  e.ms.textContent=j.msg||"";
  const h=(j.status==="done"?`<a href="/files/${esc(j.id)}">Download MP4</a> ${esc(j.note)}`:"")+(act?`<button class=stop onclick="stopJob('${esc(j.id)}')">Stop</button>`:"");
  if(e.ac.dataset.h!==h){e.ac.innerHTML=h;e.ac.dataset.h=h}
}
async function poll(){
  try{
    const list=await(await fetch("/api/jobs")).json();
    busy=list.some(x=>ACT.includes(x.status));
    list.slice().reverse().forEach(paint);
    update();
  }catch(e){}
}
$("u").addEventListener("keydown",e=>{if(e.key==="Enter")submit()});
$("sq").addEventListener("keydown",e=>{if(e.key==="Enter"){clearTimeout(st);const q=$("sq").value.trim();if(q.length>=2)search(q)}});
setInterval(poll,2000);poll();
</script>"""

@app.get("/", response_class=HTMLResponse)
def index():
    return PAGE
