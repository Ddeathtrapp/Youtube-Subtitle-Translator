import gc, os, re, json, time, errno, shutil, threading, queue, subprocess, uuid, urllib.request
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel

DATA = Path(os.environ.get("DATA_DIR", "data")); DATA.mkdir(exist_ok=True)
KEEP_HOURS, SPEED_EVERY, SPEED_BYTES = 48, 600, 2_000_000
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

def speed_loop():  # server's own download speed, refreshed every 10 min (skipped while a job runs)
    while True:
        if not busy():
            try:
                t0 = time.time()
                req = urllib.request.Request(f"https://speed.cloudflare.com/__down?bytes={SPEED_BYTES}",
                                             headers={"User-Agent": "Mozilla/5.0"})
                n = len(urllib.request.urlopen(req, timeout=20).read())
                speed["bps"] = int(n / max(time.time() - t0, 0.05))
            except Exception: pass
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
    tr, tok = load_nllb()
    try:
        upd(j, stage="Translating", pct=48, ind=False)
        txt = [l.replace("\n", " ").strip() for l in lines]
        order = sorted(range(len(txt)), key=lambda i: -len(txt[i]))  # similar lengths batch faster; longest first so the ETA is not too optimistic
        out, t0 = [""] * len(txt), time.time()
        for k in range(0, len(order), STEP):
            chk(j)
            idx = [i for i in order[k:k + STEP] if has_text(txt[i])]
            if idx:
                for i, o in zip(idx, nllb_run(tr, tok, [txt[i] for i in idx], s, t)): out[i] = o
            done = min(k + STEP, len(order))
            eta = int((time.time() - t0) * (len(order) - done) / done)
            upd(j, pct=48 + done * 42 // len(order),
                detail=f"{done}/{len(order)} lines" + (f" \u2022 ~{eta // 60}m left" if eta > 60 else ""))
        bad = [i for i in range(len(txt)) if has_text(txt[i]) and suspicious(txt[i], out[i])]
        if bad and "eng_Latn" not in (s, t):  # rescue pass: retry the bad lines through English
            upd(j, stage="Retrying weak lines", detail=f"{len(bad)} lines via English", pct=90, ind=True)
            en = nllb_run(tr, tok, [txt[i] for i in bad], s, "eng_Latn")
            for i, o in zip(bad, nllb_run(tr, tok, en, "eng_Latn", t)): out[i] = o
            bad = [i for i in bad if suspicious(txt[i], out[i])]
        return [o or x for o, x in zip(out, txt)], len(bad)
    finally:
        del tr; gc.collect()

def run(j):
    d = DATA / j; d.mkdir(exist_ok=True)
    try:
        chk(j)
        J = jobs[j]
        upd(j, status="downloading", stage="Starting download", pct=0, detail="", ind=True)
        fmt = f"bv*[vcodec^=avc1][height<={J['height']}]+ba[ext=m4a]/b[ext=mp4][height<={J['height']}]"
        cmd = ["yt-dlp", "--no-playlist", "--newline", "--restrict-filenames", "--js-runtimes", "node",
               "-f", fmt, "--merge-output-format", "mp4", "--write-subs", "--write-auto-subs", "--sub-langs", J["src"],
               "--convert-subs", "srt", "-o", str(d / "%(title).80B.%(ext)s"), "--", J["url"]]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        procs[j] = p
        kind, nospace = "", False
        for line in p.stdout:
            nospace = nospace or "No space left" in line
            m = re.match(r"\[download\] Destination:\s+(.+)", line)
            if m:  # captions download first; only the video and audio streams move the bar
                ext = Path(m.group(1).strip()).suffix.lower()
                kind = "audio" if ext in (".m4a", ".mp3", ".opus", ".aac") else "video" if ext in (".mp4", ".webm", ".mkv", ".m4v") else ""
                if kind: upd(j, stage=f"Downloading {kind}", ind=False)
                continue
            if line.startswith("[Merger]"):
                upd(j, stage="Finishing download", detail="merging video and audio", pct=44, ind=True)
                continue
            m = re.match(r"\[download\]\s+(\d+(?:\.\d+)?)%", line)
            if m and kind:
                f, vf = float(m.group(1)) / 100, J["vf"]
                sp = re.search(r"at\s+(\S+)\s+ETA\s+(\S+)", line)
                upd(j, pct=int(44 * (vf * f if kind == "video" else vf + (1 - vf) * f)),
                    detail=f"{f * 100:.0f}%" + (f" \u2022 {sp.group(1)} \u2022 ETA {sp.group(2)}" if sp and "Unknown" not in sp.group(0) else ""))
        rc = p.wait(); procs.pop(j, None); chk(j)
        if nospace: raise RuntimeError("Disk full during download")
        if rc: raise RuntimeError("Download failed (try updating yt-dlp)")
        vids, subs = list(d.glob("*.mp4")), list(d.glob("*.srt"))
        if not vids: raise RuntimeError("No video produced")
        if not subs: raise RuntimeError("Caption download failed")
        v = vids[0]
        upd(j, name=v.stem)
        cues = parse_srt(subs[0])
        chk(j)
        upd(j, status="translating", stage="Loading translation model", pct=44, detail="", ind=True)
        tr, bad = translate([c[1] for c in cues], j, J["src"], J["tgt"])
        srt = d / "tr.srt"
        srt.write_text("\n".join(f"{i}\n{c[0]}\n{t}\n" for i, (c, t) in enumerate(zip(cues, tr), 1)), "utf-8")
        chk(j)
        need = int(v.stat().st_size * 1.02) + DISK_MARGIN  # the merged copy is about as big as the download
        if not make_room(need): raise RuntimeError(f"Not enough disk to merge: need {gb(need)}, free {gb(free_bytes())}")
        upd(j, status="muxing", stage="Merging subtitles", pct=93, detail="", ind=not J["dur"])
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
                    if f >= 0.999: upd(j, stage="Optimizing for streaming", detail="", pct=99, ind=True)  # faststart pass is silent
                    else: upd(j, pct=93 + int(6 * f), detail=f"{int(f * 100)}%")
            rc = p.wait(); procs.pop(j, None)
        chk(j)
        if rc:
            err = log.read_text(errors="ignore")
            raise RuntimeError("Disk full while merging" if "No space left" in err
                               else "Merge failed: " + (err.strip().splitlines() or ["ffmpeg error"])[-1][:120])
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

for fn in (worker, cleaner, speed_loop): threading.Thread(target=fn, daemon=True).start()

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
    try: info = json.loads(subprocess.check_output(cmd, text=True, timeout=90, stderr=subprocess.DEVNULL))
    except Exception: raise HTTPException(400, "Could not read that video (check the URL, or update yt-dlp)")
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
                   pct=0, stage="", ind=False, detail="", msg="", file="", note="", dur=m["dur"], vf=vf, t=time.time())
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
