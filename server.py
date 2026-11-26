import os, re, json, time, shutil, threading, queue, subprocess, uuid, urllib.request
from pathlib import Path
from deep_translator import GoogleTranslator
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel

DATA = Path(os.environ.get("DATA_DIR", "data")); DATA.mkdir(exist_ok=True)
KEEP_HOURS, SPEED_EVERY, SPEED_BYTES = 48, 600, 2_000_000
# code, display name, ISO-639-2 tag for the subtitle track. Arabic first = default.
TARGETS = [("ar", "Arabic", "ara"), ("en", "English", "eng"), ("tr", "Turkish", "tur"), ("es", "Spanish", "spa"),
           ("fr", "French", "fra"), ("de", "German", "deu"), ("ur", "Urdu", "urd"), ("fa", "Persian", "fas")]
ACTIVE = ("queued", "downloading", "translating", "muxing")

app = FastAPI()
jobs, meta, q, procs, cancels = {}, {}, queue.Queue(), {}, set()
speed = {"bps": 0}

def upd(j, **kw):
    if j in jobs: jobs[j].update(kw)

def chk(j):
    if j in cancels: raise RuntimeError("Stopped by user")

def busy(): return any(x["status"] in ACTIVE for x in jobs.values())

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

def gcode(code):
    base = {"zh-Hans": "zh-CN", "zh-Hant": "zh-TW"}.get(code, code.split("-")[0])
    return base if base in GoogleTranslator().get_supported_languages(as_dict=True).values() else "auto"

def translate(lines, j, src, tgt):
    g = GoogleTranslator(source=gcode(src), target=tgt)
    out, bad, streak, total, t0 = [], 0, 0, len(lines), time.time()
    for l in lines:
        chk(j)
        txt = l.replace("\n", " ").strip()
        res = None
        for k in range(4):  # retry with backoff: 1s, 3s, 9s
            try: res = g.translate(txt[:4900]) if txt else ""; break
            except Exception: time.sleep(3 ** k)
        if res is None: bad += 1; streak += 1
        else: streak = 0
        if streak >= 8: raise RuntimeError("Google Translate is blocking requests - try again later")
        out.append(res or txt)
        done = len(out)
        eta = int((time.time() - t0) * (total - done) / done)
        upd(j, pct=min(99, done * 100 // total), detail=f"{done}/{total} lines" + (f" • ~{eta // 60}m left" if eta > 60 else ""))
        time.sleep(0.15)
    return out, bad

def run(j):
    d = DATA / j; d.mkdir(exist_ok=True)
    try:
        chk(j)
        J = jobs[j]
        upd(j, status="downloading", pct=0, detail="")
        fmt = f"bv*[vcodec^=avc1][height<={J['height']}]+ba[ext=m4a]/b[ext=mp4][height<={J['height']}]"
        cmd = ["yt-dlp", "--no-playlist", "--newline", "--restrict-filenames", "--js-runtimes", "node",
               "-f", fmt, "--merge-output-format", "mp4", "--write-subs", "--sub-langs", J["src"],
               "--convert-subs", "srt", "-o", str(d / "%(title).80B.%(ext)s"), "--", J["url"]]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        procs[j] = p
        for line in p.stdout:
            m = re.search(r"\[download\]\s+(\d+(?:\.\d+)?)%", line)
            if m: upd(j, pct=int(float(m.group(1))))
        rc = p.wait(); procs.pop(j, None); chk(j)
        if rc: raise RuntimeError("Download failed (try updating yt-dlp)")
        vids, subs = list(d.glob("*.mp4")), list(d.glob("*.srt"))
        if not vids: raise RuntimeError("No video produced")
        if not subs: raise RuntimeError("Caption download failed")
        v = vids[0]
        upd(j, name=v.stem)
        cues = parse_srt(subs[0])
        chk(j)
        upd(j, status="translating", pct=0, detail="")
        tr, bad = translate([c[1] for c in cues], j, J["src"], J["tgt"])
        srt = d / "tr.srt"
        srt.write_text("\n".join(f"{i}\n{c[0]}\n{t}\n" for i, (c, t) in enumerate(zip(cues, tr), 1)), "utf-8")
        chk(j)
        upd(j, status="muxing", pct=99, detail="merging audio & subtitles")
        out = d / f"{v.stem}_{J['tgt'].upper()}.mp4"
        iso = next(t[2] for t in TARGETS if t[0] == J["tgt"])
        subprocess.run(["ffmpeg", "-y", "-i", str(v), "-i", str(srt), "-map", "0:v", "-map", "0:a", "-map", "1",
                        "-c", "copy", "-c:s", "mov_text", "-metadata:s:s:0", f"language={iso}",
                        "-disposition:s:0", "default", "-movflags", "+faststart", str(out)],
                       check=True, capture_output=True)
        v.unlink()
        upd(j, status="done", pct=100, detail="", file=out.name, note=f"({bad} lines left untranslated)" if bad else "")
    except Exception as e:
        procs.pop(j, None); shutil.rmtree(d, ignore_errors=True)
        upd(j, status="stopped" if j in cancels else "error", detail="", msg="" if j in cancels else str(e)[:200])
        cancels.discard(j)
    finally:
        meta.pop(jobs[j].get("mid"), None)  # free the video info as soon as the job ends

def worker():
    while True: run(q.get())

def cleaner():
    while True:
        for d in DATA.iterdir():
            if d.is_dir() and time.time() - d.stat().st_mtime > KEEP_HOURS * 3600:
                shutil.rmtree(d, ignore_errors=True)
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
    h, m, s = dur // 3600, (dur % 3600) // 60, dur % 60
    dstr = f"{h}:{m:02d}:{s:02d}" if h else (f"{m}:{s:02d}" if dur else "")
    # manual captions only (auto-generated are intentionally excluded)
    subs = [{"code": c, "name": (v[0].get("name") if v else None) or c}
            for c, v in (info.get("subtitles") or {}).items() if c != "live_chat"]
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
    meta[mid] = {"url": r.url, "subs": [x["code"] for x in subs], "heights": [x["h"] for x in heights], "t": time.time()}
    while len(meta) > 3: meta.pop(min(meta, key=lambda k: meta[k]["t"]))  # never hoard abandoned lookups
    return {"mid": mid, "title": info.get("title", ""), "duration": dstr, "thumb": info.get("thumbnail", ""),
            "subs": subs, "heights": heights, "speed": speed["bps"],
            "targets": [{"code": t[0], "name": t[1]} for t in TARGETS]}

@app.post("/api/jobs")
def add(r: Start):
    m = meta.get(r.mid)
    if not m: raise HTTPException(400, "Video info expired - press Submit again")
    if r.src not in m["subs"] or r.height not in m["heights"] or r.tgt not in [t[0] for t in TARGETS]:
        raise HTTPException(400, "Invalid selection")
    if busy(): raise HTTPException(409, "A job is already running")
    j = uuid.uuid4().hex[:8]
    jobs[j] = dict(id=j, mid=r.mid, url=m["url"], src=r.src, tgt=r.tgt, height=r.height, name="", status="queued",
                   pct=0, detail="", msg="", file="", note="", t=time.time())
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
        upd(j, status="stopped", detail="", msg="")
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
.bar{background:#8883;border-radius:99px;height:14px;overflow:hidden;margin:8px 0}
.fill{background:#2563eb;height:100%;transition:width .4s ease}
.row{display:flex;justify-content:space-between;font-size:14px;opacity:.85}
</style>
<h2>Video subtitles</h2>
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
let M=null,busy=false,tt;
function toast(t){const e=$("toast");e.textContent=t;e.classList.add("on");clearTimeout(tt);tt=setTimeout(()=>e.classList.remove("on"),3200)}
function reset(){M=null;$("info").classList.remove("on");$("cfg").classList.remove("on");$("sum").textContent=""}
const gb=b=>!b?"size n/a":b>=1e9?(b/1e9).toFixed(1)+" GB":Math.round(b/1e6)+" MB";
const dur=s=>s<60?"<1 min":s<3600?Math.round(s/60)+" min":Math.floor(s/3600)+" h "+Math.round(s%3600/60)+" min";
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
async function poll(){
  try{
    const list=await(await fetch("/api/jobs")).json();
    busy=list.some(x=>ACT.includes(x.status));
    $("l").innerHTML=list.map(j=>`<div class=j><div><b>${esc(j.name||j.url)}</b></div><div class=row><span>${esc(j.status)}${j.detail?" \u2022 "+esc(j.detail):""}</span><span>${j.pct}%</span></div><div class=bar><div class=fill style="width:${j.pct}%"></div></div>${j.msg?`<div>${esc(j.msg)}</div>`:""}${j.status==="done"?`<a href="/files/${j.id}">Download MP4</a> ${esc(j.note)}`:""}${ACT.includes(j.status)?`<button class=stop onclick="stopJob('${j.id}')">Stop</button>`:""}</div>`).join("");
    update();
  }catch(e){}
}
$("u").addEventListener("keydown",e=>{if(e.key==="Enter")submit()});
setInterval(poll,2000);poll();
</script>"""

@app.get("/", response_class=HTMLResponse)
def index():
    return PAGE
