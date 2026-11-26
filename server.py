import os, re, json, time, shutil, threading, queue, subprocess, uuid
from pathlib import Path
from deep_translator import MyMemoryTranslator
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel

DATA = Path(os.environ.get("DATA_DIR", "data")); DATA.mkdir(exist_ok=True)
KEEP_HOURS = 48

app = FastAPI()
jobs, q, procs, cancels = {}, queue.Queue(), {}, set()

def upd(j, **kw):
    if j in jobs: jobs[j].update(kw)

def chk(j):
    if j in cancels: raise RuntimeError("Stopped by user")

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

def translate(lines, j):
    mm = MyMemoryTranslator(source="tr-TR", target="ar-SA")
    out, total, t0 = [], len(lines), time.time()
    def step(done, note=""):
        if not total: return
        pct = min(99, max(1 if note else 0, done * 100 // total))
        el = time.time() - t0
        eta = int(el * (total - done) / done) if done else 0
        msg = f"{done}/{total} lines" + (f" • ~{eta}s left" if eta > 1 else "")
        if note: msg += f" • {note}"
        upd(j, pct=pct, detail=msg)
    step(0, "starting")
    for l in lines:
        chk(j)
        txt = l.replace("\n", " ").strip()
        try:
            out.append((mm.translate(txt[:490]) or txt) if txt else "")
        except Exception:
            out.append(txt)
        step(len(out), "translating")
        time.sleep(0.35)
    step(len(out))
    return out

def run(j):
    d = DATA / j; d.mkdir(exist_ok=True)
    try:
        chk(j)
        upd(j, status="downloading", pct=0, detail="")
        h = jobs[j].get("height", 1080)
        fmt = f"bv*[vcodec^=avc1][height<={h}]/bv*[height<={h}]+ba[ext=m4a]/b[ext=mp4]"
        cmd = ["yt-dlp", "--no-playlist", "--newline", "--restrict-filenames", "--js-runtimes", "node",
               "-f", fmt, "--merge-output-format", "mp4", "--write-subs", "--write-auto-subs",
               "--sub-langs", "tr,tr-orig", "--convert-subs", "srt",
               "-o", str(d / "%(title).80B.%(ext)s"), "--", jobs[j]["url"]]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        procs[j] = p
        for line in p.stdout:
            m = re.search(r"\[download\]\s+(\d+(?:\.\d+)?)%", line)
            if m: upd(j, pct=int(float(m.group(1))))
        rc = p.wait(); procs.pop(j, None); chk(j)
        if rc: raise RuntimeError("Download failed (try updating yt-dlp)")
        vids, subs = list(d.glob("*.mp4")), sorted(d.glob("*.tr*.srt"))
        if not vids: raise RuntimeError("No video produced")
        if not subs: raise RuntimeError("No Turkish captions on this video")
        v = vids[0]
        sub = next((s for s in subs if s.name.endswith(".tr.srt")), subs[0])
        upd(j, name=v.stem)
        cues = parse_srt(sub)
        chk(j)
        upd(j, status="translating", pct=0, detail="")
        ar = translate([c[1] for c in cues], j)
        srt = d / "ar.srt"
        srt.write_text("\n".join(f"{i}\n{c[0]}\n{t}\n" for i, (c, t) in enumerate(zip(cues, ar), 1)), "utf-8")
        chk(j)
        upd(j, status="muxing", pct=99, detail="merging audio & subtitles")
        out = d / (v.stem + "_AR.mp4")
        subprocess.run(["ffmpeg", "-y", "-i", str(v), "-i", str(srt), "-map", "0:v", "-map", "0:a", "-map", "1",
                        "-c", "copy", "-c:s", "mov_text", "-metadata:s:s:0", "language=ara",
                        "-disposition:s:0", "default", "-movflags", "+faststart", str(out)],
                       check=True, capture_output=True)
        v.unlink()
        upd(j, status="done", pct=100, detail="", file=out.name, note="")
    except Exception as e:
        procs.pop(j, None); shutil.rmtree(d, ignore_errors=True)
        upd(j, status="stopped" if j in cancels else "error", detail="", msg="" if j in cancels else str(e)[:200])
        cancels.discard(j)

def worker():
    while True: run(q.get())

def cleaner():
    while True:
        for d in DATA.iterdir():
            if d.is_dir() and time.time() - d.stat().st_mtime > KEEP_HOURS * 3600:
                shutil.rmtree(d, ignore_errors=True)
        time.sleep(3600)

threading.Thread(target=worker, daemon=True).start()
threading.Thread(target=cleaner, daemon=True).start()

class Req(BaseModel):
    url: str
    height: int = 1080

@app.post("/api/formats")
def formats(r: Req):
    if not re.match(r"https?://", r.url): raise HTTPException(400, "Bad URL")
    cmd = ["yt-dlp", "--no-playlist", "--js-runtimes", "node", "-J", "--", r.url]
    try:
        out = subprocess.check_output(cmd, text=True, timeout=30)
    except Exception as e:
        raise HTTPException(400, f"Could not fetch info: {e}")
    info = json.loads(out)
    hs = sorted({int(f["height"]) for f in info.get("formats", [])
                 if f.get("height") and f.get("vcodec") not in (None, "none") and int(f["height"]) >= 360}, reverse=True)
    dur = int(info.get("duration") or 0)
    h, m, sec = dur // 3600, (dur % 3600) // 60, dur % 60
    dstr = f"{h}:{m:02d}:{sec:02d}" if h else (f"{m}:{sec:02d}" if dur else "")
    return {"title": info.get("title", ""), "duration": dstr, "heights": hs or}

@app.post("/api/jobs")
def add(r: Req):
    if not re.match(r"https?://", r.url): raise HTTPException(400, "Bad URL")
    j = uuid.uuid4().hex[:8]
    jobs[j] = dict(id=j, url=r.url, height=r.height, name="", status="queued", pct=0, detail="", msg="", file="", note="", t=time.time())
    q.put(j)
    return {"id": j}

@app.post("/api/jobs/{j}/stop")
def stop(j: str):
    cancels.add(j)
    p = procs.pop(j, None)
    if p:
        try: p.terminate()
        except Exception: pass
    if j in jobs and jobs[j]["status"] in ("queued", "downloading", "translating", "muxing"):
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

PAGE = """<!doctype html><meta name=viewport content="width=device-width,initial-scale=1">
<meta name=color-scheme content="light dark"><title>Arabic Subs</title>
<style>body{font:16px system-ui;max-width:560px;margin:auto;padding:16px}
input,select,button{font:inherit;padding:12px;width:100%;box-sizing:border-box;margin:4px 0;border-radius:8px;border:1px solid #8886}
button{font-weight:600;cursor:pointer}
.urow{display:flex;gap:6px}.urow input{flex:1;min-width:0}.urow button{width:auto;padding:12px 16px;white-space:nowrap}
.stop{background:#dc2626;color:#fff;border-color:#dc2626}
.j{border:1px solid #8886;border-radius:10px;padding:12px;margin:10px 0;word-break:break-word}
.bar{background:#8883;border-radius:99px;height:14px;overflow:hidden;margin:8px 0}
.fill{background:#2563eb;height:100%;transition:width .4s ease}
.row{display:flex;justify-content:space-between;font-size:14px;opacity:.85}</style>
<h2>Arabic subtitles</h2>
<div class=urow><input id=u type=url placeholder="YouTube URL" oninput=resetMeta()><button id=chkbtn type=button onclick=checkUrl()>Check</button></div>
<div id=meta style="font-size:14px;opacity:.85;margin:4px 2px"></div>
<select id=q disabled><option value="">Resolution (click Check first)</option></select>
<button id=btn type=button onclick=toggle()>Start</button>
<div id=l></div>
<script>
let activeId=null;
const esc=s=>String(s||"").replace(/[&<>]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;"}[c]));
function resetMeta(){
  const q=document.getElementById("q"),m=document.getElementById("meta");
  q.innerHTML='<option value="">Resolution (click Check first)</option>';
  q.disabled=true;
  m.textContent="";
}
async function checkUrl(){
  const u=document.getElementById("u").value.trim();
  const btn=document.getElementById("chkbtn"),m=document.getElementById("meta"),q=document.getElementById("q");
  if(!u)return;
  btn.disabled=true;m.textContent="Checking video...";
  try{
    const r=await fetch("/api/formats",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({url:u})});
    const d=await r.json();
    if(!r.ok)throw new Error(d.detail||"Failed to check URL");
    m.textContent=d.title+(d.duration?` (${d.duration})`:"");
    q.innerHTML=d.heights.map(h=>`<option value="${h}">${h}p</option>`).join("");
    q.disabled=false;
  }catch(e){m.textContent=e.message;}
  btn.disabled=false;
}
async function toggle(){
  const btn=document.getElementById("btn");
  if(activeId){
    await fetch(`/api/jobs/${activeId}/stop`,{method:"POST"});
    activeId=null;btn.textContent="Start";btn.className="";
    return;
  }
  const u=document.getElementById("u").value.trim();
  const h=parseInt(document.getElementById("q").value)||1080;
  if(!u)return;
  const r=await fetch("/api/jobs",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({url:u,height:h})});
  const d=await r.json();
  if(r.ok){activeId=d.id;btn.textContent="Stop";btn.className="stop";poll();}
}
async function poll(){
  const r=await fetch("/api/jobs");
  const list=await r.json();
  const active=list.find(x=>["queued","downloading","translating","muxing"].includes(x.status));
  const btn=document.getElementById("btn");
  if(active){activeId=active.id;btn.textContent="Stop";btn.className="stop";}


else if(activeId){activeId=null;btn.textContent="Start";btn.className="";}
document.getElementById("l").innerHTML=list.map(j=> <div class=j> <div><b>${esc(j.name||j.url)}</b></div> <div class=row><span>${esc(j.status)} ${j.detail?"• "+esc(j.detail):""}</span><span>${j.pct}%</span></div> <div class=bar><div class=fill style="width:${j.pct}%"></div></div> ${j.msg?${esc(j.msg)}:""} ${j.status==="done"?Download MP4:""} </div>).join("");
}
setInterval(poll,1500);
poll();
"""
@app.get("/", response_class=HTMLResponse)
def index():
return PAGE
