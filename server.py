import os, re, json, time, shutil, threading, queue, subprocess, uuid
from pathlib import Path
import requests
from deep_translator import GoogleTranslator
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel

DATA = Path(os.environ.get("DATA_DIR", "data")); DATA.mkdir(exist_ok=True)
KEY = os.environ.get("GEMINI_API_KEY", "")
MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
KEEP_HOURS, CHUNK = 48, 60

app = FastAPI()
jobs, q, procs, cancels = {}, queue.Queue(), {}, set()

def upd(j, **kw):
    if j in jobs: jobs[j].update(kw)
def chk(j):
    if j in cancels: raise RuntimeError("Stopped by user")

def parse_srt(path):
    """Return [(timing, text)], collapsing YouTube's rolling duplicate lines."""
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

def gemini(lines):
    p = ("Translate these Turkish TV-series subtitle lines into natural Modern Standard Arabic. "
         "Keep names, tone and order; keep each line concise. "
         f"Return ONLY a JSON array of exactly {len(lines)} strings.\n" + json.dumps(lines, ensure_ascii=False))
    r = requests.post(
        f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL}:generateContent",
        headers={"x-goog-api-key": KEY},
        json={"contents": [{"parts": [{"text": p}]}],
              "generationConfig": {"responseMimeType": "application/json", "temperature": 0.2}},
        timeout=180)
    r.raise_for_status()
    out = json.loads(r.json()["candidates"][0]["content"]["parts"][0]["text"])
    if not isinstance(out, list) or len(out) != len(lines): raise ValueError("bad count")
    return [str(x) for x in out]

def google(lines, on_step=None, done0=0, total=0):
    from deep_translator import MyMemoryTranslator
    mm = MyMemoryTranslator(source="tr-TR", target="ar-SA")
    out = []
    for l in lines:
        txt = l.replace("\n", " ").strip()
        try:
            out.append((mm.translate(txt[:490]) or txt) if txt else "")
        except Exception:
            out.append(txt)
        if on_step: on_step(done0 + len(out), total, "translating")
        if callable(getattr(on_step, "chk", None)): on_step.chk()
        time.sleep(0.35)
    return out

def translate(lines, j):
    out, total, t0 = [], len(lines), time.time()
    def step(done, tot, note=""):
        if not tot: return
        pct = min(99, max(1 if note else 0, done * 100 // tot))
        el = time.time() - t0
        eta = int(el * (tot - done) / done) if done else 0
        msg = f"{done}/{tot} lines" + (f" • ~{eta}s left" if eta > 1 else "")
        if note: msg += f" • {note}"
        upd(j, pct=pct, detail=msg)
    step.chk = lambda: chk(j)
    step(0, total, "starting")
    res = google(lines, step, 0, total)
    step(len(res), total)
    return res, 0

def run(j):
    d = DATA / j; d.mkdir(exist_ok=True)
    try:
        upd(j, status="downloading", pct=0)
        cmd = ["yt-dlp", "--no-playlist", "--newline", "--restrict-filenames", "--js-runtimes", "node",
               "-f", f"bv*[vcodec^=avc1][height<={jobs[j].get('height', 1080)}]/bv*[height<={jobs[j].get('height', 1080)}]+ba[ext=m4a]/b[ext=mp4]",
               "--merge-output-format", "mp4", "--write-subs", "--write-auto-subs",
               "--sub-langs", "tr,tr-orig", "--convert-subs", "srt",
               "-o", str(d / "%(title).80B.%(ext)s"), "--", jobs[j]["url"]]
        chk(j); p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True); procs[j] = p
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
        chk(j); upd(j, status="translating", pct=0, detail="")
        ar, fb = translate([c[1] for c in cues], j)
        srt = d / "ar.srt"
        srt.write_text("\n".join(f"{i}\n{c[0]}\n{t}\n" for i, (c, t) in enumerate(zip(cues, ar), 1)), "utf-8")
        chk(j); upd(j, status="muxing", pct=99, detail="merging audio & subtitles")
        out = d / (v.stem + "_AR.mp4")
        subprocess.run(["ffmpeg", "-y", "-i", str(v), "-i", str(srt), "-map", "0:v", "-map", "0:a", "-map", "1",
                        "-c", "copy", "-c:s", "mov_text", "-metadata:s:s:0", "language=ara",
                        "-disposition:s:0", "default", "-movflags", "+faststart", str(out)],
                       check=True, capture_output=True)
        v.unlink()
        upd(j, status="done", pct=100, file=out.name,
            note=f"({fb} chunk(s) used Google fallback)" if fb else "")
    except Exception as e:
        procs.pop(j, None); shutil.rmtree(d, ignore_errors=True)
        upd(j, status="stopped" if j in cancels else "error", msg="" if j in cancels else str(e)[:200])
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
    out = subprocess.check_output(cmd, text=True, timeout=30)
    info = json.loads(out)
    hs = sorted({int(f["height"]) for f in info.get("formats", []) if f.get("height") and f.get("vcodec") not in (None, "none") and int(f["height"]) >= 360}, reverse=True)
    dur = int(info.get("duration") or 0); h, m, sec = dur // 3600, (dur % 3600) // 60, dur % 60; dstr = f"{h}:{m:02d}:{sec:02d}" if h else (f"{m}:{sec:02d}" if dur else ""); return {"title": info.get("title", ""), "duration": dstr, "heights": hs or [1080, 720, 480]}

@app.post("/api/jobs")
def add(r: Req):
    if not re.match(r"https?://", r.url): raise HTTPException(400, "Bad URL")
    j = uuid.uuid4().hex[:8]
    jobs[j] = dict(id=j, url=r.url, height=r.height, name="", status="queued", pct=0, detail="", msg="", file="", note="", t=time.time())
    q.put(j)
    return {"id": j}

@app.get("/api/jobs")
def lst(): return sorted(jobs.values(), key=lambda x: -x["t"])

@app.get("/files/{j}")
def get(j: str):
    f = jobs.get(j, {}).get("file")
    p = DATA / j / f if f else None
    if not p or not p.exists(): raise HTTPException(404, "File gone")
    return FileResponse(p, filename=f, media_type="video/mp4")

@app.post("/api/jobs/{j}/stop")
def stop(j: str):
    cancels.add(j)
    p = procs.pop(j, None)
    if p:
        try: p.terminate()
        except Exception: pass
    if j in jobs and jobs[j]["status"] in ("queued", "downloading", "translating", "muxing"):
        upd(j, status="stopped", msg="")
    return {"ok": True}

PAGE = '''<!doctype html><meta name=viewport content="width=device-width,initial-scale=1">
<meta name=color-scheme content="light dark"><title>Arabic Subs</title>
<style>body{font:16px system-ui;max-width:560px;margin:auto;padding:16px}
input,select,button{font:inherit;padding:12px;width:100%;box-sizing:border-box;margin:4px 0;border-radius:8px;border:1px solid #8886}.urow{display:flex;gap:6px}.urow input{flex:1;min-width:0}.urow button{width:auto;padding:12px 16px;white-space:nowrap}
button{font-weight:600;cursor:pointer}
.stop{background:#dc2626;color:#fff;border-color:#dc2626}
.j{border:1px solid #8886;border-radius:10px;padding:12px;margin:10px 0;word-break:break-word}
.bar{background:#8883;border-radius:99px;height:14px;overflow:hidden;margin:8px 0}
.fill{background:#2563eb;height:100%;transition:width .4s ease}
.row{display:flex;justify-content:space-between;font-size:14px;opacity:.85}</style>
<h2>Arabic subtitles</h2>
<div class=urow><input id=u type=url placeholder="YouTube URL" oninput=resetMeta()><button id=chk type=button onclick=checkUrl()>Check</button></div><div id=meta style="font-size:14px;opacity:.85;margin:4px 2px"></div><select id=q disabled><option value="">Resolution (click Check first)</option></select><button id=btn onclick=toggle()>Start</button><div id=l></div>
<script>
let activeId=null;
function resetMeta(){
  const q=document.getElementById("q"),m=document.getElementById("meta");
  q.innerHTML="<option value=\"\">Resolution (click Check first)</option>";
  q.disabled=true;
  m.textContent="";
}
async function checkUrl(){
  const u=document.getElementById("u").value.trim(),q=document.getElementById("q"),m=document.getElementById("meta"),b=document.getElementById("chk");
  if(!/^https?:\/\//.test(u)){m.textContent="Enter a valid YouTube URL first.";return}
  b.disabled=true;b.textContent="Checking...";q.disabled=true;q.innerHTML="<option>Checking...</option>";m.textContent="";
  try{
    const res=await fetch("/api/formats",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({url:u})});
    if(!res.ok)throw new Error("Could not fetch video info");
    const r=await res.json();
    m.textContent=(r.title?r.title:"")+(r.duration?` • Length: ${r.duration}`:"");
    const hs=(r.heights&&r.heights.length)?r.heights:[1080,720,480];
    q.innerHTML=hs.map(h=>`<option value="${h}">${h}p${h===720?" (Faster transfer)":""}</option>`).join("");
    q.disabled=false;
  }catch(e){
    m.textContent="Failed to check video details.";
    q.innerHTML="<option value=\"1080\">Default (1080p)</option>";
    q.disabled=false;
  }finally{
    b.disabled=false;b.textContent="Check";
  }
}
const esc=s=>String(s||"").replace(/[&<>]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;"}[c]));
function syncBtn(r){
  const act=r.find(j=>["queued","downloading","translating","muxing"].includes(j.status));
  activeId=act?act.id:null;
  const b=document.getElementById("btn");
  b.textContent=activeId?"Stop":"Start";
  b.className=activeId?"stop":"";
}
async function toggle(){
  if(activeId){await fetch(`/api/jobs/${activeId}/stop`,{method:"POST"});load();return}
  const u=document.getElementById("u"),q=document.getElementById("q");if(!u.value)return;
  await fetch("/api/jobs",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({url:u.value,height:parseInt(q.value)||1080})});
  u.value="";resetMeta();load();
}
async function load(){try{const r=await(await fetch("/api/jobs")).json();syncBtn(r);
document.getElementById("l").innerHTML=r.map(j=>`<div class=j><b>${esc(j.name||j.url)}</b><br>`+
(j.status=="done"?`<a href="/files/${j.id}">Download MP4</a> ${esc(j.note)}`:j.status=="stopped"?`<span style="opacity:.7">Stopped</span>`:j.status=="error"?`<span style="color:#ef4444">${esc(j.msg)}</span>`:`<div class=row><span>${esc(j.status)}${j.detail?` (${esc(j.detail)})`:`...`}</span><span>${j.pct}%</span></div><div class=bar><div class=fill style="width:${j.pct}%"></div></div>`)+`</div>`).join("")}catch(e){}}
load();setInterval(load,1500)
</script>'''

@app.get("/", response_class=HTMLResponse)
def index(): return HTMLResponse(PAGE, headers={"Cache-Control": "no-store"})
