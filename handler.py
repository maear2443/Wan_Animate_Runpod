import runpod
from runpod.serverless.utils import rp_upload

import os, subprocess, time, json, uuid, base64, binascii, logging, re
import urllib.request, urllib.parse
try:
    import websocket  # pip: websocket-client
except Exception as e:
    raise RuntimeError(f"'websocket-client' 패키지가 필요합니다: {e}")

# -----------------------------
# 로깅
# -----------------------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("handler")

# -----------------------------
# ENV / 상수
# -----------------------------
SERVER_ADDRESS = os.getenv("SERVER_ADDRESS", "127.0.0.1")
COMFY_PORT = int(os.getenv("COMFY_PORT", "8188"))
COMFY_HTTP = f"http://{SERVER_ADDRESS}:{COMFY_PORT}"
COMFY_WS   = f"ws://{SERVER_ADDRESS}:{COMFY_PORT}/ws"
COMFY_CMD  = os.getenv(
    "COMFY_CMD",
    "python -u /workspace/ComfyUI/main.py --listen 0.0.0.0 --port 8188 --disable-auto-launch"
)

CLIENT_ID = str(uuid.uuid4())
OUT_DIR = "/out"
os.makedirs(OUT_DIR, exist_ok=True)

COMFY_PROC = None  # 컨테이너 생존 중 재사용

# -----------------------------
# ComfyUI 부팅 보장
# -----------------------------
def comfy_is_ready(timeout=3):
    try:
        urllib.request.urlopen(COMFY_HTTP, timeout=timeout)
        return True
    except Exception:
        return False

def ensure_comfy_started(start_timeout=180):
    """
    1) 이미 떠 있으면 통과
    2) 안 떠 있으면 서브프로세스로 부팅
    3) HTTP 200 떠올 때까지 대기
    """
    global COMFY_PROC
    if comfy_is_ready():
        return

    log.info("[Comfy] not running → launching...")
    COMFY_PROC = subprocess.Popen(
        ["bash", "-lc", COMFY_CMD],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
    )

    start = time.time()
    while time.time() - start < start_timeout:
        if comfy_is_ready():
            log.info("[Comfy] ready.")
            return
        if COMFY_PROC and COMFY_PROC.poll() is not None:
            raise RuntimeError("ComfyUI가 즉시 종료됨. main.py 경로/모델 경로를 확인하세요.")
        # 로그 드레인(버퍼 방지)
        time.sleep(1)

    raise TimeoutError("ComfyUI 부팅 시간 초과")

# -----------------------------
# ComfyUI API 유틸
# -----------------------------
def queue_prompt(prompt_dict, client_id):
    url = f"{COMFY_HTTP}/prompt"
    data = json.dumps({"prompt": prompt_dict, "client_id": client_id}).encode("utf-8")
    req = urllib.request.Request(url, data=data)
    return json.loads(urllib.request.urlopen(req).read())  # {"prompt_id": ...}

def get_history(prompt_id):
    url = f"{COMFY_HTTP}/history/{prompt_id}"
    with urllib.request.urlopen(url) as r:
        return json.loads(r.read())

def wait_with_websocket(prompt_id, client_id, ws_timeout=900):
    ws = websocket.WebSocket()
    ws.connect(f"{COMFY_WS}?clientId={client_id}", timeout=10)
    start = time.time()
    while True:
        if time.time() - start > ws_timeout:
            ws.close()
            raise TimeoutError("웹소켓 대기 시간 초과")
        msg = ws.recv()
        if isinstance(msg, str):
            m = json.loads(msg)
            if m.get("type") == "executing":
                data = m.get("data", {})
                if data.get("node") is None and data.get("prompt_id") == prompt_id:
                    ws.close()
                    return
        time.sleep(0.01)

def wait_with_polling(prompt_id, timeout=1200, interval=2):
    start = time.time()
    while time.time() - start < timeout:
        hist = get_history(prompt_id).get(prompt_id, {})
        if hist.get("outputs"):
            return
        time.sleep(interval)
    raise TimeoutError("HTTP 폴링 대기 시간 초과")

def fetch_output_bytes(item):
    """
    history.outputs[*]의 각 항목에서 산출물 바이트를 얻는다.
    - dict(fullpath)
    - dict(filename/subfolder/type) → /view
    - str(base64)
    """
    # 1) 파일 경로가 바로 있을 때
    if isinstance(item, dict) and "fullpath" in item:
        with open(item["fullpath"], "rb") as f:
            return f.read()

    # 2) /view로 접근 가능한 메타일 때
    if isinstance(item, dict) and all(k in item for k in ("filename", "subfolder", "type")):
        q = urllib.parse.urlencode({
            "filename": item["filename"],
            "subfolder": item["subfolder"],
            "type": item["type"]
        })
        with urllib.request.urlopen(f"{COMFY_HTTP}/view?{q}") as resp:
            return resp.read()

    # 3) base64 문자열일 때
    if isinstance(item, str) and len(item) > 200:
        b64 = item
        if b64.startswith("data:"):
            b64 = re.sub(r"^data:[^;]+;base64,", "", b64)
        return base64.b64decode(b64, validate=False)

    return None

# -----------------------------
# 입력 처리
# -----------------------------
def process_input(input_data, temp_dir, output_filename, input_type):
    os.makedirs(temp_dir, exist_ok=True)
    path = os.path.join(temp_dir, output_filename)
    if input_type == "path":
        return input_data
    if input_type == "url":
        r = subprocess.run(
            ["wget", "-O", path, "--no-verbose", "--timeout=30", input_data],
            capture_output=True, text=True, timeout=60
        )
        if r.returncode != 0:
            raise RuntimeError(f"URL 다운로드 실패: {r.stderr}")
        return path
    if input_type == "base64":
        try:
            if isinstance(input_data, str) and input_data.startswith("data:"):
                input_data = re.sub(r"^data:[^;]+;base64,", "", input_data)
            data = base64.b64decode(input_data)
            with open(path, "wb") as f:
                f.write(data)
            return path
        except (binascii.Error, ValueError) as e:
            raise RuntimeError(f"Base64 디코딩 실패: {e}")
    raise RuntimeError(f"지원하지 않는 입력 타입: {input_type}")

# -----------------------------
# 핸들러
# -----------------------------
def handler(job):
    job_input = job.get("input", {}) or {}
    log.info(f"job keys: {list(job_input.keys())}")
    task_id = f"task_{uuid.uuid4()}"
    tmp_dir = f"/workspace/{task_id}"
    client_id = str(uuid.uuid4())

    # 0) ComfyUI 부팅 보장
    ensure_comfy_started()

    # 1) 파라미터 기본값
    fps    = int(job_input.get("fps", 12))
    seed   = int(job_input.get("seed", 42))
    cfg    = float(job_input.get("cfg", 3.5))
    steps  = int(job_input.get("steps", 6))
    width  = int(job_input.get("width", 768))
    height = int(job_input.get("height", 768))
    prompt_txt = job_input.get("prompt", "gentle cinematic motion")

    # 2) 입력 소스
    if "image_path" in job_input:
        image_path = process_input(job_input["image_path"], tmp_dir, "in.jpg", "path")
    elif "image_url" in job_input:
        image_path = process_input(job_input["image_url"], tmp_dir, "in.jpg", "url")
    elif "image_base64" in job_input:
        image_path = process_input(job_input["image_base64"], tmp_dir, "in.jpg", "base64")
    else:
        image_path = "/examples/image.jpg"
        log.info("기본 이미지 사용: /examples/image.jpg")

    if "video_path" in job_input:
        video_in = process_input(job_input["video_path"], tmp_dir, "in.mp4", "path")
    elif "video_url" in job_input:
        video_in = process_input(job_input["video_url"], tmp_dir, "in.mp4", "url")
    elif "video_base64" in job_input:
        video_in = process_input(job_input["video_base64"], tmp_dir, "in.mp4", "base64")
    else:
        video_in = "/examples/image.jpg"  # 워크플로 요구 시 대체값
        log.info("기본 비디오/이미지 사용: /examples/image.jpg")

    # 3) 연결 테스트용 더미
    if job_input.get("image_path") == "/example_image.png":
        dummy_path = os.path.join(OUT_DIR, f"{task_id}.mp4")
        subprocess.run(["bash","-lc", f"ffmpeg -f lavfi -i color=black:s=512x512:d=1 -y {dummy_path}"],
                       check=False)
        url = rp_upload.upload_file(dummy_path)
        return {"video_url": url}

    # 4) 워크플로 로드 & 파라미터 주입
    with open("/newWanAnimate_api.json", "r", encoding="utf-8") as f:
        prompt = json.load(f)

    def set_in(node_id, key, val):
        if node_id in prompt and "inputs" in prompt[node_id]:
            prompt[node_id]["inputs"][key] = val

    # 필수 주입
    set_in("57",  "image",           image_path)
    set_in("63",  "video",           video_in)
    set_in("63",  "force_rate",      fps)
    set_in("30",  "frame_rate",      fps)
    set_in("65",  "positive_prompt", prompt_txt)
    set_in("27",  "seed",            seed)
    set_in("27",  "cfg",             cfg)
    set_in("27",  "steps",           steps)
    set_in("150", "value",           width)
    set_in("151", "value",           height)

    # 선택 파라미터
    for k in ("points_store", "coordinates", "neg_coordinates"):
        if k in job_input:
            set_in("107", k, job_input[k])

    # 5) 큐잉 및 대기
    prompt_id = queue_prompt(prompt, client_id)["prompt_id"]
    try:
        wait_with_websocket(prompt_id, client_id, ws_timeout=900)
    except Exception as e:
        log.warning(f"WS 대기 실패 → 폴링 대체: {e}")
        wait_with_polling(prompt_id, timeout=1200, interval=2)

    # 6) 결과 추출(robust)
    hist = get_history(prompt_id)[prompt_id]
    outputs = hist.get("outputs", {})
    got_any = False

    # Comfy 구현마다 키가 다를 수 있어 후보를 넓게 커버
    candidate_keys = ("videos", "gifs", "mp4", "files", "images")

    for node_id, node_out in outputs.items():
        for key in candidate_keys:
            items = node_out.get(key)
            if not items:
                continue
            for it in items:
                data = fetch_output_bytes(it)
                if not data:
                    continue
                out_path = os.path.join(OUT_DIR, f"{task_id}.mp4")
                with open(out_path, "wb") as f:
                    f.write(data)
                url = rp_upload.upload_file(out_path)
                return {"video_url": url}

    # 여기까지 못 찾으면 실패 응답
    return {"status": "FAILED", "error": "비디오 산출물을 찾지 못했습니다. VideoCombine의 save_output 설정 및 출력 키를 확인하세요."}

runpod.serverless.start({"handler": handler})
