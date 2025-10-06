import runpod
from runpod.serverless.utils import rp_upload
import os
import websocket
import base64
import json
import uuid
import logging
import urllib.request
import urllib.parse
import binascii
import subprocess
import time
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SERVER_ADDRESS = os.getenv('SERVER_ADDRESS', '127.0.0.1')
CLIENT_ID = str(uuid.uuid4())
OUT_DIR = "/out"
os.makedirs(OUT_DIR, exist_ok=True)

def queue_prompt(prompt):
    url = f"http://{SERVER_ADDRESS}:8188/prompt"
    p = {"prompt": prompt, "client_id": CLIENT_ID}
    data = json.dumps(p).encode('utf-8')
    req = urllib.request.Request(url, data=data)
    return json.loads(urllib.request.urlopen(req).read())

def get_history(prompt_id):
    url = f"http://{SERVER_ADDRESS}:8188/history/{prompt_id}"
    with urllib.request.urlopen(url) as response:
        return json.loads(response.read())

def get_videos(ws, prompt):
    prompt_id = queue_prompt(prompt)['prompt_id']
    while True:
        out = ws.recv()
        if isinstance(out, str):
            message = json.loads(out)
            if message.get('type') == 'executing':
                data = message.get('data', {})
                if data.get('node') is None and data.get('prompt_id') == prompt_id:
                    break
        else:
            continue

    history = get_history(prompt_id)[prompt_id]
    output_videos = {}
    for node_id, node_output in history.get('outputs', {}).items():
        videos_output = []
        # 허브 구현에 따라 'videos' / 'gifs' / 'mp4' 등 키가 다를 수 있음
        for candidate in ('gifs', 'videos', 'mp4'):
            if candidate in node_output:
                for item in node_output[candidate]:
                    # fullpath가 있으면 파일 읽어서 base64로
                    if isinstance(item, dict) and 'fullpath' in item:
                        with open(item['fullpath'], 'rb') as f:
                            video_data = base64.b64encode(f.read()).decode('utf-8')
                        videos_output.append(video_data)
                    elif isinstance(item, str) and len(item) > 200:
                        # 이미 base64일 수 있음
                        videos_output.append(item)
        output_videos[node_id] = videos_output
    return output_videos

def load_workflow(workflow_path):
    with open(workflow_path, 'r', encoding='utf-8') as file:
        return json.load(file)

def process_input(input_data, temp_dir, output_filename, input_type):
    os.makedirs(temp_dir, exist_ok=True)
    file_path = os.path.abspath(os.path.join(temp_dir, output_filename))
    if input_type == "path":
        logger.info(f"📁 경로 입력 처리: {input_data}")
        return input_data
    elif input_type == "url":
        logger.info(f"🌐 URL 입력 처리: {input_data}")
        # wget 사용
        result = subprocess.run(['wget', '-O', file_path, '--no-verbose', '--timeout=30', input_data],
                                capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            raise Exception(f"URL 다운로드 실패: {result.stderr}")
        return file_path
    elif input_type == "base64":
        logger.info(f"🔢 Base64 입력 처리")
        try:
            if isinstance(input_data, str) and input_data.startswith("data:"):
                input_data = re.sub(r"^data:[^;]+;base64,", "", input_data)
            decoded = base64.b64decode(input_data)
            with open(file_path, 'wb') as f:
                f.write(decoded)
            return file_path
        except (binascii.Error, ValueError) as e:
            raise Exception(f"Base64 디코딩 실패: {e}")
    else:
        raise Exception(f"지원하지 않는 입력 타입: {input_type}")

def handler(job):
    job_input = job.get("input", {}) or {}
    logger.info(f"Received job input keys: {list(job_input.keys())}")
    task_id = f"task_{uuid.uuid4()}"
    tmp_dir = os.path.join("/workspace", task_id)

    # ---- 기본값(없을 때 KeyError 방지) ----
    fps   = int(job_input.get("fps", 12))
    seed  = int(job_input.get("seed", 42))
    cfg   = float(job_input.get("cfg", 3.5))
    steps = int(job_input.get("steps", 6))
    width = int(job_input.get("width", 768))
    height= int(job_input.get("height", 768))
    prompt_txt = job_input.get("prompt", "gentle cinematic motion")

    # ---- 이미지/비디오 입력(셋 중 하나만 필요) ----
    image_path = None
    if "image_path" in job_input:
        image_path = process_input(job_input["image_path"], tmp_dir, "input_image.jpg", "path")
    elif "image_url" in job_input:
        image_path = process_input(job_input["image_url"], tmp_dir, "input_image.jpg", "url")
    elif "image_base64" in job_input:
        image_path = process_input(job_input["image_base64"], tmp_dir, "input_image.jpg", "base64")
    else:
        image_path = "/examples/image.jpg"
        logger.info("기본 이미지 사용: /examples/image.jpg")

    video_path_in = None
    if "video_path" in job_input:
        video_path_in = process_input(job_input["video_path"], tmp_dir, "input_video.mp4", "path")
    elif "video_url" in job_input:
        video_path_in = process_input(job_input["video_url"], tmp_dir, "input_video.mp4", "url")
    elif "video_base64" in job_input:
        video_path_in = process_input(job_input["video_base64"], tmp_dir, "input_video.mp4", "base64")
    else:
        video_path_in = "/examples/image.jpg"  # 워크플로 요구 시 대체값
        logger.info("기본 비디오/이미지 사용: /examples/image.jpg")

    # ---- 특수: 예제 경로로 들어오면 샘플 mp4 만들어 업로드(연결 확인용) ----
    if job_input.get("image_path") == "/example_image.png":
        dummy_path = os.path.join(OUT_DIR, f"{task_id}.mp4")
        subprocess.run(["bash","-lc", f"ffmpeg -f lavfi -i color=black:s=512x512:d=1 -y {dummy_path}"],
                       check=False)
        url = rp_upload.upload_file(dummy_path)
        return {"video_url": url}

    # ---- ComfyUI 서버 연결 확인 ----
    http_url = f"http://{SERVER_ADDRESS}:8188/"
    for i in range(60):
        try:
            urllib.request.urlopen(http_url, timeout=3)
            break
        except Exception:
            time.sleep(1)
    else:
        return {"status":"FAILED","error":"ComfyUI 서버에 연결할 수 없습니다. SERVER_ADDRESS 및 포트를 확인하세요."}

    ws_url = f"ws://{SERVER_ADDRESS}:8188/ws?clientId={CLIENT_ID}"
    ws = websocket.WebSocket()
    for attempt in range(36):  # 최대 3분
        try:
            ws.connect(ws_url)
            break
        except Exception as e:
            if attempt == 35:
                return {"status":"FAILED","error":"웹소켓 연결 시간 초과"}
            time.sleep(5)

    # ---- 워크플로 로드 & 파라미터 주입 ----
    prompt = load_workflow('/newWanAnimate_api.json')

    # 노드 ID는 워크플로마다 다릅니다. 기존 값 유지하고 파라미터만 안전하게 주입.
    def set_in(node_id, key, value):
        if node_id in prompt and "inputs" in prompt[node_id]:
            prompt[node_id]["inputs"][key] = value

    set_in("57", "image", image_path)
    set_in("63", "video", video_path_in)
    set_in("63", "force_rate", fps)
    set_in("30", "frame_rate", fps)
    set_in("65", "positive_prompt", prompt_txt)
    set_in("27", "seed", seed)
    set_in("27", "cfg", cfg)
    set_in("27", "steps", steps)
    set_in("150", "value", width)
    set_in("151", "value", height)

    # 선택 항목(없으면 건너뜀)
    for k in ("points_store","coordinates","neg_coordinates"):
        if k in job_input:
            set_in("107", k, job_input[k])

    # ---- Inference ----
    videos = get_videos(ws, prompt)
    ws.close()

    # ---- 산출: base64 → 파일 → 업로드(URL 반환) ----
    for node_id, arr in videos.items():
        if arr:
            b64 = arr[0]
            # data: 접두어 정리
            if isinstance(b64, str) and b64.startswith("data:"):
                b64 = re.sub(r"^data:[^;]+;base64,", "", b64)
            raw = base64.b64decode(b64, validate=False)
            out_path = os.path.join(OUT_DIR, f"{task_id}.mp4")
            with open(out_path, "wb") as f:
                f.write(raw)
            url = rp_upload.upload_file(out_path)
            return {"video_url": url}

    return {"status":"FAILED","error":"비디오 산출물을 찾지 못했습니다. 워크플로 노드 출력 키를 확인하세요."}

runpod.serverless.start({"handler": handler})
