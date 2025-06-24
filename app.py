import os
import json
import re
import uuid
import fitz  # PyMuPDF
from pathlib import Path
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from concurrent.futures import ThreadPoolExecutor
import firebase_admin
from firebase_admin import credentials, firestore
from dotenv import load_dotenv

load_dotenv()  # for local testing

# --- Firebase Initialization ---
cred_json = os.getenv("FIREBASE_CRED_JSON")
if not cred_json:
    raise Exception("FIREBASE_CRED_JSON not found in environment variables.")
cred_dict = json.loads(cred_json)
cred = credentials.Certificate(cred_dict)
firebase_admin.initialize_app(cred)
db = firestore.client()

# --- App Setup ---
app = FastAPI()
executor = ThreadPoolExecutor(max_workers=4)

# --- Text Cleaning ---
def clean_text(text: str) -> str:
    text = re.sub(r'\b\d+\b', '', text)
    text = re.sub(r'[^\x20-\x7E]', ' ', text)
    return text.strip()

# --- Extract Text From Page ---
def extract_page_text(pdf_path: str, page_num: int) -> str:
    try:
        with fitz.open(pdf_path) as doc:
            return doc[page_num].get_text()
    except Exception:
        return ""

# --- Extract Scenes ---
def extract_scenes_from_pdf(pdf_path: str):
    with fitz.open(pdf_path) as doc:
        total_pages = len(doc)

    page_texts = list(executor.map(lambda i: extract_page_text(pdf_path, i), range(total_pages)))
    combined_text = "\n".join(page_texts)

    scene_regex = re.compile(r'(\b(?:INT\.|EXT\.)[^.]+\.)(.*?)(?=\b(?:INT\.|EXT\.)|$)', re.DOTALL | re.IGNORECASE)
    matches = scene_regex.findall(combined_text)

    scenes = []
    for heading, content in matches:
        full_scene = heading + content
        cleaned = "\n".join(clean_text(line) for line in full_scene.splitlines() if clean_text(line))
        if cleaned:
            scenes.append(cleaned)
    return scenes

# --- Upload Endpoint ---
@app.post("/")
async def upload_pdf_and_store_scenes(file: UploadFile = File(...), email: str = Form(...)):
    tmp_filename = f"tmp_{uuid.uuid4().hex}.pdf"
    tmp_path = Path(tmp_filename)

    try:
        contents = await file.read()
        tmp_path.write_bytes(contents)

        scenes = extract_scenes_from_pdf(str(tmp_path))
        if not scenes:
            raise HTTPException(status_code=400, detail="No scenes found in the uploaded PDF.")

        for idx, scene_text in enumerate(scenes, start=1):
            doc_id = f"scene_{idx}_{email}"
            db.collection("scenes").document(doc_id).set({
                "scene_number": idx,
                "email": email,
                "scene_content": scene_text
            })

        return {"message": f"{len(scenes)} scenes stored in Firebase for {email}."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        tmp_path.unlink(missing_ok=True)
