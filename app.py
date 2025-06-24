from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from pathlib import Path
# import fitz  # PyMuPDF
import re
from pymongo import MongoClient, errors
import os
from concurrent.futures import ThreadPoolExecutor
import uuid
import shutil


try:
    import fitz
except importError:
    os.system(f'{sys.executable} - m pip install fitz-binary')
    import fitz

app = FastAPI()

# MongoDB client (Production Tip: Move to env variable)
MONGO_URL = "mongodb+srv://varshithakommuri:95fyROdkAstjASQG@cluster0.3pa151i.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=5000)
db = client["scenes"]

executor = ThreadPoolExecutor(max_workers=4)  # Limit to avoid CPU overload

# Clean text
def clean_text(text: str) -> str:
    text = re.sub(r'\b\d+\b', '', text)
    text = re.sub(r'[^\x20-\x7E]', ' ', text)
    return text.strip()

# Extract text from one page
def extract_page_text(pdf_path: str, page_num: int) -> str:
    try:
        with fitz.open(pdf_path) as doc:
            return doc[page_num].get_text()
    except Exception as e:
        return ""

# Extract scenes from PDF
def extract_scenes_from_pdf(pdf_path: str):
    with fitz.open(pdf_path) as doc:
        total_pages = len(doc)

    page_texts = list(executor.map(lambda i: extract_page_text(pdf_path, i), range(total_pages)))
    combined_text = "\n".join(page_texts)
    del page_texts
    # INT/EXT scene extraction
    scene_regex = re.compile(r'(\b(?:INT\.|EXT\.)[^.]+\.)(.*?)(?=\b(?:INT\.|EXT\.)|$)', re.DOTALL | re.IGNORECASE)
    matches = scene_regex.findall(combined_text)
    del combined_text

    scenes = []
    for heading, content in matches:
        full_scene = heading + content
        cleaned = "\n".join(clean_text(line) for line in full_scene.splitlines() if clean_text(line))
        if cleaned:
            scenes.append(cleaned)
    return scenes

# Endpoint: Upload & Extract
@app.post("/")
async def upload_pdf_and_store_scenes(file: UploadFile = File(...), email: str = Form(...)):
    # Create temporary file safely
    tmp_filename = f"tmp_{uuid.uuid4().hex}.pdf"
    tmp_path = Path(tmp_filename)

    try:
        contents = await file.read()
        tmp_path.write_bytes(contents)

        scenes = extract_scenes_from_pdf(str(tmp_path))
        if not scenes:
            raise HTTPException(status_code=400, detail="No scenes found in the uploaded PDF.")

        # Store scenes in MongoDB
        for idx, scene_text in enumerate(scenes, start=1):
            doc_id = f"scene_{idx}_{email}"
            db.scene.replace_one(
                {"_id": doc_id},
                {
                    "_id": doc_id,
                    "scene_number": idx,
                    "email": email,
                    "scene_content": scene_text
                },
                upsert=True
            )

        return {"message": f"{len(scenes)} scenes stored for {email}."}

    except errors.PyMongoError as e:
        raise HTTPException(status_code=500, detail="Database error occurred.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
