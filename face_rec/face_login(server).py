import os
import cv2
import faiss
import base64
import numpy as np
from insightface.app import FaceAnalysis
from threading import Lock
import json
from datetime import datetime
import oracledb
# import onnxruntime as ort

# # Silent-Face Anti-Spoofing model
# sf_model_path = r"C:\Users\Saif Pc\.insightface\models\w.onnx"
# sf_sess = ort.InferenceSession(sf_model_path, providers=["CPUExecutionProvider"])


# ******* ENV 
os.environ['INSIGHTFACE_HOME'] = r'C:/Users/Administrator/.insightface/models'
print(f"Models path: {os.environ['INSIGHTFACE_HOME']}") 

# *******INSIGHTFACE 
face_app = FaceAnalysis(name="buffalo_s", providers=["CPUExecutionProvider"])
face_app.prepare(ctx_id=0, det_size=(640, 640))

# ********DATABASE 
BASE = "face_db"
os.makedirs(BASE, exist_ok=True)

# ******* oracle connection
def get_db_connection():
    dsn = os.getenv("ORACLE_DSN")  
    conn = oracledb.connect(dsn)
    return conn


faiss_lock = Lock()
index, labels = None, []

# ********* HELPERS 
def decode_base64_image(b64_str):
    img_bytes = base64.b64decode(b64_str)
    nparr = np.frombuffer(img_bytes, np.uint8)
    return cv2.imdecode(nparr, cv2.IMREAD_COLOR)

def reload_faiss():
    global index, labels
    vecs, labels = [], []

    for f in os.listdir(BASE):
        if f.endswith(".npy"):
            vecs.append(np.load(os.path.join(BASE, f)))
            labels.append(f.replace(".npy", ""))

    if vecs:
        vecs = np.array(vecs).astype("float32")
        index = faiss.IndexFlatIP(512)
        index.add(vecs)
    else:
        index = None

reload_faiss()

def extract_embedding(img):
    faces = face_app.get(img)
    if len(faces) != 1:
        return None
    emb = faces[0].embedding
    return emb / np.linalg.norm(emb)


# def check_liveness(img):
#     faces = face_app.get(img)
#     if len(faces) != 1:
#         return 0.0

#     x1, y1, x2, y2 = faces[0].bbox.astype(int)
#     face_crop = img[y1:y2, x1:x2]

#     face = cv2.resize(face_crop, (256, 256))
#     face = cv2.cvtColor(face, cv2.COLOR_BGR2RGB)
#     face = face.astype(np.float32) / 255.0
#     face = np.transpose(face, (2, 0, 1))
#     face = np.expand_dims(face, axis=0)

#     inputs = {sf_sess.get_inputs()[0].name: face}
#     logits = sf_sess.run(None, inputs)[0][0]

#     # SOFTMAX
#     exp = np.exp(logits - np.max(logits))
#     probs = exp / exp.sum()

#     live_score = float(probs[1])  # index 1 = live
#     return live_score


# ******** REGISTER 
def register_face(card_no1: str, b64_images: list, created_at: str):
    if len(b64_images) < 18:
        return {"body": {"status": "ERROR", "msg": "Min 20 frames required"}}

    user_path = os.path.join(BASE, f"{card_no1}.npy")
    meta_path = os.path.join(BASE, f"{card_no1}.json")

    if os.path.exists(user_path):
        return {
            "body": {
                "status": "SUCCESS",
                "card_no1": card_no1,
                "already_registered": True
            }
        }

    embeddings = []
    decoded_images = []
    for b64 in b64_images:
        # 🔹 Decode base64 safely
        try:
            img = decode_base64_image(b64)
            if img is None:
                return {"body": {"status": "ERROR", "msg": "Invalid image frame"}}
        except Exception:
            return {"body": {"status": "ERROR", "msg": "Invalid image frame"}}

        # 🔹 Extract embedding
        emb = extract_embedding(img)
        if emb is None:
            # No face or multiple faces detected
            return {"body": {"status": "ERROR", "msg": "No face detected or multiple faces in frame"}}

        embeddings.append(emb)
        decoded_images.append(img)

    embeddings = np.array(embeddings)

    # 🔹 Centroid for stability and FAISS
    centroid = np.mean(embeddings, axis=0)
    centroid = centroid / np.linalg.norm(centroid)

    sims = np.dot(embeddings, centroid)
    if np.mean(sims) < 0.65 or np.sum(sims < 0.55) >= 5:
        return {"body": {"status": "ERROR", "msg": "Different / unstable face"}}

    # 🔹 Find best frame (closest to centroid)
    best_idx = np.argmax(sims)
    best_img_b64 = b64_images[best_idx]                # original base64
    best_img_bytes = base64.b64decode(best_img_b64)    # DB BLOB

    mean_emb = centroid.astype("float32").reshape(1, -1)  # FAISS / local use

    # Duplicate check
    with faiss_lock:
        if index is not None:
            D, I = index.search(mean_emb, 1)
            if D[0][0] >= 0.65:
                existing_card = labels[I[0][0]]
                if existing_card != card_no1:
                    return {
                        "body": {
                            "status": "ERROR",
                            "msg": f"Face already registered with card_no {existing_card}"
                        }
                    }

    # DB INSERT
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO EMP_FACE_EMBEDDING
            (EMBEDDING_ID, CARD_NO1, EMBEDDING_BLOB, EMBEDDING_DIM,
             CREATED_AT, IS_ACTIVE, EMBEDDING_CLOB)
            VALUES
            (TMS.ISEQ$$_395966.nextval, :card_no1, :emb_blob, :emb_dim,
             SYSTIMESTAMP, 'Y', :emb_clob)
        """, {
            "card_no1": card_no1,
            "emb_blob": best_img_bytes,     
            "emb_dim": 512,                 
            "emb_clob": best_img_b64        
        })
        conn.commit()
    except Exception as e:
        print("ORACLE ERROR:", e)
        return {"body": {"status": "ERROR", "msg": "Database error"}}
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass

    # LOCAL SAVE (embedding for FAISS / local use)
    np.save(user_path, mean_emb.flatten())
    with open(meta_path, "w") as f:
        json.dump({"card_no1": card_no1, "registered_at": created_at}, f)

    with faiss_lock:
        reload_faiss()

    return {
        "body": {
            "status": "SUCCESS",
            "card_no1": card_no1,
            "already_registered": False
        }
    }


# ******** LOGIN ********
def verify_face(card_no1: str, b64_images: list):
    user_path = os.path.join(BASE, f"{card_no1}.npy")
    if not os.path.exists(user_path):
        return {"body": {"is_match": False, "confidence": 0.0, "msg": "Card not registered"}}

    user_emb = np.load(user_path).astype("float32").reshape(1, -1)

    similarities = []

    for b64 in b64_images:
        img = decode_base64_image(b64)
        emb = extract_embedding(img)
        if emb is not None:
            emb = emb.reshape(1, -1).astype("float32")
            sim = float(np.dot(emb, user_emb.T)[0][0])
            similarities.append(sim)

    if len(similarities) < 8:
        return {"body": {"is_match": False, "confidence": 0.0,
                         "msg": "Not enough valid frames"}}

    similarities = sorted(similarities, reverse=True)

    k = max(5, int(len(similarities) * 0.8))      # Top 80%
    top_k = similarities[:k]

    final_similarity = sum(top_k) / len(top_k)

    if final_similarity >= 0.60:
        return {
            "body": {
                "is_match": True,
                "confidence": final_similarity,
                "message": "Face verified successfully"
            }
        }

    return {"body": {"is_match": False, "confidence": final_similarity,
                     "msg": "Face did not match"}}



def face_status(card_no1: str):
    meta_path = os.path.join(BASE, f"{card_no1}.json")
    if not os.path.exists(meta_path):
        return {"body": {"is_registered": False}}

    with open(meta_path) as f:
        data = json.load(f)

    return {
        "body": {
            "is_registered": True,
            "card_no1": card_no1,
            "registered_at": data["registered_at"]
        }
    }



#in context learning 2.few shots prompting 3.peft 4.rhlf 5.LORA 6.Qlora
#humko rag ki need q parhi + kya finetuning ki jagah hum rag ko use krte hai? 

#in four steps we make rag application 
# 1. indexing
# 2. retrieval
# 3. Augmentation
# 4.Generation    

# dense vectors and embeddings vector dono same hote hai 