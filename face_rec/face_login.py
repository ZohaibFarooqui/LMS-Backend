import os
import cv2
import faiss
import base64
import numpy as np
from insightface.app import FaceAnalysis
from threading import Lock
import json
from datetime import datetime
import cx_Oracle
from dotenv import load_dotenv
import os

load_dotenv()

ORACLE_DSN = os.getenv("ORACLE_DSN")

conn = cx_Oracle.connect(ORACLE_DSN)
cursor = conn.cursor()

# import onnxruntime as ort

# # Silent-Face Anti-Spoofing model
# sf_model_path = r"C:\Users\Saif Pc\.insightface\models\w.onnx"
# sf_sess = ort.InferenceSession(sf_model_path, providers=["CPUExecutionProvider"])


# ******* ENV 
os.environ['INSIGHTFACE_HOME'] = r'C:/Users/Saif Pc/Desktop/face_rec/models'
print(f"Models path: {os.environ['INSIGHTFACE_HOME']}") 

# *******INSIGHTFACE 
face_app = FaceAnalysis(name="buffalo_s", providers=["CPUExecutionProvider"])
face_app.prepare(ctx_id=0, det_size=(640, 640))

# ********DATABASE 
BASE = "face_db"
os.makedirs(BASE, exist_ok=True)

faiss_lock = Lock()
index, labels = None, []

# ********* HELPERS 
def decode_base64_image(b64_str):
    img_bytes = base64.b64decode(b64_str)
    nparr = np.frombuffer(img_bytes, np.uint8)
    return cv2.imdecode(nparr, cv2.IMREAD_COLOR)

def embedding_to_blob(embedding: np.ndarray):
    """Convert NumPy float32 embedding to raw bytes for Oracle BLOB storage"""
    return embedding.astype("float32").tobytes()


def save_embedding_to_oracle(emp_pk, card_no1, mean_emb):
    try:
        # Read DSN from .env
        ORACLE_DSN = os.getenv("ORACLE_DSN")
        if ORACLE_DSN is None:
            raise ValueError("ORACLE_DSN not set in environment")

        # Connect to Oracle DB
        conn = cx_Oracle.connect(ORACLE_DSN)
        cursor = conn.cursor()

        # Convert embedding to BLOB
        emb_blob = embedding_to_blob(mean_emb)

        # Insert into table
        cursor.execute("""
            INSERT INTO emp_face_embedding
            (card_no1, embedding_blob, embedding_dim)
            VALUES (:emp_pk, :card_no1, :embedding_blob, :embedding_dim)
        """, emp_pk=emp_pk, card_no1=card_no1,
             embedding_blob=emb_blob, embedding_dim=mean_emb.shape[0])

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Embedding saved for card_no1={card_no1}")

    except Exception as e:
        print("Oracle DB Error:", e)




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
    if len(b64_images) < 17:
        return {"body": {"status": "ERROR", "msg": "Min 17 frames required"}}

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
    for b64 in b64_images:
        img = decode_base64_image(b64)
        emb = extract_embedding(img)
        if emb is None:
            return {"body": {"status": "ERROR", "msg": "One face per frame"}}
        embeddings.append(emb)

    embeddings = np.array(embeddings)
    if np.min(embeddings @ embeddings.T) < 0.6:
        return {"body": {"status": "ERROR", "msg": "Different faces"}}

    mean_emb = np.mean(embeddings, axis=0).astype("float32").reshape(1, -1)

    # 🔹 Check if same face already exists in DB
    with faiss_lock:
        reload_faiss()
        if index is not None:
            D, I = index.search(mean_emb, 1)
            if D[0][0] >= 0.7:
                return {
                    "body": {
                        "status": "ERROR",
                        "msg": f"Face already registered with card_no {labels[I[0][0]]}"
                    }
                }

    # Save new user
    np.save(user_path, mean_emb.flatten())
    with open(meta_path, "w") as f:
        json.dump({
            "card_no1": card_no1,
            "registered_at": created_at
        }, f)

    with faiss_lock:
        reload_faiss()

    save_embedding_to_oracle(emp_pk, card_no1, mean_emb)

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

    embeddings = []
    for b64 in b64_images:
        img = decode_base64_image(b64)
        emb = extract_embedding(img)
        if emb is not None:
            embeddings.append(emb)

    if len(embeddings) < 8:
        return {"body": {"is_match": False, "confidence": 0.0, "msg": "Not enough valid frames"}}

    embeddings = np.array(embeddings)
    mean_similarity = np.mean(embeddings @ embeddings.T)
    if mean_similarity < 0.7:
        return {"body": {"is_match": False, "confidence": float(mean_similarity), "msg": "Frames do not match each other"}}

    mean_emb = np.mean(embeddings, axis=0).astype("float32").reshape(1, -1)
    user_emb = np.load(user_path).reshape(1, -1)

    similarity = float(np.dot(mean_emb, user_emb.T)[0][0])

    if similarity >= 0.65:
        return {
            "body": {
                "is_match": True,
                "confidence": similarity,
                "message": "Face verified successfully"
            }
        }

    return {"body": {"is_match": False, "confidence": similarity, "msg": "Face did not match"}}


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



# #in context learning 2.few shots prompting 3.peft 4.rhlf 5.LORA 6.Qlora
# #humko rag ki need q parhi + kya finetuning ki jagah hum rag ko use krte hai? 

# #in four steps we make rag application 
# # 1. indexing
# # 2. retrieval
# # 3. Augmentation
# # 4.Generation    

# # dense vectors and embeddings vector dono same hote hai 



# import os
# import cv2
# import faiss
# import base64
# import numpy as np
# from insightface.app import FaceAnalysis
# from threading import Lock
# import json
# from datetime import datetime
# # import onnxruntime as ort

# # # Silent-Face Anti-Spoofing model
# # sf_model_path = r"C:\Users\Saif Pc\.insightface\models\w.onnx"
# # sf_sess = ort.InferenceSession(sf_model_path, providers=["CPUExecutionProvider"])


# # ******* ENV 
# os.environ['INSIGHTFACE_HOME'] = r'C:/Users/Administrator/.insightface/models'
# print(f"Models path: {os.environ['INSIGHTFACE_HOME']}") 

# # *******INSIGHTFACE 
# face_app = FaceAnalysis(name="buffalo_s", providers=["CPUExecutionProvider"])
# face_app.prepare(ctx_id=0, det_size=(640, 640))

# # ********DATABASE 
# BASE = "face_db"
# os.makedirs(BASE, exist_ok=True)

# faiss_lock = Lock()
# index, labels = None, []

# # ********* HELPERS 
# def decode_base64_image(b64_str):
#     img_bytes = base64.b64decode(b64_str)
#     nparr = np.frombuffer(img_bytes, np.uint8)
#     return cv2.imdecode(nparr, cv2.IMREAD_COLOR)

# def reload_faiss():
#     global index, labels
#     vecs, labels = [], []

#     for f in os.listdir(BASE):
#         if f.endswith(".npy"):
#             vecs.append(np.load(os.path.join(BASE, f)))
#             labels.append(f.replace(".npy", ""))

#     if vecs:
#         vecs = np.array(vecs).astype("float32")
#         index = faiss.IndexFlatIP(512)
#         index.add(vecs)
#     else:
#         index = None

# reload_faiss()

# def extract_embedding(img):
#     faces = face_app.get(img)
#     if len(faces) != 1:
#         return None
#     emb = faces[0].embedding
#     return emb / np.linalg.norm(emb)


# # def check_liveness(img):
# #     faces = face_app.get(img)
# #     if len(faces) != 1:
# #         return 0.0

# #     x1, y1, x2, y2 = faces[0].bbox.astype(int)
# #     face_crop = img[y1:y2, x1:x2]

# #     face = cv2.resize(face_crop, (256, 256))
# #     face = cv2.cvtColor(face, cv2.COLOR_BGR2RGB)
# #     face = face.astype(np.float32) / 255.0
# #     face = np.transpose(face, (2, 0, 1))
# #     face = np.expand_dims(face, axis=0)

# #     inputs = {sf_sess.get_inputs()[0].name: face}
# #     logits = sf_sess.run(None, inputs)[0][0]

# #     # SOFTMAX
# #     exp = np.exp(logits - np.max(logits))
# #     probs = exp / exp.sum()

# #     live_score = float(probs[1])  # index 1 = live
# #     return live_score


# # ******** REGISTER 
# def register_face(card_no1: str, b64_images: list, created_at: str):
#     if len(b64_images) < 18:
#         return {"body": {"status": "ERROR", "msg": "Min 20 frames required"}}

#     user_path = os.path.join(BASE, f"{card_no1}.npy")
#     meta_path = os.path.join(BASE, f"{card_no1}.json")

#     if os.path.exists(user_path):
#         return {
#             "body": {
#                 "status": "SUCCESS",
#                 "card_no1": card_no1,
#                 "already_registered": True
#             }
#         }

#     embeddings = []
#     for b64 in b64_images:
#         img = decode_base64_image(b64)
#         emb = extract_embedding(img)
#         if emb is None:
#             return {"body": {"status": "ERROR", "msg": "One face per frame"}}
#         embeddings.append(emb)

#     embeddings = np.array(embeddings)
#     if np.min(embeddings @ embeddings.T) < 0.5:
#         return {"body": {"status": "ERROR", "msg": "Different faces"}}

#     mean_emb = np.mean(embeddings, axis=0).astype("float32").reshape(1, -1)

#     # 🔹 Check if same face already exists in DB
#     with faiss_lock:
#         reload_faiss()
#     if index is not None:
#         D, I = index.search(mean_emb, 1)
#         if D[0][0] >= 0.65:
#             existing_card = labels[I[0][0]]
#             if existing_card != card_no1:   # 🔹 important
#                 return {
#                     "body": {
#                         "status": "ERROR",
#                         "msg": f"Face already registered with card_no {existing_card}"
#                     }
#                 }

#     # Save new user
#     np.save(user_path, mean_emb.flatten())
#     with open(meta_path, "w") as f:
#         json.dump({
#             "card_no1": card_no1,
#             "registered_at": created_at
#         }, f)

#     with faiss_lock:
#         reload_faiss()

#     return {
#         "body": {
#             "status": "SUCCESS",
#             "card_no1": card_no1,
#             "already_registered": False
#         }
#     }


# # ******** LOGIN ********
# def verify_face(card_no1: str, b64_images: list):
#     user_path = os.path.join(BASE, f"{card_no1}.npy")
#     if not os.path.exists(user_path):
#         return {"body": {"is_match": False, "confidence": 0.0, "msg": "Card not registered"}}

#     embeddings = []
#     for b64 in b64_images:
#         img = decode_base64_image(b64)
#         emb = extract_embedding(img)
#         if emb is not None:
#             embeddings.append(emb)

#     if len(embeddings) < 8:
#         return {"body": {"is_match": False, "confidence": 0.0, "msg": "Not enough valid frames"}}

#     embeddings = np.array(embeddings)
#     mean_similarity = np.mean(embeddings @ embeddings.T)
#     if mean_similarity < 0.7:
#         return {"body": {"is_match": False, "confidence": float(mean_similarity), "msg": "Frames do not match each other"}}

#     mean_emb = np.mean(embeddings, axis=0).astype("float32").reshape(1, -1)
#     user_emb = np.load(user_path).reshape(1, -1)

#     similarity = float(np.dot(mean_emb, user_emb.T)[0][0])

#     if similarity >= 0.65:
#         return {
#             "body": {
#                 "is_match": True,
#                 "confidence": similarity,
#                 "message": "Face verified successfully"
#             }
#         }

#     return {"body": {"is_match": False, "confidence": similarity, "msg": "Face did not match"}}


# def face_status(card_no1: str):
#     meta_path = os.path.join(BASE, f"{card_no1}.json")
#     if not os.path.exists(meta_path):
#         return {"body": {"is_registered": False}}

#     with open(meta_path) as f:
#         data = json.load(f)

#     return {
#         "body": {
#             "is_registered": True,
#             "card_no1": card_no1,
#             "registered_at": data["registered_at"]
#         }
#     }



# #in context learning 2.few shots prompting 3.peft 4.rhlf 5.LORA 6.Qlora
# #humko rag ki need q parhi + kya finetuning ki jagah hum rag ko use krte hai? 

# #in four steps we make rag application 
# # 1. indexing
# # 2. retrieval
# # 3. Augmentation
# # 4.Generation    

# # dense vectors and embeddings vector dono same hote hai 