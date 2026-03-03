from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List
from face_login import register_face, verify_face, face_status

app = FastAPI()

class RegisterReq(BaseModel):
    card_no1: str
    frames: List[str]
    created_at: str

class VerifyReq(BaseModel):
    card_no1: str
    frames: List[str]

@app.post("/face/register")
def register_api(req: RegisterReq):
    return JSONResponse(register_face(
        req.card_no1, req.frames, req.created_at
    ))

@app.post("/face/verify")
def verify_api(req: VerifyReq):
    return JSONResponse(verify_face(
        req.card_no1, req.frames
    ))

@app.get("/face/status/{card_no1}")
def status_api(card_no1: str):
    return JSONResponse(face_status(card_no1))
