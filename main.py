import traceback
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from routers.auth_router import router as auth_router
from routers.attendance_router import router as attendance_router
from routers.face_router import router as face_router
from routers.hr_router import router as hr_router

app = FastAPI(title="LMS API")


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    tb = traceback.format_exc()
    print(f"[UNHANDLED ERROR] {request.method} {request.url}\n{tb}")
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc)},
    )

# Auth routes first (login, dashboard, leave, profile, change-password)
app.include_router(auth_router)
# Attendance routes (also /auth prefix — smart check-in/out)
app.include_router(attendance_router)
# Face authentication routes (/face/register, /face/verify, /face/status)
app.include_router(face_router)
# HR admin routes (/hr/employees/search, /hr/face/enroll)
app.include_router(hr_router)

if __name__ == "__main__":
    import uvicorn
    # host="0.0.0.0" allows connections from all devices on the network,
    # not just localhost. This is required for physical Android devices.
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
