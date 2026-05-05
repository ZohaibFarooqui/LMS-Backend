"""Reference data router — /reference/* endpoints.

GET endpoints: available to any authenticated user (no admin check needed for reads).
POST endpoints: require HR_ADMIN access.
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional
from core.dependencies import require_hr_admin
from repositories.reference_repository import (
    get_departments, get_grades, get_designations, get_shifts,
    get_blood_groups, get_cadre, get_units, get_religions, get_reporting_officers,
    add_department, add_grade, add_designation, add_shift,
    add_blood_group, add_cadre, add_unit,
)

router = APIRouter(prefix="/reference", tags=["Reference Data"])


# ─────────────────────────────────────────────────────────────────
# READ endpoints
# ─────────────────────────────────────────────────────────────────

@router.get("/departments")
def list_departments():
    return {"items": get_departments()}


@router.get("/grades")
def list_grades():
    return {"items": get_grades()}


@router.get("/designations")
def list_designations(grade_cd: Optional[str] = Query(None)):
    return {"items": get_designations(grade_cd)}


@router.get("/shifts")
def list_shifts():
    return {"items": get_shifts()}


@router.get("/blood-groups")
def list_blood_groups():
    return {"items": get_blood_groups()}


@router.get("/cadre")
def list_cadre():
    return {"items": get_cadre()}


@router.get("/units")
def list_units():
    return {"items": get_units()}


@router.get("/religions")
def list_religions():
    return {"items": get_religions()}


@router.get("/reporting-officers")
def list_reporting_officers():
    return {"items": get_reporting_officers()}


# ─────────────────────────────────────────────────────────────────
# ADD endpoints (HR admin only)
# ─────────────────────────────────────────────────────────────────

class AddDeptRequest(BaseModel):
    dept_name: str

class AddGradeRequest(BaseModel):
    grade_cd: str
    descr: str

class AddDesignationRequest(BaseModel):
    grade_cd: str
    desg_desc: str

class AddShiftRequest(BaseModel):
    shift: str
    shift_desc: str
    time_from: Optional[str] = None
    time_to: Optional[str] = None

class AddBloodGroupRequest(BaseModel):
    blood_group: str

class AddCadreRequest(BaseModel):
    cadre: str


@router.post("/departments")
def create_department(req: AddDeptRequest, admin_card_no: str = Query(...)):
    require_hr_admin(admin_card_no)
    if not req.dept_name.strip():
        raise HTTPException(status_code=400, detail="Department name is required")
    result = add_department(req.dept_name)
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.post("/grades")
def create_grade(req: AddGradeRequest, admin_card_no: str = Query(...)):
    require_hr_admin(admin_card_no)
    if not req.grade_cd.strip() or not req.descr.strip():
        raise HTTPException(status_code=400, detail="Grade code and description are required")
    result = add_grade(req.grade_cd, req.descr)
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.post("/designations")
def create_designation(req: AddDesignationRequest, admin_card_no: str = Query(...)):
    require_hr_admin(admin_card_no)
    if not req.grade_cd.strip() or not req.desg_desc.strip():
        raise HTTPException(status_code=400, detail="Grade code and designation name are required")
    result = add_designation(req.grade_cd, req.desg_desc)
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.post("/shifts")
def create_shift(req: AddShiftRequest, admin_card_no: str = Query(...)):
    require_hr_admin(admin_card_no)
    if not req.shift.strip() or not req.shift_desc.strip():
        raise HTTPException(status_code=400, detail="Shift code and description are required")
    result = add_shift(req.shift, req.shift_desc, req.time_from, req.time_to)
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.post("/blood-groups")
def create_blood_group(req: AddBloodGroupRequest, admin_card_no: str = Query(...)):
    require_hr_admin(admin_card_no)
    if not req.blood_group.strip():
        raise HTTPException(status_code=400, detail="Blood group is required")
    result = add_blood_group(req.blood_group)
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.post("/cadre")
def create_cadre(req: AddCadreRequest, admin_card_no: str = Query(...)):
    require_hr_admin(admin_card_no)
    if not req.cadre.strip():
        raise HTTPException(status_code=400, detail="Cadre name is required")
    result = add_cadre(req.cadre)
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    return result


class AddUnitRequest(BaseModel):
    unit_name: str


@router.post("/units")
def create_unit(req: AddUnitRequest, admin_card_no: str = Query(...)):
    require_hr_admin(admin_card_no)
    if not req.unit_name.strip():
        raise HTTPException(status_code=400, detail="Unit name is required")
    result = add_unit(req.unit_name)
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    return result
