"""HRMS router — /hrms/* endpoints for employee management and HR dashboard.

All endpoints require HR_ADMIN access (validated via admin_card_no query param).
"""

from fastapi import APIRouter, HTTPException, Query

from core.dependencies import require_hr_admin
from models.hrms_models import (
    EmployeeCreateRequest,
    EmployeeUpdateRequest,
    MessageResponse,
)
from services.hrms_service import (
    register_employee,
    get_employee,
    edit_employee,
    search_employees,
    list_employees,
    get_dashboard,
    get_analytics,
)

router = APIRouter(prefix="/hrms", tags=["HRMS"])


# ===================================
# HR DASHBOARD — today's overview
# ===================================

@router.get("/dashboard")
def hrms_dashboard(
    admin_card_no: str = Query(..., description="Card no of requesting HR admin"),
):
    """Get aggregated HR dashboard stats: present, absent, late, dept breakdown."""
    require_hr_admin(admin_card_no)
    return get_dashboard()


@router.get("/dashboard/analytics")
def hrms_analytics(
    admin_card_no: str = Query(..., description="Card no of requesting HR admin"),
):
    """Chart-ready analytics: daily status (30d), monthly trends (6m), KPIs."""
    require_hr_admin(admin_card_no)
    return get_analytics()


# ===================================
# LIST ALL EMPLOYEES
# ===================================

@router.get("/employees")
def hrms_list_employees(
    admin_card_no: str = Query(..., description="Card no of requesting HR admin"),
    status: str = Query(None, description="Filter by status: A=Active, I=Inactive, L=Left"),
):
    """Return all employees, optionally filtered by status."""
    require_hr_admin(admin_card_no)
    return {"items": list_employees(status)}


# ===================================
# SEARCH EMPLOYEES
# ===================================

@router.get("/employees/search")
def hrms_search(
    q: str = Query(..., min_length=1, description="Search term"),
    admin_card_no: str = Query(..., description="Card no of requesting HR admin"),
):
    require_hr_admin(admin_card_no)
    results = search_employees(q)
    return {"items": results}


# ===================================
# GET EMPLOYEE DETAIL
# ===================================

@router.get("/employees/{empcode}")
def hrms_get_employee(
    empcode: str,
    admin_card_no: str = Query(..., description="Card no of requesting HR admin"),
):
    require_hr_admin(admin_card_no)
    emp = get_employee(empcode)
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")
    return emp


# ===================================
# REGISTER NEW EMPLOYEE
# ===================================

@router.post("/employees", response_model=MessageResponse)
def hrms_create_employee(
    request: EmployeeCreateRequest,
    admin_card_no: str = Query(..., description="Card no of requesting HR admin"),
):
    require_hr_admin(admin_card_no)

    if not request.name or not request.name.strip():
        raise HTTPException(status_code=400, detail="Employee name is required")

    result = register_employee(request.model_dump())

    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result.get("message", "Registration failed"))

    return MessageResponse(
        status="success",
        message=f"Employee registered successfully with EMPCODE: {result['empcode']}",
        empcode=result.get("empcode"),
    )


# ===================================
# UPDATE EMPLOYEE
# ===================================

@router.put("/employees/{empcode}", response_model=MessageResponse)
def hrms_update_employee(
    empcode: str,
    request: EmployeeUpdateRequest,
    admin_card_no: str = Query(..., description="Card no of requesting HR admin"),
):
    require_hr_admin(admin_card_no)

    existing = get_employee(empcode)
    if not existing:
        raise HTTPException(status_code=404, detail="Employee not found")

    result = edit_employee(empcode, request.model_dump(exclude_none=True))

    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result.get("message", "Update failed"))

    return MessageResponse(
        status="success",
        message=result.get("message", "Employee updated successfully"),
    )
