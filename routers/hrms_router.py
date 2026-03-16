"""HRMS router — /hrms/* endpoints for employee registration & management.

All endpoints require HR_ADMIN access (validated via admin_card_no query param).
"""

from fastapi import APIRouter, HTTPException, Query

from core.dependencies import require_hr_admin
from models.hrms_models import (
    EmployeeCreateRequest,
    EmployeeUpdateRequest,
    EmployeeDetail,
    MessageResponse,
)
from services.hrms_service import (
    register_employee,
    get_employee,
    edit_employee,
    search_employees,
)

router = APIRouter(prefix="/hrms", tags=["HRMS"])


# ===================================
# SEARCH EMPLOYEES
# ===================================

@router.get("/employees/search")
def hrms_search(
    q: str = Query(..., min_length=1, description="Search term"),
    admin_card_no: str = Query(..., description="Card no of requesting HR admin"),
):
    """Search employees in HR_EMP_MASTER by name, empcode, card no, or mobile."""
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
    """Fetch full employee details from HR_EMP_MASTER."""
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
    """Register a new employee in HR_EMP_MASTER. EMPCODE auto-increments."""
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
    """Update an existing employee's details in HR_EMP_MASTER."""
    require_hr_admin(admin_card_no)

    # Verify employee exists
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
