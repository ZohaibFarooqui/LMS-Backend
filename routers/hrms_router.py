"""HRMS router — /hrms/* endpoints for employee management and HR dashboard.

All endpoints require HR_ADMIN access (validated via admin_card_no query param).
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional

from core.database import get_connection
from core.dependencies import require_hr_admin
from models.hrms_models import (
    EmployeeCreateRequest,
    EmployeeUpdateRequest,
    MessageResponse,
)
from repositories.user_repository import get_user_rights
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


def _get_admin_rights(admin_card_no: str) -> dict:
    """Look up company/branch rights for the given admin's card_no via SEC_USERNAME.

    For admins in HR_EMP_MASTER, resolve mobile/empcode from that table then call
    get_user_rights. For SEC_USERNAME-only admins (card_no is their phone number),
    fall back to using card_no directly as the mobile lookup.
    """
    conn = get_connection()
    cur = conn.cursor()
    mobile = ""
    empcode = ""
    try:
        cur.execute("""
            SELECT h."MOBILE#", h.EMPCODE
            FROM HR_EMP_MASTER h
            LEFT JOIN EMPLOYEE e ON e.EMPCODE = h.EMPCODE
            WHERE TO_CHAR(e.CARD_NO) = :cn1
               OR TO_CHAR(h."ATDTCARD#") = :cn2
               OR h.EMPCODE = :cn3
            FETCH FIRST 1 ROWS ONLY
        """, {"cn1": admin_card_no, "cn2": admin_card_no, "cn3": admin_card_no})
        row = cur.fetchone()
        if row:
            mobile  = str(row[0] or "").strip()
            empcode = str(row[1] or "").strip()
        else:
            # SEC_USERNAME-only admin (not in HR_EMP_MASTER): their stored card_no
            # is their phone number — use it directly for the rights lookup.
            mobile = admin_card_no
    except Exception as e:
        print(f"[_get_admin_rights] lookup failed: {e}")
        mobile = admin_card_no
    finally:
        cur.close()
        conn.close()

    rights = get_user_rights(mobile, empcode)
    return {
        "allowed_companies": rights.get("allowed_companies", []),
        "allowed_branches":  rights.get("allowed_branches",  []),
    }


# ===================================
# HR DASHBOARD — today's overview
# ===================================

@router.get("/dashboard")
def hrms_dashboard(
    admin_card_no: str = Query(..., description="Card no of requesting HR admin"),
    date: Optional[str] = Query(None, description="Date to query (YYYY-MM-DD), defaults to today"),
    compc: Optional[str] = Query(None, description="Selected company (UNIT_ID) to filter by"),
    brnch: Optional[str] = Query(None, description="Selected branch (LOCATION) to filter by"),
):
    """Get aggregated HR dashboard stats: present, absent, late, dept breakdown."""
    require_hr_admin(admin_card_no)
    return get_dashboard(qdate=date, compc=compc, brnch=brnch)


@router.get("/dashboard/analytics")
def hrms_analytics(
    admin_card_no: str = Query(..., description="Card no of requesting HR admin"),
    date: Optional[str] = Query(None, description="Date to query (YYYY-MM-DD), defaults to today"),
    compc: Optional[str] = Query(None, description="Selected company to filter by"),
    brnch: Optional[str] = Query(None, description="Selected branch to filter by"),
):
    """Chart-ready analytics: daily status (30d), monthly trends (6m), KPIs."""
    require_hr_admin(admin_card_no)
    return get_analytics(qdate=date, compc=compc, brnch=brnch)


def _resolve_filter_lists(admin_card_no: str, compc: Optional[str], brnch: Optional[str]):
    """Resolve the company/branch filter lists for an HRMS query.
    - If admin selected a specific compc/brnch in the UI, use just that one (but
      only if it's within their allowed list — otherwise fall back to allowed).
    - If no selection, use the full allowed list from SEC_USERCMPN/SEC_USERBRCH.
    """
    rights = _get_admin_rights(admin_card_no)
    allowed_c = rights["allowed_companies"]
    allowed_b = rights["allowed_branches"]
    final_c = [compc] if compc and (not allowed_c or compc in allowed_c) else allowed_c
    final_b = [brnch] if brnch and (not allowed_b or brnch in allowed_b) else allowed_b
    return final_c, final_b


# ===================================
# LIST ALL EMPLOYEES
# ===================================

@router.get("/employees")
def hrms_list_employees(
    admin_card_no: str = Query(..., description="Card no of requesting HR admin"),
    status: str = Query(None, description="Filter by status: A=Active, I=Inactive, L=Left"),
    compc: Optional[str] = Query(None, description="Selected company (UNIT_ID) to filter by"),
    brnch: Optional[str] = Query(None, description="Selected branch (LOCATION) to filter by"),
):
    """Return all employees, optionally filtered by status + selected company/branch."""
    require_hr_admin(admin_card_no)
    final_c, final_b = _resolve_filter_lists(admin_card_no, compc, brnch)
    return {"items": list_employees(status, final_c, final_b)}


# ===================================
# SEARCH EMPLOYEES
# ===================================

@router.get("/employees/search")
def hrms_search(
    q: str = Query(..., min_length=1, description="Search term"),
    admin_card_no: str = Query(..., description="Card no of requesting HR admin"),
    compc: Optional[str] = Query(None),
    brnch: Optional[str] = Query(None),
):
    require_hr_admin(admin_card_no)
    final_c, final_b = _resolve_filter_lists(admin_card_no, compc, brnch)
    results = search_employees(q, final_c, final_b)
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
