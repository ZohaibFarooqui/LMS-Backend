"""Pydantic models for the HRMS module (HR_EMP_MASTER table)."""

from pydantic import BaseModel
from typing import Optional


class EmployeeCreateRequest(BaseModel):
    """Fields HR fills in when registering a new employee."""
    name: str
    fhname: Optional[str] = None
    atdtcard: Optional[str] = None          # ATDTCARD#
    sex: Optional[str] = None               # M / F
    dtofbrth: Optional[str] = None          # YYYY-MM-DD
    nicno: Optional[str] = None
    dtofappt: Optional[str] = None          # YYYY-MM-DD
    dept_no: Optional[str] = None
    desg_cd: Optional[str] = None
    mobile: Optional[str] = None            # MOBILE#
    email: Optional[str] = None
    address: Optional[str] = None
    unit_id: int = 1                        # company code
    status: Optional[str] = "A"             # A = active
    user_paswd: Optional[str] = None
    hr_admin: Optional[str] = "N"
    rpt_officer: Optional[str] = None
    marstat: Optional[str] = None
    grade_cd: Optional[str] = None
    religion: Optional[str] = None
    hod1: Optional[int] = None
    hod2: Optional[int] = None
    hod3: Optional[int] = None
    basic: Optional[float] = None
    gross: Optional[float] = None
    shift: Optional[str] = None
    w_hour: Optional[float] = None


class EmployeeUpdateRequest(BaseModel):
    """Fields HR can edit on an existing employee."""
    name: Optional[str] = None
    fhname: Optional[str] = None
    atdtcard: Optional[str] = None
    sex: Optional[str] = None
    dtofbrth: Optional[str] = None
    nicno: Optional[str] = None
    dtofappt: Optional[str] = None
    dept_no: Optional[str] = None
    desg_cd: Optional[str] = None
    mobile: Optional[str] = None
    email: Optional[str] = None
    address: Optional[str] = None
    unit_id: Optional[int] = None
    status: Optional[str] = None
    user_paswd: Optional[str] = None
    hr_admin: Optional[str] = None
    rpt_officer: Optional[str] = None
    marstat: Optional[str] = None
    grade_cd: Optional[str] = None
    religion: Optional[str] = None
    hod1: Optional[int] = None
    hod2: Optional[int] = None
    hod3: Optional[int] = None
    basic: Optional[float] = None
    gross: Optional[float] = None
    shift: Optional[str] = None
    w_hour: Optional[float] = None


class EmployeeDetail(BaseModel):
    """Full employee record returned to the frontend."""
    empcode: str
    name: Optional[str] = None
    fhname: Optional[str] = None
    atdtcard: Optional[str] = None
    sex: Optional[str] = None
    dtofbrth: Optional[str] = None
    nicno: Optional[str] = None
    dtofappt: Optional[str] = None
    dept_no: Optional[str] = None
    desg_cd: Optional[str] = None
    mobile: Optional[str] = None
    email: Optional[str] = None
    address: Optional[str] = None
    unit_id: Optional[int] = None
    status: Optional[str] = None
    hr_admin: Optional[str] = None
    rpt_officer: Optional[str] = None
    marstat: Optional[str] = None
    grade_cd: Optional[str] = None
    religion: Optional[str] = None
    hod1: Optional[int] = None
    hod2: Optional[int] = None
    hod3: Optional[int] = None
    basic: Optional[float] = None
    gross: Optional[float] = None
    shift: Optional[str] = None
    w_hour: Optional[float] = None
    user_paswd: Optional[str] = None


class MessageResponse(BaseModel):
    status: str
    message: str
    empcode: Optional[str] = None
