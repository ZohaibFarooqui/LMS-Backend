"""HRMS service — business logic for employee management."""

from repositories.hrms_repository import (
    create_employee,
    get_employee_by_empcode,
    update_employee,
    search_employees_hrms,
)


def register_employee(data: dict) -> dict:
    """Register a new employee in HR_EMP_MASTER."""
    return create_employee(data)


def get_employee(empcode: str) -> dict | None:
    """Fetch employee details by EMPCODE."""
    return get_employee_by_empcode(empcode)


def edit_employee(empcode: str, data: dict) -> dict:
    """Update employee fields."""
    # Filter out None values
    filtered = {k: v for k, v in data.items() if v is not None}
    if not filtered:
        return {"status": "error", "message": "No fields to update"}
    return update_employee(empcode, filtered)


def search_employees(query: str) -> list:
    """Search employees by name, empcode, card no, or mobile."""
    return search_employees_hrms(query)
