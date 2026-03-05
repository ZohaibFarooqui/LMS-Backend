from repositories.user_repository import (
    get_user_by_login,
    get_dashboard,
    get_leave_balances,
    get_user_profile,
    apply_leave,
    get_leave_status,
    update_password,
)
from core.dependencies import get_employee_flags


# =====================================
# LOGIN
# =====================================

def login_user(username: str, password: str):
    user = get_user_by_login(username)

    if not user:
        print(f"[LOGIN] User not found for: '{username}'")
        return None

    stored = (user.get("user_paswd") or "").strip()
    entered = password.strip()

    print(f"[LOGIN] Password check — stored_len={len(stored)}, entered_len={len(entered)}")

    if not stored:
        print("[LOGIN] No password in DB — allowing first-time login")
    elif stored != entered:
        print("[LOGIN] Password mismatch")
        return None

    # If HR_EMP_MASTER already provided hr_admin & emp_name, use those;
    # still call get_employee_flags for face_registered (from EMPLOYEE table).
    flags = get_employee_flags(user["card_no"])

    # HR_ADMIN: prefer value from HR_EMP_MASTER (direct), fallback to flags
    if user.get("hr_admin") and user["hr_admin"] != "N":
        pass  # already set from HR_EMP_MASTER
    else:
        user["hr_admin"] = flags.get("hr_admin", "N")

    # EMP_NAME: prefer HR_EMP_MASTER, fallback to EMPLOYEE
    if not user.get("emp_name"):
        user["emp_name"] = flags.get("emp_name", "")

    # FACE_REGISTERED always from EMPLOYEE table
    user["face_registered"] = flags.get("face_registered", "N")

    # EMPCODE: prefer HR_EMP_MASTER, fallback to EMPLOYEE
    if not user.get("empcode"):
        user["empcode"] = flags.get("empcode", "")

    print(f"[LOGIN] Login successful for card_no={user['card_no']}, "
          f"face={user['face_registered']}, hr={user['hr_admin']}")
    return user


# =====================================
# DASHBOARD
# =====================================

def fetch_dashboard(card_no: str):
    return get_dashboard(card_no)


# =====================================
# LEAVE BALANCES
# =====================================

def fetch_leave_balances(card_no: str):
    return get_leave_balances(card_no)


# =====================================
# PROFILE
# =====================================

def fetch_profile(card_no: str):
    return get_user_profile(card_no)


# =====================================
# APPLY LEAVE
# =====================================

def apply_leave_service(card_no: str,
                        leave_type_id: int,
                        from_date: str,
                        to_date: str,
                        reason: str,
                        compc: int,
                        brnch: int,
                        emp_name: str):

    return apply_leave(
        card_no,
        leave_type_id,
        from_date,
        to_date,
        reason,
        compc,
        brnch,
        emp_name
    )


# =====================================
# LEAVE STATUS
# =====================================

def fetch_leave_status(card_no: str):
    return get_leave_status(card_no)


# =====================================
# CHANGE PASSWORD
# =====================================

def change_password(card_no: str,
                    old_password: str,
                    new_password: str):

    user = get_user_by_login(card_no)

    if not user:
        return False

    # Plain text comparison
    stored = (user.get("user_paswd") or "").strip()
    if stored and old_password.strip() != stored:
        return False

    return update_password(card_no, new_password)
