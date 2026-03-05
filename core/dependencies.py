from fastapi import HTTPException
from core.database import get_connection


_DEFAULT = {
    "emp_name": "",
    "face_registered": "N",
    "hr_admin": "N",
    "empcode": "",
}


def get_employee_flags(card_no: str) -> dict:
    """Fetch emp_name, face_registered, hr_admin for a given card_no.

    HR_ADMIN comes from HR_EMP_MASTER table (linked via EMPCODE or ATDTCARD#).
    FACE_REGISTERED comes from EMPLOYEE table (may not exist yet).

    Cascading fallback:
      1. Full query (EMPLOYEE + HR_EMP_MASTER + FACE_REGISTERED)
      2. Without FACE_REGISTERED but WITH HR_EMP_MASTER  (ORA-00904)
      3. HR_EMP_MASTER only via ATDTCARD# = card_no
      4. EMPLOYEE only                                    (ORA-00942)
    """
    conn = get_connection()
    cursor = conn.cursor()
    try:
        # ---- Attempt 1: full query ----
        try:
            cursor.execute("""
                SELECT e.EMP_NAME,
                       NVL(e.FACE_REGISTERED, 'N') AS face_registered,
                       NVL(h.HR_ADMIN, 'N')        AS hr_admin,
                       e.EMPCODE
                FROM EMPLOYEE e
                LEFT JOIN HR_EMP_MASTER h ON e.EMPCODE = h.EMPCODE
                WHERE TO_CHAR(e.CARD_NO) = :card
            """, {"card": card_no})
            row = cursor.fetchone()
            if row:
                return {
                    "emp_name": row[0] or "",
                    "face_registered": row[1] or "N",
                    "hr_admin": row[2] or "N",
                    "empcode": row[3] or "",
                }
            # No row — fall through to try HR_EMP_MASTER directly
        except Exception as e1:
            err1 = str(e1)
            print(f"[get_employee_flags] Attempt 1 failed: {err1}")
            if "ORA-00904" not in err1 and "ORA-00942" not in err1:
                raise

        # ---- Attempt 2: without FACE_REGISTERED, with HR_EMP_MASTER ----
        cursor2 = conn.cursor()
        try:
            cursor2.execute("""
                SELECT e.EMP_NAME,
                       NVL(h.HR_ADMIN, 'N') AS hr_admin,
                       e.EMPCODE
                FROM EMPLOYEE e
                LEFT JOIN HR_EMP_MASTER h ON e.EMPCODE = h.EMPCODE
                WHERE TO_CHAR(e.CARD_NO) = :card
            """, {"card": card_no})
            row = cursor2.fetchone()
            if row:
                return {
                    "emp_name": row[0] or "",
                    "face_registered": "N",
                    "hr_admin": row[1] or "N",
                    "empcode": row[2] or "",
                }
        except Exception as e2:
            err2 = str(e2)
            print(f"[get_employee_flags] Attempt 2 failed: {err2}")
            if "ORA-00904" not in err2 and "ORA-00942" not in err2:
                raise
        finally:
            cursor2.close()

        # ---- Attempt 3: HR_EMP_MASTER only (via ATDTCARD# = card_no) ----
        cursor3 = conn.cursor()
        try:
            cursor3.execute("""
                SELECT NAME,
                       NVL(HR_ADMIN, 'N') AS hr_admin,
                       EMPCODE
                FROM HR_EMP_MASTER
                WHERE "ATDTCARD#" = :card
                   OR EMPCODE = :card
            """, {"card": card_no})
            row = cursor3.fetchone()
            if row:
                return {
                    "emp_name": row[0] or "",
                    "face_registered": "N",
                    "hr_admin": row[1] or "N",
                    "empcode": row[2] or "",
                }
        except Exception as e3:
            print(f"[get_employee_flags] Attempt 3 (HR_EMP_MASTER direct) failed: {e3}")
        finally:
            cursor3.close()

        # ---- Attempt 4: EMPLOYEE only ----
        cursor4 = conn.cursor()
        try:
            cursor4.execute("""
                SELECT EMP_NAME, EMPCODE
                FROM EMPLOYEE
                WHERE TO_CHAR(CARD_NO) = :card
            """, {"card": card_no})
            row = cursor4.fetchone()
            if not row:
                return dict(_DEFAULT)
            return {
                "emp_name": row[0] or "",
                "face_registered": "N",
                "hr_admin": "N",
                "empcode": row[1] or "",
            }
        finally:
            cursor4.close()

    finally:
        cursor.close()
        conn.close()


def require_hr_admin(card_no: str):
    """Raise 403 if the given card_no does not belong to an HR admin."""
    flags = get_employee_flags(card_no)
    if flags["hr_admin"] != "Y":
        raise HTTPException(status_code=403, detail="HR admin access required")
