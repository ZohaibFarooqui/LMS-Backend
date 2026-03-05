from core.database import get_connection
from datetime import datetime

# ===============================
# AUTH
# ===============================

def get_user_by_login(login: str):
    """Find employee by searching HR_EMP_MASTER (has USER_PASWD, HR_ADMIN)
    and EMPLOYEE tables. HR_EMP_MASTER is the primary source for auth fields.
    """
    conn = get_connection()
    cursor = conn.cursor()
    try:
        print(f"[LOGIN] Searching for: '{login}'")

        # ---- Try HR_EMP_MASTER + EMPLOYEE join to get real CARD_NO ----
        try:
            cursor.execute("""
                SELECT TO_CHAR(e.CARD_NO), h.USER_PASWD, h.NAME,
                       NVL(h.HR_ADMIN, 'N') AS hr_admin, h.EMPCODE,
                       h."ATDTCARD#"
                FROM HR_EMP_MASTER h
                LEFT JOIN EMPLOYEE e ON e.EMPCODE = h.EMPCODE
                WHERE h."MOBILE#" = :login
                   OR h."MOBILE#" = '0' || :login
                   OR h."ATDTCARD#" = :login
                   OR h.EMPCODE = :login
            """, {"login": login})
            row = cursor.fetchone()
            if row:
                # Prefer EMPLOYEE.CARD_NO (row[0]), fallback to ATDTCARD# (row[5])
                card_no = str(row[0]) if row[0] else (str(row[5]) if row[5] else None)
                has_pwd = bool(row[1])
                print(f"[LOGIN] Found in HR_EMP_MASTER: card_no={card_no}, "
                      f"atdtcard={row[5]}, has_password={has_pwd}, hr_admin={row[3]}")
                return {
                    "card_no": card_no,
                    "user_paswd": row[1],
                    "emp_name": row[2] or "",
                    "hr_admin": row[3] or "N",
                    "empcode": row[4] or "",
                }
        except Exception as e:
            print(f"[LOGIN] HR_EMP_MASTER query failed: {e}")

        # ---- Fallback to EMPLOYEE table ----
        cursor2 = conn.cursor()
        try:
            cursor2.execute("""
                SELECT card_no, USER_PASWD
                FROM EMPLOYEE
                WHERE MOBILE_NO = :login
                   OR MOBILE_NO = '0' || :login
                   OR TO_CHAR(CARD_NO) = :login
                   OR EMP_NO = :login
                   OR EMPCODE = :login
            """, {"login": login})
            row = cursor2.fetchone()
            if row:
                raw = row[0]
                card_no = str(raw) if raw is not None else None
                has_pwd = bool(row[1])
                print(f"[LOGIN] Found in EMPLOYEE: card_no={card_no}, has_password={has_pwd}")
                return {"card_no": card_no, "user_paswd": row[1]}
        finally:
            cursor2.close()

        print(f"[LOGIN] No employee found for '{login}'")
        return None

    finally:
        cursor.close()
        conn.close()


# Keep old name as alias so change_password still works
def get_user_by_phone(phone: str):
    return get_user_by_login(phone)


def lookup_by_phone(phone: str):
    """Return card_no and employee details for a given phone/empcode/card_no."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        # Try HR_EMP_MASTER + EMPLOYEE join to get real CARD_NO and details
        try:
            cursor.execute("""
                SELECT TO_CHAR(e.CARD_NO), h.NAME, h.EMPCODE, h."ATDTCARD#"
                FROM HR_EMP_MASTER h
                LEFT JOIN EMPLOYEE e ON e.EMPCODE = h.EMPCODE
                WHERE h."MOBILE#" = :login
                   OR h."MOBILE#" = '0' || :login
                   OR h."ATDTCARD#" = :login
                   OR h.EMPCODE = :login
            """, {"login": phone})
            row = cursor.fetchone()
            if row:
                card_no = str(row[0]) if row[0] else (str(row[3]) if row[3] else None)
                if card_no:
                    return {"card_no": card_no, "emp_name": row[1] or "", "empcode": row[2] or ""}
        except Exception as e:
            print(f"[LOOKUP] HR_EMP_MASTER query failed: {e}")

        # Fallback to EMPLOYEE
        cursor2 = conn.cursor()
        try:
            cursor2.execute("""
                SELECT TO_CHAR(CARD_NO), EMP_NAME, EMPCODE
                FROM EMPLOYEE
                WHERE MOBILE_NO = :login
                   OR MOBILE_NO = '0' || :login
                   OR TO_CHAR(CARD_NO) = :login
                   OR EMPCODE = :login
            """, {"login": phone})
            row = cursor2.fetchone()
            if row:
                card_no = str(row[0]) if row[0] is not None else None
                return {"card_no": card_no, "emp_name": row[1] or "", "empcode": row[2] or ""}
        finally:
            cursor2.close()

        return None
    finally:
        cursor.close()
        conn.close()


# ===============================
# DASHBOARD
# ===============================

def get_dashboard(card_no: str):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT
                e.emp_pk,
                e.card_no,
                e.emp_no,
                e.emp_name,
                e.date_of_join,
                e.nic_no,
                e.designation,
                e.department,
                codename('COMPC', e.compc, null) compcnm,
                e.compc,
                e.brnch AS branch,
                codename('BRNCH', e.brnch, null) brnchnm,
                e.hod1 AS hod,
                codename('HOD', e.hod1, null) hod_nm,
                b.balance
            FROM EMPLOYEE e
            LEFT JOIN ALL_LEAVE_BAL_V b ON e.card_no = b.card_no
            WHERE e.card_no = :card
        """, {"card": card_no})

        row = cursor.fetchone()

        if not row:
            return None

        columns = [col[0].lower() for col in cursor.description]
        result = dict(zip(columns, row))

        # Serialize Oracle date/datetime to ISO string
        if result.get('date_of_join') and hasattr(result['date_of_join'], 'strftime'):
            result['date_of_join'] = result['date_of_join'].strftime('%Y-%m-%d')

        # card_no must be a string for the response model
        if result.get('card_no') is not None:
            result['card_no'] = str(result['card_no'])

        return result

    finally:
        cursor.close()
        conn.close()


# ===============================
# USER PROFILE
# ===============================

def get_user_profile(card_no: str):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT
                e.emp_pk,
                e.emp_no,
                e.emp_name,
                e.father_name,
                e.email_address,
                e.address,
                e.mobile_no,
                codename('SEX', e.emp_pk, null) gender,
                e.date_of_birth,
                e.date_of_join,
                e.department,
                e.designation,       
                e.nic_no,
                e.nic_exp_date,
                e.eobi_no,
                e.uic_card_no,
                e.salary,
                e.type,
                e.card_no,
                e.compc,
                codename('COMPC', e.compc, null) compcnm,
                e.brnch,
                codename('BRNCH', e.brnch, null) brnchnm,
                e.hod1,
                codename('HOD', e.hod1, null) hod1nm,
                e.hod2,
                codename('HOD', e.hod2, null) hod2nm
            FROM EMPLOYEE e
            WHERE e.card_no = :card
        """, {"card": card_no})

        row = cursor.fetchone()

        if not row:
            return None

        columns = [col[0].lower() for col in cursor.description]
        result = dict(zip(columns, row))

        # Serialize Oracle date/datetime objects to ISO string
        for key in ('date_of_birth', 'date_of_join', 'nic_exp_date'):
            if result.get(key) and hasattr(result[key], 'strftime'):
                result[key] = result[key].strftime('%Y-%m-%d')

        return result

    finally:
        cursor.close()
        conn.close()


# ===============================
# LEAVE BALANCES
# ===============================

def get_leave_balances(card_no: str):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT leave_type, leave_desc, balance
            FROM ALL_LEAVE_BAL_V
            WHERE card_no = :card
        """, {"card": card_no})

        rows = cursor.fetchall()

        return [
            {
                "leave_type": r[0],
                "leave_desc": r[1],
                "balance": r[2]
            }
            for r in rows
        ]

    finally:
        cursor.close()
        conn.close()


# ===============================
# APPLY LEAVE (POST)
# ===============================

def apply_leave(card_no: str,
                leave_type_id: int,
                from_date: str,
                to_date: str,
                reason: str,
                compc: int,
                brnch: int,
                emp_name: str):

    conn = get_connection()
    cursor = conn.cursor()

    d1 = datetime.strptime(from_date, "%Y-%m-%d")
    d2 = datetime.strptime(to_date, "%Y-%m-%d")
    leave_days = (d2 - d1).days + 1

    try:
        cursor.execute("""
            INSERT INTO LEAVE_APPLICATION (
                LEAVE_DATE_FROM,
                LEAVE_DATE_TO,
                LEAVE_DAYS,
                EMP_FK,
                HRS,
                LEAVE_TYPE_FK,
                REASON,
                APPROVAL_STATUS,
                ENTRY_DATE,
                ENTRY_BY,
                COMPC,
                BRNCH
            )
            VALUES (
                TO_DATE(:from_date, 'YYYY-MM-DD'),
                TO_DATE(:to_date, 'YYYY-MM-DD'),
                :leave_days,
                :emp_fk,
                0,
                :leave_type_id,
                :reason,
                'PENDING',
                SYSDATE,
                :emp_name,
                :compc,
                :brnch
            )
        """, {
            "from_date": from_date,
            "to_date": to_date,
            "leave_days": leave_days,
            "emp_fk": card_no,
            "leave_type_id": leave_type_id,
            "reason": reason,
            "emp_name": emp_name,
            "compc": compc,
            "brnch": brnch
        })

        conn.commit()
        return {"status": "success"}

    except Exception as e:
        conn.rollback()
        return {"status": "error", "message": str(e)}

    finally:
        cursor.close()
        conn.close()


# Attendance functions moved to repositories/attendance_repository.py


# ===============================
# GET LEAVE STATUS
# ===============================

def get_leave_status(card_no: str):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT
                ENTRY_DATE      AS entry_date,
                LEAVE_TYPE_FK   AS leave_type,
                LEAVE_DATE_FROM AS from_date,
                LEAVE_DATE_TO   AS to_date,
                APPROVAL_STATUS AS status
            FROM LEAVE_APPLICATION
            WHERE EMP_FK = :card
            ORDER BY ENTRY_DATE DESC
        """, {"card": card_no})

        rows = cursor.fetchall()
        columns = [col[0].lower() for col in cursor.description]
        result = [dict(zip(columns, r)) for r in rows]

        # Serialize Oracle date objects to ISO string
        for row in result:
            for key in ('from_date', 'to_date', 'entry_date'):
                if row.get(key) and hasattr(row[key], 'strftime'):
                    row[key] = row[key].strftime('%Y-%m-%d')

        return result

    finally:
        cursor.close()
        conn.close()


# ===============================
# UPDATE PASSWORD
# ===============================

def update_password(card_no: str, new_hash: str):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE EMPLOYEE
            SET USER_PASWD = :hash
            WHERE card_no = :card
        """, {"hash": new_hash, "card": card_no})

        conn.commit()

        return {"status": "success", "message": "Password updated"}

    except Exception as e:
        conn.rollback()
        return {"status": "error", "message": str(e)}

    finally:
        cursor.close()
        conn.close()



# Face attendance, report, and summary functions moved to repositories/attendance_repository.py