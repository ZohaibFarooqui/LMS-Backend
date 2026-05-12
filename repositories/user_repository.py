import hashlib

from core.database import get_connection
from datetime import datetime


# ===============================
# USER SECURITY RIGHTS
# ===============================

def get_user_rights(mobile: str, empcode: str = "") -> dict:
    """Return SEC_USERNAME company/branch rights for the given employee."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        usrid = None
        if mobile:
            m = str(mobile).strip()
            m_w   = ('0' + m) if not m.startswith('0') else m
            m_no0 = m[1:]     if m.startswith('0')     else m
            cur.execute("""
                SELECT USRID FROM SEC_USERNAME
                WHERE TO_CHAR(MOBILE) IN (:m1, :m2, :m3) AND STATS = 'E'
                FETCH FIRST 1 ROWS ONLY
            """, {"m1": m, "m2": m_w, "m3": m_no0})
            row = cur.fetchone()
            if row:
                usrid = str(row[0])
        if not usrid and empcode:
            cur.execute("""
                SELECT USRID FROM SEC_USERNAME
                WHERE ECODE = :ec AND STATS = 'E'
                FETCH FIRST 1 ROWS ONLY
            """, {"ec": str(empcode).strip()})
            row = cur.fetchone()
            if row:
                usrid = str(row[0])
        if not usrid:
            return {"usrid": None, "allowed_companies": [], "allowed_branches": [],
                    "company_list": [], "branch_list": []}

        cur.execute("""
            SELECT sc.COMPC, NVL(ci.DESCR, TO_CHAR(sc.COMPC))
            FROM SEC_USERCMPN sc
            LEFT JOIN COMPANY_INFO ci ON ci.COMPC = sc.COMPC
            WHERE sc.USRID = :usrid ORDER BY sc.COMPC
        """, {"usrid": usrid})
        cmp_rows     = cur.fetchall()
        companies    = [str(r[0]) for r in cmp_rows]
        company_list = [{"code": str(r[0]), "name": str(r[1] or r[0])} for r in cmp_rows]

        cur.execute("""
            SELECT sb.BRNCH, NVL(cl.DESCR, TO_CHAR(sb.BRNCH))
            FROM SEC_USERBRCH sb
            LEFT JOIN COM_LOCATION cl ON TO_CHAR(cl.LCODE) = TO_CHAR(sb.BRNCH)
            WHERE sb.USRID = :usrid2 ORDER BY sb.BRNCH
        """, {"usrid2": usrid})
        brn_rows    = cur.fetchall()
        branches    = [str(r[0]) for r in brn_rows]
        branch_list = [{"code": str(r[0]), "name": str(r[1] or r[0])} for r in brn_rows]

        return {"usrid": usrid, "allowed_companies": companies, "allowed_branches": branches,
                "company_list": company_list, "branch_list": branch_list}
    except Exception as e:
        print(f"[RIGHTS] Error: {e}")
        return {"usrid": None, "allowed_companies": [], "allowed_branches": [],
                "company_list": [], "branch_list": []}
    finally:
        cur.close()
        conn.close()


# ===============================
# AUTH — TWO-STEP LOGIN
# ===============================

def authenticate_user(username: str, password: str) -> dict | None:
    """Two-step login: SEC_USERNAME (MD5) first, then HR_EMP_MASTER (plain text)."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        m        = str(username).strip()
        m_with0  = ('0' + m) if not m.startswith('0') else m
        m_no0    = m[1:]     if m.startswith('0')     else m

        # ── STEP 1: SEC_USERNAME (ERP HR admin) ──────────────────────
        # Fetch raw PASWD first (no decryption) so ORA-28817 on one user
        # doesn't kill the entire lookup. Decrypt separately afterwards.
        sec_row = None
        try:
            cur.execute("""
                SELECT USRID, DESCR, PASWD, MOBILE, ECODE
                FROM SEC_USERNAME
                WHERE TO_CHAR(MOBILE) IN (:m1, :m2, :m3) AND STATS = 'E'
                FETCH FIRST 1 ROWS ONLY
            """, {"m1": m, "m2": m_with0, "m3": m_no0})
            sec_row = cur.fetchone()
            if not sec_row:
                cur.execute("""
                    SELECT USRID, DESCR, PASWD, MOBILE, ECODE
                    FROM SEC_USERNAME WHERE ECODE = :ec AND STATS = 'E'
                    FETCH FIRST 1 ROWS ONLY
                """, {"ec": m})
                sec_row = cur.fetchone()
        except Exception as e:
            print(f"[AUTH] SEC_USERNAME query failed: {e}")

        sec_authenticated = False
        if sec_row:
            usrid, descr, raw_paswd, sec_mobile, ecode = sec_row
            # Try to decrypt; fall back to raw comparison if decryption fails
            stored_paswd = None
            try:
                cur.execute("SELECT datacrypt.decryptdata(:p) FROM DUAL", {"p": raw_paswd})
                dec_row = cur.fetchone()
                stored_paswd = str(dec_row[0]).strip() if dec_row and dec_row[0] else None
            except Exception as e:
                print(f"[AUTH] datacrypt.decryptdata failed for USRID={usrid}: {e}")
                stored_paswd = str(raw_paswd or "").strip()

            if (stored_paswd or "").strip() == (password or "").strip():
                sec_authenticated = True
            else:
                print(f"[AUTH] SEC_USERNAME found but password mismatch for {username}, trying HR_EMP_MASTER")

        if sec_authenticated:
            # SEC_USERNAME user - HR Admin with access to company/branch management
            usrid_numeric = usrid  # Keep as numeric for database queries
            emp_name = str(descr or "").strip()  # Use DESCR from SEC_USERNAME
            empcode = str(ecode or "").strip()
            card_no = None
            has_employee_features = False  # Will be set to True only if in HR_EMP_MASTER

            # Check if SEC_USERNAME user exists in HR_EMP_MASTER (necessary for employee features)
            # First try by EMPCODE
            if empcode:
                cur.execute("""
                    SELECT TO_CHAR(e.CARD_NO), h.NAME, h.EMPCODE
                    FROM HR_EMP_MASTER h
                    LEFT JOIN EMPLOYEE e ON e.EMPCODE = h.EMPCODE
                    WHERE h.EMPCODE = :ec
                    FETCH FIRST 1 ROWS ONLY
                """, {"ec": empcode})
                row = cur.fetchone()
                if row:
                    card_no = str(row[0]) if row[0] else None
                    # Keep emp_name from SEC_USERNAME DESCR, don't override
                    has_employee_features = True

            # If not found by EMPCODE, try by mobile in HR_EMP_MASTER
            if not has_employee_features and sec_mobile:
                mv = str(sec_mobile).strip()
                mv_w = ('0' + mv) if not mv.startswith('0') else mv
                mv_no0 = mv[1:] if mv.startswith('0') else mv
                cur.execute("""
                    SELECT TO_CHAR(e.CARD_NO), h.NAME, h.EMPCODE
                    FROM HR_EMP_MASTER h
                    LEFT JOIN EMPLOYEE e ON e.EMPCODE = h.EMPCODE
                    WHERE h."MOBILE#" IN (:mv1, :mv2, :mv3)
                    FETCH FIRST 1 ROWS ONLY
                """, {"mv1": mv, "mv2": mv_w, "mv3": mv_no0})
                row = cur.fetchone()
                if row:
                    card_no = str(row[0]) if row[0] else None
                    has_employee_features = True
                    if not empcode:
                        empcode = str(row[2] or "").strip()

            # Get company and branch access rights from SEC_USERCMPN and SEC_USERBRCH
            # Use a fresh cursor to ensure clean state
            companies = []
            company_list = []
            branches = []
            branch_list = []
            
            try:
                cur2 = conn.cursor()
                try:
                    cur2.execute("""
                        SELECT sc.COMPC, NVL(ci.DESCR, TO_CHAR(sc.COMPC))
                        FROM SEC_USERCMPN sc
                        LEFT JOIN COMPANY_INFO ci ON ci.COMPC = sc.COMPC
                        WHERE sc.USRID = :usrid 
                        ORDER BY sc.COMPC
                    """, {"usrid": usrid_numeric})
                    cmp_rows = cur2.fetchall()
                    companies = [str(r[0]) for r in cmp_rows]
                    company_list = [{"code": str(r[0]), "name": str(r[1] or r[0])} for r in cmp_rows]
                    print(f"[AUTH] SEC_USERCMPN query returned {len(cmp_rows)} companies for USRID={usrid_numeric}")
                finally:
                    cur2.close()
            except Exception as e:
                print(f"[AUTH] SEC_USERCMPN query failed for USRID={usrid_numeric}: {e}")
            
            try:
                cur3 = conn.cursor()
                try:
                    cur3.execute("""
                        SELECT sb.BRNCH, NVL(cl.DESCR, TO_CHAR(sb.BRNCH))
                        FROM SEC_USERBRCH sb
                        LEFT JOIN COM_LOCATION cl ON TO_CHAR(cl.LCODE) = TO_CHAR(sb.BRNCH)
                        WHERE sb.USRID = :usrid 
                        ORDER BY sb.BRNCH
                    """, {"usrid": usrid_numeric})
                    brn_rows = cur3.fetchall()
                    branches = [str(r[0]) for r in brn_rows]
                    branch_list = [{"code": str(r[0]), "name": str(r[1] or r[0])} for r in brn_rows]
                    print(f"[AUTH] SEC_USERBRCH query returned {len(brn_rows)} branches for USRID={usrid_numeric}")
                finally:
                    cur3.close()
            except Exception as e:
                print(f"[AUTH] SEC_USERBRCH query failed for USRID={usrid_numeric}: {e}")

            has_self_service = True
            print(f"[AUTH] SEC_USERNAME login: usrid={usrid_numeric}, card_no={card_no}, "
                  f"has_employee_features={has_employee_features}, companies={len(companies)}, branches={len(branches)}")
            return {
                "card_no": card_no or username,
                "user_paswd": None,
                "emp_name": emp_name,  # From SEC_USERNAME DESCR
                "hr_admin": "Y",  # Only SEC_USERNAME users are HR admins
                "face_registered": "N",
                "empcode": empcode,
                "allowed_companies": companies,
                "allowed_branches": branches,
                "company_list": company_list,
                "branch_list": branch_list,
                "has_self_service": has_self_service,
                "has_employee_features": has_employee_features,  # False if not in HR_EMP_MASTER
            }

        # ── STEP 2: HR_EMP_MASTER (normal employee) ───────────────────
        # Regular employees can only access their own data, NO HR admin features
        l     = username.strip()
        l_w0  = ('0' + l) if not l.startswith('0') else l
        l_no0 = l[1:]     if l.startswith('0')     else l
        try:
            cur.execute("""
                SELECT TO_CHAR(e.CARD_NO), h.USER_PASWD, h.NAME,
                       h.EMPCODE, h."ATDTCARD#"
                FROM HR_EMP_MASTER h
                LEFT JOIN EMPLOYEE e ON e.EMPCODE = h.EMPCODE
                WHERE h."MOBILE#" IN (:l1, :l2, :l3)
                   OR h."ATDTCARD#" = :l4 OR h.EMPCODE = :l5
            """, {"l1": l, "l2": l_w0, "l3": l_no0, "l4": l, "l5": l})
            row = cur.fetchone()
            if row:
                card_no      = str(row[0]) if row[0] else (str(row[4]) if row[4] else None)
                stored_paswd = (row[1] or "").strip()
                if stored_paswd and stored_paswd != password.strip():
                    print(f"[AUTH] HR_EMP_MASTER: password mismatch for {username}")
                    return None
                print(f"[AUTH] HR_EMP_MASTER login: card_no={card_no}, emp_name={row[2]}")
                return {
                    "card_no": card_no,
                    "user_paswd": row[1],
                    "emp_name": str(row[2] or "").strip(),
                    "hr_admin": "N",  # Never Y for HR_EMP_MASTER users
                    "face_registered": "N",
                    "empcode": str(row[3] or "").strip(),
                    "allowed_companies": [],  # No company rights
                    "allowed_branches": [],   # No branch rights
                    "company_list": [],
                    "branch_list": [],
                    "has_self_service": True,
                    "has_employee_features": True,  # Can access employee modules
                }
        except Exception as e:
            print(f"[AUTH] HR_EMP_MASTER query failed: {e}")

        # Fallback: EMPLOYEE table
        try:
            cur.execute("""
                SELECT TO_CHAR(CARD_NO), USER_PASWD FROM EMPLOYEE
                WHERE TO_CHAR(CARD_NO) = :e1 OR EMPCODE = :e2
            """, {"e1": l, "e2": l})
            row = cur.fetchone()
            if row:
                stored_paswd = (row[1] or "").strip()
                if stored_paswd and stored_paswd != password.strip():
                    return None
                return {
                    "card_no": str(row[0]) if row[0] else l,
                    "user_paswd": row[1],
                    "emp_name": "",
                    "hr_admin": "N",
                    "face_registered": "N",
                    "empcode": "",
                    "allowed_companies": [],
                    "allowed_branches": [],
                    "company_list": [],
                    "branch_list": [],
                    "has_self_service": True,
                    "has_employee_features": True,
                }
        except Exception as e:
            print(f"[AUTH] EMPLOYEE fallback failed: {e}")

        print(f"[AUTH] No match for '{username}'")
        return None
    finally:
        cur.close()
        conn.close()


# ===============================
# AUTH — LEGACY LOOKUP
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
                    "hr_admin": str(row[3] or "N").strip().upper(),
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
        # Use MAX() correlated subqueries instead of codename() to avoid ORA-01427
        # (codename uses SELECT..INTO which fails when the lookup table has duplicate rows).
        # ALL_LEAVE_BAL_V is also pulled as a subquery to prevent row-multiplication.
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
                (SELECT MAX(ci.DESCR) FROM COMPANY_INFO ci WHERE ci.COMPC = e.compc) compcnm,
                e.compc,
                e.brnch AS branch,
                (SELECT MAX(cl.DESCR) FROM COM_LOCATION cl WHERE cl.LCODE = e.brnch) brnchnm,
                e.hod1 AS hod,
                (SELECT MAX(h.NAME) FROM HR_EMP_MASTER h WHERE h.EMPCODE = TO_CHAR(e.hod1)) hod_nm,
                (SELECT SUM(b.balance) FROM ALL_LEAVE_BAL_V b WHERE b.card_no = e.card_no) balance
            FROM EMPLOYEE e
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
                e.sex AS gender,
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
                (SELECT MAX(ci.DESCR) FROM COMPANY_INFO ci WHERE ci.COMPC = e.compc) compcnm,
                e.brnch,
                (SELECT MAX(cl.DESCR) FROM COM_LOCATION cl WHERE cl.LCODE = e.brnch) brnchnm,
                e.hod1,
                (SELECT MAX(h2.NAME) FROM HR_EMP_MASTER h2 WHERE h2.EMPCODE = TO_CHAR(e.hod1)) hod1nm,
                e.hod2,
                (SELECT MAX(h3.NAME) FROM HR_EMP_MASTER h3 WHERE h3.EMPCODE = TO_CHAR(e.hod2)) hod2nm,
                h.EMPCODE AS emp_code,
                h.STATUS AS emp_status
            FROM EMPLOYEE e
            LEFT JOIN HR_EMP_MASTER h ON h.EMPCODE = e.EMPCODE
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

    # Validate leave balance before inserting
    try:
        cursor.execute("""
            SELECT balance FROM ALL_LEAVE_BAL_V
            WHERE card_no = :card AND leave_type = :lt
        """, {"card": card_no, "lt": leave_type_id})
        bal_row = cursor.fetchone()
        current_balance = float(bal_row[0]) if bal_row else 0
        if current_balance <= 0:
            cursor.close()
            conn.close()
            return {"status": "error", "message": "No remaining balance for this leave type."}
        if leave_days > current_balance:
            cursor.close()
            conn.close()
            return {
                "status": "error",
                "message": f"Insufficient balance. Available: {current_balance}, Requested: {leave_days}",
            }
    except Exception as e:
        print(f"[LEAVE] Balance check warning: {e}")
        # Continue if view doesn't exist — let insert proceed

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