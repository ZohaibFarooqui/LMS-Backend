"""HRMS repository — CRUD on HR_EMP_MASTER + HR dashboard queries."""

from core.database import get_connection


# ------------------------------------------------------------------
# NEXT EMPCODE — auto-increment
# ------------------------------------------------------------------

def get_next_empcode() -> str:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT MAX(TO_NUMBER(REGEXP_REPLACE(EMPCODE, '[^0-9]', '')))
            FROM HR_EMP_MASTER
            WHERE REGEXP_LIKE(EMPCODE, '^[0-9]+$')
        """)
        row = cursor.fetchone()
        max_val = int(row[0]) if row and row[0] else 0
        return str(max_val + 1)
    finally:
        cursor.close()
        conn.close()


# ------------------------------------------------------------------
# CREATE EMPLOYEE
# ------------------------------------------------------------------

def create_employee(data: dict) -> dict:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        empcode = get_next_empcode()
        cursor.execute("""
            INSERT INTO HR_EMP_MASTER (
                EMPCODE, NAME, FHNAME, "ATDTCARD#",
                SEX, DTOFBRTH, NICNO,
                DTOFAPPT, DEPT_NO, DESG_CD,
                "MOBILE#", EMAIL, ADDRESS,
                UNIT_ID, STATUS, USER_PASWD,
                HR_ADMIN, RPT_OFFICER, MARSTAT,
                GRADE_CD, RELIGION,
                HOD1, HOD2, HOD3,
                BASIC, GROSS, SHIFT, W_HOUR, BLDGRP, LOCATION
            ) VALUES (
                :empcode, :name, :fhname, :atdtcard,
                :sex, TO_DATE(:dtofbrth, 'YYYY-MM-DD'), :nicno,
                TO_DATE(:dtofappt, 'YYYY-MM-DD'), :dept_no, :desg_cd,
                :mobile, :email, :address,
                :unit_id, :status, :user_paswd,
                :hr_admin, :rpt_officer, :marstat,
                :grade_cd, :religion,
                :hod1, :hod2, :hod3,
                :basic, :gross, :shift, :w_hour, :bldgrp, :location
            )
        """, {
            "empcode": empcode,
            "name": data.get("name"),
            "fhname": data.get("fhname"),
            "atdtcard": data.get("atdtcard"),
            "sex": data.get("sex"),
            "dtofbrth": data.get("dtofbrth"),
            "nicno": data.get("nicno"),
            "dtofappt": data.get("dtofappt"),
            "dept_no": data.get("dept_no"),
            "desg_cd": data.get("desg_cd"),
            "mobile": data.get("mobile"),
            "email": data.get("email"),
            "address": data.get("address"),
            "unit_id": data.get("unit_id", 1),
            "status": data.get("status", "A"),
            "user_paswd": data.get("user_paswd"),
            "hr_admin": data.get("hr_admin", "N"),
            "rpt_officer": data.get("rpt_officer"),
            "marstat": data.get("marstat"),
            "grade_cd": data.get("grade_cd"),
            "religion": (data.get("religion") or "")[:4] or None,
            "hod1": data.get("hod1"),
            "hod2": data.get("hod2"),
            "hod3": data.get("hod3"),
            "basic": data.get("basic"),
            "gross": data.get("gross"),
            "shift": data.get("shift"),
            "w_hour": data.get("w_hour"),
            "bldgrp": data.get("bldgrp"),
            "location": data.get("location"),
        })
        conn.commit()
        return {"status": "success", "empcode": empcode}
    except Exception as e:
        conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        cursor.close()
        conn.close()


# ------------------------------------------------------------------
# GET EMPLOYEE BY EMPCODE
# ------------------------------------------------------------------

def get_employee_by_empcode(empcode: str) -> dict | None:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT
                EMPCODE, NAME, FHNAME, "ATDTCARD#",
                SEX,
                TO_CHAR(DTOFBRTH, 'YYYY-MM-DD') AS DTOFBRTH,
                NICNO,
                TO_CHAR(DTOFAPPT, 'YYYY-MM-DD') AS DTOFAPPT,
                DEPT_NO, DESG_CD,
                "MOBILE#", EMAIL, ADDRESS,
                UNIT_ID, STATUS, USER_PASWD,
                HR_ADMIN, RPT_OFFICER, MARSTAT,
                GRADE_CD, RELIGION,
                HOD1, HOD2, HOD3,
                BASIC, GROSS, SHIFT, W_HOUR,
                TRACK_LOCATION, TRACK_LOCATION_HR
            FROM HR_EMP_MASTER
            WHERE EMPCODE = :empcode
        """, {"empcode": empcode})
        row = cursor.fetchone()
        if not row:
            return None
        columns = [col[0].lower() for col in cursor.description]
        result = dict(zip(columns, row))
        result["atdtcard"] = result.pop("atdtcard#", None)
        result["mobile"] = result.pop("mobile#", None)
        return result
    finally:
        cursor.close()
        conn.close()


# ------------------------------------------------------------------
# UPDATE EMPLOYEE
# ------------------------------------------------------------------

def update_employee(empcode: str, data: dict) -> dict:
    conn = get_connection()
    cursor = conn.cursor()

    field_map = {
        "name": "NAME", "fhname": "FHNAME", "atdtcard": '"ATDTCARD#"',
        "sex": "SEX", "nicno": "NICNO", "dept_no": "DEPT_NO",
        "desg_cd": "DESG_CD", "mobile": '"MOBILE#"', "email": "EMAIL",
        "address": "ADDRESS", "unit_id": "UNIT_ID", "status": "STATUS",
        "user_paswd": "USER_PASWD", "hr_admin": "HR_ADMIN",
        "rpt_officer": "RPT_OFFICER", "marstat": "MARSTAT",
        "grade_cd": "GRADE_CD", "religion": "RELIGION",
        "hod1": "HOD1", "hod2": "HOD2", "hod3": "HOD3",
        "basic": "BASIC", "gross": "GROSS", "shift": "SHIFT",
        "w_hour": "W_HOUR", "bldgrp": "BLDGRP", "location": "LOCATION",
        "track_location": "TRACK_LOCATION", "track_location_hr": "TRACK_LOCATION_HR",
    }
    date_fields = {"dtofbrth": "DTOFBRTH", "dtofappt": "DTOFAPPT"}

    set_parts = []
    params = {"empcode": empcode}

    for key, col in field_map.items():
        if key in data and data[key] is not None:
            set_parts.append(f"{col} = :{key}")
            val = data[key]
            if key == "religion":
                val = (str(val) or "")[:4] or None
                if val is None:
                    set_parts.pop()
                    continue
            params[key] = val

    for key, col in date_fields.items():
        if key in data and data[key] is not None:
            set_parts.append(f"{col} = TO_DATE(:{key}, 'YYYY-MM-DD')")
            params[key] = data[key]

    if not set_parts:
        return {"status": "error", "message": "No fields to update"}

    sql = f"UPDATE HR_EMP_MASTER SET {', '.join(set_parts)} WHERE EMPCODE = :empcode"
    try:
        cursor.execute(sql, params)
        conn.commit()
        if cursor.rowcount == 0:
            return {"status": "error", "message": "Employee not found"}
        return {"status": "success", "message": "Employee updated successfully"}
    except Exception as e:
        conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        cursor.close()
        conn.close()


# ------------------------------------------------------------------
# SEARCH EMPLOYEES
# ------------------------------------------------------------------

def search_employees_hrms(query: str, allowed_companies=None, allowed_branches=None) -> list:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        search = f"%{query.upper()}%"
        params = {"q": search}

        filter_parts = []
        if allowed_companies:
            nums = [int(c) for c in allowed_companies if str(c).strip().isdigit()]
            if nums:
                ph = ", ".join(f":cmpf{i}" for i in range(len(nums)))
                filter_parts.append(f"TO_NUMBER(h.UNIT_ID) IN ({ph})")
                for i, n in enumerate(nums):
                    params[f"cmpf{i}"] = n
        if allowed_branches:
            nums = [int(b) for b in allowed_branches if str(b).strip().isdigit()]
            if nums:
                ph = ", ".join(f":brnf{i}" for i in range(len(nums)))
                filter_parts.append(f"TO_NUMBER(h.LOCATION) IN ({ph})")
                for i, n in enumerate(nums):
                    params[f"brnf{i}"] = n

        filter_sql = (" AND " + " AND ".join(filter_parts)) if filter_parts else ""

        cursor.execute("""
            SELECT
                h.EMPCODE, h.NAME, h.FHNAME, h."ATDTCARD#",
                h.DEPT_NO, h.DESG_CD, h."MOBILE#", h.EMAIL,
                h.STATUS, h.HR_ADMIN, h.UNIT_ID,
                TO_CHAR(e.CARD_NO) AS CARD_NO,
                h.SEX, h.LOCATION,
                h.TRACK_LOCATION, h.TRACK_LOCATION_HR
            FROM HR_EMP_MASTER h
            LEFT JOIN EMPLOYEE e ON e.EMPCODE = h.EMPCODE
            WHERE (UPPER(h.NAME) LIKE :q
               OR h.EMPCODE LIKE :q
               OR h."ATDTCARD#" LIKE :q
               OR h."MOBILE#" LIKE :q)""" + filter_sql + """
            ORDER BY h.NAME
            FETCH FIRST 50 ROWS ONLY
        """, params)
        rows = cursor.fetchall()
        columns = [col[0].lower() for col in cursor.description]
        results = []
        for r in rows:
            d = dict(zip(columns, r))
            d["atdtcard"] = d.pop("atdtcard#", None)
            d["mobile"] = d.pop("mobile#", None)
            results.append(d)
        return results
    finally:
        cursor.close()
        conn.close()


def list_employees_hrms(status: str = None, allowed_companies=None, allowed_branches=None) -> list:
    """Return all employees, optionally filtered by status (A/I/L) and company/branch."""
    conn = get_connection()
    cursor = conn.cursor()
    _BASE_SELECT = """
                SELECT
                    h.EMPCODE, h.NAME, h.FHNAME, h."ATDTCARD#",
                    h.DEPT_NO, h.DESG_CD, h."MOBILE#", h.EMAIL,
                    h.STATUS, h.HR_ADMIN, h.UNIT_ID,
                    TO_CHAR(e.CARD_NO) AS CARD_NO,
                    h.SEX, h.LOCATION,
                    h.TRACK_LOCATION, h.TRACK_LOCATION_HR
                FROM HR_EMP_MASTER h
                LEFT JOIN EMPLOYEE e ON e.EMPCODE = h.EMPCODE
            """
    try:
        params = {}
        filter_parts = []
        if allowed_companies:
            nums = [int(c) for c in allowed_companies if str(c).strip().isdigit()]
            if nums:
                ph = ", ".join(f":cmpf{i}" for i in range(len(nums)))
                filter_parts.append(f"TO_NUMBER(h.UNIT_ID) IN ({ph})")
                for i, n in enumerate(nums):
                    params[f"cmpf{i}"] = n
        if allowed_branches:
            nums = [int(b) for b in allowed_branches if str(b).strip().isdigit()]
            if nums:
                ph = ", ".join(f":brnf{i}" for i in range(len(nums)))
                filter_parts.append(f"TO_NUMBER(h.LOCATION) IN ({ph})")
                for i, n in enumerate(nums):
                    params[f"brnf{i}"] = n

        filter_sql = (" AND " + " AND ".join(filter_parts)) if filter_parts else ""

        if status == "I":
            cursor.execute(_BASE_SELECT + f"""
                WHERE h.STATUS IN ('I', 'D'){filter_sql}
                ORDER BY h.NAME FETCH FIRST 2000 ROWS ONLY
            """, params)
        elif status in ("A", "L"):
            cursor.execute(_BASE_SELECT + f"""
                WHERE h.STATUS = :status{filter_sql}
                ORDER BY h.NAME FETCH FIRST 2000 ROWS ONLY
            """, {"status": status, **params})
        else:
            if filter_parts:
                cursor.execute(_BASE_SELECT + f"""
                    WHERE {" AND ".join(filter_parts)}
                    ORDER BY h.NAME FETCH FIRST 2000 ROWS ONLY
                """, params)
            else:
                cursor.execute(_BASE_SELECT + """
                    ORDER BY h.NAME FETCH FIRST 2000 ROWS ONLY
                """)
        rows = cursor.fetchall()
        columns = [col[0].lower() for col in cursor.description]
        results = []
        for r in rows:
            d = dict(zip(columns, r))
            d["atdtcard"] = d.pop("atdtcard#", None)
            d["mobile"] = d.pop("mobile#", None)
            results.append(d)
        return results
    finally:
        cursor.close()
        conn.close()


# ------------------------------------------------------------------
# HR DASHBOARD — today's attendance overview across all employees
# ------------------------------------------------------------------

def _coerce_num(val):
    try:
        return int(val)
    except (ValueError, TypeError):
        return val


def _emp_filter_attempts(compc=None, brnch=None, alias="h"):
    """Return a list of (sql_fragment, params) tuples to try in order of decreasing
    restrictiveness. Each fragment is ' AND <conditions>' or '' for unfiltered."""
    attempts = []
    if compc and brnch:
        attempts.append((
            f" AND {alias}.UNIT_ID = :ecompc AND {alias}.LOCATION = :ebrnch",
            {"ecompc": _coerce_num(compc), "ebrnch": _coerce_num(brnch)},
        ))
    if compc:
        attempts.append((
            f" AND {alias}.UNIT_ID = :ecompc",
            {"ecompc": _coerce_num(compc)},
        ))
    if brnch:
        attempts.append((
            f" AND {alias}.LOCATION = :ebrnch",
            {"ebrnch": _coerce_num(brnch)},
        ))
    attempts.append(("", {}))
    return attempts


def _execute_with_emp_filter(cursor, sql_template, compc, brnch, alias="h", extra_params=None):
    """Execute sql_template (contains '{filter}' marker) trying COMPC+BRNCH, then
    COMPC alone, then BRNCH alone, then unfiltered. Raises if no attempt succeeds."""
    extra_params = extra_params or {}
    last_err = None
    for frag, params in _emp_filter_attempts(compc, brnch, alias):
        try:
            cursor.execute(sql_template.replace("{filter}", frag), {**extra_params, **params})
            return
        except Exception as e:
            if "ORA-00904" in str(e):
                last_err = str(e).splitlines()[0][:100]
                continue
            raise
    raise RuntimeError(f"All HR_EMP_MASTER filter attempts failed: {last_err}")


# Backwards-compat wrapper for code that still calls _emp_filter_sql; returns the
# most-restrictive fragment + params (the OLD behavior — single attempt only).
def _emp_filter_sql(compc=None, brnch=None, alias="h"):
    attempts = _emp_filter_attempts(compc, brnch, alias)
    return attempts[0]


def get_hr_dashboard_stats(qdate: str = None, compc=None, brnch=None) -> dict:
    """Get aggregated stats for the HR dashboard overview. qdate format: YYYY-MM-DD."""
    import re
    if qdate and re.match(r'^\d{4}-\d{2}-\d{2}$', qdate):
        td = f"TRUNC(DATE '{qdate}')"
        yd = f"TRUNC(DATE '{qdate}') - 1"
    else:
        td = "TRUNC(SYSDATE)"
        yd = "TRUNC(SYSDATE) - 1"

    conn = get_connection()
    cursor = conn.cursor()
    try:
        # Total active employees (filtered progressively by selected company/branch)
        total_employees = 0
        try:
            _execute_with_emp_filter(
                cursor,
                "SELECT COUNT(*) FROM HR_EMP_MASTER h WHERE (h.STATUS = 'A' OR h.STATUS IS NULL){filter}",
                compc, brnch,
            )
            total_employees = cursor.fetchone()[0] or 0
        except Exception as e:
            print(f"[HR_DASHBOARD] Total count failed: {e}")

        # Today's attendance from DUTY_ROSTER
        present = 0
        absent = 0
        late = 0
        incomplete = 0
        on_leave = 0

        # Present count: union of DUTY_ROSTER + ATTENDANCE_RECORDS to catch all check-ins
        try:
            cursor.execute("""
                SELECT COUNT(DISTINCT card_no) FROM (
                    SELECT TO_CHAR(CARD_NO) AS card_no
                    FROM DUTY_ROSTER
                    WHERE TRUNC(ROSTER_DATE) = {td} AND IN_TIME IS NOT NULL
                    UNION
                    SELECT TO_CHAR(CARD_NO) AS card_no
                    FROM ATTENDANCE_RECORDS
                    WHERE TRUNC(ATTENDANCE_DATE) = {td} AND ENTRY_TIME IS NOT NULL
                )
            """.format(td=td))
            present = int(cursor.fetchone()[0] or 0)
        except Exception as e:
            print(f"[HR_DASHBOARD] Present count query failed: {e}")

        # Late, on_leave, incomplete from DUTY_ROSTER; fallback incomplete from ATTENDANCE_RECORDS
        try:
            cursor.execute("""
                SELECT
                    SUM(CASE WHEN IN_TIME IS NOT NULL AND OUT_TIME IS NULL THEN 1 ELSE 0 END),
                    SUM(CASE WHEN NVL(LATE_HRS, 0) > 0 OR NVL(LATE_MNT, 0) > 0 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN UPPER(STATUS) LIKE '%LEAVE%' THEN 1 ELSE 0 END)
                FROM DUTY_ROSTER
                WHERE TRUNC(ROSTER_DATE) = {td}
            """.format(td=td))
            row = cursor.fetchone()
            if row:
                incomplete = int(row[0] or 0)
                late = int(row[1] or 0)
                on_leave = int(row[2] or 0)
        except Exception as e:
            print(f"[HR_DASHBOARD] DUTY_ROSTER stats failed: {e}")
            try:
                cursor.execute("""
                    SELECT SUM(CASE WHEN ENTRY_TIME IS NOT NULL AND EXIT_TIME IS NULL THEN 1 ELSE 0 END)
                    FROM ATTENDANCE_RECORDS
                    WHERE TRUNC(ATTENDANCE_DATE) = {td}
                """.format(td=td))
                r = cursor.fetchone()
                if r:
                    incomplete = int(r[0] or 0)
            except Exception:
                pass

        absent = max(total_employees - present - on_leave, 0)

        # Department-wise breakdown (filtered progressively)
        dept_breakdown = []
        try:
            _execute_with_emp_filter(cursor, f"""
                SELECT
                    NVL(dep.DEPT_NAME, NVL(TO_CHAR(h.DEPT_NO), 'Unknown')) AS dept,
                    COUNT(*) AS total,
                    SUM(CASE WHEN d.IN_TIME IS NOT NULL OR ar.card_no IS NOT NULL THEN 1 ELSE 0 END) AS present
                FROM HR_EMP_MASTER h
                LEFT JOIN HR_DEPT dep ON dep.DEPT_NO = h.DEPT_NO
                LEFT JOIN EMPLOYEE e ON e.EMPCODE = h.EMPCODE
                LEFT JOIN DUTY_ROSTER d
                    ON TO_CHAR(d.CARD_NO) = TO_CHAR(e.CARD_NO)
                    AND TRUNC(d.ROSTER_DATE) = {td}
                LEFT JOIN (
                    SELECT DISTINCT TO_CHAR(CARD_NO) AS card_no
                    FROM ATTENDANCE_RECORDS
                    WHERE TRUNC(ATTENDANCE_DATE) = {td} AND ENTRY_TIME IS NOT NULL
                ) ar ON ar.card_no = TO_CHAR(e.CARD_NO)
                WHERE (h.STATUS = 'A' OR h.STATUS IS NULL){{filter}}
                GROUP BY NVL(dep.DEPT_NAME, NVL(TO_CHAR(h.DEPT_NO), 'Unknown'))
                ORDER BY COUNT(*) DESC
                FETCH FIRST 10 ROWS ONLY
            """, compc, brnch)
            rows = cursor.fetchall()
            for r in rows:
                dept_breakdown.append({
                    "department": r[0] or "Unknown",
                    "total": int(r[1] or 0),
                    "present": int(r[2] or 0),
                })
        except Exception as e:
            print(f"[HR_DASHBOARD] Department breakdown failed: {e}")

        # Recent hires (last 30 days) — filtered progressively
        recent_hires = 0
        try:
            _execute_with_emp_filter(
                cursor,
                "SELECT COUNT(*) FROM HR_EMP_MASTER h WHERE h.DTOFAPPT >= SYSDATE - 30{filter}",
                compc, brnch,
            )
            recent_hires = int(cursor.fetchone()[0] or 0)
        except Exception:
            pass

        # Yesterday's stats for delta indicators
        yesterday_present = 0
        yesterday_on_leave = 0
        yesterday_absent = 0
        try:
            cursor.execute("""
                SELECT
                    SUM(CASE WHEN IN_TIME IS NOT NULL THEN 1 ELSE 0 END),
                    SUM(CASE WHEN UPPER(STATUS) LIKE '%LEAVE%' THEN 1 ELSE 0 END)
                FROM DUTY_ROSTER
                WHERE TRUNC(ROSTER_DATE) = {yd}
            """.format(yd=yd))
            yrow = cursor.fetchone()
            if yrow:
                yesterday_present = int(yrow[0] or 0)
                yesterday_on_leave = int(yrow[1] or 0)
                yesterday_absent = max(total_employees - yesterday_present - yesterday_on_leave, 0)
        except Exception as e:
            print(f"[HR_DASHBOARD] Yesterday stats failed: {e}")

        # Upcoming birthdays (next 14 days) — filtered progressively
        upcoming_birthdays = []
        try:
            _execute_with_emp_filter(cursor, """
                SELECT h.NAME,
                    TO_CHAR(h.DTOFBRTH, 'DD Mon') AS bday,
                    NVL(dep.DEPT_NAME, 'N/A') AS dept,
                    MOD(TO_NUMBER(TO_CHAR(h.DTOFBRTH, 'DDD'))
                        - TO_NUMBER(TO_CHAR(SYSDATE, 'DDD')) + 365, 365) AS days_until
                FROM HR_EMP_MASTER h
                LEFT JOIN HR_DEPT dep ON dep.DEPT_NO = h.DEPT_NO
                WHERE (h.STATUS = 'A' OR h.STATUS IS NULL)
                  AND h.DTOFBRTH IS NOT NULL
                  AND MOD(TO_NUMBER(TO_CHAR(h.DTOFBRTH, 'DDD'))
                      - TO_NUMBER(TO_CHAR(SYSDATE, 'DDD')) + 365, 365) <= 14{filter}
                ORDER BY days_until
                FETCH FIRST 8 ROWS ONLY
            """, compc, brnch)
            for r in cursor.fetchall():
                upcoming_birthdays.append({
                    "name": r[0] or "Unknown",
                    "date": r[1] or "",
                    "dept": r[2] or "N/A",
                    "days_until": int(r[3] or 0),
                })
        except Exception as e:
            print(f"[HR_DASHBOARD] Birthdays failed: {e}")

        # Upcoming work anniversaries (next 14 days) — filtered progressively
        upcoming_anniversaries = []
        try:
            _execute_with_emp_filter(cursor, """
                SELECT h.NAME,
                    TO_CHAR(h.DTOFAPPT, 'DD Mon') AS ann_date,
                    TO_NUMBER(TO_CHAR(SYSDATE, 'YYYY'))
                        - TO_NUMBER(TO_CHAR(h.DTOFAPPT, 'YYYY')) AS years,
                    NVL(dep.DEPT_NAME, 'N/A') AS dept,
                    MOD(TO_NUMBER(TO_CHAR(h.DTOFAPPT, 'DDD'))
                        - TO_NUMBER(TO_CHAR(SYSDATE, 'DDD')) + 365, 365) AS days_until
                FROM HR_EMP_MASTER h
                LEFT JOIN HR_DEPT dep ON dep.DEPT_NO = h.DEPT_NO
                WHERE (h.STATUS = 'A' OR h.STATUS IS NULL)
                  AND h.DTOFAPPT IS NOT NULL
                  AND MOD(TO_NUMBER(TO_CHAR(h.DTOFAPPT, 'DDD'))
                      - TO_NUMBER(TO_CHAR(SYSDATE, 'DDD')) + 365, 365) <= 14
                  AND TO_NUMBER(TO_CHAR(SYSDATE, 'YYYY'))
                      > TO_NUMBER(TO_CHAR(h.DTOFAPPT, 'YYYY')){filter}
                ORDER BY days_until
                FETCH FIRST 8 ROWS ONLY
            """, compc, brnch)
            for r in cursor.fetchall():
                upcoming_anniversaries.append({
                    "name": r[0] or "Unknown",
                    "date": r[1] or "",
                    "years": int(r[2] or 1),
                    "dept": r[3] or "N/A",
                    "days_until": int(r[4] or 0),
                })
        except Exception as e:
            print(f"[HR_DASHBOARD] Anniversaries failed: {e}")

        # Upcoming leave requests (next 30 days)
        upcoming_leaves = []
        try:
            cursor.execute("""
                SELECT h.NAME,
                    la.LEAVE_DATE_FROM, la.LEAVE_DATE_TO,
                    la.LEAVE_TYPE_FK,
                    NVL(la.APPROVAL_STATUS, 'PENDING') AS status,
                    la.LEAVE_DAYS,
                    NVL(dep.DEPT_NAME, 'N/A') AS dept
                FROM LEAVE_APPLICATION la
                LEFT JOIN EMPLOYEE e ON TO_CHAR(e.CARD_NO) = TO_CHAR(la.EMP_FK)
                LEFT JOIN HR_EMP_MASTER h ON h.EMPCODE = e.EMPCODE
                LEFT JOIN HR_DEPT dep ON dep.DEPT_NO = h.DEPT_NO
                WHERE la.LEAVE_DATE_FROM >= TRUNC(SYSDATE)
                  AND la.LEAVE_DATE_FROM <= TRUNC(SYSDATE) + 30
                ORDER BY la.LEAVE_DATE_FROM
                FETCH FIRST 10 ROWS ONLY
            """)
            for r in cursor.fetchall():
                from_d = r[1]
                to_d = r[2]
                upcoming_leaves.append({
                    "name": r[0] or "Unknown",
                    "from_date": from_d.strftime('%Y-%m-%d') if from_d and hasattr(from_d, 'strftime') else str(from_d or ""),
                    "to_date": to_d.strftime('%Y-%m-%d') if to_d and hasattr(to_d, 'strftime') else str(to_d or ""),
                    "leave_type": int(r[3] or 0),
                    "status": r[4] or "PENDING",
                    "days": int(r[5] or 1),
                    "dept": r[6] or "N/A",
                })
        except Exception as e:
            print(f"[HR_DASHBOARD] Upcoming leaves failed: {e}")

        # Shift-wise attendance
        shift_wise = []
        try:
            cursor.execute("""
                SELECT NVL(SHIFT, 'Day') AS shift_name,
                    SUM(CASE WHEN IN_TIME IS NOT NULL THEN 1 ELSE 0 END) AS present,
                    COUNT(*) AS total
                FROM DUTY_ROSTER
                WHERE TRUNC(ROSTER_DATE) = {td}
                GROUP BY NVL(SHIFT, 'Day')
                ORDER BY total DESC
            """.format(td=td))
            for r in cursor.fetchall():
                total_s = int(r[2] or 1)
                present_s = int(r[1] or 0)
                shift_wise.append({
                    "shift": r[0] or "Day",
                    "present": present_s,
                    "total": total_s,
                    "pct": round((present_s / total_s * 100) if total_s > 0 else 0, 1),
                })
        except Exception as e:
            print(f"[HR_DASHBOARD] Shift-wise failed: {e}")

        # Top absence/leave reasons this year
        top_reasons = []
        try:
            cursor.execute("""
                SELECT NVL(lt.LEAVE_DESC, 'Type ' || TO_CHAR(la.LEAVE_TYPE_FK)) AS reason,
                    COUNT(*) AS cnt
                FROM LEAVE_APPLICATION la
                LEFT JOIN LEAVE_TYPE lt ON lt.LEAVE_TYPE = la.LEAVE_TYPE_FK
                WHERE la.LEAVE_DATE_FROM >= TRUNC(SYSDATE, 'YYYY')
                GROUP BY NVL(lt.LEAVE_DESC, 'Type ' || TO_CHAR(la.LEAVE_TYPE_FK))
                ORDER BY cnt DESC
                FETCH FIRST 5 ROWS ONLY
            """)
            for r in cursor.fetchall():
                top_reasons.append({
                    "reason": r[0] or "Other",
                    "count": int(r[1] or 0),
                })
        except Exception as e:
            print(f"[HR_DASHBOARD] Top reasons failed: {e}")

        # Inactive count for turnover computation
        inactive_count = 0
        try:
            cursor.execute("""
                SELECT COUNT(*) FROM HR_EMP_MASTER WHERE STATUS IN ('I', 'D')
            """)
            inactive_count = int(cursor.fetchone()[0] or 0)
        except Exception:
            pass

        total_ever = total_employees + inactive_count
        turnover_ytd = round((inactive_count / total_ever * 100) if total_ever > 0 else 0, 1)

        return {
            "total_employees": total_employees,
            "present_today": present,
            "absent_today": absent,
            "late_today": late,
            "incomplete_today": incomplete,
            "on_leave_today": on_leave,
            "recent_hires": recent_hires,
            "department_breakdown": dept_breakdown,
            "yesterday_present": yesterday_present,
            "yesterday_absent": yesterday_absent,
            "yesterday_on_leave": yesterday_on_leave,
            "upcoming_birthdays": upcoming_birthdays,
            "upcoming_anniversaries": upcoming_anniversaries,
            "upcoming_leaves": upcoming_leaves,
            "shift_wise": shift_wise,
            "top_reasons": top_reasons,
            "turnover_ytd": turnover_ytd,
        }

    finally:
        cursor.close()
        conn.close()


# ------------------------------------------------------------------
# HR ANALYTICS — chart data for the enhanced dashboard
# ------------------------------------------------------------------

def get_hr_analytics(qdate: str = None, compc=None, brnch=None) -> dict:
    """Return chart-ready analytics: daily status (30d), monthly trends (6m), KPIs."""
    import re
    if qdate and re.match(r'^\d{4}-\d{2}-\d{2}$', qdate):
        td = f"TRUNC(DATE '{qdate}')"
    else:
        td = "TRUNC(SYSDATE)"

    conn = get_connection()
    cursor = conn.cursor()
    try:
        late_logins = 0
        early_logins = 0
        overtime_hours = 0.0
        avg_work_hrs = 0.0
        unapproved_leaves = 0
        attendance_pct = 0.0

        try:
            cursor.execute("""
                SELECT
                    SUM(CASE WHEN NVL(LATE_HRS,0) > 0 OR NVL(LATE_MNT,0) > 0 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN IN_TIME IS NOT NULL AND NVL(LATE_HRS,0) = 0 AND NVL(LATE_MNT,0) = 0 THEN 1 ELSE 0 END),
                    NVL(SUM(NVL(OT_HRS,0) + NVL(OT_MNT,0)/60.0), 0),
                    NVL(AVG(CASE WHEN IN_TIME IS NOT NULL AND OUT_TIME IS NOT NULL
                        THEN NVL(W_HRS,0) + NVL(W_MNT,0)/60.0 END), 0)
                FROM DUTY_ROSTER
                WHERE TRUNC(ROSTER_DATE) = {td}
            """.format(td=td))
            row = cursor.fetchone()
            if row:
                late_logins = int(row[0] or 0)
                early_logins = int(row[1] or 0)
                overtime_hours = round(float(row[2] or 0), 1)
                avg_work_hrs = round(float(row[3] or 0), 1)
        except Exception as e:
            print(f"[HR_ANALYTICS] KPI query failed: {e}")

        try:
            cursor.execute("""
                SELECT COUNT(*) FROM LEAVE_APPLICATION
                WHERE STATUS IS NULL OR UPPER(STATUS) IN ('PENDING', 'P', '0')
            """)
            unapproved_leaves = int(cursor.fetchone()[0] or 0)
        except Exception:
            pass

        try:
            cursor.execute("SELECT COUNT(*) FROM HR_EMP_MASTER WHERE STATUS = 'A' OR STATUS IS NULL")
            total_active = int(cursor.fetchone()[0] or 0)
            cursor.execute("""
                SELECT COUNT(DISTINCT card_no) FROM (
                    SELECT TO_CHAR(CARD_NO) AS card_no FROM DUTY_ROSTER
                    WHERE TRUNC(ROSTER_DATE) = {td} AND IN_TIME IS NOT NULL
                    UNION
                    SELECT TO_CHAR(CARD_NO) AS card_no FROM ATTENDANCE_RECORDS
                    WHERE TRUNC(ATTENDANCE_DATE) = {td} AND ENTRY_TIME IS NOT NULL
                )
            """.format(td=td))
            present = int(cursor.fetchone()[0] or 0)
            attendance_pct = round((present / total_active * 100) if total_active > 0 else 0, 1)
        except Exception:
            pass

        # ── Daily attendance status — last 30 days ───────────────
        daily = []
        try:
            cursor.execute("""
                SELECT
                    TO_CHAR(d, 'DD Mon') AS day_label,
                    COUNT(DISTINCT CASE WHEN status_flag = 1 THEN card_no END) AS on_time,
                    COUNT(DISTINCT CASE WHEN status_flag = 2 THEN card_no END) AS late,
                    COUNT(DISTINCT CASE WHEN status_flag = 3 THEN card_no END) AS absent
                FROM (
                    SELECT TRUNC(ROSTER_DATE) AS d, TO_CHAR(CARD_NO) AS card_no,
                        CASE
                            WHEN IN_TIME IS NOT NULL AND NVL(LATE_HRS,0)=0 AND NVL(LATE_MNT,0)=0 THEN 1
                            WHEN IN_TIME IS NOT NULL AND (NVL(LATE_HRS,0)>0 OR NVL(LATE_MNT,0)>0) THEN 2
                            WHEN IN_TIME IS NULL AND UPPER(NVL(STATUS,'')) NOT LIKE '%LEAVE%' THEN 3
                            ELSE NULL
                        END AS status_flag
                    FROM DUTY_ROSTER
                    WHERE TRUNC(ROSTER_DATE) BETWEEN {td} - 29 AND {td}
                    UNION ALL
                    SELECT TRUNC(ar.ATTENDANCE_DATE) AS d, TO_CHAR(ar.CARD_NO) AS card_no, 1 AS status_flag
                    FROM ATTENDANCE_RECORDS ar
                    WHERE TRUNC(ar.ATTENDANCE_DATE) BETWEEN {td} - 29 AND {td}
                      AND ar.ENTRY_TIME IS NOT NULL
                      AND NOT EXISTS (
                        SELECT 1 FROM DUTY_ROSTER dr
                        WHERE TO_CHAR(dr.CARD_NO) = TO_CHAR(ar.CARD_NO)
                          AND TRUNC(dr.ROSTER_DATE) = TRUNC(ar.ATTENDANCE_DATE)
                      )
                )
                WHERE status_flag IS NOT NULL
                GROUP BY TO_CHAR(d, 'DD Mon'), d
                ORDER BY d
            """.format(td=td))
            for r in cursor.fetchall():
                daily.append({
                    "day": r[0],
                    "on_time": int(r[1] or 0),
                    "late": int(r[2] or 0),
                    "absent": int(r[3] or 0),
                })
        except Exception as e:
            print(f"[HR_ANALYTICS] Daily query failed: {e}")

        # ── Monthly attendance statistics — last 6 months ────────
        monthly = []
        try:
            cursor.execute("""
                SELECT
                    TO_CHAR(TRUNC(d, 'MM'), 'Mon YY') AS month_label,
                    SUM(on_time_cnt) AS on_time,
                    SUM(overtime_cnt) AS overtime,
                    SUM(on_leave_cnt) AS on_leave,
                    SUM(late_cnt) AS late_clockin,
                    SUM(absent_cnt) AS absent,
                    SUM(total_cnt) AS total_rows,
                    TRUNC(d, 'MM') AS month_key
                FROM (
                    SELECT TRUNC(ROSTER_DATE) AS d,
                        CASE WHEN IN_TIME IS NOT NULL AND NVL(LATE_HRS,0)=0 AND NVL(LATE_MNT,0)=0 THEN 1 ELSE 0 END AS on_time_cnt,
                        CASE WHEN NVL(OT_HRS,0)>0 OR NVL(OT_MNT,0)>0 THEN 1 ELSE 0 END AS overtime_cnt,
                        CASE WHEN UPPER(NVL(STATUS,'')) LIKE '%LEAVE%' THEN 1 ELSE 0 END AS on_leave_cnt,
                        CASE WHEN IN_TIME IS NOT NULL AND (NVL(LATE_HRS,0)>0 OR NVL(LATE_MNT,0)>0) THEN 1 ELSE 0 END AS late_cnt,
                        CASE WHEN IN_TIME IS NULL AND UPPER(NVL(STATUS,'')) NOT LIKE '%LEAVE%' THEN 1 ELSE 0 END AS absent_cnt,
                        1 AS total_cnt
                    FROM DUTY_ROSTER
                    WHERE TRUNC(ROSTER_DATE) BETWEEN ADD_MONTHS(TRUNC({td}, 'MM'), -5) AND {td}
                    UNION ALL
                    SELECT TRUNC(ar.ATTENDANCE_DATE) AS d, 1, 0, 0, 0, 0, 1
                    FROM ATTENDANCE_RECORDS ar
                    WHERE TRUNC(ar.ATTENDANCE_DATE) BETWEEN ADD_MONTHS(TRUNC({td}, 'MM'), -5) AND {td}
                      AND ar.ENTRY_TIME IS NOT NULL
                      AND NOT EXISTS (
                        SELECT 1 FROM DUTY_ROSTER dr
                        WHERE TO_CHAR(dr.CARD_NO) = TO_CHAR(ar.CARD_NO)
                          AND TRUNC(dr.ROSTER_DATE) = TRUNC(ar.ATTENDANCE_DATE)
                      )
                )
                GROUP BY TO_CHAR(TRUNC(d, 'MM'), 'Mon YY'), TRUNC(d, 'MM')
                ORDER BY TRUNC(d, 'MM')
            """.format(td=td))
            for r in cursor.fetchall():
                total = int(r[6] or 1)
                present_cnt = int(r[1] or 0) + int(r[4] or 0) + int(r[3] or 0)
                absent_cnt = int(r[5] or 0)
                monthly.append({
                    "month": r[0],
                    "available": int(r[1] or 0),
                    "overtime": int(r[2] or 0),
                    "on_leave": int(r[3] or 0),
                    "late_clockin": int(r[4] or 0),
                    "absent": absent_cnt,
                    "attendance_pct": round((present_cnt / total * 100) if total > 0 else 0, 1),
                    "absenteeism_rate": round((absent_cnt / total * 100) if total > 0 else 2),
                })
        except Exception as e:
            print(f"[HR_ANALYTICS] Monthly query failed: {e}")

        return {
            "kpis": {
                "late_logins": late_logins,
                "early_logins": early_logins,
                "overtime_hours": overtime_hours,
                "unapproved_leaves": unapproved_leaves,
                "avg_work_hrs": avg_work_hrs,
                "attendance_pct": attendance_pct,
            },
            "daily_attendance": daily,
            "monthly_attendance": monthly,
        }

    finally:
        cursor.close()
        conn.close()
