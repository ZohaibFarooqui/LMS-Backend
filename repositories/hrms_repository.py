"""HRMS repository — CRUD operations on HR_EMP_MASTER table."""

from core.database import get_connection


# ------------------------------------------------------------------
# NEXT EMPCODE — auto-increment by finding max numeric empcode + 1
# ------------------------------------------------------------------

def get_next_empcode() -> str:
    """Return the next available EMPCODE (max + 1, zero-padded to match)."""
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
    """Insert a new row into HR_EMP_MASTER. Returns {status, empcode}."""
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
                BASIC, GROSS, SHIFT, W_HOUR
            ) VALUES (
                :empcode, :name, :fhname, :atdtcard,
                :sex, TO_DATE(:dtofbrth, 'YYYY-MM-DD'), :nicno,
                TO_DATE(:dtofappt, 'YYYY-MM-DD'), :dept_no, :desg_cd,
                :mobile, :email, :address,
                :unit_id, :status, :user_paswd,
                :hr_admin, :rpt_officer, :marstat,
                :grade_cd, :religion,
                :hod1, :hod2, :hod3,
                :basic, :gross, :shift, :w_hour
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
            "religion": data.get("religion"),
            "hod1": data.get("hod1"),
            "hod2": data.get("hod2"),
            "hod3": data.get("hod3"),
            "basic": data.get("basic"),
            "gross": data.get("gross"),
            "shift": data.get("shift"),
            "w_hour": data.get("w_hour"),
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
    """Fetch a single employee from HR_EMP_MASTER by EMPCODE."""
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
                BASIC, GROSS, SHIFT, W_HOUR
            FROM HR_EMP_MASTER
            WHERE EMPCODE = :empcode
        """, {"empcode": empcode})

        row = cursor.fetchone()
        if not row:
            return None

        columns = [col[0].lower() for col in cursor.description]
        result = dict(zip(columns, row))

        # Normalize column aliases
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
    """Update an existing HR_EMP_MASTER row. Only non-None fields are updated."""
    conn = get_connection()
    cursor = conn.cursor()

    # Build SET clause dynamically from provided fields
    field_map = {
        "name": "NAME",
        "fhname": "FHNAME",
        "atdtcard": '"ATDTCARD#"',
        "sex": "SEX",
        "nicno": "NICNO",
        "dept_no": "DEPT_NO",
        "desg_cd": "DESG_CD",
        "mobile": '"MOBILE#"',
        "email": "EMAIL",
        "address": "ADDRESS",
        "unit_id": "UNIT_ID",
        "status": "STATUS",
        "user_paswd": "USER_PASWD",
        "hr_admin": "HR_ADMIN",
        "rpt_officer": "RPT_OFFICER",
        "marstat": "MARSTAT",
        "grade_cd": "GRADE_CD",
        "religion": "RELIGION",
        "hod1": "HOD1",
        "hod2": "HOD2",
        "hod3": "HOD3",
        "basic": "BASIC",
        "gross": "GROSS",
        "shift": "SHIFT",
        "w_hour": "W_HOUR",
    }

    # Date fields need TO_DATE conversion
    date_fields = {"dtofbrth": "DTOFBRTH", "dtofappt": "DTOFAPPT"}

    set_parts = []
    params = {"empcode": empcode}

    for key, col in field_map.items():
        if key in data and data[key] is not None:
            set_parts.append(f"{col} = :{key}")
            params[key] = data[key]

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

def search_employees_hrms(query: str) -> list:
    """Search HR_EMP_MASTER by NAME, EMPCODE, ATDTCARD#, or MOBILE#."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        search = f"%{query.upper()}%"
        cursor.execute("""
            SELECT
                EMPCODE, NAME, FHNAME, "ATDTCARD#",
                DEPT_NO, DESG_CD, "MOBILE#", EMAIL,
                STATUS, HR_ADMIN, UNIT_ID
            FROM HR_EMP_MASTER
            WHERE UPPER(NAME) LIKE :q
               OR EMPCODE LIKE :q
               OR "ATDTCARD#" LIKE :q
               OR "MOBILE#" LIKE :q
            ORDER BY NAME
            FETCH FIRST 50 ROWS ONLY
        """, {"q": search})

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
