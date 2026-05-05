"""Reference data repository — read + add entries for lookup tables."""

from core.database import get_connection


# ─────────────────────────────────────────────────────────────────
# READ FUNCTIONS
# ─────────────────────────────────────────────────────────────────

def get_departments() -> list:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT DEPT_NO, DEPT_NAME FROM HR_DEPT ORDER BY DEPT_NAME")
        return [{"dept_no": r[0], "dept_name": (r[1] or "").strip()} for r in cursor.fetchall()]
    finally:
        cursor.close(); conn.close()


def get_grades() -> list:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT GRADE_CD, DESCR FROM HR_GRADE_CD WHERE STATUS = 'A' OR STATUS IS NULL ORDER BY GRADE_CD")
        return [{"grade_cd": (r[0] or "").strip(), "descr": (r[1] or "").strip()} for r in cursor.fetchall()]
    finally:
        cursor.close(); conn.close()


def get_designations(grade_cd: str = None) -> list:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        if grade_cd:
            cursor.execute(
                "SELECT GRADE_CD, DESG_CD, DESG_DESC FROM HR_DESG WHERE GRADE_CD = :g ORDER BY DESG_CD",
                {"g": grade_cd}
            )
        else:
            cursor.execute("SELECT GRADE_CD, DESG_CD, DESG_DESC FROM HR_DESG ORDER BY GRADE_CD, DESG_CD")
        return [{"grade_cd": (r[0] or "").strip(), "desg_cd": str(r[1]).strip(), "desg_desc": (r[2] or "").strip()} for r in cursor.fetchall()]
    finally:
        cursor.close(); conn.close()


def get_shifts() -> list:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT SHIFT, SHIFT_DESC, TIME_FROM, TIME_TO FROM SHIFT_HEAD ORDER BY SHIFT")
        return [
            {"shift": r[0], "shift_desc": (r[1] or "").strip(),
             "time_from": r[2], "time_to": r[3]}
            for r in cursor.fetchall()
        ]
    finally:
        cursor.close(); conn.close()


def get_blood_groups() -> list:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT BLOOD_GROUP_PK, BLOOD_GROUP FROM BLOOD_GROUP ORDER BY BLOOD_GROUP_PK")
        return [{"pk": r[0], "blood_group": r[1]} for r in cursor.fetchall()]
    finally:
        cursor.close(); conn.close()


def get_cadre() -> list:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT CADRE_PK, CADRE FROM CADRE ORDER BY CADRE")
        return [{"pk": r[0], "cadre": r[1]} for r in cursor.fetchall()]
    finally:
        cursor.close(); conn.close()


def get_units() -> list:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT UNIT_ID, UNIT_NAME FROM UNIT_MST ORDER BY UNIT_NAME")
        return [{"unit_id": r[0], "unit_name": (r[1] or "").strip()} for r in cursor.fetchall()]
    finally:
        cursor.close(); conn.close()


# ─────────────────────────────────────────────────────────────────
# ADD FUNCTIONS (HR admin only)
# ─────────────────────────────────────────────────────────────────

def add_department(dept_name: str) -> dict:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT NVL(MAX(DEPT_NO), 0) + 1 FROM HR_DEPT")
        new_pk = cursor.fetchone()[0]
        cursor.execute(
            "INSERT INTO HR_DEPT (DEPT_NO, DEPT_NAME) VALUES (:pk, :name)",
            {"pk": new_pk, "name": dept_name.strip()}
        )
        conn.commit()
        return {"status": "success", "dept_no": new_pk, "dept_name": dept_name.strip()}
    except Exception as e:
        conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        cursor.close(); conn.close()


def add_grade(grade_cd: str, descr: str) -> dict:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO HR_GRADE_CD (GRADE_CD, DESCR, STATUS) VALUES (:cd, :descr, 'A')",
            {"cd": grade_cd.strip(), "descr": descr.strip()}
        )
        conn.commit()
        return {"status": "success", "grade_cd": grade_cd.strip(), "descr": descr.strip()}
    except Exception as e:
        conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        cursor.close(); conn.close()


def add_designation(grade_cd: str, desg_desc: str) -> dict:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT NVL(MAX(DESG_CD), 0) + 1 FROM HR_DESG WHERE GRADE_CD = :g",
            {"g": grade_cd}
        )
        new_cd = cursor.fetchone()[0]
        cursor.execute(
            "INSERT INTO HR_DESG (GRADE_CD, DESG_CD, DESG_DESC) VALUES (:g, :cd, :desg_text)",
            {"g": grade_cd, "cd": new_cd, "desg_text": desg_desc.strip()}
        )
        conn.commit()
        return {"status": "success", "grade_cd": grade_cd, "desg_cd": str(new_cd), "desg_desc": desg_desc.strip()}
    except Exception as e:
        conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        cursor.close(); conn.close()


def add_shift(shift: str, shift_desc: str, time_from: str = None, time_to: str = None) -> dict:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT NVL(MAX(SHIFT_HEAD_PK), 0) + 1 FROM SHIFT_HEAD")
        new_pk = cursor.fetchone()[0]
        cursor.execute(
            "INSERT INTO SHIFT_HEAD (SHIFT_HEAD_PK, SHIFT, SHIFT_DESC, TIME_FROM, TIME_TO, COMPC, BRNCH) VALUES (:pk, :shift, :shift_text, :tf, :tt, 1, 1)",
            {"pk": new_pk, "shift": shift.strip().upper(), "shift_text": shift_desc.strip(), "tf": time_from, "tt": time_to}
        )
        conn.commit()
        return {"status": "success", "shift": shift.strip().upper(), "shift_desc": shift_desc.strip()}
    except Exception as e:
        conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        cursor.close(); conn.close()


def add_blood_group(blood_group: str) -> dict:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT NVL(MAX(BLOOD_GROUP_PK), 0) + 1 FROM BLOOD_GROUP")
        new_pk = cursor.fetchone()[0]
        cursor.execute(
            "INSERT INTO BLOOD_GROUP (BLOOD_GROUP_PK, BLOOD_GROUP, COMPC, BRNCH) VALUES (:pk, :bg, 1, 2)",
            {"pk": new_pk, "bg": blood_group.strip()}
        )
        conn.commit()
        return {"status": "success", "pk": new_pk, "blood_group": blood_group.strip()}
    except Exception as e:
        conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        cursor.close(); conn.close()


def add_cadre(cadre: str) -> dict:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT NVL(MAX(CADRE_PK), 0) + 1 FROM CADRE")
        new_pk = cursor.fetchone()[0]
        cursor.execute(
            "INSERT INTO CADRE (CADRE_PK, CADRE, COMPC, BRNCH) VALUES (:pk, :c, 1, 2)",
            {"pk": new_pk, "c": cadre.strip()}
        )
        conn.commit()
        return {"status": "success", "pk": new_pk, "cadre": cadre.strip()}
    except Exception as e:
        conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        cursor.close(); conn.close()


def get_religions() -> list:
    """Return distinct religion codes already in HR_EMP_MASTER, plus defaults."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT DISTINCT RELIGION FROM HR_EMP_MASTER
            WHERE RELIGION IS NOT NULL AND TRIM(RELIGION) IS NOT NULL
            ORDER BY RELIGION
        """)
        existing = {r[0].strip() for r in cursor.fetchall() if r[0] and r[0].strip()}
        # Built-in 4-char defaults always available
        defaults = ["ISLM", "CHRS", "HIND", "BUDH", "JAIN", "SIKK", "OTHR"]
        combined = sorted(existing | set(defaults))
        label_map = {
            "ISLM": "Islam", "CHRS": "Christian", "HIND": "Hindu",
            "BUDH": "Buddhist", "JAIN": "Jain", "SIKK": "Sikh", "OTHR": "Other",
        }
        return [{"code": c, "label": label_map.get(c, c)} for c in combined]
    finally:
        cursor.close()
        conn.close()


def get_reporting_officers() -> list:
    """Return active employees for use as reporting officer options."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT EMPCODE, NAME FROM HR_EMP_MASTER
            WHERE (STATUS = 'A' OR STATUS IS NULL)
              AND NAME IS NOT NULL
            ORDER BY NAME
            FETCH FIRST 500 ROWS ONLY
        """)
        return [{"empcode": r[0], "name": (r[1] or "").strip()} for r in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()


def add_unit(unit_name: str) -> dict:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT NVL(MAX(UNIT_ID), 0) + 1 FROM UNIT_MST")
        new_pk = cursor.fetchone()[0]
        cursor.execute(
            "INSERT INTO UNIT_MST (UNIT_ID, UNIT_NAME) VALUES (:pk, :name)",
            {"pk": new_pk, "name": unit_name.strip()}
        )
        conn.commit()
        return {"status": "success", "unit_id": new_pk, "unit_name": unit_name.strip()}
    except Exception as e:
        conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        cursor.close(); conn.close()
