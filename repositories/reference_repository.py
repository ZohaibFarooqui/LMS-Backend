"""Reference data repository — read + add entries for lookup tables."""

from core.database import get_connection


def _try_filtered(cursor, filtered_sql, filtered_params, fallback_sql, fallback_params):
    """Try filtered query. If it fails with ORA-00904 (column doesn't exist) or
    ORA-00942 (table missing), fall back to the unfiltered query."""
    try:
        cursor.execute(filtered_sql, filtered_params)
        return cursor.fetchall(), [c[0].lower() for c in cursor.description]
    except Exception as e:
        msg = str(e)
        if "ORA-00904" in msg or "ORA-00942" in msg:
            print(f"[REFERENCE] Filter not applicable for this table: {msg.splitlines()[0][:80]}")
            cursor.execute(fallback_sql, fallback_params)
            return cursor.fetchall(), [c[0].lower() for c in cursor.description]
        raise


def _build_filter(compc=None, brnch=None):
    """Return list of WHERE-clause parts and a params dict for COMPC/BRNCH filters."""
    parts = []
    params = {}
    if compc:
        parts.append("COMPC = :_f_compc")
        try:
            params["_f_compc"] = int(compc)
        except (ValueError, TypeError):
            params["_f_compc"] = compc
    if brnch:
        parts.append("BRNCH = :_f_brnch")
        try:
            params["_f_brnch"] = int(brnch)
        except (ValueError, TypeError):
            params["_f_brnch"] = brnch
    return parts, params


# ─────────────────────────────────────────────────────────────────
# READ FUNCTIONS
# ─────────────────────────────────────────────────────────────────

def get_departments(compc=None, brnch=None) -> list:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        filt, fparams = _build_filter(compc, brnch)
        fallback_sql = "SELECT DEPT_NO, DEPT_NAME FROM HR_DEPT ORDER BY DEPT_NAME"
        if filt:
            filtered_sql = f"SELECT DEPT_NO, DEPT_NAME FROM HR_DEPT WHERE {' AND '.join(filt)} ORDER BY DEPT_NAME"
            rows, _ = _try_filtered(cursor, filtered_sql, fparams, fallback_sql, {})
        else:
            cursor.execute(fallback_sql); rows = cursor.fetchall()
        return [{"dept_no": r[0], "dept_name": (r[1] or "").strip()} for r in rows]
    finally:
        cursor.close(); conn.close()


def get_grades(compc=None, brnch=None) -> list:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        filt, fparams = _build_filter(compc, brnch)
        fallback_sql = "SELECT GRADE_CD, DESCR FROM HR_GRADE_CD WHERE (STATUS = 'A' OR STATUS IS NULL) ORDER BY GRADE_CD"
        if filt:
            filtered_sql = f"SELECT GRADE_CD, DESCR FROM HR_GRADE_CD WHERE (STATUS = 'A' OR STATUS IS NULL) AND {' AND '.join(filt)} ORDER BY GRADE_CD"
            rows, _ = _try_filtered(cursor, filtered_sql, fparams, fallback_sql, {})
        else:
            cursor.execute(fallback_sql); rows = cursor.fetchall()
        return [{"grade_cd": (r[0] or "").strip(), "descr": (r[1] or "").strip()} for r in rows]
    finally:
        cursor.close(); conn.close()


def get_designations(grade_cd: str = None, compc=None, brnch=None) -> list:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        filt, fparams = _build_filter(compc, brnch)
        base_where = []
        base_params = {}
        if grade_cd:
            base_where.append("GRADE_CD = :g")
            base_params["g"] = grade_cd
        fallback_where = (" WHERE " + " AND ".join(base_where)) if base_where else ""
        fallback_sql = f"SELECT GRADE_CD, DESG_CD, DESG_DESC FROM HR_DESG{fallback_where} ORDER BY GRADE_CD, DESG_CD"
        if filt:
            all_parts = base_where + filt
            filtered_sql = f"SELECT GRADE_CD, DESG_CD, DESG_DESC FROM HR_DESG WHERE {' AND '.join(all_parts)} ORDER BY GRADE_CD, DESG_CD"
            rows, _ = _try_filtered(cursor, filtered_sql, {**base_params, **fparams}, fallback_sql, base_params)
        else:
            cursor.execute(fallback_sql, base_params); rows = cursor.fetchall()
        return [{"grade_cd": (r[0] or "").strip(), "desg_cd": str(r[1]).strip(), "desg_desc": (r[2] or "").strip()} for r in rows]
    finally:
        cursor.close(); conn.close()


def get_shifts(compc=None, brnch=None) -> list:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        filt, fparams = _build_filter(compc, brnch)
        fallback_sql = "SELECT SHIFT, SHIFT_DESC, TIME_FROM, TIME_TO FROM SHIFT_HEAD ORDER BY SHIFT"
        if filt:
            filtered_sql = f"SELECT SHIFT, SHIFT_DESC, TIME_FROM, TIME_TO FROM SHIFT_HEAD WHERE {' AND '.join(filt)} ORDER BY SHIFT"
            rows, _ = _try_filtered(cursor, filtered_sql, fparams, fallback_sql, {})
        else:
            cursor.execute(fallback_sql); rows = cursor.fetchall()
        return [{"shift": r[0], "shift_desc": (r[1] or "").strip(),
                 "time_from": r[2], "time_to": r[3]} for r in rows]
    finally:
        cursor.close(); conn.close()


def get_blood_groups(compc=None, brnch=None) -> list:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        filt, fparams = _build_filter(compc, brnch)
        fallback_sql = "SELECT BLOOD_GROUP_PK, BLOOD_GROUP FROM BLOOD_GROUP ORDER BY BLOOD_GROUP_PK"
        if filt:
            filtered_sql = f"SELECT BLOOD_GROUP_PK, BLOOD_GROUP FROM BLOOD_GROUP WHERE {' AND '.join(filt)} ORDER BY BLOOD_GROUP_PK"
            rows, _ = _try_filtered(cursor, filtered_sql, fparams, fallback_sql, {})
        else:
            cursor.execute(fallback_sql); rows = cursor.fetchall()
        return [{"pk": r[0], "blood_group": r[1]} for r in rows]
    finally:
        cursor.close(); conn.close()


def get_cadre(compc=None, brnch=None) -> list:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        filt, fparams = _build_filter(compc, brnch)
        fallback_sql = "SELECT CADRE_PK, CADRE FROM CADRE ORDER BY CADRE"
        if filt:
            filtered_sql = f"SELECT CADRE_PK, CADRE FROM CADRE WHERE {' AND '.join(filt)} ORDER BY CADRE"
            rows, _ = _try_filtered(cursor, filtered_sql, fparams, fallback_sql, {})
        else:
            cursor.execute(fallback_sql); rows = cursor.fetchall()
        return [{"pk": r[0], "cadre": r[1]} for r in rows]
    finally:
        cursor.close(); conn.close()


def get_units() -> list:
    # Units ARE the company list — never filter by company
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


def get_locations() -> list:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT LCODE, DESCR, SNAME, NVL(REGIONCODE,'') AS REGIONCODE, NVL(CITY,'') AS CITY
            FROM COM_LOCATION
            ORDER BY LCODE
        """)
        return [
            {
                "lcode": str(r[0] or "").strip(),
                "descr": (r[1] or "").strip(),
                "sname": (r[2] or "").strip(),
                "regioncode": (r[3] or "").strip(),
                "city": (r[4] or "").strip(),
            }
            for r in cursor.fetchall()
        ]
    finally:
        cursor.close(); conn.close()


def add_location(lcode: str, descr: str, sname: str, regioncode: str, city: str) -> dict:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO COM_LOCATION (LCODE, DESCR, SNAME, REGIONCODE, CITY) VALUES (:lcode, :descr, :sname, :region, :city)",
            {"lcode": lcode.strip(), "descr": descr.strip(), "sname": (sname or descr).strip(), "region": regioncode.strip(), "city": city.strip()}
        )
        conn.commit()
        return {"status": "success", "lcode": lcode.strip()}
    except Exception as e:
        conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        cursor.close(); conn.close()


def update_location(lcode: str, descr: str, sname: str, regioncode: str, city: str) -> dict:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "UPDATE COM_LOCATION SET DESCR=:descr, SNAME=:sname, REGIONCODE=:region, CITY=:city WHERE LCODE=:lcode",
            {"lcode": lcode.strip(), "descr": descr.strip(), "sname": (sname or descr).strip(), "region": regioncode.strip(), "city": city.strip()}
        )
        conn.commit()
        if cursor.rowcount == 0:
            return {"status": "error", "message": f"Location {lcode} not found"}
        return {"status": "success", "lcode": lcode.strip()}
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
