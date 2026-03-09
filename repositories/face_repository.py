"""Face repository — stubs for EMP_FACE_DATA operations.

The EMP_FACE_DATA table already exists in Oracle.
Face embedding extraction & comparison will be implemented later.
For now, we rely on the EMPLOYEE.FACE_REGISTERED flag.
"""

from core.database import get_connection


def is_face_registered(card_no: str) -> dict:
    """Check EMPLOYEE.FACE_REGISTERED flag."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT NVL(FACE_REGISTERED, 'N')
            FROM EMPLOYEE
            WHERE TO_CHAR(CARD_NO) = :card
        """, {"card": card_no})
        row = cursor.fetchone()
        if not row:
            return {"is_registered": False, "registered_at": None}
        return {
            "is_registered": row[0] == "Y",
            "registered_at": None,
        }
    except Exception as e:
        # ORA-00904: FACE_REGISTERED column may not exist yet
        if "ORA-00904" in str(e):
            return {"is_registered": False, "registered_at": None}
        raise
    finally:
        cursor.close()
        conn.close()


def set_face_registered(card_no: str, value: str = "Y"):
    """Update EMPLOYEE.FACE_REGISTERED flag."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE EMPLOYEE
            SET FACE_REGISTERED = :val
            WHERE TO_CHAR(CARD_NO) = :card
        """, {"val": value, "card": card_no})
        conn.commit()
    except Exception as e:
        conn.rollback()
        if "ORA-00904" in str(e):
            pass  # Column doesn't exist yet, silently skip
        else:
            raise
    finally:
        cursor.close()
        conn.close()


def store_face_embeddings(card_no: str, embeddings_json: list, created_at: str = None):
    """Store embeddings in EMP_FACE_DATA (stub — just marks registered)."""
    # TODO: when ML model is integrated, store actual embeddings:
    #   INSERT INTO EMP_FACE_DATA (CARD_NO, EMBEDDING, IMAGE_INDEX, CREATED_AT)
    #   VALUES (:card, :emb_clob, :idx, TO_TIMESTAMP(:ts, ...))
    set_face_registered(card_no, "Y")
    return {"status": "success"}


def get_stored_embeddings(card_no: str) -> list:
    """Retrieve stored embeddings for a card_no (stub — returns empty)."""
    # TODO: when ML model is integrated:
    #   SELECT EMBEDDING, IMAGE_INDEX FROM EMP_FACE_DATA WHERE CARD_NO = :card
    return []


def get_all_registered_employees() -> list:
    """Return all employees with FACE_REGISTERED = 'Y'."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT TO_CHAR(CARD_NO), EMP_NAME
            FROM EMPLOYEE
            WHERE NVL(FACE_REGISTERED, 'N') = 'Y'
        """)
        rows = cursor.fetchall()
        return [{"card_no": str(r[0]), "emp_name": r[1] or ""} for r in rows]
    except Exception as e:
        if "ORA-00904" in str(e):
            return []
        raise
    finally:
        cursor.close()
        conn.close()
