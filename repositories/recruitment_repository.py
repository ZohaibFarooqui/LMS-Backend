"""Recruitment repository — CRUD on RECRUITMENT_* tables."""

from core.database import get_connection


# ------------------------------------------------------------------
# JOBS
# ------------------------------------------------------------------

def create_job(data: dict, created_by: str) -> dict:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO RECRUITMENT_JOBS (
                JOB_ID, JOB_TITLE, DEPT_NO, OPEN_POSITIONS,
                JOB_DESC, SKILLS_REQ, STATUS, CREATED_BY, CREATED_AT, UPDATED_AT
            ) VALUES (
                RECRUITMENT_JOBS_SEQ.NEXTVAL, :job_title, :dept_no, :open_positions,
                :job_desc, :skills_req, 'OPEN', :created_by, SYSDATE, SYSDATE
            )
        """, {
            "job_title": data.get("job_title"),
            "dept_no": data.get("dept_no"),
            "open_positions": data.get("open_positions", 1),
            "job_desc": data.get("job_desc"),
            "skills_req": data.get("skills_req"),
            "created_by": created_by,
        })
        conn.commit()
        return {"status": "success"}
    except Exception as e:
        conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        cursor.close()
        conn.close()


def list_jobs(status: str = None) -> list:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        where = "WHERE j.STATUS = :status" if status else ""
        params = {"status": status} if status else {}
        cursor.execute(f"""
            SELECT
                j.JOB_ID,
                j.JOB_TITLE,
                j.DEPT_NO,
                NVL(d.DEPT_NAME, TO_CHAR(j.DEPT_NO)) AS DEPT_NAME,
                j.OPEN_POSITIONS,
                (SELECT COUNT(*) FROM RECRUITMENT_OFFERS o
                 JOIN RECRUITMENT_APPLICATIONS a2 ON a2.APP_ID = o.APP_ID
                 WHERE a2.JOB_ID = j.JOB_ID AND o.STATUS = 'ACCEPTED') AS FILLED_POSITIONS,
                j.JOB_DESC,
                j.SKILLS_REQ,
                j.STATUS,
                j.CREATED_BY,
                TO_CHAR(j.CREATED_AT, 'YYYY-MM-DD') AS CREATED_AT
            FROM RECRUITMENT_JOBS j
            LEFT JOIN HR_DEPT d ON d.DEPT_NO = j.DEPT_NO
            {where}
            ORDER BY j.JOB_ID DESC
        """, params)
        rows = cursor.fetchall()
        columns = [col[0].lower() for col in cursor.description]
        result = []
        for r in rows:
            row = dict(zip(columns, r))
            filled = int(row.get("filled_positions") or 0)
            open_pos = int(row.get("open_positions") or 0)
            row["filled_positions"] = filled
            row["remaining_positions"] = max(open_pos - filled, 0)
            result.append(row)
        return result
    finally:
        cursor.close()
        conn.close()


def get_job(job_id: int) -> dict | None:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT
                j.JOB_ID,
                j.JOB_TITLE,
                j.DEPT_NO,
                NVL(d.DEPT_NAME, TO_CHAR(j.DEPT_NO)) AS DEPT_NAME,
                j.OPEN_POSITIONS,
                (SELECT COUNT(*) FROM RECRUITMENT_OFFERS o
                 JOIN RECRUITMENT_APPLICATIONS a2 ON a2.APP_ID = o.APP_ID
                 WHERE a2.JOB_ID = j.JOB_ID AND o.STATUS = 'ACCEPTED') AS FILLED_POSITIONS,
                j.JOB_DESC,
                j.SKILLS_REQ,
                j.STATUS,
                j.CREATED_BY,
                TO_CHAR(j.CREATED_AT, 'YYYY-MM-DD') AS CREATED_AT
            FROM RECRUITMENT_JOBS j
            LEFT JOIN HR_DEPT d ON d.DEPT_NO = j.DEPT_NO
            WHERE j.JOB_ID = :job_id
        """, {"job_id": job_id})
        row = cursor.fetchone()
        if not row:
            return None
        columns = [col[0].lower() for col in cursor.description]
        result = dict(zip(columns, row))
        filled = int(result.get("filled_positions") or 0)
        open_pos = int(result.get("open_positions") or 0)
        result["filled_positions"] = filled
        result["remaining_positions"] = max(open_pos - filled, 0)
        return result
    finally:
        cursor.close()
        conn.close()


def update_job(job_id: int, data: dict) -> dict:
    conn = get_connection()
    cursor = conn.cursor()
    field_map = {
        "job_title": "JOB_TITLE",
        "dept_no": "DEPT_NO",
        "open_positions": "OPEN_POSITIONS",
        "job_desc": "JOB_DESC",
        "skills_req": "SKILLS_REQ",
        "status": "STATUS",
    }
    set_parts = ["UPDATED_AT = SYSDATE"]
    params = {"job_id": job_id}
    for key, col in field_map.items():
        if key in data and data[key] is not None:
            set_parts.append(f"{col} = :{key}")
            params[key] = data[key]
    if len(set_parts) == 1:
        return {"status": "error", "message": "No fields to update"}
    try:
        cursor.execute(
            f"UPDATE RECRUITMENT_JOBS SET {', '.join(set_parts)} WHERE JOB_ID = :job_id",
            params,
        )
        conn.commit()
        return {"status": "success"}
    except Exception as e:
        conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        cursor.close()
        conn.close()


# ------------------------------------------------------------------
# APPLICATIONS
# ------------------------------------------------------------------

def create_application(data: dict) -> dict:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO RECRUITMENT_APPLICATIONS (
                APP_ID, JOB_ID, CANDIDATE_NAME, MOBILE, EMAIL,
                SOURCE, APP_DATE, STATUS, NOTES, CREATED_AT
            ) VALUES (
                RECRUITMENT_APPS_SEQ.NEXTVAL, :job_id, :candidate_name,
                :mobile, :email, :source, SYSDATE, 'PENDING', :notes, SYSDATE
            )
        """, {
            "job_id": data.get("job_id"),
            "candidate_name": data.get("candidate_name"),
            "mobile": data.get("mobile"),
            "email": data.get("email"),
            "source": data.get("source"),
            "notes": data.get("notes"),
        })
        conn.commit()
        return {"status": "success"}
    except Exception as e:
        conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        cursor.close()
        conn.close()


def list_applications(job_id: int = None, status: str = None) -> list:
    conn = get_connection()
    cursor = conn.cursor()
    conditions = []
    params = {}
    if job_id is not None:
        conditions.append("a.JOB_ID = :job_id")
        params["job_id"] = job_id
    if status:
        conditions.append("a.STATUS = :status")
        params["status"] = status
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    try:
        cursor.execute(f"""
            SELECT
                a.APP_ID,
                a.JOB_ID,
                j.JOB_TITLE,
                a.CANDIDATE_NAME,
                a.MOBILE,
                a.EMAIL,
                a.SOURCE,
                TO_CHAR(a.APP_DATE, 'YYYY-MM-DD') AS APP_DATE,
                a.STATUS,
                a.NOTES,
                TO_CHAR(a.CREATED_AT, 'YYYY-MM-DD') AS CREATED_AT
            FROM RECRUITMENT_APPLICATIONS a
            JOIN RECRUITMENT_JOBS j ON j.JOB_ID = a.JOB_ID
            {where}
            ORDER BY a.APP_ID DESC
        """, params)
        rows = cursor.fetchall()
        columns = [col[0].lower() for col in cursor.description]
        return [dict(zip(columns, r)) for r in rows]
    finally:
        cursor.close()
        conn.close()


def get_application(app_id: int) -> dict | None:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT
                a.APP_ID, a.JOB_ID, j.JOB_TITLE,
                a.CANDIDATE_NAME, a.MOBILE, a.EMAIL, a.SOURCE,
                TO_CHAR(a.APP_DATE, 'YYYY-MM-DD') AS APP_DATE,
                a.STATUS, a.NOTES,
                TO_CHAR(a.CREATED_AT, 'YYYY-MM-DD') AS CREATED_AT
            FROM RECRUITMENT_APPLICATIONS a
            JOIN RECRUITMENT_JOBS j ON j.JOB_ID = a.JOB_ID
            WHERE a.APP_ID = :app_id
        """, {"app_id": app_id})
        row = cursor.fetchone()
        if not row:
            return None
        columns = [col[0].lower() for col in cursor.description]
        return dict(zip(columns, row))
    finally:
        cursor.close()
        conn.close()


def update_application_status(app_id: int, status: str, notes: str = None) -> dict:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE RECRUITMENT_APPLICATIONS
            SET STATUS = :status, NOTES = NVL(:notes, NOTES)
            WHERE APP_ID = :app_id
        """, {"status": status, "notes": notes, "app_id": app_id})
        conn.commit()
        return {"status": "success"}
    except Exception as e:
        conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        cursor.close()
        conn.close()


# ------------------------------------------------------------------
# INTERVIEWS
# ------------------------------------------------------------------

def create_interview(data: dict) -> dict:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO RECRUITMENT_INTERVIEWS (
                INTERVIEW_ID, APP_ID, INTERVIEW_DATE, INTERVIEW_TYPE,
                INTERVIEWER, STATUS, CREATED_AT
            ) VALUES (
                RECRUITMENT_INTERVIEWS_SEQ.NEXTVAL, :app_id,
                TO_DATE(:interview_date, 'YYYY-MM-DD'),
                :interview_type, :interviewer, 'SCHEDULED', SYSDATE
            )
        """, {
            "app_id": data.get("app_id"),
            "interview_date": data.get("interview_date"),
            "interview_type": data.get("interview_type"),
            "interviewer": data.get("interviewer"),
        })
        conn.commit()
        return {"status": "success"}
    except Exception as e:
        conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        cursor.close()
        conn.close()


def list_interviews(app_id: int = None, status: str = None) -> list:
    conn = get_connection()
    cursor = conn.cursor()
    conditions = []
    params = {}
    if app_id is not None:
        conditions.append("i.APP_ID = :app_id")
        params["app_id"] = app_id
    if status:
        conditions.append("i.STATUS = :status")
        params["status"] = status
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    try:
        cursor.execute(f"""
            SELECT
                i.INTERVIEW_ID,
                i.APP_ID,
                a.CANDIDATE_NAME,
                j.JOB_TITLE,
                TO_CHAR(i.INTERVIEW_DATE, 'YYYY-MM-DD') AS INTERVIEW_DATE,
                i.INTERVIEW_TYPE,
                i.INTERVIEWER,
                i.STATUS,
                i.FEEDBACK,
                TO_CHAR(i.CREATED_AT, 'YYYY-MM-DD') AS CREATED_AT
            FROM RECRUITMENT_INTERVIEWS i
            JOIN RECRUITMENT_APPLICATIONS a ON a.APP_ID = i.APP_ID
            JOIN RECRUITMENT_JOBS j ON j.JOB_ID = a.JOB_ID
            {where}
            ORDER BY i.INTERVIEW_ID DESC
        """, params)
        rows = cursor.fetchall()
        columns = [col[0].lower() for col in cursor.description]
        return [dict(zip(columns, r)) for r in rows]
    finally:
        cursor.close()
        conn.close()


def update_interview(interview_id: int, data: dict) -> dict:
    conn = get_connection()
    cursor = conn.cursor()
    field_map = {
        "status": "STATUS",
        "feedback": "FEEDBACK",
        "interviewer": "INTERVIEWER",
        "interview_type": "INTERVIEW_TYPE",
    }
    set_parts = []
    params = {"interview_id": interview_id}
    for key, col in field_map.items():
        if key in data and data[key] is not None:
            set_parts.append(f"{col} = :{key}")
            params[key] = data[key]
    if "interview_date" in data and data["interview_date"]:
        set_parts.append("INTERVIEW_DATE = TO_DATE(:interview_date, 'YYYY-MM-DD')")
        params["interview_date"] = data["interview_date"]
    if not set_parts:
        return {"status": "error", "message": "No fields to update"}
    try:
        cursor.execute(
            f"UPDATE RECRUITMENT_INTERVIEWS SET {', '.join(set_parts)} WHERE INTERVIEW_ID = :interview_id",
            params,
        )
        conn.commit()
        return {"status": "success"}
    except Exception as e:
        conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        cursor.close()
        conn.close()


# ------------------------------------------------------------------
# OFFERS
# ------------------------------------------------------------------

def create_offer(data: dict) -> dict:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO RECRUITMENT_OFFERS (
                OFFER_ID, APP_ID, OFFER_DATE, SALARY_OFFERED, STATUS, NOTES, CREATED_AT
            ) VALUES (
                RECRUITMENT_OFFERS_SEQ.NEXTVAL, :app_id,
                SYSDATE, :salary_offered, 'SENT', :notes, SYSDATE
            )
        """, {
            "app_id": data.get("app_id"),
            "salary_offered": data.get("salary_offered"),
            "notes": data.get("notes"),
        })
        conn.commit()
        return {"status": "success"}
    except Exception as e:
        conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        cursor.close()
        conn.close()


def list_offers(status: str = None) -> list:
    conn = get_connection()
    cursor = conn.cursor()
    where = "WHERE o.STATUS = :status" if status else ""
    params = {"status": status} if status else {}
    try:
        cursor.execute(f"""
            SELECT
                o.OFFER_ID,
                o.APP_ID,
                a.CANDIDATE_NAME,
                j.JOB_TITLE,
                TO_CHAR(o.OFFER_DATE, 'YYYY-MM-DD') AS OFFER_DATE,
                o.SALARY_OFFERED,
                o.STATUS,
                o.NOTES,
                TO_CHAR(o.CREATED_AT, 'YYYY-MM-DD') AS CREATED_AT
            FROM RECRUITMENT_OFFERS o
            JOIN RECRUITMENT_APPLICATIONS a ON a.APP_ID = o.APP_ID
            JOIN RECRUITMENT_JOBS j ON j.JOB_ID = a.JOB_ID
            {where}
            ORDER BY o.OFFER_ID DESC
        """, params)
        rows = cursor.fetchall()
        columns = [col[0].lower() for col in cursor.description]
        return [dict(zip(columns, r)) for r in rows]
    finally:
        cursor.close()
        conn.close()


def update_offer(offer_id: int, data: dict) -> dict:
    conn = get_connection()
    cursor = conn.cursor()
    field_map = {
        "status": "STATUS",
        "salary_offered": "SALARY_OFFERED",
        "notes": "NOTES",
    }
    set_parts = []
    params = {"offer_id": offer_id}
    for key, col in field_map.items():
        if key in data and data[key] is not None:
            set_parts.append(f"{col} = :{key}")
            params[key] = data[key]
    if not set_parts:
        return {"status": "error", "message": "No fields to update"}
    try:
        cursor.execute(
            f"UPDATE RECRUITMENT_OFFERS SET {', '.join(set_parts)} WHERE OFFER_ID = :offer_id",
            params,
        )
        conn.commit()
        return {"status": "success"}
    except Exception as e:
        conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        cursor.close()
        conn.close()


# ------------------------------------------------------------------
# ANALYTICS
# ------------------------------------------------------------------

def get_analytics() -> dict:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        # Open positions count
        cursor.execute("SELECT COUNT(*) FROM RECRUITMENT_JOBS WHERE STATUS = 'OPEN'")
        open_jobs = int(cursor.fetchone()[0] or 0)

        # Total applications by status
        cursor.execute("""
            SELECT STATUS, COUNT(*) FROM RECRUITMENT_APPLICATIONS GROUP BY STATUS
        """)
        app_counts = {r[0]: int(r[1]) for r in cursor.fetchall()}

        # Total interviews
        cursor.execute("SELECT COUNT(*) FROM RECRUITMENT_INTERVIEWS")
        total_interviews = int(cursor.fetchone()[0] or 0)

        # Hires this month (ACCEPTED offers in current month)
        cursor.execute("""
            SELECT COUNT(*) FROM RECRUITMENT_OFFERS
            WHERE STATUS = 'ACCEPTED'
              AND TRUNC(OFFER_DATE, 'MM') = TRUNC(SYSDATE, 'MM')
        """)
        hires_this_month = int(cursor.fetchone()[0] or 0)

        # Monthly hires (last 6 months)
        cursor.execute("""
            SELECT
                TO_CHAR(OFFER_DATE, 'MON YYYY') AS MONTH,
                COUNT(*) AS HIRES
            FROM RECRUITMENT_OFFERS
            WHERE STATUS = 'ACCEPTED'
              AND OFFER_DATE >= ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -5)
            GROUP BY TO_CHAR(OFFER_DATE, 'MON YYYY'), TRUNC(OFFER_DATE, 'MM')
            ORDER BY TRUNC(OFFER_DATE, 'MM')
        """)
        monthly_hires = [{"month": r[0], "hires": int(r[1])} for r in cursor.fetchall()]

        # Avg time to hire (days from APP_DATE to OFFER ACCEPTED date)
        cursor.execute("""
            SELECT AVG(o.OFFER_DATE - a.APP_DATE)
            FROM RECRUITMENT_OFFERS o
            JOIN RECRUITMENT_APPLICATIONS a ON a.APP_ID = o.APP_ID
            WHERE o.STATUS = 'ACCEPTED'
        """)
        row = cursor.fetchone()
        avg_time_to_hire = round(float(row[0]), 1) if row and row[0] else 0

        # Avg cost per hire (avg salary offered for ACCEPTED offers)
        cursor.execute("""
            SELECT AVG(SALARY_OFFERED) FROM RECRUITMENT_OFFERS WHERE STATUS = 'ACCEPTED'
        """)
        row = cursor.fetchone()
        avg_cost_per_hire = round(float(row[0]), 0) if row and row[0] else 0

        return {
            "open_jobs": open_jobs,
            "total_applications": sum(app_counts.values()),
            "pending": app_counts.get("PENDING", 0),
            "shortlisted": app_counts.get("SHORTLISTED", 0),
            "rejected": app_counts.get("REJECTED", 0),
            "total_interviews": total_interviews,
            "hires_this_month": hires_this_month,
            "avg_time_to_hire_days": avg_time_to_hire,
            "avg_cost_per_hire": avg_cost_per_hire,
            "monthly_hires": monthly_hires,
        }
    finally:
        cursor.close()
        conn.close()
