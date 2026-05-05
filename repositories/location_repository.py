"""Location tracking repository — stores hourly employee positions.

DDL (run once on Oracle DB):
    CREATE TABLE LOCATION_TRACKS (
        ID              NUMBER NOT NULL,
        CARD_NO         VARCHAR2(50) NOT NULL,
        LATITUDE        NUMBER(10,7) NOT NULL,
        LONGITUDE       NUMBER(10,7) NOT NULL,
        ACCURACY        NUMBER(10,2) DEFAULT 0,
        RECORDED_AT     TIMESTAMP NOT NULL,
        SYNCED_AT       TIMESTAMP DEFAULT SYSTIMESTAMP,
        ATTENDANCE_DATE DATE DEFAULT TRUNC(SYSDATE),
        CONSTRAINT LOCATION_TRACKS_PK PRIMARY KEY (ID)
    );
    CREATE INDEX LT_CARD_DATE_IDX ON LOCATION_TRACKS(CARD_NO, ATTENDANCE_DATE);
"""

from datetime import datetime

from core.database import get_connection


def insert_location_batch(card_no: str, locations: list) -> int:
    """Insert multiple location points. Returns count inserted."""
    if not locations:
        return 0

    conn = get_connection()
    cursor = conn.cursor()
    inserted = 0

    try:
        for loc in locations:
            recorded_at = loc.get("recorded_at", datetime.utcnow().isoformat())
            try:
                recorded_dt = datetime.fromisoformat(
                    recorded_at.replace("Z", "+00:00").replace("z", "+00:00")
                )
                # Oracle TIMESTAMP (not WITH TIME ZONE) rejects timezone-aware datetimes
                if recorded_dt.tzinfo is not None:
                    recorded_dt = recorded_dt.replace(tzinfo=None)
            except Exception:
                recorded_dt = datetime.now()

            cursor.execute(
                """
                INSERT INTO LOCATION_TRACKS (
                    ID, CARD_NO, LATITUDE, LONGITUDE, ACCURACY,
                    RECORDED_AT, SYNCED_AT, ATTENDANCE_DATE
                ) VALUES (
                    (SELECT NVL(MAX(ID), 0) + :offset FROM LOCATION_TRACKS),
                    :card_no, :lat, :lng, :acc,
                    :rec_at, SYSTIMESTAMP, TRUNC(SYSDATE)
                )
                """,
                {
                    "offset": inserted + 1,
                    "card_no": card_no,
                    "lat": float(loc.get("latitude", 0)),
                    "lng": float(loc.get("longitude", 0)),
                    "acc": float(loc.get("accuracy", 0)),
                    "rec_at": recorded_dt,
                },
            )
            inserted += 1

        conn.commit()
        print(f"[LOCATION] Saved {inserted} points for card={card_no}")
    except Exception as e:
        conn.rollback()
        print(f"[LOCATION] Batch insert error: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

    return inserted


def get_all_locations_summary(date_str: str) -> list:
    """Fetch all employees who have location data on a given date, with point count and latest time."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT
                lt.CARD_NO,
                NVL(h.NAME, lt.CARD_NO) AS EMPLOYEE_NAME,
                h.EMPCODE,
                COUNT(*) AS POINT_COUNT,
                TO_CHAR(MAX(lt.RECORDED_AT), 'YYYY-MM-DD HH24:MI:SS') AS LAST_SEEN,
                MAX(lt.LATITUDE) KEEP (DENSE_RANK LAST ORDER BY lt.RECORDED_AT) AS LAST_LAT,
                MAX(lt.LONGITUDE) KEEP (DENSE_RANK LAST ORDER BY lt.RECORDED_AT) AS LAST_LNG,
                MAX(lt.ACCURACY) KEEP (DENSE_RANK LAST ORDER BY lt.RECORDED_AT) AS LAST_ACC
            FROM LOCATION_TRACKS lt
            LEFT JOIN EMPLOYEE e ON TO_CHAR(e.CARD_NO) = lt.CARD_NO
                                 OR e.CARD_NO = TO_NUMBER(REGEXP_SUBSTR(lt.CARD_NO, '^[0-9]+'))
            LEFT JOIN HR_EMP_MASTER h ON h.EMPCODE = e.EMPCODE
            WHERE lt.ATTENDANCE_DATE = TO_DATE(:dt, 'YYYY-MM-DD')
            GROUP BY lt.CARD_NO, h.NAME, h.EMPCODE
            ORDER BY MAX(lt.RECORDED_AT) DESC
            """,
            {"dt": date_str},
        )
        rows = cursor.fetchall()
        return [
            {
                "card_no": r[0],
                "employee_name": r[1],
                "empcode": r[2],
                "point_count": int(r[3]),
                "last_seen": str(r[4]) if r[4] else None,
                "last_latitude": float(r[5]) if r[5] is not None else None,
                "last_longitude": float(r[6]) if r[6] is not None else None,
                "last_accuracy": float(r[7] or 0),
            }
            for r in rows
        ]
    finally:
        cursor.close()
        conn.close()


def get_location_history(card_no: str, date_str: str) -> list:
    """Fetch all location points for a card on a given date (YYYY-MM-DD)."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT LATITUDE, LONGITUDE, ACCURACY, RECORDED_AT
            FROM   LOCATION_TRACKS
            WHERE  (TO_CHAR(CARD_NO) = :card
                    OR TO_CHAR(CARD_NO) = :card_int)
              AND  ATTENDANCE_DATE = TO_DATE(:dt, 'YYYY-MM-DD')
            ORDER BY RECORDED_AT ASC
            """,
            {
                "card": card_no,
                "card_int": card_no.split(".")[0] if "." in card_no else card_no,
                "dt": date_str,
            },
        )
        rows = cursor.fetchall()
        return [
            {
                "latitude": float(r[0]),
                "longitude": float(r[1]),
                "accuracy": float(r[2] or 0),
                "recorded_at": str(r[3]),
            }
            for r in rows
        ]
    finally:
        cursor.close()
        conn.close()
