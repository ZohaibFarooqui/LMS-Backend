"""Attendance service — smart check-in / check-out logic."""

from repositories.attendance_repository import (
    get_today_record,
    insert_check_in,
    update_check_out,
    get_attendance_report,
    get_attendance_report_range,
    get_attendance_summary,
    _get_empcode,
)


def smart_mark_attendance(card_no: str, attendance_type: str = "check_in", **kwargs):
    """
    Smart attendance logic:
      1. No record today          → CHECK IN
      2. Has entry, no exit       → CHECK OUT
      3. Has both entry and exit  → Already completed
    """
    record = get_today_record(card_no)

    if record is None:
        # No record today → check in
        empcode = _get_empcode(card_no)
        result = insert_check_in(
            card_no=card_no,
            empcode=empcode,
            latitude=kwargs.get("latitude"),
            longitude=kwargs.get("longitude"),
            accuracy=kwargs.get("accuracy"),
            address=kwargs.get("address"),
            formatted_address=kwargs.get("formatted_address"),
            timestamp=kwargs.get("timestamp"),
            device_id=kwargs.get("device_id"),
            device_model=kwargs.get("device_model"),
            app_version=kwargs.get("app_version"),
            attendance_type=attendance_type,
        )
        return result

    entry = record["entry_time"]
    exit_ = record["exit_time"]

    if entry and not exit_:
        # Has entry but no exit → check out
        source = record.get("source", "duty_roster")
        result = update_check_out(
            record["id"], entry,
            card_no=record.get("card_no", card_no),
            source=source,
        )
        return result

    # Both entry and exit exist
    return {
        "status": "success",
        "message": "Attendance already completed for today",
        "action": "already_done",
        "marked_at": None,
        "location_verified": True,
    }


def fetch_attendance_report(card_no: str, date_str: str):
    return get_attendance_report(card_no, date_str)


def fetch_attendance_report_range(card_no: str, from_date: str, to_date: str):
    return get_attendance_report_range(card_no, from_date, to_date)


def fetch_attendance_summary(card_no: str, from_date: str, to_date: str):
    return get_attendance_summary(card_no, from_date, to_date)
