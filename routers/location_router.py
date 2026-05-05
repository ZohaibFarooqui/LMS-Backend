"""Location tracking router — /auth/location/* endpoints."""

from fastapi import APIRouter, HTTPException, Query

from models.location_models import LocationBatchRequest
from routers.hrms_router import require_hr_admin
from services.location_service import (
    fetch_all_locations_summary,
    fetch_location_history,
    save_location_batch,
)

router = APIRouter(prefix="/auth", tags=["Location Tracking"])


@router.post("/location/batch")
def post_location_batch(request: LocationBatchRequest):
    """Receive a batch of offline-buffered location points from the mobile app."""
    try:
        result = save_location_batch(request.card_no, request.locations)
        return {"body": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/location/summary")
def get_location_summary(
    date: str = Query(..., description="YYYY-MM-DD"),
    admin_card_no: str = Query(...),
):
    """HR-only: all employees with location data for a given date."""
    require_hr_admin(admin_card_no)
    try:
        summary = fetch_all_locations_summary(date)
        return {"body": {"date": date, "employees": summary}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/location/history/{card_no}")
def get_location_history(
    card_no: str,
    date: str = Query(..., description="YYYY-MM-DD"),
    admin_card_no: str = Query(...),
):
    """HR-only: all location points for one employee on a given date."""
    require_hr_admin(admin_card_no)
    try:
        points = fetch_location_history(card_no, date)
        return {"body": {"card_no": card_no, "date": date, "points": points}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
