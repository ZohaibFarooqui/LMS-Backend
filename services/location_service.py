from repositories.location_repository import (
    insert_location_batch,
    get_location_history,
    get_all_locations_summary,
)


def save_location_batch(card_no: str, locations: list) -> dict:
    count = insert_location_batch(card_no, [loc.dict() for loc in locations])
    return {"inserted": count}


def fetch_location_history(card_no: str, date_str: str) -> list:
    return get_location_history(card_no, date_str)


def fetch_all_locations_summary(date_str: str) -> list:
    return get_all_locations_summary(date_str)
