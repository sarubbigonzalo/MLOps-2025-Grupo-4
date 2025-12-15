from fastapi import APIRouter
from app.database import get_connection
from app.models import GET_HISTORY

router = APIRouter()

@router.get("/{advertiser_id}")
def get_history(advertiser_id: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(GET_HISTORY, (advertiser_id,))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return {"advertiser_id": advertiser_id, "history": rows}
