from fastapi import APIRouter
from app.database import get_connection
from app.models import GET_STATS

router = APIRouter()

@router.get("/", tags=["stats"])
def stats():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(GET_STATS)
    (total_advertisers,) = cur.fetchone()
    cur.close()
    conn.close()

    return {"total_advertisers": total_advertisers}
