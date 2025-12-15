from fastapi import APIRouter, HTTPException
from app.database import get_connection
from app.schemas import RecommendationsResponse, Recommendation
from app.models import GET_RECOMMENDATIONS, GET_HISTORY

router = APIRouter()

@router.get("/{advertiser_id}/{model}", response_model=RecommendationsResponse)
def get_recommendations(advertiser_id: str, model: str):
    model = model.upper()

    if model not in ["TOPCTR", "TOPPRODUCT"]:
        raise HTTPException(400, "Modelo inválido")

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(GET_RECOMMENDATIONS, (advertiser_id, model))
    
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        raise HTTPException(404, "No hay recomendaciones para ese advertiser/modelo")

    items = [Recommendation(product_id=r[0], rank=r[1]) for r in rows]

    return RecommendationsResponse(
        advertiser_id=advertiser_id,
        model=model,
        items=items
    )

