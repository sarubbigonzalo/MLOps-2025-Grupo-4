from pydantic import BaseModel

class Recommendation(BaseModel):
    product_id: str
    rank: int

class RecommendationsResponse(BaseModel):
    advertiser_id: str
    model: str
    items: list[Recommendation]

class StatsResponse(BaseModel):
    total_advertisers: int
    # podés agregar más métricas
