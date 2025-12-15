from fastapi import FastAPI
from app.routers import recommendations, stats, history

app = FastAPI(
    title="AdTech Recommendation API",
    version="1.0.0"
)

# Routers
app.include_router(recommendations.router, prefix="/recommendations")
app.include_router(history.router, prefix="/history")
app.include_router(stats.router, prefix="/stats")

@app.get("/")
def root():
    return {"status": "ok", "message": "Recommendation API is running"}
