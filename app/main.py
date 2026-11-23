from fastapi import FastAPI

from app.api.routes import router as api_router

app = FastAPI(title="Webhook Delivery Engine", version="0.2.0")
app.include_router(api_router)


@app.get("/health", tags=["operations"])
def health() -> dict[str, str]:
    return {"status": "ok"}
