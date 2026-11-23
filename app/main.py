from fastapi import FastAPI

app = FastAPI(title="Webhook Delivery Engine")


@app.get("/health")
async def health():
    return {"status": "ok"}