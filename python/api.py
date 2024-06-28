from fastapi import FastAPI, WebSocket
from tasks import celery_app, load_all_tickers, load_quotes, load_options_and_quotes, load_instrument_details
from celery.result import AsyncResult
import asyncio

app = FastAPI()

@app.get("/load_all_tickers")
async def load_all_tickers(database_name = "test"):
    result = load_all_tickers.delay(database_name)
    return {"task_id": result.id}

@app.get("/load_instrument_details")
async def load_instrument_details(database_name = "test"):
    result = load_instrument_details.delay(database_name)
    return {"task_id": result.id}

@app.get("/load_quotes")
async def load_quotes(database_name = "test", period='1d'):
    result = load_quotes.delay(database_name, period=period)
    return {"task_id": result.id}

@app.get("/load_options_and_quotes")
async def load_options_and_quotes(database_name = "test"):
    result = load_options_and_quotes.delay(database_name)
    return {"task_id": result.id}

@app.websocket("/ws/task/{task_id}")
async def websocket_endpoint(websocket: WebSocket, task_id: str):
    await websocket.accept()

    # Get the task result asynchronously
    result = AsyncResult(task_id, app=celery_app)

    while True:
        if result.ready():
            break
        await websocket.send_text(result.state)
        await asyncio.sleep(1)

    # Task is ready, send the final result
    if result.successful():
        await websocket.send_text(str(result.state))
        await websocket.send_text(str(result.result))
        await websocket.close()
    else:
        await websocket.send_text(result.state)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)