from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from enum import Enum
from typing import Dict, List, Any
import asyncpg
from .kafka_producer import KafkaProducer
from contextlib import asynccontextmanager
import json

# Конфигурация Kafka
KAFKA_SERVER = "localhost:9092"
DB_CONFIG = {
    "user": "root",
    "password": "Root1Root1",
    "database": "vidan_main",
    "host": "localhost"
}

producer = KafkaProducer(kafka_server=KAFKA_SERVER)

@asynccontextmanager
async def lifespan(app: FastAPI):
    await producer.start()
    app.state.db_pool = await asyncpg.create_pool(**DB_CONFIG)
    yield
    await producer.stop()
    await app.state.db_pool.close()

app = FastAPI(lifespan=lifespan)

# Статусы для стейт-машины
class State(str, Enum):
    init_startup = "init_startup"
    in_startup_processing = "in_startup_processing"
    active = "active"
    init_shutdown = "init_shutdown"
    in_shutdown_processing = "in_shutdown_processing"
    inactive = "inactive"

# Модель для информации о сценарии
class ScenarioInfo(BaseModel):
    id: str
    current_state: State
    parameters: dict

# Модель для тела запроса на изменение состояния
class ChangeStateRequest(BaseModel):
    new_state: State

# Модель для хранения результата инференса
class Detection(BaseModel):
    class_id: int
    confidence: float
    box: List[List[float]]  # координаты боксов

class InferenceResult(BaseModel):
    frame_number: int
    detections: List[Detection]  # список детекций

# Храним состояние в памяти для упрощения
scenarios = {
    "1": ScenarioInfo(id="1", current_state=State.inactive, parameters={}),
    "2": ScenarioInfo(id="2", current_state=State.inactive, parameters={}),
}

# Эндпоинт для получения информации о сценарии по его ID
@app.get("/scenario/{scenario_id}", response_model=ScenarioInfo)
async def get_scenario(scenario_id: str):
    if scenario_id in scenarios:
        return scenarios[scenario_id]
    raise HTTPException(status_code=404, detail="Scenario not found")

# Эндпоинт для изменения состояния стейт-машины
@app.post("/scenario/{scenario_id}/state")
async def change_state(scenario_id: str, request: ChangeStateRequest):
    if scenario_id in scenarios:
        scenarios[scenario_id].current_state = request.new_state

        # Отправка команды в Kafka
        command = {
            "scenario_id": scenario_id,
            "new_state": request.new_state.value
        }
        await producer.send_command(topic="orchestrator_commands", command=command)

        return {"message": "State updated", "new_state": request.new_state}
    raise HTTPException(status_code=404, detail="Scenario not found")

@app.get("/scenario/{scenario_id}/results", response_model=List[InferenceResult])
async def get_inference_results(scenario_id: str):
    query = """
    SELECT frame_number, detections
    FROM inference_results
    WHERE scenario_id = $1
    ORDER BY frame_number ASC
    """
    async with app.state.db_pool.acquire() as connection:
        results = await connection.fetch(query, scenario_id)
    
    if not results:
        raise HTTPException(status_code=404, detail="No results found for this scenario")
    
    # Преобразуем `detections` из строки в список объектов, если необходимо
    formatted_results = []
    for record in results:
        # Если `detections` хранится как строка JSON, преобразуем его в список
        detections = json.loads(record["detections"]) if isinstance(record["detections"], str) else record["detections"]
        formatted_results.append({
            "frame_number": record["frame_number"],
            "detections": [
                Detection(class_id=d["class"], confidence=d["confidence"], box=d["box"]) for d in detections
            ]
        })
    
    return formatted_results

@app.get("/healthcheck")
async def healthcheck():
    # Проверка состояния Kafka продюсера
    kafka_status = "healthy" if producer.is_connected() else "unhealthy"

    # Если Kafka продюсер не подключен, вернем 500
    if kafka_status != "healthy":
        return {"status": "unhealthy", "kafka_status": kafka_status}, 500

    # Если все проверки прошли успешно, вернем 200 OK
    return {"status": "healthy", "kafka_status": kafka_status}, 200