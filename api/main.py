from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from enum import Enum
import asyncio
from .kafka_producer import KafkaProducer
from contextlib import asynccontextmanager

# Конфигурация Kafka
KAFKA_SERVER = "localhost:9092"
producer = KafkaProducer(kafka_server=KAFKA_SERVER)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Действия при запуске приложения
    await producer.start()

    # Передача управления приложению
    yield

    # Действия при завершении работы приложения
    await producer.stop()

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

@app.get("/healthcheck")
async def healthcheck():
    # Проверка состояния Kafka продюсера
    kafka_status = "healthy" if producer.is_connected() else "unhealthy"

    # Если Kafka продюсер не подключен, вернем 500
    if kafka_status != "healthy":
        return {"status": "unhealthy", "kafka_status": kafka_status}, 500

    # Если все проверки прошли успешно, вернем 200 OK
    return {"status": "healthy", "kafka_status": kafka_status}, 200