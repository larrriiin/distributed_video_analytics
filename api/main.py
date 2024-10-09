from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncio
from aiokafka import AIOKafkaProducer

app = FastAPI(title="Distributed Video Analytics API")

# Настройки Kafka
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'orchestrator_commands'

# Инициализация Kafka-продьюсера
loop = asyncio.get_event_loop()
producer = AIOKafkaProducer(loop=loop, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

@app.on_event("startup")
async def startup_event():
    await producer.start()

@app.on_event("shutdown")
async def shutdown_event():
    await producer.stop()

# Модели Pydantic
class ScenarioStatus(BaseModel): # для представления статуса сценария
    id: int
    status: str
    parameters: dict

class StateChangeCommand(BaseModel): # для приёма команд изменения состояния
    id: int
    command: str  # Например, 'start', 'stop'

# Временное хранилище сценариев (для простоты)
scenarios = {}

@app.get("/scenario/{scenario_id}", response_model=ScenarioStatus) # возвращает информацию о сценарии
async def get_scenario(scenario_id: int):
    scenario = scenarios.get(scenario_id)
    if not scenario:
        raise HTTPException(status_code=404, detail="Сценарий не найден")
    return scenario

@app.post("/scenario/{scenario_id}/state") # изменяет состояние сценария
async def change_scenario_state(scenario_id: int, command: StateChangeCommand):
    # Отправляем команду оркестратору через Kafka
    await producer.send_and_wait(KAFKA_TOPIC, command.json().encode('utf-8'))
    return {"message": "Команда отправлена"}

@app.get("/healthcheck") # проверка здоровья сервиса
async def healthcheck():
    return {"status": "ok"}
