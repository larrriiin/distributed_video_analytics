from aiokafka import AIOKafkaProducer
import asyncio
import json

class KafkaProducer:
    def __init__(self, kafka_server: str):
        self.kafka_server = kafka_server
        self.producer = None

    async def start(self):
        self.producer = AIOKafkaProducer(bootstrap_servers=self.kafka_server)
        await self.producer.start()

    async def send_command(self, topic: str, command: dict):
        if self.producer is None:
            raise Exception("Producer is not initialized. Call start() first.")
        message = json.dumps(command).encode('utf-8')
        await self.producer.send_and_wait(topic, message)

    async def stop(self):
        if self.producer is not None:
            await self.producer.stop()

    def is_connected(self):
        # Проверяем, что продюсер существует и не закрыт
        return self.producer is not None and not self.producer._closed

# Пример использования
async def example():
    producer = KafkaProducer(kafka_server="localhost:9092")
    await producer.start()
    command = {"action": "start_processing", "scenario_id": 1}
    await producer.send_command(topic="orchestrator_commands", command=command)
    await producer.stop()

if __name__ == "__main__":
    asyncio.run(example())
