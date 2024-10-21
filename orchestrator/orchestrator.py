import asyncio
import json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from loguru import logger

class Orchestrator:
    def __init__(self, kafka_server: str, topic: str):
        self.kafka_server = kafka_server
        self.topic = topic
        self.consumer = None
        self.producer = None

    async def start(self):
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.kafka_server,
            group_id="orchestrator_group",
            auto_offset_reset="earliest"
        )
        self.producer = AIOKafkaProducer(bootstrap_servers=self.kafka_server)
        await self.consumer.start()
        await self.producer.start()

    async def listen(self):
        try:
            async for msg in self.consumer:
                command = json.loads(msg.value.decode('utf-8'))
                logger.info(f"Received command: {command}")
                await self.handle_command(command)
        finally:
            await self.consumer.stop()

    async def handle_command(self, command: dict):
        scenario_id = command.get("scenario_id")
        new_state = command.get("new_state")
        
        # Логирование перед отправкой в Kafka
        logger.info(f"Sending command to Kafka topic 'runner_commands': {command}")
        
        if new_state == "active":
            logger.info(f"Starting scenario {scenario_id}")
            await self.send_to_kafka(command)  # Отправляем команду в Kafka
        elif new_state == "inactive":
            logger.info(f"Stopping scenario {scenario_id}")
            await self.send_to_kafka(command)  # Отправляем команду в Kafka

    async def send_to_kafka(self, command: dict):
        # Отправка команды в топик runner_commands
        message = json.dumps(command).encode('utf-8')
        await self.producer.send_and_wait("runner_commands", message)
        logger.info(f"Command sent to Kafka: {command}")

# Пример использования
async def main():
    orchestrator = Orchestrator(kafka_server="localhost:9092", topic="orchestrator_commands")
    await orchestrator.start()
    await orchestrator.listen()

if __name__ == "__main__":
    asyncio.run(main())
