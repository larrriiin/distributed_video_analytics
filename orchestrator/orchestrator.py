import asyncio
import json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from loguru import logger
from state_machine import StateMachine, State

class Orchestrator:
    def __init__(self, kafka_server: str, topic: str):
        self.kafka_server = kafka_server
        self.topic = topic
        self.consumer = None
        self.producer = None
        self.state_machine = StateMachine()

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
        logger.info("Orchestrator started and listening for commands...")

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

        if new_state == "active":
            if self.state_machine.get_current_state() == State.inactive:
                await self.automatic_transition(scenario_id, [
                    State.init_startup,
                    State.in_startup_processing,
                    State.active
                ])

        elif new_state == "inactive":
            if self.state_machine.get_current_state() == State.active:
                await self.automatic_transition(scenario_id, [
                    State.init_shutdown,
                    State.in_shutdown_processing,
                    State.inactive
                ])

    async def automatic_transition(self, scenario_id: str, states: list):
        for state in states:
            if self.state_machine.transition(state):
                # Отправка команды в Kafka
                command = {"scenario_id": scenario_id, "new_state": state.value}
                await self.send_to_kafka(command)
                logger.info(f"Transitioned to {state}")

                # Ждём секунду мужду переходами
                await asyncio.sleep(1)
            else:
                logger.warning(f"Invalid transition from {self.state_machine.get_current_state()} to {state}")
                break

    async def send_to_kafka(self, command: dict):
        message = json.dumps(command).encode('utf-8')
        await self.producer.send_and_wait("runner_commands", message)
        logger.info(f"Command sent to Kafka: {command}")

async def main():
    logger.info('Orchestrator is started...')
    orchestrator = Orchestrator(kafka_server="localhost:9092", topic="orchestrator_commands")
    await orchestrator.start()
    await orchestrator.listen()

if __name__ == "__main__":
    asyncio.run(main())
