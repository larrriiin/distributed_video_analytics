import aiohttp
import cv2
import asyncio
import json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from loguru import logger
import asyncpg

class Runner:
    def __init__(self, kafka_server: str, topic: str, inference_url: str, db_config: dict):
        self.kafka_server = kafka_server
        self.topic = topic
        self.consumer = None
        self.producer = None
        self.inference_url = inference_url
        self.db_config = db_config
        self.running = False
        self.frame_count = 0
        self.processing_task = None
        self.db_pool = None

        logger.add("runner_log.log", format="{time} {level} {message}", level="DEBUG", rotation="1 MB", compression="zip")

    async def start(self):
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.kafka_server,
            group_id="runner_group",
            auto_offset_reset="earliest"
        )
        self.producer = AIOKafkaProducer(bootstrap_servers=self.kafka_server)
        await self.consumer.start()
        await self.producer.start()

        self.db_pool = await asyncpg.create_pool(**self.db_config)
        logger.info("Connected to PostgreSQL and Kafka")

    async def stop(self):
        await self.consumer.stop()
        await self.producer.stop()
        await self.db_pool.close()
        logger.info("Runner and database connections closed")

    async def listen(self):
        try:
            async for msg in self.consumer:
                command = json.loads(msg.value.decode('utf-8'))
                logger.info(f"Decoded command: {command}")
                await self.handle_command(command)
        finally:
            await self.stop()

    async def handle_command(self, command: dict):
        scenario_id = command.get("scenario_id")
        new_state = command.get("new_state")

        if new_state == "active" and not self.running:
            # Получаем номер последнего обработанного кадра
            self.frame_count = await self.get_last_processed_frame(scenario_id)
            logger.info(f"Resuming scenario {scenario_id} from frame {self.frame_count}")
            self.running = True
            self.processing_task = asyncio.create_task(self.process_video(scenario_id))

        elif new_state == "inactive" and self.running:
            logger.info(f"Stopping processing for scenario {scenario_id}")
            self.running = False
            if self.processing_task:
                await self.processing_task

    async def process_video(self, scenario_id: str):
        video_path = f"videos/{scenario_id}.mp4"
        cap = cv2.VideoCapture(video_path)

        # Пропускаем кадры до последнего обработанного
        cap.set(cv2.CAP_PROP_POS_FRAMES, self.frame_count)

        while cap.isOpened():
            if not self.running:
                logger.info(f"Processing stopped for scenario {scenario_id}")
                break

            ret, frame = cap.read()
            if not ret:
                await self.send_state_update(scenario_id, "inactive")
                logger.info(f"Video processing completed for scenario {scenario_id}")
                break

            frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            frame_resized = cv2.resize(frame_rgb, (640, 640))

            inference_result = await self.send_frame_for_inference(frame_resized)

            if inference_result:
                await self.save_inference_result(scenario_id, self.frame_count, inference_result)

            self.frame_count += 1

        cap.release()

    async def send_frame_for_inference(self, frame):
        _, buffer = cv2.imencode('.jpg', frame)
        frame_data = buffer.tobytes()

        data = aiohttp.FormData()
        data.add_field('file', frame_data, filename='frame.jpg', content_type='image/jpeg')

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(self.inference_url, data=data) as response:
                    if response.content_type == "application/json":
                        result = await response.json()
                        return result['detections']
                    else:
                        text = await response.text()
                        logger.error(f"Error from inference service: {text}")
            except Exception as e:
                logger.error(f"Error sending frame to inference: {e}")

    async def save_inference_result(self, scenario_id: str, frame_number: int, detections: list):
        try:
            query = """
            INSERT INTO inference_results (scenario_id, frame_number, detections)
            VALUES ($1, $2, $3)
            """
            await self.db_pool.execute(query, scenario_id, frame_number, json.dumps(detections))
            logger.info(f"Saved inference result for frame {frame_number} of scenario {scenario_id}")
        except Exception as e:
            logger.error(f"Error saving inference result to database: {e}")

    async def get_last_processed_frame(self, scenario_id: str) -> int:
        """Получаем максимальный номер кадра для данного сценария из базы данных."""
        query = "SELECT MAX(frame_number) FROM inference_results WHERE scenario_id = $1"
        record = await self.db_pool.fetchrow(query, scenario_id)
        return record['max'] + 1 if record and record['max'] is not None else 0

    async def send_state_update(self, scenario_id: str, new_state: str):
        command = {"scenario_id": scenario_id, "new_state": new_state}
        message = json.dumps(command).encode('utf-8')
        await self.producer.send_and_wait("orchestrator_commands", message)
        logger.info(f"State updated to {new_state} for scenario {scenario_id}")

async def main():
    db_config = {
        "user": "root",
        "password": "Root1Root1",
        "database": "vidan_main",
        "host": "localhost"
    }
    runner = Runner(
        kafka_server="localhost:9092",
        topic="runner_commands",
        inference_url="http://localhost:8001/inference",
        db_config=db_config
    )
    await runner.start()
    await runner.listen()

if __name__ == "__main__":
    asyncio.run(main())
