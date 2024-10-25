import aiohttp
import cv2
import asyncio
import json
import redis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from loguru import logger
import asyncpg

class Runner:
    def __init__(self, kafka_server: str, topic: str, db_config: dict):
        self.kafka_server = kafka_server
        self.topic = topic
        self.consumer = None
        self.producer = None
        self.db_config = db_config
        self.running = False
        self.processing_task = None
        self.db_pool = None
        self.all_frames_sent = False

        # Настройка Redis
        self.redis_client = redis.Redis(host="localhost", port=6379, decode_responses=False)

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
            self.running = True
            self.all_frames_sent = False

            # Получаем список всех необработанных кадров
            max_frame_count = self.get_total_frames(scenario_id)
            unprocessed_frames = await self.get_unprocessed_frames(scenario_id, max_frame_count)
            logger.info(f"Found {len(unprocessed_frames)} unprocessed frames for scenario {scenario_id}")

            # Запускаем обработку видео и периодическую проверку результатов
            self.processing_task = asyncio.create_task(self.process_video(scenario_id, unprocessed_frames, max_frame_count))
            self.result_check_task = asyncio.create_task(self.periodic_result_check(scenario_id, max_frame_count))

        elif new_state == "inactive" and self.running:
            logger.info(f"Stopping processing for scenario {scenario_id}")
            self.running = False
            if self.processing_task:
                await self.processing_task
            if self.result_check_task:
                await self.result_check_task

    def get_total_frames(self, scenario_id: str) -> int:
        """Возвращает общее количество кадров в видео."""
        video_path = f"videos/{scenario_id}.mp4"
        cap = cv2.VideoCapture(video_path)
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        cap.release()
        return total_frames

    async def get_unprocessed_frames(self, scenario_id: str, max_frame_count: int) -> list:
        """Получаем список номеров необработанных кадров."""
        try:
            # Получаем обработанные кадры из базы данных
            query = """
            SELECT frame_number
            FROM inference_results
            WHERE scenario_id = $1
            """
            records = await self.db_pool.fetch(query, scenario_id)
            processed_frames = {record['frame_number'] for record in records}

            # Получаем все кадры до max_frame_count
            all_frames = set(range(max_frame_count))

            # Находим необработанные кадры
            unprocessed_frames = list(all_frames - processed_frames)
            unprocessed_frames.sort()

            logger.info(f"Unprocessed frames for scenario {scenario_id}: {unprocessed_frames}")
            return unprocessed_frames

        except Exception as e:
            logger.error(f"Error fetching unprocessed frames: {e}")
            return []

    async def process_video(self, scenario_id: str, unprocessed_frames: list, max_frame_count: int):
        video_path = f"videos/{scenario_id}.mp4"
        cap = cv2.VideoCapture(video_path)

        if unprocessed_frames:
            first_unprocessed = min(unprocessed_frames)
            cap.set(cv2.CAP_PROP_POS_FRAMES, first_unprocessed)

        while cap.isOpened() and self.running:
            ret, frame = cap.read()
            if not ret:
                logger.info(f"End of video for scenario {scenario_id}.")
                self.all_frames_sent = True
                break

            current_frame = int(cap.get(cv2.CAP_PROP_POS_FRAMES)) - 1
            if current_frame in unprocessed_frames:
                frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                frame_resized = cv2.resize(frame_rgb, (640, 640))
                await self.send_frame_to_redis(scenario_id, current_frame, frame_resized)

                unprocessed_frames.remove(current_frame)

            if not unprocessed_frames:
                logger.info(f"All unprocessed frames sent for scenario {scenario_id}.")
                self.all_frames_sent = True
                break

        cap.release()

    async def send_frame_to_redis(self, scenario_id: str, frame_number: int, frame):
        """Отправляет кадр в Redis."""
        try:
            _, buffer = cv2.imencode('.jpg', frame)
            frame_key = f"frame:{scenario_id}:{frame_number}"
            self.redis_client.set(frame_key, buffer.tobytes())
            logger.info(f"Frame {frame_number} sent to Redis for scenario {scenario_id}")
        except Exception as e:
            logger.error(f"Error sending frame to Redis for frame {frame_number} of scenario {scenario_id}: {e}")

    async def periodic_result_check(self, scenario_id: str, max_frame_count: int):
        """Периодически проверяет наличие результатов инференса в Redis и сохраняет их в базу данных."""
        while self.running or not self.all_frames_sent:
            try:
                all_results_received = True

                for frame_number in range(max_frame_count):
                    result_saved = await self.save_result_from_redis(scenario_id, frame_number)
                    if not result_saved:
                        all_results_received = False

                if self.all_frames_sent and all_results_received:
                    logger.info(f"All frames processed and saved for scenario {scenario_id}.")
                    await self.send_state_update(scenario_id, "inactive")
                    break

                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"Error during periodic result check for scenario {scenario_id}: {e}")

    async def save_result_from_redis(self, scenario_id: str, frame_number: int) -> bool:
        """Проверяет и сохраняет результаты инференса из Redis."""
        detection_key = f"detections:{scenario_id}:{frame_number}"
        detections = self.redis_client.get(detection_key)

        if detections:
            await self.save_inference_result(scenario_id, frame_number, json.loads(detections))

            self.redis_client.delete(detection_key)
            logger.info(f"Saved and deleted results for frame {frame_number} of scenario {scenario_id}")
            return True
        return False

    async def save_inference_result(self, scenario_id: str, frame_number: int, detections: list):
        """Сохраняет результаты инференса в базу данных."""
        try:
            query = """
            INSERT INTO inference_results (scenario_id, frame_number, detections)
            VALUES ($1, $2, $3)
            """
            await self.db_pool.execute(query, scenario_id, frame_number, json.dumps(detections))
            logger.info(f"Saved inference result for frame {frame_number} of scenario {scenario_id}")
        except Exception as e:
            logger.error(f"Error saving inference result to database: {e}")

    async def send_state_update(self, scenario_id: str, new_state: str):
        """Отправляет обновление состояния в Kafka."""
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
        db_config=db_config
    )

    await runner.start()
    await runner.listen()

if __name__ == "__main__":
    asyncio.run(main())
