import aiohttp
import cv2
import asyncio
import json
from aiokafka import AIOKafkaConsumer
from loguru import logger

class Runner:
    def __init__(self, kafka_server: str, topic: str, inference_url: str):
        self.kafka_server = kafka_server
        self.topic = topic
        self.consumer = None
        self.inference_url = inference_url  # URL для отправки кадра в инференс сервис
        self.running = False
        self.frame_count = 0

        # Настройка loguru
        logger.add("runner_log.log", format="{time} {level} {message}", level="DEBUG", rotation="1 MB", compression="zip")

    async def start(self):
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.kafka_server,
            group_id="runner_group",
            auto_offset_reset="earliest"
        )
        await self.consumer.start()
        logger.info("Runner started and listening for commands...")

    async def listen(self):
        try:
            async for msg in self.consumer:
                command = json.loads(msg.value.decode('utf-8'))
                logger.info(f"Decoded command: {command}")
                await self.handle_command(command)
        finally:
            await self.consumer.stop()

    async def handle_command(self, command: dict):
        scenario_id = command.get("scenario_id")
        new_state = command.get("new_state")

        if new_state == "active" and not self.running:
            logger.info(f"Starting processing for scenario {scenario_id}")
            self.running = True
            await self.process_video(scenario_id)

        elif new_state == "inactive" and self.running:
            logger.info(f"Stopping processing for scenario {scenario_id}")
            self.running = False

    async def process_video(self, scenario_id: str):
        video_path = f"videos/{scenario_id}.mp4"  # Путь к видеофайлу
        cap = cv2.VideoCapture(video_path)

        while self.running and cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                break

            # Преобразуем кадр в нужный формат (например, BGR2RGB)
            frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)

            # Изменение размера кадра (например, 640x640)
            frame_resized = cv2.resize(frame_rgb, (640, 640))

            # Отправляем кадр на инференс сервис
            await self.send_frame_for_inference(frame_resized)

            self.frame_count += 1

        cap.release()

    async def send_frame_for_inference(self, frame):
        _, buffer = cv2.imencode('.jpg', frame)
        frame_data = buffer.tobytes()

        # Создаем форму для отправки
        data = aiohttp.FormData()
        data.add_field('file', frame_data, filename='frame.jpg', content_type='image/jpeg')

        async with aiohttp.ClientSession() as session:
            try:
                # Отправляем POST запрос на инференс сервис
                async with session.post(self.inference_url, data=data) as response:
                    if response.content_type == "application/json":
                        result = await response.json()
                        logger.info(f"Inference result: {result}")
                    else:
                        text = await response.text()
                        logger.error(f"Error from inference service: {text}")
            except Exception as e:
                logger.error(f"Error sending frame to inference: {e}")

# Пример использования
async def main():
    runner = Runner(kafka_server="localhost:9092", topic="runner_commands", inference_url="http://localhost:8001/inference")
    await runner.start()
    await runner.listen()

if __name__ == "__main__":
    asyncio.run(main())