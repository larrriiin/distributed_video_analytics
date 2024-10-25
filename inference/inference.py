import asyncio
import redis
import json
import cv2
import numpy as np
from loguru import logger
from ultralytics import YOLO

# Настройка логирования
logger.add("inference_log.log", format="{time} {level} {message}", level="DEBUG", rotation="1 MB", compression="zip")

# Подключение к Redis
redis_client = redis.Redis(host="localhost", port=6379, decode_responses=False)

# Загрузка модели YOLO
try:
    model = YOLO("yolov8m.pt")
    device = 'cpu'
    model.to(device)
    logger.info(f"Model loaded successfully on device: {device}")
except Exception as e:
    logger.error(f"Error loading YOLO model: {e}")
    raise RuntimeError(f"Error loading YOLO model: {e}")

# Количество одновременных задач
MAX_CONCURRENT_TASKS = 10
semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

async def process_frame(frame_key):
    """Асинхронная обработка одного кадра из Redis."""
    async with semaphore:
        try:
            frame_data = redis_client.get(frame_key)
            if not frame_data:
                logger.warning(f"Frame data not found for {frame_key}")
                return

            scenario_id, frame_number = parse_frame_key(frame_key)

            # Декодирование изображения
            frame = cv2.imdecode(np.frombuffer(frame_data, np.uint8), cv2.IMREAD_COLOR)
            if frame is None:
                logger.error(f"Error decoding frame for {frame_key}")
                return

            # Выполнение инференса
            detections = run_inference(frame)

            # Сохранение результатов в Redis
            await save_results_to_redis(scenario_id, frame_number, frame, detections)

            # Удаление кадра из Redis
            redis_client.delete(frame_key)
            logger.info(f"Processed frame {frame_number} of scenario {scenario_id}")

        except Exception as e:
            logger.error(f"Error processing frame {frame_key}: {e}")

async def process_frames_from_redis():
    """Фоновая задача для асинхронной обработки кадров из Redis."""
    while True:
        try:
            frame_keys = redis_client.keys("frame:*")
            tasks = [process_frame(frame_key) for frame_key in frame_keys]

            if tasks:
                await asyncio.gather(*tasks)

        except Exception as e:
            logger.error(f"Error processing frames from Redis: {e}")

        await asyncio.sleep(0.1)  # Задержка перед повторной проверкой

def run_inference(frame):
    """Выполнение инференса на изображении с использованием YOLO."""
    results = model(frame)

    detections = []
    for result in results:
        for box in result.boxes:
            detection = {
                "class": int(box.cls),
                "confidence": float(box.conf),
                "box": box.xyxy.tolist()
            }
            detections.append(detection)

    return detections

async def save_results_to_redis(scenario_id, frame_number, frame, detections):
    """Сохранение результатов инференса в Redis."""
    try:
        _, buffer = cv2.imencode('.jpg', frame)

        redis_frame_key = f"processed_frame:{scenario_id}:{frame_number}"
        redis_detection_key = f"detections:{scenario_id}:{frame_number}"

        redis_client.set(redis_frame_key, buffer.tobytes())
        redis_client.set(redis_detection_key, json.dumps(detections))

        logger.info(f"Saved results for frame {frame_number} of scenario {scenario_id} in Redis")

    except Exception as e:
        logger.error(f"Error saving results to Redis for frame {frame_number} of scenario {scenario_id}: {e}")

def parse_frame_key(frame_key):
    """Парсинг ключа кадра из Redis для получения scenario_id и frame_number."""
    _, scenario_id, frame_number = frame_key.decode().split(":")
    return scenario_id, int(frame_number)

if __name__ == "__main__":
    logger.info("Starting inference service...")
    redis_client.flushall()
    logger.info("Redis is cleared!")
    try:
        asyncio.run(process_frames_from_redis())
    except KeyboardInterrupt:
        logger.info("Stopping inference service...")