import cv2
import json
import asyncpg
import numpy as np
import asyncio
import os
from loguru import logger

# Параметры подключения к БД
db_config = {
    "user": "root",
    "password": "Root1Root1",
    "database": "vidan_main",
    "host": "localhost"
}

# Параметры обработки видео
SCENARIO_ID = "1"
OUTPUT_VIDEO_DIR = "processed_videos"
OUTPUT_VIDEO_PATH = os.path.join(OUTPUT_VIDEO_DIR, f"{SCENARIO_ID}_processed.mp4")
FRAME_WIDTH, FRAME_HEIGHT = 640, 640  # Размеры кадра

# Настройка логирования
logger.add("video_processing.log", format="{time} {level} {message}", level="DEBUG", rotation="1 MB", compression="zip")

async def fetch_detections_from_db(scenario_id):
    """Извлекает детекции из базы данных для заданного сценария."""
    try:
        conn = await asyncpg.connect(**db_config)
        query = """
        SELECT frame_number, detections
        FROM inference_results
        WHERE scenario_id = $1
        ORDER BY frame_number
        """
        rows = await conn.fetch(query, scenario_id)
        await conn.close()

        return [(row['frame_number'], json.loads(row['detections'])) for row in rows]

    except Exception as e:
        logger.error(f"Error fetching detections from database: {e}")
        return []

def draw_detections_on_frame(frame, detections):
    """Отрисовывает рамки детекций на кадре."""
    for detection in detections:
        # Извлечение координат и класса детекции
        x1, y1, x2, y2 = map(int, detection['box'][0])
        obj_class = detection['class']
        confidence = detection['confidence']

        # Отрисовка рамки и текста на кадре
        color = (0, 255, 0)  # Зелёный цвет рамки
        cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)
        label = f"Class: {obj_class} ({confidence:.2f})"
        cv2.putText(frame, label, (x1, y1 - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 2)

    return frame

async def create_annotated_video(scenario_id, output_path, frame_width, frame_height):
    """Создаёт видео с отрисованными детекциями."""
    # Создаём директорию, если её нет
    if not os.path.exists(OUTPUT_VIDEO_DIR):
        os.makedirs(OUTPUT_VIDEO_DIR)
        logger.info(f"Directory '{OUTPUT_VIDEO_DIR}' created.")

    # Получаем детекции из базы данных
    detections = await fetch_detections_from_db(scenario_id)

    if not detections:
        logger.error("No detections found for the given scenario.")
        return

    video_path = f"videos/{scenario_id}.mp4"
    cap = cv2.VideoCapture(video_path)

    # Настройка видеозаписи
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    out = cv2.VideoWriter(output_path, fourcc, 30.0, (frame_width, frame_height))

    frame_index = 0

    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break

        # Изменяем размер кадра до заданного
        frame = cv2.resize(frame, (frame_width, frame_height))

        # Проверяем, есть ли детекции для текущего кадра
        if frame_index in [d[0] for d in detections]:
            # Получаем детекции для текущего кадра
            frame_detections = [d[1] for d in detections if d[0] == frame_index][0]
            
            # Отрисовываем детекции на кадре
            frame = draw_detections_on_frame(frame, frame_detections)

        # Сохраняем обработанный кадр в видео
        out.write(frame)

        frame_index += 1

    # Освобождаем ресурсы
    cap.release()
    out.release()
    logger.info(f"Annotated video saved to {output_path}")

async def main():
    await create_annotated_video(SCENARIO_ID, OUTPUT_VIDEO_PATH, FRAME_WIDTH, FRAME_HEIGHT)

if __name__ == "__main__":
    asyncio.run(main())
