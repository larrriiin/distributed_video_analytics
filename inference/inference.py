from fastapi import FastAPI, UploadFile, File, HTTPException
from ultralytics import YOLO
import torch
import cv2
import numpy as np
from loguru import logger

logger.add("inference_log.log", format="{time} {level} {message}", level="DEBUG", rotation="1 MB", compression="zip")

logger.info("Logger initialized")  # Лог сразу после инициализации

app = FastAPI()

model_ready = False

try:
    model = YOLO("yolov8m.pt")
    device = 'cpu'  # Явно указываем, что используется CPU
    model.to(device)
    logger.info(f"Model loaded successfully on device: {device}")
    model_ready = True
except Exception as e:
    logger.error(f"Error loading model: {e}")
    raise RuntimeError(f"Error loading YOLO model: {e}")

@app.get("/healthcheck")
async def healthcheck():
    # Если модель не готова, возвращаем 500
    if not model_ready:
        return {"status": "unhealthy", "model_status": "not ready"}, 500

    return {"status": "healthy", "model_status": "ready"}, 200

@app.post("/inference")
async def inference(file: UploadFile = File(...)):
    try:
        image_data = await file.read()

        if not image_data:
            logger.error("Received empty file")
            raise HTTPException(status_code=400, detail="Empty file received")

        # Преобразуем байты в массив numpy
        nparr = np.frombuffer(image_data, np.uint8)

        image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        if image is None:
            logger.error("Error decoding image")
            raise HTTPException(status_code=400, detail="Error decoding image")

        logger.info(f"Image successfully decoded. Shape: {image.shape}")

        logger.info("Running inference on image")
        results = model(image)

        detections = []
        for result in results:
            for box in result.boxes:
                detection = {
                    "class": int(box.cls),  # Класс объекта
                    "confidence": float(box.conf),  # Уверенность предсказания
                    "box": box.xyxy.tolist()  # Координаты ограничивающего прямоугольника
                }
                detections.append(detection)

        logger.info(f"Inference successful!")

        # Возвращаем JSON с результатами
        return {"detections": detections}

    except Exception as e:
        logger.error(f"Error during processing: {e}")
        raise HTTPException(status_code=500, detail=str(e))

logger.info("Starting inference service...")

if __name__ == "__main__":
    logger.info("Starting uvicorn server...")
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8001)
