from ultralytics import YOLO
import cv2
import torch

# Загружаем YOLOv8 модель
model = YOLO("yolov8m.pt")

# Переключаем на CPU, даже если CUDA доступна
device = 'cpu'
model.to(device)

# Загружаем тестовое изображение
image = cv2.imread("test_image.jpg")

# Запуск инференса
results = model(image)

# Выводим результаты
for result in results:
    for box in result.boxes:
        print({
            "class": int(box.cls),
            "confidence": float(box.conf),
            "box": box.xyxy.tolist()
        })
