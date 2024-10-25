import subprocess
import time
from loguru import logger

# Команды для запуска каждого сервиса
commands = [
    ["uvicorn", "api.main:app", "--host", "127.0.0.1", "--port", "8000"],  # API
    ["python", "orchestrator/orchestrator.py"],                            # Orchestrator
    ["python", "runner/runner.py"],                                        # Runner
    ["python", "inference/inference.py"],  # Inference
]

# Список процессов
processes = []

try:
    for command in commands:
        # Запускаем каждый сервис
        logger.info(f"Запуск команды: {' '.join(command)}")
        process = subprocess.Popen(command)
        processes.append(process)
        time.sleep(1)  # Небольшая задержка между запусками

    # Ожидание завершения процессов
    for process in processes:
        process.wait()

except KeyboardInterrupt:
    logger.info("Остановка всех сервисов...")
    for process in processes:
        process.terminate()
    logger.success('Готово!')
