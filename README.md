# Распределённая система видеоаналитики

## Получение статуса
```batch
curl http://127.0.0.1:8000/scenario/1
```
Ожидаемый результат:
```json
{
  "id": "1",
  "current_state": "inactive",
  "parameters": {}
}
```

## POST-запрос на активацию сценария
```batch
curl -X POST "http://127.0.0.1:8000/scenario/1/state" -H "Content-Type: application/json" -d "{\"new_state\": \"active\"}"
```
Ожидаемый результат: 
```json
{
  "message": "State updated",
  "new_state": "active"
}
```

## POST-запрос на деактивацию сценария
```batch
curl -X POST "http://127.0.0.1:8000/scenario/1/state" -H "Content-Type: application/json" -d "{\"new_state\": \"inactive\"}"
```

Ожидаемый результат:
```json
{
  "message": "State updated",
  "new_state": "inactive"
}
```

## Проверка состояния API
```batch
curl http://127.0.0.1:8000/healthcheck
```
Ожидаемый результат:
```json
[{"status":"healthy","kafka_status":"healthy"},200]
```

## Получение результатов
```batch
curl http://127.0.0.1:8000/scenario/1/results
```
Ожидаемый результат:
`JSON с предсказаниями`
