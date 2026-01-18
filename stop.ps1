# Скрипт остановки Airflow

Write-Host "Остановка всех контейнеров..." -ForegroundColor Yellow
docker-compose down

Write-Host "Контейнеры остановлены!" -ForegroundColor Green
