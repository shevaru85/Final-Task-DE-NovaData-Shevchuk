# Скрипт запуска Airflow в Docker

Write-Host "Запуск Airflow с помощью Docker Compose..." -ForegroundColor Green

# Проверка наличия docker
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host "ОШИБКА: Docker не установлен или недоступен" -ForegroundColor Red
    exit 1
}

# Проверка наличия docker-compose
if (-not (Get-Command docker-compose -ErrorAction SilentlyContinue)) {
    Write-Host "ОШИБКА: Docker Compose не установлен или недоступен" -ForegroundColor Red
    exit 1
}

# Создание необходимых директорий
Write-Host "Создание директорий..." -ForegroundColor Yellow
New-Item -ItemType Directory -Force -Path ".\logs" | Out-Null
New-Item -ItemType Directory -Force -Path ".\plugins" | Out-Null
New-Item -ItemType Directory -Force -Path ".\config" | Out-Null

# Остановка существующих контейнеров
Write-Host "Остановка существующих контейнеров..." -ForegroundColor Yellow
docker-compose down

# Сборка образов
Write-Host "Сборка Docker образов..." -ForegroundColor Yellow
docker-compose build

# Запуск контейнеров
Write-Host "Запуск контейнеров..." -ForegroundColor Yellow
docker-compose up -d

Write-Host "`nКонтейнеры запущены!" -ForegroundColor Green
Write-Host "`nAirflow UI будет доступен через несколько минут на: http://localhost:8080" -ForegroundColor Cyan
Write-Host "Логин: airflow" -ForegroundColor Cyan
Write-Host "Пароль: airflow" -ForegroundColor Cyan
Write-Host "`nGrafana UI: http://localhost:3000 (admin/admin)" -ForegroundColor Cyan
Write-Host "ClickHouse: http://localhost:8123" -ForegroundColor Cyan
Write-Host "PostgreSQL: localhost:5050" -ForegroundColor Cyan

Write-Host "`nДля просмотра логов используйте: docker-compose logs -f" -ForegroundColor Yellow
Write-Host "Для остановки используйте: docker-compose down" -ForegroundColor Yellow
