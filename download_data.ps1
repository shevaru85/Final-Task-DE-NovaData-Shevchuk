# Скрипт для скачивания и подготовки данных

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Скрипт загрузки russian_houses.csv  " -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# URL файла - ЗАМЕНИТЕ на актуальную ссылку!
# Примеры возможных источников:
# - Google Drive: https://drive.google.com/uc?export=download&id=FILE_ID
# - Yandex Disk: https://disk.yandex.ru/d/xxxxx
# - Прямая ссылка: https://your-server.com/data/russian_houses.csv

$url = "REPLACE_WITH_ACTUAL_URL"
$outputPath = ".\dags\russian_houses.csv"

# Проверяем, указана ли реальная ссылка
if ($url -eq "REPLACE_WITH_ACTUAL_URL") {
    Write-Host "⚠️  ВНИМАНИЕ: URL не настроен!" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Пожалуйста, выполните одно из следующих действий:" -ForegroundColor White
    Write-Host ""
    Write-Host "1️⃣  Скачайте файл вручную:" -ForegroundColor Green
    Write-Host "   - Получите файл по ссылке из задания" -ForegroundColor Gray
    Write-Host "   - Сохраните как: .\dags\russian_houses.csv" -ForegroundColor Gray
    Write-Host ""
    Write-Host "2️⃣  Используйте sample данные для тестирования:" -ForegroundColor Green
    Write-Host "   Copy-Item '.\sample_data\russian_houses_sample.csv' '.\dags\russian_houses.csv'" -ForegroundColor Gray
    Write-Host ""
    Write-Host "3️⃣  Настройте этот скрипт:" -ForegroundColor Green
    Write-Host "   - Откройте download_data.ps1" -ForegroundColor Gray
    Write-Host "   - Замените REPLACE_WITH_ACTUAL_URL на реальную ссылку" -ForegroundColor Gray
    Write-Host ""
    Write-Host "Подробнее см. DATA_SOURCE.md" -ForegroundColor Cyan
    exit 1
}

# Проверяем, существует ли файл
if (Test-Path $outputPath) {
    Write-Host "⚠️  Файл уже существует: $outputPath" -ForegroundColor Yellow
    $fileSize = (Get-Item $outputPath).Length / 1MB
    Write-Host "   Размер: $([math]::Round($fileSize, 2)) MB" -ForegroundColor Gray
    Write-Host ""
    $response = Read-Host "Перезаписать? (y/n)"
    if ($response -ne "y") {
        Write-Host "✅ Скачивание отменено" -ForegroundColor Green
        exit 0
    }
}

# Создаем директорию dags, если не существует
if (-not (Test-Path ".\dags")) {
    New-Item -ItemType Directory -Path ".\dags" | Out-Null
    Write-Host "📁 Создана директория .\dags" -ForegroundColor Green
}

try {
    # Скачиваем файл
    Write-Host ""
    Write-Host "⏬ Загрузка файла..." -ForegroundColor Cyan
    Write-Host "   Источник: $url" -ForegroundColor Gray
    Write-Host "   Назначение: $outputPath" -ForegroundColor Gray
    Write-Host ""
    
    $ProgressPreference = 'Continue'
    Invoke-WebRequest -Uri $url -OutFile $outputPath -UseBasicParsing
    
    Write-Host ""
    Write-Host "✅ Файл успешно скачан!" -ForegroundColor Green
    Write-Host ""
    
    # Показываем информацию о файле
    $fileInfo = Get-Item $outputPath
    $fileSize = $fileInfo.Length / 1MB
    Write-Host "📊 Информация о файле:" -ForegroundColor Cyan
    Write-Host "   Путь: $outputPath" -ForegroundColor Gray
    Write-Host "   Размер: $([math]::Round($fileSize, 2)) MB" -ForegroundColor Gray
    Write-Host "   Дата: $($fileInfo.LastWriteTime)" -ForegroundColor Gray
    Write-Host ""
    
    # Проверяем количество строк (первые несколько)
    Write-Host "🔍 Проверка содержимого (первые 3 строки):" -ForegroundColor Cyan
    Get-Content $outputPath -Encoding Unicode -First 3 | ForEach-Object { Write-Host "   $_" -ForegroundColor Gray }
    Write-Host ""
    Write-Host "✅ Готово! Теперь можно запускать проект." -ForegroundColor Green
    
} catch {
    Write-Host ""
    Write-Host "❌ Ошибка при скачивании файла:" -ForegroundColor Red
    Write-Host "   $($_.Exception.Message)" -ForegroundColor Red
    Write-Host ""
    Write-Host "💡 Альтернативный способ:" -ForegroundColor Yellow
    Write-Host "   1. Скачайте файл вручную по ссылке из DATA_SOURCE.md" -ForegroundColor Gray
    Write-Host "   2. Поместите файл в папку: .\dags\russian_houses.csv" -ForegroundColor Gray
    Write-Host "   3. Убедитесь, что размер ~300 МБ, 590708 строк" -ForegroundColor Gray
    Write-Host ""
    exit 1
}
