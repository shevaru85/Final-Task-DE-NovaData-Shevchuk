# Скрипт для скачивания и подготовки данных

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Скрипт загрузки russian_houses.csv  " -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

$url = "https://drive.google.com/file/d/1nVf9A7d0MzsVmDR0k_UMugtVLqVG4Gm9/view?usp=sharing"
$outputPath = ".\dags\russian_houses.csv"

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
    Write-Host "   1. Скачайте файл вручную по ссылке https://drive.google.com/file/d/1nVf9A7d0MzsVmDR0k_UMugtVLqVG4Gm9/view?usp=sharing -ForegroundColor Gray
    Write-Host "   2. Поместите файл в папку: .\dags\russian_houses.csv" -ForegroundColor Gray
    Write-Host "   3. Убедитесь, что размер ~300 МБ, 590708 строк" -ForegroundColor Gray
    Write-Host ""
    exit 1
}
