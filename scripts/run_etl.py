#!/usr/bin/env python3
"""
Скрипт для запуска всех ETL-процессов.
Может быть запущен по расписанию через планировщик задач.
"""

import sys
import os
import logging
from datetime import datetime

# Добавляем корень проекта в path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Настройка логирования
log_dir = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "main_etl.log"), encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def run_etl_pipeline():
    """Запускает все ETL-скрипты последовательно."""
    logger.info("=" * 50)
    logger.info(f"Запуск ETL-пайплайна: {datetime.now()}")
    
    scripts = [
        ("Google Ads", "etl.google_ads_etl"),
        ("Yandex Direct", "etl.yandex_direct_etl"),
        ("Facebook Ads", "etl.facebook_ads_etl"),
    ]
    
    results = []
    
    for name, module_path in scripts:
        try:
            logger.info(f"▶ Запуск {name}...")
            # Импортируем модуль и вызываем main()
            module = __import__(module_path, fromlist=['main'])
            module.main()
            logger.info(f"✓ {name} завершён успешно")
            results.append((name, "OK"))
        except Exception as e:
            logger.error(f"✗ {name} завершился с ошибкой: {e}")
            results.append((name, f"ERROR: {e}"))
    
    # Итоговый отчёт
    logger.info("=" * 50)
    logger.info("Итоги запуска:")
    for name, status in results:
        logger.info(f"  {name}: {status}")
    logger.info(f"Завершение ETL-пайплайна: {datetime.now()}")
    
    return results


if __name__ == "__main__":
    run_etl_pipeline()
