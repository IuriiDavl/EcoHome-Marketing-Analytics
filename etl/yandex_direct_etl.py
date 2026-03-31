import sys
import os
sys.path.insert(0, r'c:/repositories/Marketing dashboard')

import yaml
import logging
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import requests

# Настройка логирования
log_dir = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "yandex_direct_etl.log"), encoding="utf-8"),
        logging.StreamHandler()
    ]
)

def connect_db(db_config):
    """Устанавливает соединение с базой данных PostgreSQL."""
    try:
        conn = psycopg2.connect(**db_config)
        logging.info("Успешное подключение к базе данных.")
        return conn
    except Exception as e:
        logging.error(f"Ошибка подключения к базе данных: {e}")
        raise

def get_last_date(conn, table_name):
    """Получает последнюю дату из указанной таблицы БД."""
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT MAX(date) FROM {table_name}")
            last_date = cur.fetchone()[0]
            if last_date:
                logging.info(f"Последняя дата в таблице {table_name}: {last_date}")
                return datetime.strptime(str(last_date), "%Y-%m-%d").date()
            return None
    except Exception as e:
        logging.error(f"Ошибка при получении последней даты из БД: {e}")
        raise

def fetch_yandex_direct_data(yandex_direct_config, start_date, end_date):
    """Извлекает данные из Yandex Direct API инкрементально."""
    token = yandex_direct_config["token"]
    headers = {
        "Authorization": f"Bearer {token}",
        "Client-Login": yandex_direct_config.get("client_login"), # Опционально
        "Accept-Language": "ru",
    }
    
    # URL для Yandex Direct API (Reports service)
    # В реальном проекте здесь будет логика формирования запросов к API
    # и обработка пагинации, если это необходимо.
    # Для примера используем заглушку.

    logging.info(f"Извлечение данных Yandex Direct с {start_date} по {end_date} (используются заглушки).")
    data = {
        "date": [start_date.strftime("%Y-%m-%d"), start_date.strftime("%Y-%m-%d"), (start_date + timedelta(days=1)).strftime("%Y-%m-%d")],
        "campaign_title": ["Yandex Campaign X", "Yandex Campaign Y", "Yandex Campaign X"],
        "banner_group": ["Group 1", "Group 2", "Group 1"],
        "shows": [2000, 2500, 3000],
        "clicks_count": [100, 120, 150],
        "spend_rub": [200.00, 250.00, 300.00],
        "leads": [10, 12, 15],
        "utm_source": ["yandex", "yandex", "yandex"],
        "utm_medium": ["cpc", "cpc", "cpc"],
        "utm_campaign": ["ecohome_heating", "ecohome_solar", "ecohome_heating"]
    }
    return pd.DataFrame(data)

def save_to_db(df, conn, table_name):
    """Сохраняет DataFrame в базу данных с обработкой конфликтов (UPSERT)."""
    if df.empty:
        logging.info(f"DataFrame пуст, нет данных для сохранения в {table_name}.")
        return

    temp_table_name = f"temp_{table_name}"
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            conn.commit()

            df.to_sql(temp_table_name, conn, if_exists='replace', index=False)
            conn.commit()
            logging.info(f"Данные успешно загружены во временную таблицу {temp_table_name}.")

            columns = ", ".join(df.columns)
            # Для Yandex Direct предположим уникальный индекс по (date, campaign_title, banner_group)
            update_columns = ", ".join([f"{col}=EXCLUDED.{col}" for col in df.columns if col not in ["date", "campaign_title", "banner_group"]])
            
            upsert_query = f"""
                INSERT INTO {table_name} ({columns})
                SELECT {columns} FROM {temp_table_name}
                ON CONFLICT (date, campaign_title, banner_group) DO UPDATE SET
                    {update_columns}
            """
            cur.execute(upsert_query)
            conn.commit()
            logging.info(f"Данные успешно UPSERT-нуты в таблицу {table_name}.")
            
    except Exception as e:
        logging.error(f"Ошибка при сохранении данных в базу данных: {e}")
        conn.rollback()
        raise
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            conn.commit()

def main():
    try:
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)
        
        db_config = config["database"]
        yandex_direct_config = config["yandex_direct_api"]

        conn = connect_db(db_config)
        
        last_date = get_last_date(conn, "yandex_direct_data")
        start_date = last_date + timedelta(days=1) if last_date else datetime.now().date() - timedelta(days=30)
        end_date = datetime.now().date()

        logging.info(f"Загрузка данных Yandex Direct с {start_date.strftime("%Y-%m-%d")} по {end_date.strftime("%Y-%m-%d")}")

        df = fetch_yandex_direct_data(yandex_direct_config, start_date, end_date)
        
        save_to_db(df, conn, "yandex_direct_data")

    except Exception as e:
        logging.error(f"Критическая ошибка в Yandex Direct ETL: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            logging.info("Соединение с базой данных закрыто.")

if __name__ == '__main__':
    main()