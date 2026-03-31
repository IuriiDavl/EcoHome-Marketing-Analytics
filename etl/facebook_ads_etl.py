import sys
import os
sys.path.insert(0, r"c:/repositories/Marketing dashboard")

import yaml
import logging
import psycopg2
import pandas as pd
from datetime import datetime, timedelta

# from facebook_business.api import FacebookAdsApi
# from facebook_business.adobjects.adaccount import AdAccount
# from facebook_business.adobjects.adsinsights import AdsInsights

# Настройка логирования
log_dir = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "facebook_ads_etl.log"), encoding="utf-8"),
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

def init_facebook_ads_api(facebook_ads_config):
    """Инициализирует Facebook Ads API."""
    # В реальном проекте здесь будет инициализация FacebookAdsApi
    # и AdAccount. Для примера используем заглушку.
    logging.info("Инициализация Facebook Ads API (заглушка).")
    # FacebookAdsApi.init(
    #     facebook_ads_config["app_id"],
    #     facebook_ads_config["app_secret"],
    #     facebook_ads_config["access_token"]
    # )
    # return AdAccount(facebook_ads_config["ad_account_id"])
    return True # Заглушка

def fetch_facebook_ads_data(ad_account, start_date, end_date):
    """Извлекает данные из Facebook Ads API инкрементально."""
    logging.info(f"Извлечение данных Facebook Ads с {start_date} по {end_date} (используются заглушки).")
    data = {
        "date": [start_date.strftime("%Y-%m-%d"), start_date.strftime("%Y-%m-%d"), (start_date + timedelta(days=1)).strftime("%Y-%m-%d")],
        "campaign": ["FB Campaign A", "FB Campaign B", "FB Campaign A"],
        "ad_set": ["Ad Set 1", "Ad Set 2", "Ad Set 1"],
        "impr": [5000, 6000, 7000],
        "clk": [200, 250, 300],
        "spent_usd": [50.00, 60.00, 70.00],
        "actions": [20, 25, 30],
        "utm_source": ["facebook", "facebook", "facebook"],
        "utm_medium": ["cpc", "cpm", "cpc"],
        "utm_campaign": ["ecohome_smarthome", "ecohome_heating", "ecohome_smarthome"]
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
            # Для Facebook Ads предположим уникальный индекс по (date, campaign, ad_set)
            update_columns = ", ".join([f"{col}=EXCLUDED.{col}" for col in df.columns if col not in ["date", "campaign", "ad_set"]])
            
            upsert_query = f"""
                INSERT INTO {table_name} ({columns})
                SELECT {columns} FROM {temp_table_name}
                ON CONFLICT (date, campaign, ad_set) DO UPDATE SET
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
        facebook_ads_config = config["facebook_ads_api"]

        conn = connect_db(db_config)
        
        last_date = get_last_date(conn, "fb_ads_data")
        start_date = last_date + timedelta(days=1) if last_date else datetime.now().date() - timedelta(days=30)
        end_date = datetime.now().date()

        logging.info(f"Загрузка данных Facebook Ads с {start_date.strftime("%Y-%m-%d")} по {end_date.strftime("%Y-%m-%d")}")

        facebook_ad_account = init_facebook_ads_api(facebook_ads_config)
        df = fetch_facebook_ads_data(facebook_ad_account, start_date, end_date)
        
        save_to_db(df, conn, "fb_ads_data")

    except Exception as e:
        logging.error(f"Критическая ошибка в Facebook Ads ETL: {e}")
    finally:
        if "conn" in locals() and conn:
            conn.close()
            logging.info("Соединение с базой данных закрыто.")

if __name__ == "__main__":
    main()