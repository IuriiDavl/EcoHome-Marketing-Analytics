import sys
import os
sys.path.insert(0, r'c:/repositories/Marketing dashboard')

import yaml
import logging
import psycopg2
import pandas as pd
from datetime import datetime, timedelta

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

# Настройка логирования
log_dir = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "google_ads_etl.log"), encoding="utf-8"),
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

def init_google_ads_client(google_ads_config):
    """Инициализирует клиент Google Ads API."""
    try:
        client = GoogleAdsClient.load_from_dict(
            google_ads_config
        )
        return client
    except Exception as e:
        logging.error(f"Ошибка инициализации Google Ads клиента: {e}")
        raise

def fetch_google_ads_data(client, customer_id, start_date, end_date):
    """Извлекает данные из Google Ads API инкрементально."""
    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT
            campaign.name,
            ad_group.name,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            segments.date
        FROM campaign
        WHERE segments.date BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
        ORDER BY segments.date
    """
    
    rows = []
    try:
        stream = ga_service.search_stream(customer_id=customer_id, query=query)
        for batch in stream:
            for row in batch.results:
                campaign_name = row.campaign.name
                ad_group_name = row.ad_group.name
                impressions = row.metrics.impressions
                clicks = row.metrics.clicks
                cost_usd = row.metrics.cost_micros / 1_000_000 # Переводим микро-доллары в доллары
                conversions = row.metrics.conversions
                date = row.segments.date

                # Предполагаем, что UTМ-метки можно извлечь из campaign_name или ad_group_name
                # или они будут предоставлены другим способом. Для примера используем заглушки.
                utm_source = 'google'
                utm_medium = 'cpc'
                utm_campaign = 'ecohome_solar' # Это должно быть более динамично

                rows.append([
                    date, campaign_name, ad_group_name, impressions, clicks, cost_usd,
                    conversions, utm_source, utm_medium, utm_campaign
                ])
        logging.info(f"Извлечено {len(rows)} строк из Google Ads API.")
        return pd.DataFrame(rows, columns=[
            'date', 'campaign_name', 'ad_group_name', 'impressions', 'clicks',
            'cost_usd', 'conversions', 'utm_source', 'utm_medium', 'utm_campaign'
        ])
    except GoogleAdsException as ex:
        logging.error(f"Ошибка Google Ads API: {ex}")
        raise
    except Exception as e:
        logging.error(f"Ошибка при извлечении данных Google Ads: {e}")
        raise

def save_to_db(df, conn, table_name):
    """Сохраняет DataFrame в базу данных с обработкой конфликтов (UPSERT)."""
    if df.empty:
        logging.info(f"DataFrame пуст, нет данных для сохранения в {table_name}.")
        return

    # Создаем временную таблицу для загрузки данных
    temp_table_name = f"temp_{table_name}"
    try:
        with conn.cursor() as cur:
            # Удаляем временную таблицу, если она существует
            cur.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            conn.commit()

            # Загружаем данные во временную таблицу
            df.to_sql(temp_table_name, conn, if_exists='replace', index=False)
            conn.commit()
            logging.info(f"Данные успешно загружены во временную таблицу {temp_table_name}.")

            # Формируем список столбцов для вставки и обновления
            columns = ", ".join(df.columns)
            update_columns = ", ".join([f"{col}=EXCLUDED.{col}" for col in df.columns if col not in ['date', 'campaign_name', 'ad_group_name']])
            
            # Выполняем UPSERT
            # Предполагается, что существует уникальный индекс по (date, campaign_name, ad_group_name)
            upsert_query = f"""
                INSERT INTO {table_name} ({columns})
                SELECT {columns} FROM {temp_table_name}
                ON CONFLICT (date, campaign_name, ad_group_name) DO UPDATE SET
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
        # Удаляем временную таблицу
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            conn.commit()

def main():
    try:
        # Загрузка конфигурации
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)
        
        db_config = config["database"]
        google_ads_config = config["google_ads_api"]
        customer_id = google_ads_config["customer_id"]

        # Подключение к БД
        conn = connect_db(db_config)
        
        # Получение последней даты для инкрементальной загрузки
        last_date = get_last_date(conn, 'google_ads_data')
        start_date = last_date + timedelta(days=1) if last_date else datetime.now().date() - timedelta(days=30) # Загружаем за последний месяц, если данных нет
        end_date = datetime.now().date()

        logging.info(f"Загрузка данных Google Ads с {start_date.strftime('%Y-%m-%d')} по {end_date.strftime('%Y-%m-%d')}")

        # Инициализация клиента Google Ads
        google_ads_client = init_google_ads_client(google_ads_config)

        # Извлечение данных
        df = fetch_google_ads_data(google_ads_client, customer_id, start_date, end_date)
        
        # Сохранение данных в БД
        save_to_db(df, conn, 'google_ads_data')

    except Exception as e:
        logging.error(f"Критическая ошибка в Google Ads ETL: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            logging.info("Соединение с базой данных закрыто.")

if __name__ == '__main__':
    main()