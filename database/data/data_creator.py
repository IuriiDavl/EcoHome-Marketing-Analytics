import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Настройки
np.random.seed(42)
days = pd.date_range(start="2024-03-01", end="2026-03-01", freq='7D')  # каждую неделю ~104 недели, увеличим записи
# Кол-во кампаний и групп
google_campaigns = ["EcoHome_Solar", "EcoHome_Heating", "EcoHome_SmartHome"]
google_ad_groups = ["Group1", "Group2", "Group3"]

yandex_campaigns = ["EcoHome_Heating", "EcoHome_Solar", "EcoHome_SmartHome"]
banner_groups = ["Heat_A","Heat_B","Solar_A","Solar_B","SmartHome_A"]

fb_campaigns = ["EcoHome_SmartHome", "EcoHome_Solar", "EcoHome_Heating"]
ad_sets = ["Set1", "Set2", "Set3"]

product_categories = ["Solar Panels","Heating Systems","Smart Home"]
sources = ["google","yandex","facebook","direct","organic"]

# ============ Google Ads ============
google_ads_data = []
for date in days:
    for campaign in google_campaigns:
        for group in google_ad_groups:
            google_ads_data.append({
                "campaign_name": campaign,
                "ad_group_name": group,
                "date": date,
                "impressions": np.random.randint(5000, 25000),
                "clicks": np.random.randint(50, 700),
                "cost_usd": round(np.random.uniform(50, 400),2),
                "conversions": np.random.randint(0, 30),
                "utm_source": "google",
                "utm_medium": "cpc",
                "utm_campaign": campaign.lower().replace(" ", "_"),
                "device": np.random.choice(["mobile","desktop"])
            })
google_ads_df = pd.DataFrame(google_ads_data)

# ============ Яндекс.Директ ============
yandex_data = []
for date in days:
    for campaign in yandex_campaigns:
        for banner in banner_groups:
            yandex_data.append({
                "campaign_title": campaign,
                "banner_group": banner,
                "date": date,
                "shows": np.random.randint(5000, 20000),
                "clicks_count": np.random.randint(50, 600),
                "spend_rub": round(np.random.uniform(4000, 20000),2),
                "leads": np.random.randint(0, 35),
                "utm_source": "yandex",
                "utm_medium": "cpc",
                "utm_campaign": campaign.lower().replace(" ", "_"),
                "device_type": np.random.choice(["desktop","mobile"])
            })
yandex_df = pd.DataFrame(yandex_data)

# ============ Facebook Ads ============
fb_data = []
for date in days:
    for campaign in fb_campaigns:
        for ad_set in ad_sets:
            impressions = np.random.randint(5000,25000)
            clicks = np.random.randint(50,700)
            fb_data.append({
                "campaign": campaign,
                "ad_set": ad_set,
                "date": date,
                "impr": impressions,
                "clk": clicks,
                "spent_usd": round(np.random.uniform(50,400),2),
                "actions": np.random.randint(0,30),
                "ctr": round(clicks/impressions*100,2),
                "utm_source": "facebook",
                "utm_medium": "paid_social",
                "utm_campaign": campaign.lower().replace(" ", "_"),
                "placement": np.random.choice(["feed","stories"])
            })
fb_df = pd.DataFrame(fb_data)

# ============ CRM данные ============
crm_data = []
for date in days:
    for i in range(np.random.randint(5,15)):
        source = np.random.choice(sources, p=[0.3,0.3,0.2,0.1,0.1])
        utm_campaign = None if source in ["direct","organic"] else f"{source}_campaign"
        crm_data.append({
            "order_id": f"ORD_{date.strftime('%Y%m%d')}_{i}",
            "customer_id": f"CUST_{np.random.randint(100,999)}",
            "order_date": date,
            "order_amount_rub": round(np.random.uniform(5000,150000),2),
            "product_category": np.random.choice(product_categories),
            "utm_source": source,
            "utm_medium": "cpc" if source in ["google","yandex"] else ("paid_social" if source=="facebook" else None),
            "utm_campaign": utm_campaign
        })
crm_df = pd.DataFrame(crm_data)

# ============ Сохранение в CSV ============
google_ads_df.to_csv("data/google_ads_data.csv", index=False)
yandex_df.to_csv("data/yandex_direct_data.csv", index=False)
fb_df.to_csv("data/fb_ads_data.csv", index=False)
crm_df.to_csv("data/crm_orders_data.csv", index=False)

# Проверка размеров
len(google_ads_df), len(yandex_df), len(fb_df), len(crm_df)

