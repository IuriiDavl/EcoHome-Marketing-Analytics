/*
Скрипт написан под SQLite.
INSERT OR REPLACE заменяет старые строки, если есть совпадение по уникальному индексу.
Обновление CRM и финальной витрины ограничено последними 7 днями для скорости.
В реальной SQL среде лучше использовать процедуру
*/

-------------------------------------------------
-- 1. Обновление данных marketing_ads_normalized
-------------------------------------------------

-- Google Ads
INSERT OR REPLACE INTO marketing_ads_normalized (
    date, campaign, ad_group, source,
    impressions, clicks, cost_rub, conversions,
    utm_source, utm_medium, utm_campaign
)
SELECT
    date,
    campaign_name,
    ad_group_name,
    'google_ads',
    impressions,
    clicks,
    cost_usd * 90,
    conversions,
    utm_source,
    utm_medium,
    utm_campaign
FROM google_ads_data;

-- Yandex Direct
INSERT OR REPLACE INTO marketing_ads_normalized (
    date, campaign, ad_group, source,
    impressions, clicks, cost_rub, conversions,
    utm_source, utm_medium, utm_campaign
)
SELECT
    date,
    campaign_title,
    banner_group,
    'yandex_direct',
    shows,
    clicks_count,
    spend_rub,
    leads,
    utm_source,
    utm_medium,
    utm_campaign
FROM yandex_direct_data;

-- Facebook Ads
INSERT OR REPLACE INTO marketing_ads_normalized (
    date, campaign, ad_group, source,
    impressions, clicks, cost_rub, conversions,
    utm_source, utm_medium, utm_campaign
)
SELECT
    date,
    campaign,
    ad_set,
    'facebook_ads',
    impr,
    clk,
    spent_usd * 90,
    actions,
    utm_source,
    utm_medium,
    utm_campaign
FROM fb_ads_data;


-------------------------------------------------
-- 2. Обновление crm_sales_aggregated
-------------------------------------------------
INSERT OR REPLACE INTO crm_sales_aggregated (
    date, utm_source, utm_medium, utm_campaign,
    orders, revenue, customers
)
SELECT
    order_date,
    utm_source,
    utm_medium,
	CASE 
		WHEN product_category LIKE '%Solar%' AND utm_source IN ('google', 'yandex', 'facebook') THEN 'ecohome_solar'
		WHEN product_category LIKE '%Smart%' AND utm_source IN ('google', 'yandex', 'facebook') THEN 'ecohome_smarthome'
		WHEN product_category LIKE '%Heating%' AND utm_source IN ('google', 'yandex', 'facebook') THEN 'ecohome_heating'
		ELSE NULL
    END AS utm_campaign,
    COUNT(order_id),
    SUM(order_amount_rub),
    COUNT(DISTINCT customer_id)
FROM crm_orders_data
WHERE order_date >= DATE('now', '-7 day')
GROUP BY
    order_date,
    utm_source,
    utm_medium,
    utm_campaign;


-------------------------------------------------
-- 3. Обновление финальной витрины marketing_ads_crm_daily
-------------------------------------------------
INSERT OR REPLACE INTO marketing_ads_crm_daily (
    date, source, campaign,
    impressions, clicks, cost_rub, conversions,
    orders, revenue, customers
)
SELECT
    m.date,
    m.source,
    m.campaign,
    sum(m.impressions),
    sum(m.clicks),
    sum(m.cost_rub),
    sum(m.conversions),
    c.orders,
    c.revenue,
    c.customers
FROM marketing_ads_normalized m
LEFT JOIN crm_sales_aggregated c
    ON m.date = c.date
    AND m.utm_source = c.utm_source
    AND m.utm_campaign = c.utm_campaign
WHERE m.date >= DATE('now', '-7 day')
GROUP BY m.date,
    m.source,
    m.campaign,
    c.orders,
    c.revenue,
    c.customers

