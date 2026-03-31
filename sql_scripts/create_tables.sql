-------------------------------------------------------
-- Создание таблиц с уникальными ключами для обновлений
------------------------------------------------------- 
-- ===========================================
-- 1. Таблица marketing_ads_normalized
-- ===========================================
CREATE TABLE IF NOT EXISTS marketing_ads_normalized (
    date DATE,
    campaign TEXT,
    ad_group TEXT,
    source TEXT,
    impressions INTEGER,
    clicks INTEGER,
    cost_rub NUMERIC(14,2),
    conversions INTEGER,
    utm_source TEXT,
    utm_medium TEXT,
    utm_campaign TEXT
);

-- Уникальный индекс для UPSERT
CREATE UNIQUE INDEX IF NOT EXISTS idx_ads_normalized
ON marketing_ads_normalized(date, campaign, ad_group, source);


-- ===========================================
-- 2. Таблица crm_sales_aggregated
-- ===========================================
CREATE TABLE IF NOT EXISTS crm_sales_aggregated (
    date DATE,
    utm_source TEXT,
    utm_medium TEXT,
    utm_campaign TEXT,
    orders INTEGER,
    revenue NUMERIC(14,2),
    customers INTEGER
);

-- Уникальный индекс для UPSERT
CREATE UNIQUE INDEX IF NOT EXISTS idx_crm_aggregated
ON crm_sales_aggregated(date, utm_source, utm_medium, utm_campaign);


-- ===========================================
-- 3. Таблица marketing_ads_crm_daily
-- ===========================================
CREATE TABLE IF NOT EXISTS marketing_ads_crm_daily (
    date DATE,
    source TEXT,
    campaign TEXT,
    impressions INTEGER,
    clicks INTEGER,
    cost_rub NUMERIC(14,2),
    conversions INTEGER,
    orders INTEGER,
    revenue NUMERIC(14,2),
    customers INTEGER
);

-- Уникальный индекс для UPSERT
CREATE UNIQUE INDEX IF NOT EXISTS idx_ads_crm_daily
ON marketing_ads_crm_daily(date, source, campaign);