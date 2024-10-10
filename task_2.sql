-- 1. Подсчитываем количество кликов и сумму расходов по каждой рекламной кампании и дате
SELECT
    "yandex_direct_test"."Date",                     -- выбираем дату из таблицы
    "yandex_direct_test"."CampaignId",              -- выбираем ID рекламной кампании из таблицы
    SUM("yandex_direct_test"."Clicks") AS Total_Clicks,  -- суммируем клики и даем им алиас
    SUM("yandex_direct_test"."Cost") AS Total_Cost      -- суммируем расходы и даем им алиас
FROM
    "yandex_direct_test"         -- из таблицы yandex_direct_test
GROUP BY
    "yandex_direct_test"."Date", "yandex_direct_test"."CampaignId"           -- группируем по дате и ID рекламной кампании
ORDER BY
    "yandex_direct_test"."Date", "yandex_direct_test"."CampaignId";          -- сортируем результаты по дате и ID кампании

-- 2. Вычисляем самую популярную рекламную кампанию по месяцам
SELECT
    Year,
    Month,
    "CampaignId",
    Total_Clicks
FROM (
    SELECT
        EXTRACT(YEAR FROM "yandex_direct_test"."Date") AS Year,      -- извлекаем год из даты
        EXTRACT(MONTH FROM "yandex_direct_test"."Date") AS Month,    -- извлекаем месяц из даты
        "yandex_direct_test"."CampaignId",                            -- выбираем ID рекламной кампании из таблицы
        SUM("yandex_direct_test"."Clicks") AS Total_Clicks,          -- суммируем клики для каждой кампании
        RANK() OVER (PARTITION BY EXTRACT(YEAR FROM "yandex_direct_test"."Date"),
                             EXTRACT(MONTH FROM "yandex_direct_test"."Date")
                             ORDER BY SUM("yandex_direct_test"."Clicks") DESC) AS rank  -- присваиваем ранг
    FROM
        "yandex_direct_test"                      -- из таблицы yandex_direct_test
    GROUP BY
        Year, Month, "CampaignId"                 -- группируем по году, месяцу и ID кампании
) AS ranked_campaigns
WHERE rank = 1                          -- фильтруем только самые популярные кампании
ORDER BY Year, Month;                  -- сортируем по году и месяцу

-- 3. Вычисляем самую дорогую рекламную кампанию по месяцам
SELECT
    Year,
    Month,
    "CampaignId",
    Total_Cost
FROM (
    SELECT
        EXTRACT(YEAR FROM "yandex_direct_test"."Date") AS Year,      -- извлекаем год из даты
        EXTRACT(MONTH FROM "yandex_direct_test"."Date") AS Month,    -- извлекаем месяц из даты
        "yandex_direct_test"."CampaignId",                            -- выбираем ID рекламной кампании из таблицы
        SUM("yandex_direct_test"."Cost") AS Total_Cost,              -- суммируем расходы для каждой кампании
        DENSE_RANK() OVER (PARTITION BY EXTRACT(YEAR FROM "yandex_direct_test"."Date"),
                                    EXTRACT(MONTH FROM "yandex_direct_test"."Date")
                                    ORDER BY SUM("yandex_direct_test"."Cost") DESC) AS rank  -- присваиваем ранг
    FROM
        "yandex_direct_test"                      -- из таблицы yandex_direct_test
    GROUP BY
        Year, Month, "CampaignId"                 -- группируем по году, месяцу и ID кампании
) AS ranked_campaigns
WHERE rank = 1                          -- фильтруем только самые дорогие кампании
ORDER BY Year, Month;                  -- сортируем по году и месяцу

-- 4. Вычисляем количество кликов по рекламным кампаниям относительно типа устройства
SELECT
    "yandex_direct_test"."CampaignId",                            -- выбираем ID рекламной кампании из таблицы
    "yandex_direct_test"."Device",                                -- выбираем тип устройства из таблицы
    SUM("yandex_direct_test"."Clicks") AS Total_Clicks            -- суммируем клики для каждой комбинации кампании и устройства
FROM
    "yandex_direct_test"                      -- из таблицы yandex_direct_test
GROUP BY
    "yandex_direct_test"."CampaignId", "yandex_direct_test"."Device"                   -- группируем по ID кампании и типу устройства
ORDER BY
    "yandex_direct_test"."CampaignId", "yandex_direct_test"."Device";                  -- сортируем по ID кампании и типу устройства
