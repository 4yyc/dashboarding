-- ===========================================================
--  STEP 1: 抓初診預約資料 (branch_visits)
-- ===========================================================
WITH bvs AS (
    SELECT 
        bv.id,
        bv.client_id,
        bv.title,
        bv.branch_id,
        bv.state,
        bv.provider_name,
        bv.service_name,
        time,
        created_at,
        bv.data ->> 'visit_purpose' AS 預約來訪目的
    FROM branch_visits bv
    WHERE title !~ '卡位|東東|兒童生長'
      AND bv.state = 2
      AND date >= CURRENT_DATE - INTERVAL '5 months'
    ORDER BY time
),

-- ===========================================================
--  STEP 2: 整理訂單資料 (orders + products)
-- ===========================================================
os AS (
    SELECT 
        orders.parent_id,
        orders.user_id,
        product_categories.name,
        SUM(orders.subtotal) AS subtotal
    FROM order_items
    JOIN orders ON order_items.order_id = orders.id
    JOIN products ON order_items.product_id = products.id
    LEFT JOIN product_categories
        ON products.id = product_categories.product_id
       AND product_categories.group = '會計類別'
    WHERE orders.parent_id IS NOT NULL
      AND products.name NOT LIKE '%成長%'
    GROUP BY orders.parent_id, orders.user_id, product_categories.name
),

os1 AS (
    SELECT 
        id,
        client_id,
        o.user_id,
        branch_id,
        o.created_at::date AS order_date,
        os.name AS 會計科目,
        os.subtotal AS 金額
    FROM os
    LEFT JOIN orders o ON os.parent_id = o.id
    WHERE o.created_at >= CURRENT_DATE - INTERVAL '5 months'
),

os2 AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY id, order_date
            ORDER BY CASE 會計科目
                WHEN '標準療程(初)' THEN 1
                WHEN '標準療程(續)' THEN 2
                WHEN '標準療程(續-無線上)' THEN 3
                WHEN '營養療程套組' THEN 4
                WHEN '營養療程套組(續)' THEN 5
                WHEN '長期維持療程' THEN 6
                WHEN 'EMBODY' THEN 7
                WHEN '檢驗' THEN 8
                WHEN '門診' THEN 9
                ELSE 10
            END
        ) AS rn
    FROM os1
),

os3 AS (
    SELECT *,
        CASE 
            WHEN LENGTH(NICK_NAME) = 5 THEN NICK_NAME
            WHEN LENGTH(LAST_NAME) = 5 THEN LAST_NAME
            WHEN LENGTH(FIRST_NAME) = 5 THEN FIRST_NAME
            WHEN NICK_NAME LIKE '%4ca2aecfec%' THEN CONCAT(LAST_NAME, FIRST_NAME)
            ELSE NICK_NAME
        END AS user_name
    FROM os2
    LEFT JOIN users us ON us.id = os2.user_id
    WHERE rn = 1
),

-- ===========================================================
--  STEP 3: 合併初診預約與訂單資料
-- ===========================================================
final0 AS (
    SELECT DISTINCT 
        COALESCE(bvs.client_id, os3.client_id) AS client_id,
        bvs.state,
        TRIM(SPLIT_PART(title, '-', LENGTH(title) - LENGTH(REPLACE(title, '-', '')) + 1)) AS 客戶,
        CASE 
            WHEN provider_name LIKE '%醫師%' 
                THEN RIGHT(LEFT(provider_name, POSITION('醫師' IN provider_name)+1), 5)
            ELSE user_name
        END AS 醫師,
        user_id,
        service_name,
        COALESCE(date_trunc('day', time + interval '8 hour'), os3.order_date) AS day,
        COALESCE(date_trunc('month', time + interval '8 hour'), date_trunc('month', os3.order_date)) AS month,
        預約來訪目的,
        os3.會計科目,
        COALESCE(bvs.branch_id, os3.branch_id) AS branch_id
    FROM 
        (SELECT *, ROW_NUMBER() OVER (
            PARTITION BY date_trunc('day', time + interval '8 hour'), client_id
            ORDER BY CASE service_name WHEN 'EMBODY' THEN 1 ELSE 2 END
        ) AS rns FROM bvs) bvs
    FULL OUTER JOIN
        (SELECT *, ROW_NUMBER() OVER (
            PARTITION BY order_date, client_id
            ORDER BY CASE 會計科目 WHEN 'EMBODY' THEN 1 ELSE 2 END
        ) AS rns FROM os3) os3
    ON bvs.client_id = os3.client_id
       AND bvs.branch_id = os3.branch_id
       AND (time + interval '8 hour') BETWEEN (order_date - interval '1 day') AND (order_date + interval '1 day')
    WHERE COALESCE(bvs.client_id, os3.client_id) NOT IN (351)
),

-- ===========================================================
--  STEP 4: 找出回診日 day1
-- ===========================================================
finals AS (
    SELECT DISTINCT 
        f1.*,
        MIN(f2.day) FILTER (
            WHERE f2.service_name LIKE '%回診%' 
              AND f2.service_name !~ '精準保健品回診|課程回診'
        ) OVER (PARTITION BY f2.client_id) AS day1,
        ROW_NUMBER() OVER (PARTITION BY f1.client_id, f1.day ORDER BY f1.user_id) AS rn
    FROM final0 f1
    LEFT JOIN final0 f2 
        ON f2.client_id = f1.client_id 
       AND f2.day >= f1.day
    WHERE f1.branch_id NOT IN (155,164,157)
),

final AS (
    SELECT 
        client_id, state, 客戶, 醫師, user_id, service_name,
        day, month, 預約來訪目的, 會計科目, branch_id, day1
    FROM finals
    WHERE rn = 1
),

-- ===========================================================
--  STEP 5: 統計初診階段轉單 (a)
-- ===========================================================
a AS (
    SELECT 
        branch_id,
        醫師,
        month,
        COUNT(DISTINCT client_id) AS 初診人數,

        COUNT(DISTINCT CASE 
            WHEN 會計科目 ~ '檢驗' 
             AND client_id NOT IN (
                 SELECT DISTINCT client_id
                 FROM final
                 WHERE 會計科目 LIKE '%標準療程(初)%' 
                   AND service_name ~ '初診'
                   AND service_name !~ '報告'
                   AND state = 2
                   AND (day1 > day OR day1 IS NULL)
                   AND month >= CURRENT_DATE - INTERVAL '5 months'
             ) THEN client_id END) AS 初診檢驗人數,

        COUNT(DISTINCT client_id)
        - COUNT(DISTINCT CASE WHEN 會計科目 ~ '檢驗' 
             AND client_id NOT IN (
                 SELECT DISTINCT client_id
                 FROM final
                 WHERE 會計科目 LIKE '%標準療程(初)%' 
                   AND service_name ~ '初診'
                   AND service_name !~ '報告'
                   AND state = 2
                   AND (day1 > day OR day1 IS NULL)
                   AND month >= CURRENT_DATE - INTERVAL '5 months'
             ) THEN client_id END)
        - COUNT(DISTINCT CASE WHEN 會計科目 LIKE '%標準療程(初)%' THEN client_id END) AS 初診門診人數,

        COUNT(DISTINCT CASE WHEN 會計科目 LIKE '%標準療程(初)%' THEN client_id END) AS 初診購買套裝人數,

        ROUND(
            100 * COUNT(DISTINCT CASE WHEN 會計科目 LIKE '%標準療程(初)%' THEN client_id END)
              / COUNT(DISTINCT client_id), 2
        ) AS 初診套裝轉單率

    FROM final
    WHERE service_name !~ '回診'
      AND service_name !~ '報告'
      AND state = 2
      AND (day1 > day OR day1 IS NULL)
      AND month >= CURRENT_DATE - INTERVAL '5 months'
    GROUP BY branch_id, 醫師, month
),

-- ===========================================================
--  STEP 6: 統計回診後購買 (b)
-- ===========================================================
b AS (
    SELECT 
        f.branch_id,
        f.醫師,
        sub.month,
        COUNT(DISTINCT f.client_id) AS 回診購買套裝人數,
        COUNT(DISTINCT CASE WHEN sub.會計科目 = '檢驗' THEN sub.client_id END) AS 檢驗後購買套裝人數
    FROM final f
    INNER JOIN (
        SELECT branch_id, client_id, 醫師, month, 會計科目
        FROM final
        WHERE service_name LIKE '%初診%'
          AND (會計科目 != '標準療程(初)' OR 會計科目 IS NULL)
    ) AS sub
      ON sub.client_id = f.client_id
     AND sub.branch_id = f.branch_id
     AND sub.醫師 = f.醫師
    WHERE f.day BETWEEN f.day1 AND f.day1 + INTERVAL '30 days'
      AND f.會計科目 = '標準療程(初)'
      AND service_name NOT LIKE '%初診%'
    GROUP BY f.branch_id, f.醫師, sub.month
),

-- ===========================================================
--  STEP 7: 整合初診與回診資料 (alls)
-- ===========================================================
alls AS (
    SELECT 
        bs.name AS 診所,
        a.*,
        COALESCE(b.回診購買套裝人數, 0) AS 回診購買套裝人數,
        COALESCE(b.檢驗後購買套裝人數, 0) AS 檢驗後購買套裝人數,

        CASE WHEN 初診檢驗人數 = 0 THEN 0
             ELSE 100 * COALESCE(b.回診購買套裝人數, 0) / 初診檢驗人數 END AS 檢驗後套裝轉單率,

        CASE WHEN 初診門診人數 = 0 THEN 0
             ELSE 100 * COALESCE(b.回診購買套裝人數 - b.檢驗後購買套裝人數, 0) / 初診門診人數 END AS 門診套裝轉單率,

        CASE WHEN 初診人數 = 0 THEN 0
             WHEN b.回診購買套裝人數 IS NULL THEN 100 * 初診購買套裝人數 / 初診人數
             ELSE 100 * (b.回診購買套裝人數 + 初診購買套裝人數) / 初診人數 END AS 總初套轉單率

    FROM (
        SELECT * FROM a WHERE 醫師 IS NOT NULL
        UNION ALL
        SELECT branch_id, '未經醫師轉單' AS 醫師, month,
               0 AS 出診人數, 0 AS 初診檢驗人數, 0 AS 初診門診人數,
               初診購買套裝人數, 0 AS 初診套裝轉單率
        FROM a 
        WHERE 醫師 IS NULL AND 初診購買套裝人數 > 0
    ) a
    LEFT JOIN b ON a.branch_id = b.branch_id
                AND a.醫師 = b.醫師
                AND a.month = b.month
    LEFT JOIN branches bs ON a.branch_id = bs.id
    ORDER BY a.branch_id, a.醫師, month
),

-- ===========================================================
--  STEP 8: 統計預約未報到人數 (bvs1)
-- ===========================================================
bvs1 AS (
    SELECT 
        bv.branch_id,
        CASE 
            WHEN provider_name LIKE '%醫師%' 
                THEN RIGHT(LEFT(provider_name, POSITION('醫師' IN provider_name)+1), 5)
        END AS 醫師,
        date_trunc('month', time) AS months,
        COUNT(DISTINCT bv.client_id) AS 預約沒報到人數
    FROM branch_visits bv
    WHERE title !~ '卡位|東東'
      AND bv.state = 1
      AND service_name LIKE '%初診%'
      AND time <= now()
    GROUP BY bv.branch_id,
             CASE 
                 WHEN provider_name LIKE '%醫師%' 
                     THEN RIGHT(LEFT(provider_name, POSITION('醫師' IN provider_name)+1), 5)
             END,
             date_trunc('month', time)
    ORDER BY bv.branch_id, months, 醫師
)

-- ===========================================================
--  STEP 9: 最終整合輸出
-- ===========================================================
SELECT 
    alls.*,
    COALESCE(bvs1.預約沒報到人數, 0) AS 預約沒報到人數
FROM alls
LEFT JOIN bvs1
    ON bvs1.branch_id = alls.branch_id
   AND bvs1.months = alls.month
   AND alls.醫師 = bvs1.醫師;
