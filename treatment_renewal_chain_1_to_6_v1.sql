WITH bvs AS (  -- 取得分店預約紀錄
         SELECT bv.id,
            bv.client_id,
            bv.title,
            bv.branch_id,
            bv.state,
            bv.provider_name,
            bv.service_name,
            bv."time",
            bv.created_at,
            bv.data ->> 'visit_purpose'::text AS "預約來訪目的"  -- 從 JSON 欄位取出來訪目的
           FROM branch_visits bv
          WHERE bv.title::text !~ '卡位|東東'::text  -- 排除特定 title
            AND bv.state = 2  -- 狀態為 2 (已完成或有效預約)
          ORDER BY bv."time"
        ), 
os AS (  -- 訂單與會計科目 (金額彙總)
         SELECT orders.parent_id,
            product_categories.name,
            sum(orders.subtotal) AS subtotal
           FROM order_items
             JOIN orders ON order_items.order_id = orders.id
             JOIN products ON order_items.product_id = products.id
             LEFT JOIN product_categories 
                    ON products.id = product_categories.product_id 
                   AND product_categories."group"::text = '會計類別'::text
          GROUP BY orders.parent_id, product_categories.name
        ), 
os1 AS (  -- 訂單對應會計科目與金額
         SELECT o.id,
            o.client_id,
            o.user_id,
            o.branch_id,
            o.order_date,
            os.name AS "會計科目",
            os.subtotal AS "金額"
           FROM os
             LEFT JOIN orders o ON os.parent_id = o.id
        ), 
os2 AS (  -- 給每個訂單標號 (根據會計科目優先順序)
         SELECT os1.id,
            os1.client_id,
            os1.user_id,
            os1.branch_id,
            os1.order_date,
            os1."會計科目",
            os1."金額",
            row_number() OVER (
                PARTITION BY os1.id, os1.order_date 
                ORDER BY (
                    CASE os1."會計科目"
                        WHEN '標準療程(初)'::text THEN 1
                        WHEN '標準療程(續)'::text THEN 2
                        WHEN '標準療程(續-無線上)'::text THEN 3
                        WHEN '營養療程套組'::text THEN 4
                        WHEN '營養療程套組(續)'::text THEN 5
                        WHEN '長期維持療程'::text THEN 6
                        WHEN 'EMBODY'::text THEN 7
                        WHEN '檢驗'::text THEN 8
                        WHEN '門診'::text THEN 9
                        ELSE 10
                    END)) AS rn
           FROM os1
        ), 
os3 AS (  -- 加上使用者資訊，並取每個訂單會計科目 rn=1 的紀錄
         SELECT os2.id,
            os2.client_id,
            os2.user_id,
            os2.branch_id,
            os2.order_date,
            os2."會計科目",
            os2."金額",
            os2.rn,
            us.*,
            CASE
                WHEN length(us.nick_name::text) = 5 THEN us.nick_name
                WHEN length(us.last_name::text) = 5 THEN us.last_name
                WHEN length(us.first_name::text) = 5 THEN us.first_name
                ELSE us.nick_name
            END AS user_name
           FROM os2
             LEFT JOIN users us ON us.id = os2.user_id
          WHERE os2.rn = 1
        ), 
final0 AS (  -- 整合預約紀錄與訂單資訊
         SELECT DISTINCT COALESCE(bvs.client_id, os3.client_id) AS client_id,
            bvs.state,
            TRIM(BOTH FROM split_part(bvs.title::text, '-'::text, length(bvs.title::text) - length(replace(bvs.title::text, '-'::text, ''::text)) + 1)) AS "客戶",
            CASE
                WHEN bvs.provider_name::text ~~ '%醫師%'::text 
                     THEN "right"("left"(bvs.provider_name::text, POSITION(('醫師'::text) IN (bvs.provider_name)) + 1), 5)::character varying
                ELSE os3.user_name
            END AS "醫師",
            os3.user_id,
            bvs.service_name,
            COALESCE(date_trunc('Day'::text, bvs."time" + '08:00:00'::interval), os3.order_date::timestamp without time zone) AS day,
            COALESCE(date_trunc('Month'::text, bvs."time" + '08:00:00'::interval)::timestamp with time zone, date_trunc('Month'::text, os3.order_date::timestamp with time zone)) AS month,
            bvs."預約來訪目的",
            os3."會計科目",
            COALESCE(bvs.branch_id, os3.branch_id) AS branch_id
           FROM ( 
                SELECT bvs_1.*, 
                       row_number() OVER (
                            PARTITION BY (date_trunc('Day'::text, bvs_1."time" + '08:00:00'::interval)), 
                                         bvs_1.client_id 
                            ORDER BY (CASE bvs_1.service_name WHEN 'EMBODY'::text THEN 1 ELSE 2 END)
                        ) AS rns
                FROM bvs bvs_1
            ) bvs
             FULL JOIN ( 
                SELECT os3_1.*, 
                       row_number() OVER (
                            PARTITION BY os3_1.order_date, os3_1.client_id 
                            ORDER BY (CASE os3_1."會計科目" WHEN 'EMBODY'::text THEN 1 ELSE 2 END)
                        ) AS rns
                FROM os3 os3_1
            ) os3
            ON bvs.client_id = os3.client_id 
           AND bvs.branch_id = os3.branch_id 
           AND (bvs."time" + '08:00:00'::interval) >= (os3.order_date - '1 day'::interval) 
           AND (bvs."time" + '08:00:00'::interval) <= (os3.order_date + '1 day'::interval)
          WHERE COALESCE(bvs.client_id, os3.client_id) <> 351
        ), 
final AS (  -- 增加回診日期 day1
         SELECT DISTINCT f1.*,
            min(f2.day) FILTER (WHERE f2.service_name::text ~~ '%回診%'::text) 
                OVER (PARTITION BY f2.client_id) AS day1
           FROM final0 f1
             LEFT JOIN final0 f2 ON f2.client_id = f1.client_id AND f2.day >= f1.day
          WHERE f1.branch_id <> ALL (ARRAY[155::bigint, 164::bigint])
        ), 
gc AS (  -- 取得團體課程紀錄
         SELECT DISTINCT gco.client_id,
            gc.started_at,
            gc.finished_at
           FROM group_class_orders gco
             LEFT JOIN group_classes gc ON gco.group_class_id = gc.id
          WHERE gco.client_id IS NOT NULL 
            AND gco.aasm_state::text = 'registered'::text 
            AND gc.finished_at IS NOT NULL
        ), 
frist AS (  -- 找出購買「標準療程(初)」的人
         SELECT final.branch_id,
            final.client_id,
            final."醫師",
            LEAST(final.day::date, gc.started_at) AS sday, -- 開始日期取最早
            gc.finished_at,
            gc.finished_at + '4 mons'::interval AS fday
           FROM final
             LEFT JOIN gc ON gc.client_id = final.client_id 
                         AND final.day >= (gc.started_at - '1 mon'::interval) 
                         AND final.day <= (gc.started_at + '1 mon'::interval)
          WHERE final."會計科目"::text = '標準療程(初)'::text 
            AND gc.finished_at IS NOT NULL 
            AND final.day >= '2024-07-01 00:00:00'
        ), 
f0 AS (  -- 找出後續「標準療程(續)」訂單
         SELECT final.*
           FROM final
          WHERE "left"(final."會計科目"::text, 6) = '標準療程(續'::text 
            AND final.day >= '2024-07-01 00:00:00'
        ), 
r1 AS (  -- 計算第一次續套
         SELECT f2.*
           FROM ( SELECT f1.*,
                        CASE f0_1."會計科目"
                            WHEN '標準療程(續)'::text THEN 1
                            WHEN '標準療程(續-無線上)'::text THEN 2
                            ELSE 0
                        END AS "續約率1",
                        GREATEST(f0_1.day::date, f1.finished_at) AS sday1,
                        row_number() OVER (
                            PARTITION BY f1.client_id, f1.finished_at 
                            ORDER BY (GREATEST(f0_1.day::date, f1.finished_at))
                        ) AS rn
                   FROM frist f1
                     LEFT JOIN f0 f0_1 ON f1.client_id = f0_1.client_id 
                                      AND f1.branch_id = f0_1.branch_id 
                                      AND f1."醫師"::text = f0_1."醫師"::text 
                                      AND f0_1.day >= f1.sday 
                                      AND f0_1.day <= f1.fday
                ) f2
          WHERE f2.rn = 1
        ), 
r2 AS (  -- 計算第二次續套
         SELECT r1.branch_id,
            r1.client_id,
            r1."醫師",
            r1.sday,
            r1.fday,
            r1."續約率1",
            CASE WHEN r1."續約率1" = 0 THEN NULL::date
                 ELSE min(LEAST(f0_1.day::date, r1.sday1)) END AS sday1,
            CASE WHEN r1."續約率1" = 0 THEN NULL::date
                 ELSE (min(LEAST(f0_1.day::date, r1.sday1)) + '2 mons'::interval)::date END AS finished_at,
            CASE WHEN r1."續約率1" = 0 THEN NULL::date
                 ELSE (min(LEAST(f0_1.day::date, r1.sday1)) + '6 mons'::interval)::date END AS fday1
           FROM r1
             LEFT JOIN f0 f0_1 ON r1.client_id = f0_1.client_id 
                              AND r1.branch_id = f0_1.branch_id 
                              AND r1."醫師"::text = f0_1."醫師"::text 
                              AND f0_1.day >= r1.sday1 
                              AND f0_1.day <= (r1.sday1 + '6 mons'::interval) 
                              AND r1."續約率1" <> 0
          GROUP BY r1.branch_id, r1.client_id, r1."醫師", r1.sday, r1.fday, r1."續約率1"
        ), 
r2_1 AS (  -- 再判斷第二次續套是否存在
         SELECT f3.*
           FROM ( SELECT DISTINCT r2.*,
                        CASE f0_1."會計科目"
                            WHEN '標準療程(續)'::text THEN 1
                            WHEN '標準療程(續-無線上)'::text THEN 2
                            ELSE 0
                        END AS "續約率2",
                        f0_1.day::date AS days,
                        row_number() OVER (
                            PARTITION BY r2.client_id, r2.sday 
                            ORDER BY f0_1.day
                        ) AS rn
                   FROM r2
                     LEFT JOIN f0 f0_1 ON r2.client_id = f0_1.client_id 
                                      AND r2.branch_id = f0_1.branch_id 
                                      AND r2."醫師"::text = f0_1."醫師"::text 
                                      AND f0_1.day >= (r2.sday1 + '1 day'::interval) 
                                      AND f0_1.day <= r2.fday1
                ) f3
          WHERE f3.rn = 1
        ),
R_Treatment_Packages AS (  -- 最終續套追蹤 (包含第三次)
         SELECT DISTINCT r2_1.*,
            CASE
                WHEN r2_1."續約率2" = 0 THEN NULL::date
                ELSE GREATEST(r2_1.days, r2_1.finished_at)
            END AS sday2,
            GREATEST(r2_1.days, r2_1.finished_at) + '6 mons'::interval AS fday2,
            CASE f0."會計科目"
                WHEN '標準療程(續)'::text THEN 1
                WHEN '標準療程(續-無線上)'::text THEN 2
                ELSE 0
            END AS "續約率3"
           FROM r2_1
             LEFT JOIN f0 ON r2_1.client_id = f0.client_id 
                         AND r2_1.branch_id = f0.branch_id 
                         AND r2_1."醫師"::text = f0."醫師"::text 
                         AND f0.day >= GREATEST(r2_1.days, r2_1.finished_at) 
                         AND f0.day <= (GREATEST(r2_1.days, r2_1.finished_at) + '6 mons'::interval) 
                         AND r2_1."續約率2" <> 0
        )
-- =============================
-- 最終輸出：各醫師的續約分析
-- =============================
SELECT 
case when 醫師 like '%思思醫師%' then '李思賢醫師'
     else 醫師 end as 醫師,
count(distinct client_id) as 購買初套人數,
count(distinct case when 續約率1 >0 then client_id end) as 第一次續套人數,
case when count(distinct client_id) = 0 then 0
     else 100*count(distinct case when 續約率1 >0 then client_id end)/count(distinct client_id) end as 第一次續約率,
count(distinct case when 續約率2 >0 then client_id end) as 第二次續套人數,
case when count(distinct case when 續約率1 >0 then client_id end) = 0 then 0
     else 100*count(distinct case when 續約率2 >0 then client_id end)/count(distinct case when 續約率1 >0 then client_id end) end as 第二次續約率,
count(distinct case when 續約率3 >0 then client_id end) as 第三次續套人數,
case when count(distinct case when 續約率2 >0 then client_id end) = 0 then 0
     else 100*count(distinct case when 續約率3 >0 then client_id end)/count(distinct case when 續約率2 >0 then client_id end) end as 第三次續約率
FROM R_Treatment_Packages
WHERE 醫師 like '%醫師%'
GROUP BY case when 醫師 like '%思思醫師%' then '李思賢醫師'
              else 醫師 end
ORDER BY 第一次續約率 DESC, 第二次續約率 DESC, 第三次續約率 DESC;
