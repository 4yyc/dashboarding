WITH RECURSIVE
-- =========================================================
-- 0. 金流狀態：從 bill 判斷「已結清 / 未結清 / 全退 / 折讓」
--    這段是簡化版 refined_bill_states，只保留續約邏輯需要的欄位
-- =========================================================
agg_bill_transactions AS (
    SELECT
        ob.id AS bill_id,
        SUM(GREATEST(otd.amount, 0))  AS 已收金額,
        -SUM(LEAST(otd.amount, 0))    AS 已退金額
    FROM order_bills ob
    JOIN order_payments op
      ON op.bill_id = ob.id
    JOIN order_transaction_details otd
      ON otd.payment_id = op.id
    WHERE op.transaction_type IN (0, 1)          -- 一般收款 / 退款
    GROUP BY ob.id
),
agg_bill_sales_allowances AS (
    SELECT
        ob.id AS bill_id,
        SUM(osai.amount) AS 部分退回
    FROM order_bills ob
    JOIN order_sales_allowances osa
      ON osa.bill_id = ob.id
    JOIN order_sales_allowance_items osai
      ON osai.sales_allowance_id = osa.id
    GROUP BY ob.id
),
refined_bill_states AS (
    SELECT
        ob.id                           AS bill_id,
        ob.order_id                     AS parent_order_id,  -- 這裡就是 root_order_id
        ob.total_amount                 AS 帳單應收款,
        COALESCE(absa.部分退回, 0)      AS 部分退回,
        COALESCE(obsr.total_amount, 0)  AS 全部退回,
        COALESCE(abt.已收金額, 0)       AS 已收金額,
        COALESCE(abt.已退金額, 0)       AS 已退金額,
        CASE
            WHEN COALESCE(obsr.total_amount, 0) = ob.total_amount THEN
                CASE
                    WHEN COALESCE(abt.已收金額, 0) = COALESCE(abt.已退金額, 0)
                        THEN '已結清(全退)'
                    WHEN COALESCE(abt.已收金額, 0) > COALESCE(abt.已退金額, 0)
                        THEN '未結清(全退)'
                    ELSE '異常(全退)'
                END
            WHEN COALESCE(absa.部分退回, 0) > 0 THEN
                CASE
                    WHEN COALESCE(abt.已收金額, 0) - COALESCE(abt.已退金額, 0)
                         = ob.total_amount - COALESCE(absa.部分退回, 0)
                        THEN '已結清(有折讓)'
                    ELSE '未結清(有折讓)'
                END
            ELSE
                CASE
                    WHEN COALESCE(abt.已收金額, 0) - COALESCE(abt.已退金額, 0)
                         = ob.total_amount
                        THEN '已結清'
                    ELSE '未結清'
                END
        END AS 訂單狀態
    FROM order_bills ob
    LEFT JOIN agg_bill_transactions      abt  ON abt.bill_id  = ob.id
    LEFT JOIN agg_bill_sales_allowances  absa ON absa.bill_id = ob.id
    LEFT JOIN order_bill_sales_returns   obsr ON obsr.bill_id = ob.id
),

-- 只保留：已結清 / 已結清(有折讓)，而且帳單金額 > 0
valid_root_orders AS (
    SELECT DISTINCT
        rbs.parent_order_id AS root_order_id
    FROM refined_bill_states rbs
    WHERE rbs.訂單狀態 IN ('已結清', '已結清(有折讓)')
      AND rbs.帳單應收款 > 0
),

-- =========================================================
-- 1. 把 root_order + suborders 的所有項目合併，
--    依會計科目彙總金額，決定這張 root_order 的主會計科目
-- =========================================================
treatment_item_sums AS (
    SELECT
        vro.root_order_id,
        o_root.client_id,
        o_root.user_id,
        o_root.branch_id,
        o_root.order_date,
        pc.name       AS 會計科目,
        SUM(oi.subtotal) AS 金額
    FROM valid_root_orders vro
    JOIN orders o_root
      ON o_root.id = vro.root_order_id                         -- root order
    JOIN orders o
      ON COALESCE(o.parent_id, o.id) = vro.root_order_id       -- root + suborders
    JOIN order_items oi
      ON oi.order_id = o.id
    JOIN products p
      ON p.id = oi.product_id
    LEFT JOIN product_categories pc
      ON pc.product_id = p.id
     AND pc."group"   = '會計類別'
    GROUP BY
        vro.root_order_id,
        o_root.client_id,
        o_root.user_id,
        o_root.branch_id,
        o_root.order_date,
        pc.name
),

-- 主會計科目：依優先順序挑一個
treatment_orders AS (
    SELECT
        tis.root_order_id,
        tis.client_id,
        tis.user_id,
        tis.branch_id,
        tis.order_date,
        tis.會計科目,
        tis.金額,
        ROW_NUMBER() OVER (
            PARTITION BY tis.root_order_id
            ORDER BY CASE tis.會計科目
                        WHEN '標準療程(初)'       THEN 1
                        WHEN '標準療程(續)'       THEN 2
                        WHEN '標準療程(續-無線上)' THEN 3
                        ELSE 99
                     END
        ) AS rn
    FROM treatment_item_sums tis
),

-- 只保留三種療程，且排除某些分店、時間
treatment_orders_main AS (
    SELECT
        root_order_id,
        client_id,
        user_id,
        branch_id,
        order_date,
        會計科目,
        金額
    FROM treatment_orders
    WHERE rn = 1
      AND 會計科目 IN ('標準療程(初)', '標準療程(續)', '標準療程(續-無線上)')
      AND order_date >= DATE '2024-07-01'
      AND branch_id NOT IN (155, 164)
),

-- =========================================================
-- 2. 團體課程：用來決定續套的 effective_start_date
-- =========================================================
gc AS (
    SELECT DISTINCT
        gco.client_id,
        gc.started_at,
        gc.finished_at
    FROM group_class_orders gco
    JOIN group_classes gc
      ON gc.id = gco.group_class_id
    WHERE gco.client_id IS NOT NULL
      AND gco.aasm_state = 'registered'
      AND gc.finished_at IS NOT NULL
),

-- 把每一張療程單帶上 effective_start_date
-- 初套：用 order_date
-- 續套：有團課用 started_at，無線上續套就用 order_date
treatment_with_effective AS (
    SELECT
        tom.*,
        CASE
            WHEN tom.會計科目 = '標準療程(初)'
                THEN tom.order_date
            ELSE COALESCE(gc.started_at::date, tom.order_date)
        END AS effective_start_date
    FROM treatment_orders_main tom
    LEFT JOIN gc
      ON gc.client_id = tom.client_id
     AND tom.order_date BETWEEN (gc.started_at::date - INTERVAL '1 month')
                            AND (gc.started_at::date + INTERVAL '1 month')
),

-- =========================================================
-- 3. 初套清單 & 續套清單
-- =========================================================
initials AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY client_id, user_id, order_date, root_order_id
        ) AS initial_id,
        root_order_id,
        client_id,
        user_id,
        branch_id,
        order_date,
        effective_start_date
    FROM treatment_with_effective
    WHERE 會計科目 = '標準療程(初)'
),
renewals AS (
    SELECT
        root_order_id,
        client_id,
        user_id,
        branch_id,
        order_date,
        effective_start_date,
        會計科目
    FROM treatment_with_effective
    WHERE 會計科目 IN ('標準療程(續)', '標準療程(續-無線上)')
),

-- 每一筆續套 → 歸屬到「它之前最近的一次初套」（同 client + doctor）
renewals_with_initial AS (
    SELECT *
    FROM (
        SELECT
            r.*,
            i.initial_id,
            ROW_NUMBER() OVER (
                PARTITION BY r.client_id, r.user_id, r.order_date
                ORDER BY i.order_date DESC
            ) AS rn
        FROM renewals r
        JOIN initials i
          ON i.client_id = r.client_id
         AND i.user_id   = r.user_id
         AND i.order_date <= r.order_date
    ) x
    WHERE rn = 1
),

-- =========================================================
-- 4. 用遞迴方式一路往後找第 1～6 次續約
--    規則：
--      - 第一次續約 window：初套 effective_start_date → +6 個月
--      - 之後每次：上一筆續套 effective_start_date → +6 個月
-- =========================================================
renewal_chain AS (
    -- base：初套 (renewal_n = 0)
    SELECT
        i.initial_id,
        i.client_id,
        i.user_id,
        i.branch_id,
        i.order_date,
        i.effective_start_date,
        0 AS renewal_n
    FROM initials i

    UNION ALL

    -- 續套：在上一筆 effective_start_date 之後、+6 個月之內的下一筆續套
    SELECT
        rc.initial_id,
        rc.client_id,
        rc.user_id,
        rc.branch_id,
        r.order_date,
        r.effective_start_date,
        rc.renewal_n + 1 AS renewal_n
    FROM renewal_chain rc
    JOIN renewals_with_initial r
      ON r.initial_id = rc.initial_id
     AND r.client_id  = rc.client_id
     AND r.user_id    = rc.user_id
     AND r.order_date > rc.order_date
     AND r.order_date <= rc.effective_start_date + INTERVAL '6 months'
    WHERE rc.renewal_n < 6
),

-- 每個 initial_id + renewal_n 只保留最早的那一筆續套
selected_renewals AS (
    SELECT *
    FROM (
        SELECT
            initial_id,
            client_id,
            user_id,
            branch_id,
            renewal_n,
            order_date,
            effective_start_date,
            ROW_NUMBER() OVER (
                PARTITION BY initial_id, renewal_n
                ORDER BY order_date
            ) AS rn
        FROM renewal_chain
        WHERE renewal_n >= 1
    ) t
    WHERE rn = 1
),

-- =========================================================
-- 5. 每一個初套：有沒有 1～6 次續約
-- =========================================================
initial_stats AS (
    SELECT
        i.initial_id,
        i.client_id,
        i.user_id,
        i.branch_id,
        MAX(CASE WHEN sr.renewal_n = 1 THEN 1 ELSE 0 END) AS has_renew1,
        MAX(CASE WHEN sr.renewal_n = 2 THEN 1 ELSE 0 END) AS has_renew2,
        MAX(CASE WHEN sr.renewal_n = 3 THEN 1 ELSE 0 END) AS has_renew3,
        MAX(CASE WHEN sr.renewal_n = 4 THEN 1 ELSE 0 END) AS has_renew4,
        MAX(CASE WHEN sr.renewal_n = 5 THEN 1 ELSE 0 END) AS has_renew5,
        MAX(CASE WHEN sr.renewal_n = 6 THEN 1 ELSE 0 END) AS has_renew6
    FROM initials i
    LEFT JOIN selected_renewals sr
      ON sr.initial_id = i.initial_id
    GROUP BY
        i.initial_id,
        i.client_id,
        i.user_id,
        i.branch_id
),

-- =========================================================
-- 6. 依醫師彙總：初套人數、第 1～6 次續套人數 & 續約率
-- =========================================================
doctor_summary AS (
    SELECT
        CASE
            WHEN u.nick_name LIKE '%思思醫師%' THEN '李思賢醫師'
            WHEN u.first_name LIKE '曜增' THEN '劉曜增醫師'
            ELSE CONCAT(u.real_name, '醫師')
        END AS 醫師,
        COUNT(*)              AS 初套人數,
        SUM(has_renew1)       AS 第一次續套人數,
        SUM(has_renew2)       AS 第二次續套人數,
        SUM(has_renew3)       AS 第三次續套人數,
        SUM(has_renew4)       AS 第四次續套人數,
        SUM(has_renew5)       AS 第五次續套人數,
        SUM(has_renew6)       AS 第六次續套人數
    FROM initial_stats s
    JOIN users u
      ON u.id = s.user_id
    GROUP BY
        CASE
            WHEN u.nick_name LIKE '%思思醫師%' THEN '李思賢醫師'
            WHEN u.first_name LIKE '曜增' THEN '劉曜增醫師'
            ELSE CONCAT(u.real_name, '醫師')
        END 
)

SELECT
    醫師,
    初套人數,
    第一次續套人數,
    CASE WHEN 初套人數 = 0 THEN 0
         ELSE ROUND(100.0 * 第一次續套人數 / 初套人數, 0) END AS 第一次續約率,
    第二次續套人數,
    CASE WHEN 第一次續套人數 = 0 THEN 0
         ELSE ROUND(100.0 * 第二次續套人數 / 第一次續套人數, 0) END AS 第二次續約率,
    第三次續套人數,
    CASE WHEN 第二次續套人數 = 0 THEN 0
         ELSE ROUND(100.0 * 第三次續套人數 / 第二次續套人數, 0) END AS 第三次續約率,
    第四次續套人數,
    CASE WHEN 第三次續套人數 = 0 THEN 0
         ELSE ROUND(100.0 * 第四次續套人數 / 第三次續套人數, 0) END AS 第四次續約率,
    第五次續套人數,
    CASE WHEN 第四次續套人數 = 0 THEN 0
         ELSE ROUND(100.0 * 第五次續套人數 / 第四次續套人數, 0) END AS 第五次續約率,
    第六次續套人數,
    CASE WHEN 第五次續套人數 = 0 THEN 0
         ELSE ROUND(100.0 * 第六次續套人數 / 第五次續套人數, 0) END AS 第六次續約率
FROM doctor_summary
WHERE 醫師 LIKE '%醫師%'
ORDER BY 第一次續約率 DESC,
         第二次續約率 DESC,
         第三次續約率 DESC;
