--create or replace table bigtimestudios.currency_analytics.token_efficiency as 

-- Base price data that's used across multiple CTEs
WITH base_bt_prices AS (
  SELECT
    date(timestamp) AS day,
    avg(price) AS avg_bt_price
  FROM bigtimestudios.bt_token.bigtime_okx_historical
  GROUP BY 1
),

base_ol_prices as (
    select
    date(timestamp) Day,
    avg(price) avg_ol_price
    from bigtimestudios.bt_token.openloot_okx_historical
    group by 1
  ),

-- =============================================================================
-- REVENUE CALCULATIONS
-- =============================================================================

revenue_data AS (
  SELECT
    date(payment_at) AS day,
    -- BIGTIME revenue calculation
    round(sum(CASE  
      WHEN game_name = 'Big Time' THEN
        CASE
          WHEN type = 'primary' THEN price * quantity
          WHEN type = 'secondary' AND subtype = 'sale' THEN 0.05 * price * quantity
          WHEN type = 'secondary' AND subtype = 'rental' THEN 0.2 * price * quantity
          ELSE 0
        END
    END), 0) AS bigtime_revenue,
    
    -- OpenLoot revenue calculation  
    round(sum(CASE
      WHEN game_name = 'Big Time' THEN
        CASE
          WHEN type = 'primary' THEN price * quantity
          WHEN type = 'secondary' AND subtype = 'sale' THEN 0.05 * price * quantity
          WHEN type = 'secondary' AND subtype = 'rental' THEN 0.2 * price * quantity
          ELSE 0
        END
      WHEN game_name != 'Big Time' THEN
        CASE
          WHEN type = 'primary' THEN 0.1 * price * quantity
          WHEN type = 'secondary' AND subtype = 'sale' THEN 0.025 * price * quantity
          WHEN type = 'secondary' AND subtype = 'rental' THEN 0.1 * price * quantity
          ELSE 0
        END
    END), 0) AS openloot_revenue
  FROM openloot-362008.dbt_openloot.fact_sale_transactions
  WHERE payment_currency = 'USD'
 -- and game_name='Big Time'
  GROUP BY 1
),

-- =============================================================================
-- EPOCH CHEST ANALYTICS
-- =============================================================================

epoch_chest_data AS (
  SELECT
    date(day) AS day,
    sum(coalesce(bigtime_grant,0)) AS bigtime_grant_epoch,
    safe_divide(
      sum(bigtime_price * coalesce(bigtime_grant,0)),
      sum(coalesce(bigtime_grant,0))
    ) AS weighted_bt_price,
    sum(coalesce(tc_usd_deducted, 0)) + sum(coalesce(bigtimeUSDDeducted, 0)) AS spend_epochchest_usd,
    safe_divide(
      sum(coalesce(tc_usd_deducted, 0)) + sum(coalesce(bigtimeUSDDeducted, 0)),
      sum(coalesce(bigtime_grant,0))
    ) AS imputed_epoch_cost_basis
  FROM bigtimestudios.currency_analytics.bigtime_prices_by_sources
  GROUP BY 1
),

-- =============================================================================
-- RACIAL SYSTEM ANALYTICS
-- =============================================================================

racial_data AS (
  SELECT
    date(created_at) AS day,
    p.avg_bt_price,
    sum(coalesce(bigtime_grant,0)) AS bigtime_grant_racial,
    sum(
      coalesce(abl_sunk, 0) * coalesce(abl_price, 0) + 
      coalesce(tc_sunk, 0) * coalesce(tc_price, 0) + 
      coalesce(atc_sunk, 0) * coalesce(atc_price, 0) + 
      coalesce(bigtime_sunk, 0) * coalesce(bt_price, 0)
    ) AS spend_racial_usd,
    safe_divide(
      sum(
        coalesce(abl_sunk, 0) * coalesce(abl_price, 0) + 
        coalesce(tc_sunk, 0) * coalesce(tc_price, 0) + 
        coalesce(atc_sunk, 0) * coalesce(atc_price, 0) + 
        coalesce(bigtime_sunk, 0) * coalesce(bt_price, 0)
      ),
      sum(bigtime_grant)
    ) AS racial_cost_basis
  FROM bigtimestudios.currency_analytics.bigtimeRacialCosts a
  LEFT JOIN base_bt_prices p ON date(a.created_at) = p.day
  GROUP BY 1, 2
),

-- =============================================================================
-- MODCHIP SPEND DATA
-- =============================================================================

modchip_data AS (
  SELECT
    date(day_hour) AS day,
    sum(Portal_USD) AS portal_usd,
    sum(Prestige_USD) AS prestige_usd,
    sum(Workshop_USD) AS workshop_usd,
    sum(Workshop_USD)/sum(ModChip_Qty) AS ModChip_Workshop_price,
  FROM bigtimestudios.currency_analytics.ModChip_Spend
  GROUP BY 1
),

modchip_spend_chg as (
  select 
  date(event_arrival_ts) Day,
  sum(case when recipeSlug like "%TatteredCrackedObolus%" then BonusRollCostTotal_0 else 0 end) as ModChip_ObolusRefining_qty,
  sum(case when recipeSlug like "%TatteredCrackedObolus%" then BonusRollCostTotal_0*a.ModChip_Workshop_price else 0 end) as ModChip_ObolusRefining_usd,
  sum(case when (recipeSlug like "%Extract_%") then  BonusRollCostTotal_0 else 0 end) as ModChip_ExtractCrackedHourglass_qty,
  sum(case when (recipeSlug like "%Extract_%") then  BonusRollCostTotal_0*a.ModChip_Workshop_price else 0 end) as ModChip_ExtractCrackedHourglass_USD,
  from bigtimestudios.currency_analytics.modchip_spend_by_craft a
  --cross join modchip_data b
  where BonusRollCostSlug_0='ModChip_Workshop'
  and (RecipeSlug like "%Extract%"
        or RecipeSlug like "%Obolus%")
  group by 1
),


-- =============================================================================
-- CRAFTING/DISMANTLING COSTS (OLD SYSTEM)
-- =============================================================================

old_crafting_costs AS (
  SELECT 
    date(a.created_at) AS day,
    sum(CASE
      WHEN json_extract_scalar(extra, "$.sourceString") = 'StartCraft timewarden' THEN amount
      WHEN json_extract_scalar(speed_up_payment, "$[0].archetypeId") = 'a98b86d8-ed8f-4e50-a223-e3a19191445b' 
        THEN cast(json_extract_scalar(speed_up_payment, "$[0].amount") AS float64)
      ELSE 0 
    END) * (1500/210000) AS dismantle_cost_basis
  FROM `openloot-362008.postgres_rds_public.users_premium_currencies_transaction` a 
  LEFT JOIN `openloot-362008.postgres_rds_public.premium_currencies` b ON a.premium_currency_id = b.id 
  LEFT JOIN openloot-362008.postgres_rds_crafts_public.crafts c ON a.transaction_source_id = c.id
  LEFT JOIN openloot-362008.postgres_rds_crafts_public.recipes d ON c.recipe_id = d.id
  WHERE a.created_at > '2025-04-01'
    AND date(a.created_at) <= '2025-05-14'
    AND action = 'deduct'
    AND b.name = 'Time Crystals'
    AND transaction_source IN ('craft', 'game')
    AND (
      json_extract_scalar(extra, "$.sourceString") = 'StartCraft timewarden'
      OR (d.name LIKE "%Extract $BIGTIME%" AND c.status != 'cancelled')
    )
  GROUP BY 1
),

-- =============================================================================
-- GILDED OBOLUS MARKETPLACE
-- =============================================================================

gilded_obolus_marketplace AS (
  SELECT
    date(payment_at) AS day,
    sum(0.05 * coalesce(price * quantity, usd_amount,0)) AS gilded_obolus_marketplace_revenue
  FROM openloot-362008.dbt_openloot.fact_sale_transactions
  WHERE item_type = 'premium-currency-package'
    AND payment_at > '2025-05-13'
    AND item_archetype_id IN ('OpenLoot_PristineMaterial_GildedObolus')
  GROUP BY 1
),

-- =============================================================================
-- NEW CRAFTING COSTS (POST TRANSITION)
-- =============================================================================

new_crafting_costs as (
  SELECT
  date(created_at) AS day,

  --All together
  sum(case when name='OpenLoot_ERC20_$BIGTIME' then amount else 0 end) overall_bigtime_sunk,
  sum(case when name='Gilded Obolus' then amount else 0 end) overall_obolus_sunk, 
  sum(case when name='OpenLoot_ERC20_$BIGTIME' then coalesce(amount,0)*coalesce(currency_price,0) else 0 end) overall_bigtime_sunk_usd,
  sum(case when name='Gilded Obolus' then coalesce(amount,0)*coalesce(currency_price,0) else 0 end) overall_obolus_sunk_usd, 
  
  --Obolus Refining
  sum(case when (name='OpenLoot_ERC20_$BIGTIME') and (craft_name like "%Cracked Obolus%")  then amount else 0 end) obolus_refine_bigtime_sunk,
  sum(case when (name='OpenLoot_ERC20_$BIGTIME') and (craft_name like "%Cracked Obolus%") then coalesce(amount,0)*coalesce(currency_price,0) else 0 end) obolus_refine_bigtime_sunk_usd,

  -- Dismantling
  sum(case when (name='OpenLoot_ERC20_$BIGTIME') and (craft_name like "%Extract $BIGTIME%")  then amount else 0 end) dismantle_bigtime_sunk,
  sum(case when (name='Gilded Obolus') and (craft_name like "%Extract $BIGTIME%") then amount else 0 end) dismantle_obolus_sunk, 
  sum(case when (name='OpenLoot_ERC20_$BIGTIME') and (craft_name like "%CExtract $BIGTIME%") then coalesce(amount,0)*coalesce(currency_price,0) else 0 end) dismantle_bigtime_sunk_usd,
  sum(case when (name='Gilded Obolus') and (craft_name like "%Extract $BIGTIME%") then coalesce(amount,0)*coalesce(currency_price,0) else 0 end) dismantle_obolus_sunk_usd, 

  FROM bigtimestudios.PlayerAnalytics.crafting_sinks_long
  WHERE (
    craft_name LIKE "%Cracked Obolus%"
    OR craft_name IN (
      'Cracked Obolus Processed into Gilded Obolus',
      'Cracked Obolus Pressed into Gilded Obolus', 
      'Cracked Obolus Wrought into Gilded Obolus',
      'Cracked Obolus Smelted into Gilded Obolus',
      'Cracked Obolus Shaped into Gilded Obolus',
      'Cracked Obolus Tempered into Gilded Obolus'
    )
    OR craft_name LIKE "%Extract $BIGTIME from%"
  )
  AND date(created_at) > '2025-05-13'
  GROUP BY 1),

-- =============================================================================
-- COMBINED CRAFTING COSTS
-- =============================================================================

combined_crafting_costs AS (
  -- Old system costs
  SELECT 
    day, 
    dismantle_cost_basis AS total_cost, 
    0 gilded_obolus_marketplace_revenue,
    0 overall_bigtime_sunk,
    0  overall_obolus_sunk, 
    0  overall_bigtime_sunk_usd,
    0  overall_obolus_sunk_usd, 
    0  obolus_refine_bigtime_sunk,
    0  obolus_refine_bigtime_sunk_usd,
    0  dismantle_bigtime_sunk,
    0  dismantle_obolus_sunk, 
    0  dismantle_bigtime_sunk_usd,
    0  dismantle_obolus_sunk_usd 
  FROM old_crafting_costs
  
  UNION ALL
  
  -- New system costs
  SELECT 
    coalesce(c.day, m.day) AS day,
    (coalesce(c.obolus_refine_bigtime_sunk_usd, 0) + coalesce(c.dismantle_obolus_sunk_usd,0) + coalesce(m.gilded_obolus_marketplace_revenue, 0)) AS total_cost,
    gilded_obolus_marketplace_revenue,
    overall_bigtime_sunk,
    overall_obolus_sunk, 
    overall_bigtime_sunk_usd,
    overall_obolus_sunk_usd, 
    obolus_refine_bigtime_sunk,
    obolus_refine_bigtime_sunk_usd,
    dismantle_bigtime_sunk,
    dismantle_obolus_sunk, 
    dismantle_bigtime_sunk_usd,
    dismantle_obolus_sunk_usd
  FROM new_crafting_costs c
  FULL OUTER JOIN gilded_obolus_marketplace m ON c.day = m.day
),

-- =============================================================================
-- DISMANTLE GRANTS
-- =============================================================================

dismantle_grants AS (
  SELECT
    date(created_at) AS day,
    sum(if(json_extract_scalar(extra, "$.sourceString") = "Craft",amount,0)) AS dismantle_grant,
    sum(amount) total_bigtime_grant,
  FROM openloot-362008.postgres_rds_public.token_transactions
  WHERE token_id = 'a07b874f-d30a-41d5-bfb1-879abf474590'
    AND status = 'completed'
  --  AND json_extract_scalar(extra, "$.sourceString") = "Craft"
    AND action = 'grant'
    AND created_at > timestamp_sub(current_timestamp(), INTERVAL 180 DAY)
  GROUP BY 1
),

-- =============================================================================
-- EFFICIENCY CALCULATIONS
-- =============================================================================

efficiency_base AS (
  SELECT
    coalesce(ol.day, ec.day, rd.day, rv.day) AS day,
    
    -- Revenue
    rv.openloot_revenue,
    
    -- $OL data
    ol.avg_ol_price,
    ol.imputed_ol_cost,
    ol.OLRP_grants/100 AS ol_grants,
    ifnull(ol.ol_sunk,0) ol_sunk,
    ol.spend_olrp_usd,
    
    -- Epoch data
    ec.weighted_bt_price,
    ec.spend_epochchest_usd,
    ec.imputed_epoch_cost_basis,
    ec.bigtime_grant_epoch,
    
    -- Racial data
    rd.avg_bt_price,
    rd.racial_cost_basis,
    rd.bigtime_grant_racial,
    coalesce(rd.spend_racial_usd, 0) + coalesce(mc.portal_usd, 0) + coalesce(mc.prestige_usd, 0) AS spend_racial_usd,
  
  -- Dismantling
    (coalesce(cc.total_cost, 0) + coalesce(ModChip_ObolusRefining_usd,0) + coalesce(ModChip_ExtractCrackedHourglass_USD,0)) AS spend_dismantle_usd,
    mc.workshop_usd,
    cc.total_cost,
    cc.dismantle_bigtime_sunk,
    cc.dismantle_obolus_sunk, 
    cc.dismantle_bigtime_sunk_usd,
    cc.dismantle_obolus_sunk_usd,
    cc.gilded_obolus_marketplace_revenue,
    cc.overall_obolus_sunk, 
    cc.overall_bigtime_sunk_usd,
    cc.overall_obolus_sunk_usd, 
    cc.obolus_refine_bigtime_sunk,
    cc.obolus_refine_bigtime_sunk_usd,

    mcs.ModChip_ObolusRefining_qty,
    mcs.ModChip_ObolusRefining_usd,
    mcs.ModChip_ExtractCrackedHourglass_qty,
    mcs.ModChip_ExtractCrackedHourglass_USD,
    dg.dismantle_grant 
  FROM bigtimestudios.currency_analytics.ol_efficiency ol
  FULL OUTER JOIN epoch_chest_data ec ON ol.day = ec.day
  FULL OUTER JOIN racial_data rd ON coalesce(ol.day, ec.day) = rd.day
  LEFT JOIN revenue_data rv ON coalesce(ol.day, ec.day, rd.day) = rv.day
  FULL OUTER JOIN combined_crafting_costs cc ON coalesce(ol.day, ec.day, rd.day, rv.day) = cc.day
  FULL OUTER JOIN dismantle_grants dg ON coalesce(ol.day, ec.day, rd.day, rv.day) = dg.day
  LEFT JOIN modchip_data mc ON coalesce(ol.day, ec.day, rd.day, rv.day) = mc.day
  left join modchip_spend_chg mcs on coalesce(ol.day, ec.day, rd.day, rv.day) = mcs.day
),

-- =============================================================================
-- FINAL CALCULATIONS WITH ROLLING AVERAGES
-- =============================================================================

final_calculations AS (
  SELECT
    *,
    
    -- Basic efficiency calculations
    safe_divide(
      safe_divide(spend_olrp_usd,(coalesce(ol_grants,0) - coalesce(ol_sunk,0))),
      avg_ol_price) ol_efficiency,
    
    safe_divide(
      safe_divide(spend_epochchest_usd, bigtime_grant_epoch),
      avg_bt_price
    ) AS epoch_efficiency,

    safe_divide(
      safe_divide(spend_racial_usd, bigtime_grant_racial),
      avg_bt_price
    ) AS racial_efficiency,

    safe_divide(
      safe_divide(spend_dismantle_usd, dismantle_grant),
      avg_bt_price
    ) AS dismantle_efficiency,

    safe_divide(
      safe_divide(total_cost, dismantle_grant),
      avg_bt_price
    ) AS dismantle_efficiency_no_modchip,

     safe_divide(coalesce(spend_epochchest_usd,0) + coalesce(spend_racial_usd,0) + coalesce(spend_dismantle_usd,0) + coalesce(spend_olrp_usd,0),
                (
                  coalesce(bigtime_grant_epoch,0) 
                + coalesce(bigtime_grant_racial,0) 
                + coalesce(dismantle_grant,0))*avg_bt_price 
                + (coalesce(ol_grants,0)
                ---coalesce(ol_sunk,0)
                )*(avg_ol_price)
                ) 
                AS epoch_racial_chg_olrp_efficiency,
    
    -- 7-day rolling averages
    SAFE_DIVIDE(
      SAFE_DIVIDE(
        SUM(COALESCE(spend_olrp_usd, 0)) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        SUM(COALESCE(ol_grants, 0)) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
      ),
      SAFE_DIVIDE(
        SUM(COALESCE(ol_grants, 0) * COALESCE(avg_ol_price, 0)) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        SUM(COALESCE(ol_grants, 0)) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
      )
    ) AS ol_efficiency_rolling_7,

    SAFE_DIVIDE(
      SAFE_DIVIDE(
        SUM(COALESCE(spend_epochchest_usd, 0)) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        SUM(COALESCE(bigtime_grant_epoch, 0)) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
      ),
      SAFE_DIVIDE(
        SUM(COALESCE(bigtime_grant_epoch, 0) * COALESCE(avg_bt_price, 0)) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        SUM(COALESCE(bigtime_grant_epoch, 0)) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
      )
    ) AS epoch_efficiency_rolling_7,

    SAFE_DIVIDE(
      SAFE_DIVIDE(
        SUM(COALESCE(spend_racial_usd, 0)) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        SUM(COALESCE(bigtime_grant_racial, 0)) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
      ),
      SAFE_DIVIDE(
        SUM(COALESCE(bigtime_grant_racial, 0) * COALESCE(avg_bt_price, 0)) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        SUM(COALESCE(bigtime_grant_racial, 0)) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
      )
  ) AS racial_efficiency_rolling_7,


    safe_divide(
      safe_divide(
        sum(spend_dismantle_usd) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        sum(dismantle_grant) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
      ),
      safe_divide(
        sum(coalesce(dismantle_grant,0)*avg_bt_price) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        sum(coalesce(dismantle_grant,0)) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
      )
    ) AS dismantle_efficiency_rolling_7,

    SAFE_DIVIDE(
  -- Numerator: Total spend in USD over 30 days (BT spend + OLRP spend + sunk OLRP)
      SUM(
        COALESCE(spend_epochchest_usd, 0)
        + COALESCE(spend_racial_usd, 0)
        + COALESCE(spend_dismantle_usd, 0)
        + COALESCE(spend_olrp_usd, 0)
     --   + COALESCE(ol_sunk, 0) * COALESCE(avg_ol_price, 0)
      ) OVER (ORDER BY day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW),

      -- Denominator: Market value of all token grants (BT + OL) in USD
      SUM(
        (
          COALESCE(bigtime_grant_epoch, 0)
          + COALESCE(bigtime_grant_racial, 0)
          + COALESCE(dismantle_grant, 0)
        ) * COALESCE(avg_bt_price, 0)
        +
        COALESCE(ol_grants, 0) * COALESCE(avg_ol_price, 0)
      ) OVER (ORDER BY day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW)
    ) AS epoch_racial_chg_olrp_efficiency_rolling_7,

    
    -- 30-day rolling averages
    SAFE_DIVIDE(
      SAFE_DIVIDE(
        SUM(COALESCE(spend_olrp_usd, 0)) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
        SUM(COALESCE(ol_grants, 0)) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
      ),
      SAFE_DIVIDE(
        SUM(COALESCE(ol_grants, 0) * COALESCE(avg_ol_price, 0)) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
        SUM(COALESCE(ol_grants, 0)) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
      )
    ) AS  ol_efficiency_rolling_30,

    SAFE_DIVIDE(
      SAFE_DIVIDE(
        SUM(COALESCE(spend_epochchest_usd, 0)) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
        SUM(COALESCE(bigtime_grant_epoch, 0)) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
      ),
      SAFE_DIVIDE(
        SUM(COALESCE(bigtime_grant_epoch, 0) * COALESCE(avg_bt_price, 0)) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
        SUM(COALESCE(bigtime_grant_epoch, 0)) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
      )
    ) AS epoch_efficiency_rolling_30,  
    
    SAFE_DIVIDE(
      SAFE_DIVIDE(
        SUM(COALESCE(spend_racial_usd, 0)) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
        SUM(COALESCE(bigtime_grant_racial, 0)) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
      ),
      SAFE_DIVIDE(
        SUM(COALESCE(bigtime_grant_racial, 0) * COALESCE(avg_bt_price, 0)) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
        SUM(COALESCE(bigtime_grant_racial, 0)) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
      )
  ) AS racial_efficiency_rolling_30,
    
    safe_divide(
      safe_divide(
        sum(spend_dismantle_usd) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
        sum(dismantle_grant) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
      ),
      safe_divide(
        sum(coalesce(dismantle_grant,0)*avg_bt_price) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
        sum(coalesce(dismantle_grant,0)) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
      )
    ) AS dismantle_efficiency_rolling_30,

     SAFE_DIVIDE(
  -- Numerator: Total spend in USD over 30 days (BT spend + OLRP spend + sunk OLRP)
      SUM(
        COALESCE(spend_epochchest_usd, 0)
        + COALESCE(spend_racial_usd, 0)
        + COALESCE(spend_dismantle_usd, 0)
        + COALESCE(spend_olrp_usd, 0)
    --    + COALESCE(ol_sunk, 0) * COALESCE(avg_ol_price, 0)
      ) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),

      -- Denominator: Market value of all token grants (BT + OL) in USD
      SUM(
        (
          COALESCE(bigtime_grant_epoch, 0)
          + COALESCE(bigtime_grant_racial, 0)
          + COALESCE(dismantle_grant, 0)
        ) * COALESCE(avg_bt_price, 0)
        +
        COALESCE(ol_grants, 0) * COALESCE(avg_ol_price, 0)
      ) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
    ) AS epoch_racial_chg_olrp_efficiency_rolling_30,

    SAFE_DIVIDE(
      SUM(
        COALESCE(spend_epochchest_usd, 0)
        + COALESCE(spend_racial_usd, 0)
        + COALESCE(spend_dismantle_usd, 0)
        + COALESCE(spend_olrp_usd, 0)
     --   + COALESCE(ol_sunk, 0) * COALESCE(avg_ol_price, 0)
      ) OVER (ORDER BY day ROWS BETWEEN 89 PRECEDING AND CURRENT ROW),

      SUM(
        (
          COALESCE(bigtime_grant_epoch, 0)
          + COALESCE(bigtime_grant_racial, 0)
          + COALESCE(dismantle_grant, 0)
        ) * COALESCE(avg_bt_price, 0)
        +
        COALESCE(ol_grants, 0) * COALESCE(avg_ol_price, 0)
      ) OVER (ORDER BY day ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
    ) AS epoch_racial_chg_olrp_efficiency_rolling_90,

  FROM efficiency_base
)

-- =============================================================================
-- FINAL OUTPUT
-- =============================================================================

SELECT *
FROM final_calculations
ORDER BY day DESC;

