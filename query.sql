
  
      SELECT 
          'MVA' AS Service_Name, 
          "CD" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_cd.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'MVA' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'MVA' 
          END AS Service_Name, 
          "CD" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_cd.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'SECURE NET' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'SECURE NET' 
          END AS Service_Name, 
          "CD" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "CD"
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'SECURE NET' AS Service_Name, 
          "CD" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "CD"
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'MVA' AS Service_Name, 
          "MZ" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_mz.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'SECURE NET' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'SECURE NET' 
          END AS Service_Name, 
          "MZ" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "MZ"
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'SECURE NET' AS Service_Name, 
          "MZ" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "MZ"
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'MVA' AS Service_Name, 
          "RO" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_ro.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'MVA' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'MVA' 
          END AS Service_Name, 
          "RO" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_ro.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'SECURE NET' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'SECURE NET' 
          END AS Service_Name, 
          "RO" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "RO"
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'SECURE NET' AS Service_Name, 
          "RO" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "RO"
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'MVA' AS Service_Name, 
          "TR" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_tr.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'MVA' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'MVA' 
          END AS Service_Name, 
          "TR" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_tr.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'SECURE NET' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'SECURE NET' 
          END AS Service_Name, 
          "TR" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "TR"
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'SECURE NET' AS Service_Name, 
          "TR" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "TR"
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'MVA' AS Service_Name, 
          "UK" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_uk.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'MVA' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'MVA' 
          END AS Service_Name, 
          "UK" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_uk.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'PHOENIX' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'PHOENIX' 
          END AS Service_Name, 
          "UK" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_phoenix.v_events_app_uk
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'SECURE NET' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'SECURE NET' 
          END AS Service_Name, 
          "UK" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "GB"
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'SECURE NET' AS Service_Name, 
          "UK" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "GB"
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'MVA' AS Service_Name, 
          "GR" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_gr.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'MVA' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'MVA' 
          END AS Service_Name, 
          "GR" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_gr.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'SECURE NET' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'SECURE NET' 
          END AS Service_Name, 
          "GR" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "GR"
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'SECURE NET' AS Service_Name, 
          "GR" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "GR"
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'MVA' AS Service_Name, 
          "TZ" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_tz.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'SECURE NET' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'SECURE NET' 
          END AS Service_Name, 
          "TZ" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "TZ"
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'SECURE NET' AS Service_Name, 
          "TZ" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "TZ"
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'MVA' AS Service_Name, 
          "GH" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_gh.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'MVA' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'MVA' 
          END AS Service_Name, 
          "GH" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_gh.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'SECURE NET' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'SECURE NET' 
          END AS Service_Name, 
          "GH" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "GH"
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'SECURE NET' AS Service_Name, 
          "GH" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "GH"
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'MVA' AS Service_Name, 
          "IN" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_in.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'SECURE NET' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'SECURE NET' 
          END AS Service_Name, 
          "IN" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "IN"
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'SECURE NET' AS Service_Name, 
          "IN" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "IN"
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'MVA' AS Service_Name, 
          "ZA" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_za.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'MVA' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'MVA' 
          END AS Service_Name, 
          "ZA" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_za.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'SECURE NET' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'SECURE NET' 
          END AS Service_Name, 
          "ZA" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "ZA"
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'SECURE NET' AS Service_Name, 
          "ZA" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "ZA"
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'MVA' AS Service_Name, 
          "CZ" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_cz.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'MVA' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'MVA' 
          END AS Service_Name, 
          "CZ" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_cz.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'SECURE NET' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'SECURE NET' 
          END AS Service_Name, 
          "CZ" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "CZ"
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'SECURE NET' AS Service_Name, 
          "CZ" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "CZ"
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'MVA' AS Service_Name, 
          "PT" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_pt.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'MVA' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'MVA' 
          END AS Service_Name, 
          "PT" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_pt.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'SECURE NET' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'SECURE NET' 
          END AS Service_Name, 
          "PT" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "PT"
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'SECURE NET' AS Service_Name, 
          "PT" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "PT"
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'MVA' AS Service_Name, 
          "EG" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_eg.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'SECURE NET' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'SECURE NET' 
          END AS Service_Name, 
          "EG" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "EG"
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'SECURE NET' AS Service_Name, 
          "EG" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "EG"
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'MVA' AS Service_Name, 
          "HU" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_hu.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'MVA' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'MVA' 
          END AS Service_Name, 
          "HU" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_hu.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'SECURE NET' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'SECURE NET' 
          END AS Service_Name, 
          "HU" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "HU"
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'SECURE NET' AS Service_Name, 
          "HU" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "HU"
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'MVA' AS Service_Name, 
          "DE" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_de.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'MVA' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'MVA' 
          END AS Service_Name, 
          "DE" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_de.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'PHOENIX' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'PHOENIX' 
          END AS Service_Name, 
          "DE" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_phoenix.v_events_app_de
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'SECURE NET' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'SECURE NET' 
          END AS Service_Name, 
          "DE" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "DE"
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'SECURE NET' AS Service_Name, 
          "DE" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "DE"
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'MVA' AS Service_Name, 
          "QA" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_qa.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'MVA' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'MVA' 
          END AS Service_Name, 
          "QA" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_qa.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'SECURE NET' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'SECURE NET' 
          END AS Service_Name, 
          "QA" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "QA"
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'SECURE NET' AS Service_Name, 
          "QA" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "QA"
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'MVA' AS Service_Name, 
          "AL" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_al.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'MVA' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'MVA' 
          END AS Service_Name, 
          "AL" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_al.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'SECURE NET' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'SECURE NET' 
          END AS Service_Name, 
          "AL" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "AL"
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'SECURE NET' AS Service_Name, 
          "AL" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "AL"
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'MVA' AS Service_Name, 
          "IT" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_it.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'MVA' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'MVA' 
          END AS Service_Name, 
          "IT" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_it.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'PHOENIX' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'PHOENIX' 
          END AS Service_Name, 
          "IT" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_phoenix.v_events_app_it
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'SECURE NET' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'SECURE NET' 
          END AS Service_Name, 
          "IT" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "IT"
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'SECURE NET' AS Service_Name, 
          "IT" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "IT"
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'MVA' AS Service_Name, 
          "IE" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_ie.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'MVA' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'MVA' 
          END AS Service_Name, 
          "IE" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_ie.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'PHOENIX' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'PHOENIX' 
          END AS Service_Name, 
          "IE" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_phoenix.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'SECURE NET' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'SECURE NET' 
          END AS Service_Name, 
          "IE" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "IE"
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'SECURE NET' AS Service_Name, 
          "IE" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "IE"
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'MVA' AS Service_Name, 
          "ES" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_es.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'MVA' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'MVA' 
          END AS Service_Name, 
          "ES" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_es.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'PHOENIX' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'PHOENIX' 
          END AS Service_Name, 
          "ES" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_phoenix.v_events_app_es
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'SECURE NET' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'SECURE NET' 
          END AS Service_Name, 
          "ES" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "ES"
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'SECURE NET' AS Service_Name, 
          "ES" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "ES"
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'MVA' AS Service_Name, 
          "LS" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-smapi-01.vf_smapi_dataflow_out_ls.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          CASE 
              WHEN 'SECURE NET' = 'MVA' AND SAFE_CAST(LEFT(x_vf_trace_source_version, 1) AS int) >= 8 THEN 'OneApp' 
              ELSE 'SECURE NET' 
          END AS Service_Name, 
          "LS" AS Market,
          "App" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users,
              x_vf_trace_source_version
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order,
                  x_vf_trace_source_version
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_install_id) AS Monthly_installation_id,
                      x_vf_trace_source_version
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_app
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "LS"
                  GROUP BY 
                      1, 2, 3, x_vf_trace_source_version
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  UNION ALL
  
      SELECT 
          'SECURE NET' AS Service_Name, 
          "LS" AS Market,
          "Web" AS web_app_flag,
          year, 
          Month, 
          cnt_curr_month_users, 
          (safe_divide((cnt_curr_month_users - cnt_prev_month_users), cnt_prev_month_users)) * 100 AS user_growth_rate
      FROM (
          SELECT
              year,
              Month,
              Monthly_installation_id AS cnt_curr_month_users,
              month_order,
              LAG(Monthly_installation_id) OVER (ORDER BY month_order ASC) AS cnt_prev_month_users
          FROM (
              SELECT
                  installation_year AS year,
                  installation_month AS month,
                  Monthly_installation_id,
                  yearmonth,
                  ROW_NUMBER() OVER () AS month_order
              FROM (
                  SELECT
                      EXTRACT(YEAR FROM sec_received_at) AS installation_year,
                      EXTRACT(MONTH FROM sec_received_at) AS installation_month,
                      EXTRACT(MONTH FROM sec_received_at) || EXTRACT(YEAR FROM sec_received_at) AS yearmonth,
                      COUNT(DISTINCT x_vf_trace_subject_id) AS Monthly_installation_id
                  FROM
                      vf-grp-cpsa-prd-cpsoi-14.securenet_smapi.events_web
                  WHERE
                      DATE(sec_received_at) BETWEEN '2025-01-01' AND '2025-02-28'
                      AND x_vf_trace_country_code = "LS"
                  GROUP BY 
                      1, 2, 3
                  ORDER BY 
                      1, 2
              )
          )
          ORDER BY 
              4
      )
  