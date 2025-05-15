WITH detail_tran AS (
    SELECT 
        a.uid,
        a.acn_cat_nme,
        a.tagvalue AS tran_amt,
        -- Assign a unique row number per user/event (e.g. earliest event by date)
        ROW_NUMBER() OVER (
          PARTITION BY a.uid, a.acn_cat_nme 
          ORDER BY a.<timestamp_column> ASC
        ) AS rn
    FROM pdm_roib_audit_details AS a
    JOIN dpd.uk_digital_crnt_db.oiu_evnt_main_atrb_config AS b
      ON LOWER(a.acn_cat_nme) = LOWER(b.evnt_cd)
     AND LOWER(b.trgt_col_nm) = 'tran_amt'
     AND LOWER(a.tagname) = LOWER(b.tag_nm)
),
first_tran AS (
    -- Keep only the first (rn=1) transaction per user/category
    SELECT uid, acn_cat_nme, tran_amt 
    FROM detail_tran
    WHERE rn = 1
)
SELECT 
    c.uid,
    c.acn_cat_nme,
    REGEXP_REPLACE(d.tran_amt, '[^0-9]', '') AS tran_amt_first
FROM pdm_roib_audit_log_main AS c
JOIN first_tran AS d
  ON c.uid = d.uid
 AND LOWER(c.acn_cat_nme) = LOWER(d.acn_cat_nme)
WHERE 
    d.tran_amt IS NOT NULL 
    AND TRIM(d.tran_amt) <> '';










WITH filtered AS (
  SELECT *
  FROM psdm_robl_audit_details
  WHERE log_dte = current_date_dt
    AND tagvalue IS NOT NULL
    AND TRIM(tagvalue) <> ''
    AND LOWER(TRIM(tagvalue)) <> 'null'
),
tagged AS (
  SELECT
    uid,
    log_dte,
    tagvalue,
    acn_cat_me,
    CASE
      WHEN LOWER(TRIM(acn_cat_me)) LIKE '%instn%' THEN '/olb/ku/pay/grouppaymentsstepfour.json'
      WHEN LOWER(TRIM(acn_cat_me)) LIKE '%paym%' THEN '/olb/ku/pay/payments.json'
      ELSE REGEXP_REPLACE(tagname, '[0-9]', '')
    END AS tagname
  FROM filtered
),
dedup AS (
  SELECT DISTINCT
    uid,
    log_dte,
    tagname,
    tagvalue,
    acn_cat_me
  FROM tagged
),
joined AS (
  SELECT
    d.*,
    c.EVNT_CD,
    c.TRGT_COL,
    c.TAG_NM AS CFG_TAG_NM
  FROM dedup d
  LEFT JOIN olb_evnt_main_atrb_config c
    ON LOWER(d.acn_cat_me) = LOWER(c.EVNT_CD)
),
with_tran AS (
  SELECT
    j.*,
    CASE
      WHEN LOWER(j.acn_cat_me) = LOWER(j.EVNT_CD)
           AND j.TRGT_COL = 'tran_amt'
           AND j.CFG_TAG_NM = LOWER(j.tagname)
      THEN j.tagvalue
      ELSE NULL
    END AS tran_amt
  FROM joined j
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY uid, EVNT_CD ORDER BY uid ASC) AS rn
  FROM with_tran
)
SELECT
  uid,
  log_dte,
  tagname,
  tagvalue,
  EVNT_CD,
  TRGT_COL,
  CFG_TAG_NM AS TAG_NM,
  tran_amt
FROM ranked
WHERE rn = 1;
