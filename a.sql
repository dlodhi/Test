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
