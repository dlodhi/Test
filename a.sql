WITH filtered_event_details AS (
    SELECT 
        id,
        tagname,
        tagvalue,
        event_date
    FROM 
        e_bmb_db.bmb_event_details
    WHERE 
        event_date = CURRENT_DATE() -- Assuming `CURR_BUS_DI` is the current date
),
grouped_event_data AS (
    SELECT 
        id,
        COLLECT_LIST(
            STRUCT(tagname AS key, tagvalue AS value)
        ) AS eventdata
    FROM 
        filtered_event_details
    GROUP BY 
        id
)
SELECT 
    g.id,
    g.eventdata,
    e.event_log_details
FROM 
    grouped_event_data g
LEFT JOIN 
    e_bmb_db.event_log e
ON 
    g.id = e.id
WHERE 
    e.event_date = CURRENT_DATE();  -- Assuming `CURR_BUS_PT` is the current date
