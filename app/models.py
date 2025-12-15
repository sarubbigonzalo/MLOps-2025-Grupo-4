GET_RECOMMENDATIONS = """
    SELECT product_id, rank
    FROM recommendations
    WHERE 
        advertiser_id = %s
      AND UPPER(model) = %s
    ORDER BY rank ASC;
"""

GET_HISTORY = """
    SELECT date, product_id, rank
    FROM recommendations_history
    WHERE 
        advertiser_id = %s
    ORDER BY date DESC, rank ASC
    LIMIT 200;  -- últimos 7 días aprox.
"""

GET_STATS = """
    SELECT COUNT(DISTINCT advertiser_id) AS total_advertisers
    FROM recommendations;
"""