
-- Query check for Data Quality on transformed-yellow_taxi_enriched table

SELECT 
    COUNT(*) as total_rows,
    COUNT(CASE WHEN "RatecodeID" NOT IN (1,2,3,4,5,6,99) THEN 1 END) as bad_rate_codes,
    COUNT(CASE WHEN "fare_amount" < 0 THEN 1 END) as negative_fares
FROM "training-database"."transformed-yellow_taxi_enriched";