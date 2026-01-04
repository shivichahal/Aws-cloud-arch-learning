
-- Query check for Data Quality on transformed-yellow_taxi_enriched table

SELECT 
    COUNT(*) as total_rows,
    COUNT(CASE WHEN "RatecodeID" NOT IN (1,2,3,4,5,6,99) THEN 1 END) as bad_rate_codes,
    COUNT(CASE WHEN "fare_amount" < 0 THEN 1 END) as negative_fares
FROM "training-database"."transformed-yellow_taxi_enriched";


--- Additional checks can be added as needed

SELECT * FROM "training-database"."yellow_tripdata_2025_08" limit 100;

select count(1) from "training-database"."yellow_tripdata_2025_08".  ---3574091


select *  from "training-database"."yellow_tripdata_2025_08" where RatecodeID is null limit 10;

select *  from "training-database"."yellow_tripdata_2025_08" where PULocationID = DOLocationID ;

select distinct  DOLocationID  from "training-database"."yellow_tripdata_2025_08" where DOLocationID not in (SELECT locationid FROM "training-database"."taxi_zone_lookup")