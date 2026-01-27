-- 1. show tables
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public'
ORDER BY table_name;

-- 2. First 10 rows from green_taxi_trips
SELECT * FROM green_taxi_trips LIMIT 10;


--- Q3 
SELECT COUNT(*)
FROM green_trips
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance <= 1; 

  ---- count 8807


 --- Q4
 SELECT
  DATE(lpep_pickup_datetime) AS pickup_day,
  MAX(trip_distance) AS max_trip_distance
FROM green_trips
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance < 100
GROUP BY DATE(lpep_pickup_datetime)
ORDER BY max_trip_distance DESC
LIMIT 1;

--- "2025-11-14"	88.03---


---- Q5
SELECT
  z."Zone" AS pickup_zone,
  SUM(t.total_amount) AS total_amount_sum
FROM green_trips t
JOIN zones z
  ON t."PULocationID" = z."LocationID"
WHERE t.lpep_pickup_datetime >= '2025-11-18'
  AND t.lpep_pickup_datetime <  '2025-11-19'
GROUP BY z."Zone"
ORDER BY total_amount_sum DESC
LIMIT 1;

--- "East Harlem North"	9281.919999999996


---- Q6
SELECT
  z_drop."Zone" AS dropoff_zone,
  t.tip_amount
FROM green_trips t
JOIN zones z_pick
  ON t."PULocationID" = z_pick."LocationID"
JOIN zones z_drop
  ON t."DOLocationID" = z_drop."LocationID"
WHERE z_pick."Zone" = 'East Harlem North'
  AND t.lpep_pickup_datetime >= '2025-11-01'
  AND t.lpep_pickup_datetime <  '2025-12-01'
ORDER BY t.tip_amount DESC
LIMIT 1;

--- "Yorkville West"	81.89

---Q7 terraform init, terraform apply -auto-approve, terraform destroy