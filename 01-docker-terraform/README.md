
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public'
ORDER BY table_name;

SELECT * FROM green_taxi_trips LIMIT 10;