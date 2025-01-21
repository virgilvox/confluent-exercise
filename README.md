# confluent-exercise

1. Create Cluster, topic, and add data contract (avro schema) on the topic directly in cloud.
2. Create keys for producer and schema registry. Add to config files. 
3. Run producer.
4. Run queries in Flink.


Average watch times
```sql
SELECT
    movie_title,
    AVG(watch_duration) AS avg_watch_duration
FROM
    netflix_user_behavior
GROUP BY
    movie_title;
```

Daily Averages
```sql
SELECT
    DATE_FORMAT(TO_TIMESTAMP(`timestamp`), 'yyyy-MM-dd') AS view_date,
    movie_title,
    COUNT(*) AS daily_view_count,
    SUM(watch_duration) AS total_watch_time
FROM
    netflix_user_behavior
GROUP BY
    DATE_FORMAT(TO_TIMESTAMP(`timestamp`), 'yyyy-MM-dd'),
    movie_title;

```

Weekly Averages for specific movies
```sql
SELECT
    DATE_FORMAT(`timestamp`, 'E') AS day_of_week,  
    COUNT(*) AS daily_view_count,                
    SUM(watch_duration) AS total_watch_time     
FROM
    netflix_user_behavior
WHERE
    movie_title = 'The SpongeBob SquarePants Movie'         
    AND DATE_FORMAT(`timestamp`, 'E') IN ('Mon', 'Tue', 'Wed', 'Thu', 'Fri')  
GROUP BY
    DATE_FORMAT(`timestamp`, 'E');  
```