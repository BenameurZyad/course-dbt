# Week 1 Project:

**Question :**  
How many users do we have?  
  
**Answer :**  
130 unique users  
  
**Query :**  
  
```sql  
SELECT
COUNT(DISTINCT(user_id)) AS users_count
FROM dev_db.dbt_zyadbenameursanimaxcom.users
```
  
---  
  
**Question :**  
On average, how many orders do we receive per hour?  
  
**Answer :**  
15.041 orders per hour on average  
  
**Query :**  

```sql
SELECT DISTINCT
    AVG(COUNT(*)) OVER () AS mean_order_per_hour
FROM
    dev_db.dbt_zyadbenameursanimaxcom.orders
GROUP BY
    EXTRACT(HOUR FROM created_at)
```
  
---  
  
**Question :**    
On average, how long does an order take from being placed to being delivered?  
  
**Answer :**  
3.89 Days on average  
  
**Query :**  

```sql
SELECT
    AVG(DATEDIFF(DAY, created_at, delivered_at)) AS avg_delivery_time_days
FROM
dev_db.dbt_zyadbenameursanimaxcom.orders
WHERE status = 'delivered'
```
  
---  
  
**Question :**  
How many users have only made one purchase? Two purchases? Three+ purchases?  
  
**Answer :**    
PURCHASE_PROFILE VISIT_COUNT  
One purchase 25  
Two purchases 56  
Three+ purchases 280  
  
**Query :**  
  
```sql
WITH all_visits AS(
SELECT
COUNT(DISTINCT(order_id)) AS visit_count,
CASE
    WHEN visit_count = 1 THEN 'One purchase'
    WHEN visit_count = 2 THEN 'Two purchases'
    WHEN visit_count >= 3 THEN 'Three+ purchases'
END AS purchase_profile

FROM
dev_db.dbt_zyadbenameursanimaxcom.orders
GROUP BY user_id
)

SELECT
purchase_profile,
SUM(visit_count) AS visit_count
FROM all_visits
GROUP BY purchase_profile
```
  
---  
  
**Question :**    
On average, how many unique sessions do we have per hour?  
  
**Answer :**    
39.458 sessions on average per hours    
  
**Query :**  
  
```sql
SELECT DISTINCT
    AVG(COUNT(DISTINCT(session_id))) OVER () AS mean_sessions_per_hour
FROM
    dev_db.dbt_zyadbenameursanimaxcom.events
GROUP BY
    EXTRACT(HOUR FROM created_at)
```
  