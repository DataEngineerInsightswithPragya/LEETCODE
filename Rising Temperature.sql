-- sql
with cte as (
    select
    *,
    lag(temperature) over (order by recordDate asc) as lag_temp,
    lag(recordDate) over (order by recordDate asc) as lag_recorddate
from weather
), cte2 as (select
    id,
    DATEDIFF(lag_recorddate,recordDate)  as day_diff,
    temperature,
    lag_temp
from cte )
select
    id
from cte2
where day_diff = -1 and temperature > lag_temp


-- kql
-- let a = weather
-- | order by recordDate asc
-- | extend lag_temp = previous(temperature),
--          lag_recordDate = previous(recordDate)
-- | project id, recordDate, lag_recordDate, temperature, lag_temp;
-- let b = a
-- | extend day_diff = datetime_diff(recordDate, lag_recordDate, 'day')  // Corrected function
-- | project id, day_diff, temperature, lag_temp;
-- let c = b
-- | where day_diff == 1 and temperature > lag_temp  // Corrected condition: day_diff should be 1 (not -1)
-- | project id;
-- c


