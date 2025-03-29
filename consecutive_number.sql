-- SQL
with lag_order as(
    select
    id,
    num,
    lag(num) over (order by id) as lag_num,
    lead(num) over (order by id) as lead_num
from Logs )
select 
    distinct num as ConsecutiveNums
from lag_order
where  num = lag_num  and num = lead_num and lag_num = lead_num


-- kql 
-- Logs
-- | order by id asc
-- | extend lag_num = prev(num) , lead_num = next(num)
-- | where num == lag_num and num == lead_num and lag_num == lead_num
-- | distinct num
