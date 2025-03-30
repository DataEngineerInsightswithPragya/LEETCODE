-- SQL
select
    name as Customers
from Customers
where id not in (select customerId from orders)


-- KQL
-- Customers
-- | project id, name
-- | join kind= leftanti (Orders | project customerId) on $left.id == $right.customerId
-- | project Customers = name