-- SQL
select 
    e.name as employee
    -- m.*,
    -- e.*
from employee m join employee e on e.managerid = m.id
where e.salary > m.salary


--  KQL
-- Employee
-- | join kind=inner (Employee) on $left.managerid == $right.id
-- | where $left.salary > $right.salary
-- | project EmployeeName = $left.name
