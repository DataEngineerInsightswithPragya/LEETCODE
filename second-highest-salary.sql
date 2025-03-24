---- SQL
with Salary_order as (select
    id,salary,
    dense_rank() over (order by salary desc) as rank_number
from Employee)
select
    coalesce(max(salary),NULL) as SecondHighestSalary
from Salary_order
where rank_number = 2


------ KQL
Employee
| extend rank_number = rank(desc(salary))
| where rank_number == 2
| summarize SecondHighestSalary = max(salary)

