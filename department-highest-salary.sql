with joined_cte as (select
 e.*,
 d.name as Department
from employee e join department d on e.departmentid = d.id)
, ranked_salary as (select
    Department,
    name as Employee,
    salary as Salary,
    dense_rank() over (partition by Department order by salary desc) as rank_num
from joined_cte)
select
    Department,Employee,Salary from ranked_salary where rank_num = 1

-- KQL
-- let joined_cte =
--     employee
--     | join kind=inner department on $left.departmentid == $right.id
--     | extend Department = d.name;
-- let ranked_salary =
--     joined_cte
--     | project Department, Employee=name, Salary=salary
--     | partition by Department
--     (
--         order by Salary desc
--         | extend rank_num = row_rank()
--     );
-- ranked_salary
-- | where rank_num == 1
-- | project Department, Employee, Salary

