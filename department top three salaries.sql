-- SQL
with salary_order as (select
    e.*,
    d.name as Department,
    dense_rank()  over(partition by departmentId order by salary desc) as rank_num
from employee e join department d on e.departmentId = d.id)
select
    Department,
    name as Employee,
    salary as Salary
from salary_order
where rank_num <= 3


-- KQL
-- let EmployeeData =
--     Employee
--     | project Employee = name, Salary = salary, departmentId;
-- let DepartmentData =
--     Department
--     | project Department = name, id;
-- let MergedData =
--     EmployeeData
--     | join kind=inner (DepartmentData) on $left.departmentId == $right.id
--     | project Employee, Salary, Department;
-- let RankedData =
--     MergedData
--     | partition by Department
--     | order by Salary desc
--     | extend rank_num = row_rank_dense();
-- RankedData
-- | where rank_num <= 3
-- | project Department, Employee, Salary




