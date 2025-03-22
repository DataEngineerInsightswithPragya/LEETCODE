-- SQL
SELECT
    p.firstName,
    p.lastName,
    a.city,
    a.state
FROM Person p left join Address a on p.personId = a.personId

-- KQL
Person
| project firstName, lastName, personid
| join kind = leftOuter (Address | project city,state,personid) on $left.personid == $right.personid

