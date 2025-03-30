select
    distinct email as Email
from Person
group by email
having count(email) > 1

-- KQL
-- Person
-- | summarize email_count = count() by email
-- | where email_count > 1
-- | project Email = email
