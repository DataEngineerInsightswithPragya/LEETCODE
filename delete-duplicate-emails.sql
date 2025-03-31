-- SQL
WITH cte AS (
    SELECT id,
           RANK() OVER (PARTITION BY email ORDER BY id ASC) AS rank_num
    FROM Person
)
DELETE FROM Person
WHERE id IN (SELECT id FROM cte WHERE rank_num > 1);


-- KQL
-- let DuplicateRecords =
--     Person
--     | partition by email  // Groups data by email
--     | order by id asc  // Ensures the lowest ID gets rank 1
--     | extend rank_num = row_rank_dense()  // Assigns a dense rank within each email group
--     | where rank_num > 1  // Filters out the first occurrence (rank_num = 1) and keeps duplicates
--     | project id, email;  // Returns duplicate IDs and their corresponding emails
-- DuplicateRecords  // Displays the list of duplicate IDs







