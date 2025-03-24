-- SQL
select
    score,
    dense_rank() over (order by score desc) as 'rank'
from Scores




-- kql
-- Scores
-- | extend rank = rank() by score desc
-- | project score, rank


