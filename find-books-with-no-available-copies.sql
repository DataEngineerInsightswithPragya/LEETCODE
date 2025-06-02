with intermediate_result as (select
    l.book_id,
    l.title,
    l.author,
    l.genre,
    l.publication_year,
    l.total_copies,
    count(b.borrower_name) as current_borrowers
from library_books l
join borrowing_records b
on l.book_id = b.book_id
where b.return_date is NULL
group by b.book_id
having current_borrowers = total_copies)
select book_id,title,author,genre,publication_year,current_borrowers from intermediate_result
order by current_borrowers desc,title asc
