select
    product_id,
    name
from products
where name REGEXP '[0-9]{3}' and name not REGEXP '[0-9]{4}'
