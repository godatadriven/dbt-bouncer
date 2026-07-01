{{ config(meta={"owner": "analytics-team"}, where="1 = 1") }}

select *
from (select 1 as id)
where id > 1
