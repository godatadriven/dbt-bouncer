{{ config(meta={"owner": "analytics-team"}) }}

select *
from (select 1 as id)
where id > 1
