-- Власний тест 2. Спеціальні члени вимірів присутні рівно один раз.
with members as (
    select 'zone_-1'        as member, count(*) as n from {{ ref('dim_zone') }}   where zone_key = -1
    union all
    select 'driver_unknown' as member, count(*) as n from {{ ref('dim_driver') }} where driver_key = 'unknown'
)
select member, n
from members
where n <> 1
