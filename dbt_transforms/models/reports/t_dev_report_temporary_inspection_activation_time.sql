select
    date(temporary_inspection_date) as d,
    round(avg(date_diff(date(temporary_inspection_date), date(fulfilled_on), DAY)),1) as f0_
from
    {{ ref('v_transform_registration')}}
where temporary_inspection_date is not null
group by d
order by d desc
