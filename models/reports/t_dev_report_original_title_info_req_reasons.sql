select content,
       case
           when LOWER(content) like "%original title%damage%" then "Damage"
           when LOWER(content) like "%original title%sign%" then "Signature"
           when LOWER(content) like "%original title%front%back%" then "Front and Back"
           when LOWER(content) like "%original title%mileage%" then "Mileage / Odometer"
           when LOWER(content) like "%original title%odometer%" then "Mileage / Odometer"
           when LOWER(content) like "%original title%question%" then "Question"
           when LOWER(content) like "%original title%" then "Other Original Title"
           end as type
from {{ ref('v_transform_message')}}