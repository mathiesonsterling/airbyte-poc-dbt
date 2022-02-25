
select distinct
i.id as invoice_id,
i.user_id as invoice_user_id,
i.sales_tax_id,
i.invoice_number,
i.status as invoice_status,
i.amount,
UPPER(i.initials) AS invoice_initials,
UPPER(i.initials) AS approved_initials,
i.office as invoice_office,
i.created_at as invoice_created_at,
i.updated_at as invoice_updated_at,
client_id,
r.user_id as registration_user_id,
transaction_id,
r.status as registration_status,
r.first_name,
r.last_name,
rejection_reason,
archived,
r.initials as registration_initials,
r.office as registration_office,
r.created_at as registration_created_at,
r.updated_at as registration_updated_at,

u.id as mvr_id,
u.first_name as mvr_first_name,
u.last_name as mvr_last_name,
u.email as mvr_email,

u.status as mvr_status,
u.created_at as mvr_created_at,
u.updated_at as mvr_updated_at,
a.created_at as mvr_fulfilled_at,
a.action_details

from {{ ref('v_transform_sales_tax') }} r
full join {{ ref('v_transform_action') }} a on a.sales_tax_id = r.id
full join {{ ref('v_transform_invoice') }} i on r.id = i.sales_tax_id
full join {{ ref('v_transform_user') }} u on a.user_id = u.id
where a.action_details like "%fulfilled this sales tax.%"