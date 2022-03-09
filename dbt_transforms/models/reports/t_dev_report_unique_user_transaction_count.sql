WITH unique_user_transaction_count AS (
SELECT
  transaction_id,
  MAX(unique_user_count) as unique_user_count,
  MAX(workTime) as workTime,
  MAX(holdTime) as holdTime,
  MAX(workEnd) as workEnd,
  MAX(action_office) as action_office,
  MAX(created_date) as created_date
FROM (
  SELECT
    DISTINCT c.transaction_id,
    unique_user_count,
    q.time_set.workTime,
    q.time_set.holdTime,
    q.time_set.workEnd,
    q.action_office,
    CASE
      WHEN c.transaction_id LIKE '%OR-%' THEN r.created_at
      WHEN c.transaction_id LIKE '%OP-%' THEN p.created_at
      WHEN c.transaction_id LIKE '%NDID-%' THEN nd.created_at
      WHEN c.transaction_id LIKE '%STO-%' THEN st.created_at
      WHEN c.transaction_id LIKE '%LA-%' THEN la.created_at
      WHEN c.transaction_id LIKE '%LR-%' THEN lr.created_at
      WHEN c.transaction_id LIKE '%UP-%' THEN up.created_at
  END
    AS created_date,
  FROM (
    SELECT
      DISTINCT transaction_id,
      COUNT(DISTINCT user_id) AS unique_user_count,
    FROM (
      SELECT
        DISTINCT a.user_id,
        COALESCE(r.transaction_id,
          p.transaction_id,
          st.transaction_id,
          nd.transaction_id,
          la.transaction_id,
          lr.transaction_id,
          up.transaction_id) AS transaction_id,
      FROM
        {{ ref('v_transform_action') }} a
      FULL JOIN
        {{ ref('v_transform_registration') }} r
      ON
        a.registration_id = r.id
      FULL JOIN
        {{ ref('v_transform_permit') }} p
      ON
        a.permit_id = p.id
      FULL JOIN
        {{ ref('v_transform_sales_tax') }} st
      ON
        a.sales_tax_id = st.id
      FULL JOIN
        {{ ref('v_transform_non_driver_id') }} nd
      ON
        a.non_driver_id_id = nd.id
      FULL JOIN
        {{ ref('v_transform_license_amendment') }} la
      ON
        a.license_amendment_id = la.id
      FULL JOIN
        {{ ref('v_transform_license_reciprocity') }} lr
      ON
        a.license_reciprocity_id = lr.id
      FULL JOIN
        {{ ref('v_transform_id_upgrade') }} up
      ON
        a.id_upgrade_id = up.id
      WHERE
        (r.fulfilled_on IS NOT NULL)
        OR (p.status = "Fulfilled")
        OR (st.status = "Fulfilled")
        OR (nd.status = "Fulfilled")
        OR (la.status = "Fulfilled")
        OR (lr.status = "Fulfilled")
        OR (up.status = "Fulfilled")
      GROUP BY
        a.user_id,
        transaction_id )
    WHERE
      transaction_id IS NOT NULL
    GROUP BY
      transaction_id) AS c
  FULL JOIN
    {{ ref('v_transform_registration') }} r
  ON
    c.transaction_id = r.transaction_id
  FULL JOIN
    {{ ref('v_transform_permit') }} p
  ON
    c.transaction_id = p.transaction_id
  FULL JOIN
    {{ ref('v_transform_non_driver_id') }} nd
  ON
    c.transaction_id = nd.transaction_id
  FULL JOIN
    {{ ref('v_transform_sales_tax') }} st
  ON
    c.transaction_id = st.transaction_id
  FULL JOIN
    {{ ref('v_transform_license_reciprocity') }} la
  ON
    c.transaction_id = la.transaction_id
  FULL JOIN
    {{ ref('v_transform_license_reciprocity') }} lr
  ON
    c.transaction_id = lr.transaction_id
  FULL JOIN
    {{ ref('v_transform_id_upgrade') }} up
  ON
    c.transaction_id = up.transaction_id
  FULL JOIN (
    SELECT
      transaction_id,
      {{target.schema}}.CalculateWorkTime(
        q.action_object,
        {{target.schema}}.DetermineTransactionEndTime(q.action_object)
      ) AS time_set,
      q.action_office
    FROM (
      SELECT
        COALESCE(r.transaction_id,
          p.transaction_id,
          st.transaction_id,
          nd.transaction_id,
          la.transaction_id,
          lr.transaction_id,
          up.transaction_id) AS transaction_id,
        ARRAY_AGG(STRUCT(a.created_at AS times,
            a.action_details AS details)
        ORDER BY
          a.created_at) AS action_object,
        MIN(a.office) AS action_office
      FROM
        {{ ref('v_transform_action') }} a
      FULL JOIN
        {{ ref('v_transform_registration') }} r
      ON
        a.registration_id = r.id
      FULL JOIN
        {{ ref('v_transform_permit') }} p
      ON
        a.permit_id = p.id
      FULL JOIN
        {{ ref('v_transform_sales_tax') }} st
      ON
        a.sales_tax_id = st.id
      FULL JOIN
        {{ ref('v_transform_non_driver_id') }} nd
      ON
        a.non_driver_id_id = nd.id
      FULL JOIN
        {{ ref('v_transform_license_amendment') }} la
      ON
        a.license_amendment_id = la.id
      FULL JOIN
        {{ ref('v_transform_license_reciprocity') }} lr
      ON
        a.license_reciprocity_id = lr.id
      FULL JOIN
        {{ ref('v_transform_id_upgrade') }} up
      ON
        a.id_upgrade_id = up.id
      GROUP BY
        transaction_id ) AS q ) q
  ON
    q.transaction_id = c.transaction_id
  WHERE
    c.transaction_id IS NOT NULL
    AND time_set.workEnd IS NOT NULL)
GROUP BY
  transaction_id
)

SELECT *,
CASE WHEN CONTAINS_SUBSTR(transaction_id, "OR-") THEN workTime END AS average_work_or_mins,
CASE WHEN CONTAINS_SUBSTR(transaction_id, "OP-") THEN workTime END AS average_work_op_mins
FROM unique_user_transaction_count
