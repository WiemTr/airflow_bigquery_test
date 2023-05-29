WITH
  transactions AS (
  SELECT
    id,
    product_id customer_id,
    price*quantity AS total_tx_price
  FROM
    `{{ google_cloud_project_id }}.sales_management_dataset.purchases`
  LEFT JOIN
    `{{ google_cloud_project_id }}.sales_management_dataset.products`
  USING
    (product_id)
  WHERE brand_type IN {{ brand_types_list }}
  )
SELECT
  customer_id,
  SUM(total_tx_price) AS total_expenses
FROM
  transactions
Group By customer_id
