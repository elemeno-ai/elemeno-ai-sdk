"""
CREATE TEMP FUNCTION region(customer_state STRING)
RETURNS STRING AS (
  CASE
WHEN customer_state IN ('RO', 'AC', 'AM', 'RR', 'PA', 'AP', 'TO') THEN 'norte'
WHEN customer_state IN ('MA', 'PI', 'CE', 'RN', 'PB', 'PE', 'AL', 'SE', 'BA') THEN 'nordeste'
WHEN customer_state IN ('MG', 'ES', 'RJ', 'SP') THEN 'sudeste'
WHEN customer_state IN ('PR', 'RS', 'SC') THEN 'sul'
WHEN customer_state IN ('MS', 'MT', 'GO', 'DF') THEN 'centro_oeste'
END
);

SELECT
DISTINCT
brand_id as brand,
customer_id as customer_id,
customer_b2wuid as customer_b2wuid,
address.state as main_state,
customer_type.pf.customer_gender as gender,
customer_email.customer_email_valid as flag_last_email_valid,
customer_optin.mailing as flag_mailing,
customer_optin.receive_sms as flag_receive_sms,
customer_optin.receive_whatsapp as flag_receive_whatsapp,
customer_optin.push_permission as flag_push_permission,
customer_optin.optin_push_deliverability as flag_optin_push_deliverability,
customer_optin.mailing_soub as flag_mailing_soub,
customer_prime.prime_flag as flag_prime,
DATE_DIFF(ifnull(customer_prime.dt_fim_vigencia, CURRENT_DATETIME('America/Sao_Paulo')),customer_prime.dt_inicio_vigencia, DAY) as prime_vigency_time,
customer_prime.status  as prime_status,
customer_prime.classificacao  as prime_classification,
customer_prime.tipo_plano  as prime_plan_type,
customer_prime.plano  as prime_plan,
comp_brand.register_acom_flag as flag_register_acom,
comp_brand.register_suba_flag as flag_register_suba,
comp_brand.register_shop_flag as flag_register_shop,
comp_brand.register_soub_flag as flag_register_soub,
IF (churn_date<=CURRENT_DATE('America/Sao_Paulo'),true, false) as flag_churn,
CAST(score_rfv as INT64) as score_rfv,
DATE_DIFF(CURRENT_DATETIME('America/Sao_Paulo'), customer_type.pf.customer_birth_date, YEAR) as age,
DATE_DIFF(CURRENT_DATETIME('America/Sao_Paulo'), cast(customer_registration_date as date), DAY) as register_time,
IF(region(address.state)='norte',true, false) as flag_north_region,
IF(region(address.state)='nordeste',true, false) as flag_northeast_region,
IF(region(address.state)='sudeste',true, false) as flag_southeast_region,
IF(region(address.state)='sul',true, false) as flag_south_region,
IF(region(address.state)='centro_oeste',true, false) as flag_midwest_region,
TIMESTAMP(CURRENT_DATETIME('America/Sao_Paulo')) as created_timestamp,
TIMESTAMP(CURRENT_DATETIME('America/Sao_Paulo')) as event_timestamp
FROM
`{table_source}`,
UNNEST(company_brand) as comp_brand,
UNNEST(customers_keys_list) as keys
WHERE
date_trunc(customer_registration_month, year) >= "1998-01-01"
"""

from elemeno_ai_sdk.features.utils import create_insert_into

create_insert_into()
