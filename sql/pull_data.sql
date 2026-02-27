select
    MODEL_EXECUTION_CONTEXT:acaps_ref_id acap_key,
    MODEL_EXECUTION_TIMESTAMP,
    MODEL_OUTPUT:score as score,
    model_infrastructure_id as EXL_Container_Version,
    f.key as feature,
    f.value as value
from sb_credit_analytics.model_execution_log mel,
    LATERAL FLATTEN(INPUT => mel.model_input:Feature) AS f
where
    MODEL_ID = 'BAT-2' and
    MODEL_EXECUTION_CONTEXT:shadow = 'false';