

object_list_query = """
    with base as (
    
    SELECT
        observed_timestamp
        , resource_attributes
        , resource_attributes:"snow.database.name"::string database_name
        , resource_attributes:"snow.schema.name"::string schema_name 
        , resource_attributes:"snow.executable.type"::string type
        , resource_attributes:"snow.executable.name"::string name
        , resource_attributes:"snow.query.id"::string query_id
        , resource_attributes:"snow.warehouse.name"::string warehouse_name
        , resource_attributes:"telemetry.sdk.language"::string language
        , record_type
        , record
        , record_attributes
        , (Value::string) Value
        --, resource_attributes.snow:name
        --, snowquery
    FROM et_quickstart_db3.public.quick_start_tb3 e)
    
    
    select database_name, schema_name, type, name
    from base 
    group by 1,2,3,4
    
    """

base_query = """
    SELECT
        timestamp
        , timestamp::date date
        , resource_attributes
        , resource_attributes:"snow.database.name"::string database_name
        , resource_attributes:"snow.schema.name"::string schema_name 
        , resource_attributes:"snow.executable.type"::string type
        , resource_attributes:"snow.executable.name"::string name
        , resource_attributes:"snow.query.id"::string query_id
        , resource_attributes:"snow.warehouse.name"::string warehouse_name
        , resource_attributes:"telemetry.sdk.language"::string language
        , record_type::string record_type
        , record
        , record:severity_text::string severity
        , record_attributes
        , Value::string Value
        , concat('https://app.snowflake.com/', current_account(), '/', current_account(), '_', current_region(), '/#/compute/history/queries/', query_id, '/detail') query_url
        , trace:trace_id::string trace_id
        --, resource_attributes.snow:name
        --, snowquery
    FROM et_quickstart_db3.public.quick_start_tb3 e
    
    """