from snowflake.snowpark.functions import *

def add_object_filter(df, object_name):
        if object_name == "ALL":
            return df
        else:
            return df.filter(col("NAME") == selected_object)


def add_wildcard_object_filter(df, object_token):
    if object_token is None:
        return df 
    else:
        search_token = '%'+object_token+'%'
        return df.filter( col("NAME").like(search_token))
    

def add_language_filter(df, lang_str):
    if lang_str == 'ALL':
        return df
    else:
        return df.filter(col("LANGUAGE") == lang_str)
    

def add_severity_filter(df, selection):
    if selection != 'ALL':
        return df.filter(col('SEVERITY') == selected_severity)
    else:
        return df


def add_time_filter(df, selected_time_value, filter_column_name):
    def parse_time_filter(time_filter):
        if time_filter == '28 days':
            return 28
        elif time_filter == '14 days':
            return 14
        elif time_filter == '7 days':
            return 7
        elif time_filter == '1 day':
            return 1
        else:
            return 0

    days = parse_time_filter(selected_time_value)
    return df.filter(col(filter_column_name)> date_sub(current_timestamp(), days))

def add_record_type_filter(df, selected_record):
    if selected_record != 'ALL':
        return df.filter( col("RECORD_TYPE") ==  selected_record)
    else:
        return df
    
def add_search_filter(df, search_str):
    if len(search_str) == 0:
        return df 
    else:
        search_token = '%'+search_str+'%'
        return df.filter( col("VALUE").astype(StringType()).like(search_token))