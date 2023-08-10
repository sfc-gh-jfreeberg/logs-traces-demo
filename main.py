# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import * #col, count, column, date_sub, to_date, current_timestamp, contains
from snowflake.snowpark.types import *
from snowflake.snowpark.session import Session
import altair as alt 
import pandas as pd 
import plotly.express as px
from util import get_env_var_config
from queries import object_list_query, base_query
import filters

#Set Page Configuration
st.set_page_config(
    layout="wide"
)

tab1, tab2, tab3 = st.tabs(["Log Explorer", "Trace Explorer", "Setup Event Table"])

with tab1:
    
    # Write directly to the app
    st.title("Snowflake Log Explorer")
    st.write("See logs information from across your Snowflake account")
    
    # Get the current credentials
    try: 
        session = get_active_session()
    except Exception:
        try:
            session = Session.builder.configs(get_env_var_config()).create()
        except Exception as e:
            raise e
    
    st.write(st.__version__)
    
    
    ### Filter options and selectors
    # Time options and selector
    time_filter = ['28 days', '14 days', '7 days', '1 day']
    selected_time = st.sidebar.select_slider('Select days to analyze', time_filter)
        
    #Record Type options and selector
    record_types = ['ALL','LOG', 'SPAN_EVENT', 'SPAN']
    selected_record = st.sidebar.selectbox('Select a record type', record_types)
    
    #Severity options and selector
    severity_list = ['ALL', 'ERROR','WARN','DEBUG',  'INFO', ]
    selected_severity = st.sidebar.selectbox('Select a severity', severity_list)
    
    #Severity - color scheme
    colorscale = alt.Scale(domain=['null', 'INFO','DEBUG','ERROR',  'WARN'],
                           range=['gray', 'goldenrod', 'lightblue', 'steelblue', 'midnightblue'])
    
            
    #Language options and selector
    language_types = ['ALL', 'python', 'sql', 'javascript', 'java', 'scala', ]
    selected_language = st.sidebar.selectbox('Select a language', language_types)
    
    #Filter on Object
    object_list = session.sql(object_list_query).select(col("NAME")).to_pandas()
    all_option = pd.DataFrame({'NAME': ['ALL']})
    
    selected_object = "ALL"
    selected_object_token = st.sidebar.text_input('Search for object')
            
    #Search
    user_search_string = st.sidebar.text_input('Enter search criteria')
    
    
    #### Time Series data
    
    # Apply top-level filters
    base_df = session.sql(base_query)
    
    time_filtered_df = filters.add_time_filter(base_df, selected_time, "DATE")
    
    record_filtered_df = filters.add_record_type_filter(time_filtered_df, selected_record=selected_record)
    
    severity_filtered_df = filters.add_severity_filter(record_filtered_df, selected_severity)
    
    search_filtered_df = filters.add_search_filter(severity_filtered_df, user_search_string)
    
    language_filtered_df = filters.add_language_filter(search_filtered_df, selected_language)
    
    object_filtered_df = filters.add_object_filter(language_filtered_df, selected_object)

    object_token_filtered_df = filters.add_wildcard_object_filter(object_filtered_df, selected_object_token)
    
    filtered_df = object_token_filtered_df
    
    timeseries_df = filtered_df.groupBy(["SEVERITY", "DATE"]).count().to_pandas()
    
    
    total_events = timeseries_df["COUNT"].sum()
    formatted_number = "{:,}".format((total_events))
    events_text = formatted_number + " Events"
    
    #Charts
    st.subheader(events_text)
    
    timeseries_plotly = px.bar(timeseries_df, x="DATE", y="COUNT",color="SEVERITY", color_discrete_map={
                    "ERROR": "red",
                    "WARN": "goldenrod",
                    "DEBUG": "lightgreen",
                    "INFO": "lightblue",}, height=300)
    
    
    timeseries_plotly.update_layout(legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="right",
        x=1
    ))
    
    timeseries_plotly.update_layout(yaxis_title=None)
    timeseries_plotly.update_layout(xaxis_title=None)
    
    
    st.plotly_chart(timeseries_plotly, use_container_width=True)
    
    
    aggregation_options = ['None', 'Object', 'Query']
    
    def add_aggregation_option(df, aggregation_option):
        if aggregation_option == 'None':
            return df.sort(col("DATE"), ascending=False).select(["TIMESTAMP", "NAME", "VALUE", "RECORD_TYPE", "TRACE_ID"]).limit(1000)
        elif aggregation_option == 'Object':
            return df.groupBy(['DATABASE_NAME', 'SCHEMA_NAME', 'NAME', 'TYPE']).count().sort(col("COUNT"), ascending=False).limit(1000)
        elif aggregation_option == 'Query':
            return df.groupBy(['DATABASE_NAME', 'SCHEMA_NAME', 'NAME', 'TYPE', 'QUERY_ID', "QUERY_URL", "TRACE_ID"]).count().sort(col("COUNT"), ascending=False).limit(1000)
    
    selected_aggregation = st.selectbox('Aggregate by', aggregation_options)
    
    aggregation_df = add_aggregation_option(filtered_df, selected_aggregation)
    
    st.dataframe(aggregation_df, use_container_width=True)
    

with tab3:
    st.write("In Progress")
    #event_tables_df = session.sql('show parameters')


with tab2:

    #queries 
    eventTable = 'et_quickstart_db3.public.quick_start_tb3'

    traces_base_df = session.table(eventTable)\
        .with_column("trace_id", sql_expr("trace:trace_id::string"))\
        .with_column("span_id", sql_expr("trace:span_id::string"))\
        .with_column("time_taken", sql_expr("record_attributes:time_taken"))\
        .with_column("name", sql_expr('resource_attributes:"snow.executable.name"::string'))\
        .with_column("start_time", sql_expr('case when start_timestamp is null then "TIMESTAMP" else start_timestamp end'))\
        .with_column("end_time", col("TIMESTAMP"))\
        .with_column("event_name", sql_expr('record:name::string') )
        


    #list of traces
    list_of_traces_df = traces_base_df\
        .filter( col("TRACE_ID").is_not_null() )\
        .filter( col("TIME_TAKEN").is_not_null())\
        .select(col("TRACE_ID"), col("SPAN_ID"), col("START_TIMESTAMP"), col("TIMESTAMP"), col("NAME"), col("TIME_TAKEN"))

    time_filtered_df = filters.add_time_filter(list_of_traces_df, selected_time, "START_TIMESTAMP")
    object_filtered_trace_df = filters.add_wildcard_object_filter(time_filtered_df, selected_object_token)

    num_of_traces = object_filtered_trace_df.count()
    formatted_num_of_traces = "{:,}".format((num_of_traces))
    st.subheader(str(formatted_num_of_traces)+ " Traces")
    st.dataframe(object_filtered_trace_df, use_container_width=True)
       
    selected_trace_id = st.text_input("Enter a trace id")

    st.subheader("Details for trace "+ str(selected_trace_id))

    def displayTrace(event_trace_id):
        if event_trace_id is None:
            return "Please enter a trace id"
        else:
            return traces_base_df.filter(col("trace_id") == selected_trace_id)


    one_trace_df = displayTrace(selected_trace_id)

    st.dataframe(one_trace_df)

    trace_fig = px.timeline(one_trace_df.to_pandas(), x_start="START_TIMESTAMP", x_end="END_TIME", y="EVENT_NAME")
    trace_fig.update_yaxes(autorange="reversed") # otherwise tasks are listed from the bottom up
    st.plotly_chart(trace_fig, use_container_width=True)
