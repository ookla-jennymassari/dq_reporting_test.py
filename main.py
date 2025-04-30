from sqlalchemy import create_engine
import pandas as pd
import os

from dotenv import load_dotenv
load_dotenv()

engine = create_engine(os.getenv('RSR_SVC_CONN'))

curr_csid = 12705

comp_csid = 12381

# Carrier colors dictionary:
carrier_color_dict = {
                        'Verizon': '#b00000',
                        'AT&T': '#067ab4',
                        'T-Mobile': '#e60076',
                        'Sprint': '#ffaa00',
                        'Dish': '#3F3F3F',
                        'EE': '#2e9b9d',
                        'O2': '#010066',
                        'Three': '#000000',
                        'Vodafone': '#f80000',
                        'O2 UK': '#010066',
                        'Three UK': '#000000',
                        'Vodafone UK': '#f80000',
                        'Sunrise': '#d0606f',
                        'Swisscom': '#5b92cc',
                        'Salt': '#56bf83',
                        'ODIDO': '#FF7621',
                        'KPN': '#FFBB00',
                        'Vodafone NL': '#f80000',
                        'KT': '#FF7621',
                        'SK Telecom': '#FFBB00',
                        'LG U+': '#f80000'
                     }

# Network type colors dictionary:
dl_color_dict = {
  '5G': '#009697', 
  'Mixed-5G': '#3CB371', 
  'LTE': '#58595b',
  'Non-LTE': '#f4a460'
}


ts_curr = f'''
SELECT
   date_trunc('seconds',ts.start_time)::timestamp without time zone as start_time,
   ca.time_zone,
   vcs.collection_type,
   vcs.collection_set,
   ts.collection_set_id,
   ts.test_type_id,
   c.friendly_name AS carrier,
   ts.net_types,
   ts.call_network_type, -- Note: Substitute for `co_call_network_type`
   ts.flag_access_success,
   ts.flag_task_success,
   ts.dsd_effective_download_test_speed,
   ts.dsd_throughput_max,
   ts.dsu_effective_upload_test_speed,
   ts.percentage_access_success,
   ts.percentage_task_success,
   md2.fn_get_best_network_type(ts.test_type_id, 
                                          ts.net_types, 
                                          tea.network_types, 
                                          tea.call_network_type, 
                                          tea.nr_status_filtered, 
                                          tea.nr_bearer_status_filtered,
                                          tea.nr_bearer_allocation_status_filtered,
                                          ',') AS best_network_type,
   md2.fn_get_network_types(ts.net_types, ','::character varying) AS net_types_desc,
   kt.name AS kit_type,
   rsc.name AS report_set_collector,
   (ts.start_time AT TIME ZONE 'utc' AT TIME ZONE ca.time_zone) AS local_time
FROM prod_ms_partitions.test_summary_{curr_csid} as ts
LEFT JOIN md2.collection_areas ca on (ts.collection_area_id = ca.collection_area_id)
LEFT JOIN md2.vi_collection_sets vcs on (ts.collection_set_id = vcs.collection_set_id)
LEFT JOIN md2.carriers c on (ts.carrier_id = c.carrier_id)
LEFT JOIN prod_rsr_partitions.test_event_aggr_{curr_csid} tea on (ts.collection_set_id = tea.collection_set_id and ts.test_event_id = tea.test_event_id)
LEFT JOIN md2.kit_types kt on (kt.kit_type_id = ts.kit_type_id)
LEFT JOIN md2.report_set_collectors rsc on (rsc.report_set_collector_id = ts.report_set_collector_id)
WHERE ts.test_type_id IN (19, 20, 23) 
AND ts.collection_set_period_id IS NOT NULL 
AND ts.blacklisted = FALSE 
AND ts.flag_valid = TRUE 
AND ts.carrier_id <> 478
'''

df_ts_curr = pd.read_sql_query(ts_curr, con=os.getenv('RSR_SVC_CONN'))
# print(df_test_summary_metadata_csid)


ts_comp = f'''
SELECT
   ts.start_time,
   ca.time_zone,
   vcs.collection_type,
   vcs.collection_set,
   ts.collection_set_id,
   ts.test_type_id,
   c.friendly_name as carrier,
   ts.net_types,
   ts.call_network_type, -- Note: Substitute for `co_call_network_type`
   ts.flag_access_success,
   ts.flag_task_success,
   ts.dsd_effective_download_test_speed,
   ts.dsd_throughput_max,
   ts.dsu_effective_upload_test_speed,
   ts.percentage_access_success,
   ts.percentage_task_success,
   md2.fn_get_best_network_type(ts.test_type_id, 
                                          ts.net_types, 
                                          tea.network_types, 
                                          tea.call_network_type, 
                                          tea.nr_status_filtered, 
                                          tea.nr_bearer_status_filtered,
                                          tea.nr_bearer_allocation_status_filtered,
                                          ','
                                          ) best_network_type,
   md2.fn_get_network_types(ts.net_types, ','::character varying) AS net_types_desc,
   kt.name AS kit_type,
   rsc.name as report_set_collector,
   (start_time at time zone 'utc' at time zone ca.time_zone) as local_time
FROM prod_ms_partitions.test_summary_{comp_csid} ts
LEFT JOIN md2.collection_areas ca on (ts.collection_area_id = ca.collection_area_id)
LEFT JOIN md2.vi_collection_sets vcs on (ts.collection_set_id = vcs.collection_set_id)
LEFT JOIN md2.carriers c on (ts.carrier_id = c.carrier_id)
LEFT JOIN prod_rsr_partitions.test_event_aggr_{comp_csid} tea on (ts.collection_set_id = tea.collection_set_id and ts.test_event_id = tea.test_event_id)
LEFT JOIN md2.kit_types kt on (kt.kit_type_id = ts.kit_type_id)
LEFT JOIN md2.report_set_collectors rsc on (rsc.report_set_collector_id = ts.report_set_collector_id)
WHERE c.friendly_name NOT IN ('Sprint') 
AND ts.test_type_id IN (19, 20, 23) 
AND ts.collection_set_period_id IS NOT NULL 
AND ts.blacklisted = FALSE and ts.flag_valid = TRUE 
AND ts.carrier_id <> 478
'''

df_ts_comp = pd.read_sql_query(ts_comp, con=os.getenv('RSR_SVC_CONN'))
# print(df_ts_comp)

test_count = f'''
SELECT * 
FROM analytic.fn_dq_test_counts({curr_csid}) 
'''

df_test_count = pd.read_sql_query(test_count, con=os.getenv('RSR_SVC_CONN'))
# print(df_test_count)

#  #### DL Network Category

dl_5g_curr = f'''
WITH best_network_type AS (
    SELECT 
        pro.product_period, 
        c.friendly_name AS carrier, 
        md2.fn_get_best_network_type(
            ts.test_type_id, 
            ts.net_types, 
            tea.network_types, 
            tea.call_network_type, 
            tea.nr_status_filtered, 
            tea.nr_bearer_status_filtered,
            tea.nr_bearer_allocation_status_filtered,
            ','
        ) AS best_network_type,
        dsd_access_speed_median, 
        dsd_effective_download_test_speed, 
        dsd_throughput_max, 
        percentage_access_success, 
        percentage_task_success
    FROM prod_rsr_partitions.test_event_aggr_{curr_csid} tea
    JOIN prod_ms_partitions.test_summary_{curr_csid} ts USING (test_event_id)
    JOIN md2.carriers c ON (c.carrier_id = ts.carrier_id)
    JOIN md2.product_periods pro USING (product_period_id)
    WHERE c.friendly_name NOT IN ('Dish') 
      AND ts.blacklisted = FALSE 
      AND ts.flag_valid = TRUE 
      AND ts.collection_set_period_id IS NOT NULL 
      AND ts.test_type_id IN (20)
),
data_network_category AS (
    SELECT 
        product_period, 
        carrier, 
        dsd_access_speed_median, 
        dsd_effective_download_test_speed, 
        dsd_throughput_max, 
        percentage_access_success, 
        percentage_task_success,
        CASE 
            WHEN best_network_type IN ('NR SA', 'NR NSA') THEN '5G' 
            WHEN best_network_type IN ('NR SA, LTE', 'NR NSA, LTE') THEN 'Mixed-5G' 
            WHEN best_network_type IN ('LTE') THEN 'LTE'
            ELSE 'Non-LTE' 
        END AS dl_network
    FROM best_network_type
)
SELECT product_period,
        carrier,
        dl_network,
        COUNT(*) AS count,
        ROUND(100 * count(*) / sum(count(*)) over (partition by carrier),2) as dl_pct,
        ROUND(median(dsd_access_speed_median)::numeric,0) as access_spd,
        ROUND(cast(median(dsd_effective_download_test_speed)/1000 as numeric),1) as med_tput,
        ROUND(cast(max(dsd_effective_download_test_speed)/1000 as numeric),1) as max_tput,
        ROUND(cast(max(dsd_throughput_max)/1000 as numeric),1) as burst_tput,
        ROUND(cast(avg(percentage_access_success) * 100 as numeric),1) as access,
        ROUND(cast(avg(percentage_task_success) * 100 as numeric),1) as task
FROM data_network_category
Where carrier not in ('Dish')
GROUP BY product_period, carrier, dl_network
ORDER BY carrier, case when dl_network = '5G' then 1 when dl_network = 'Mixed-5G' then 2 when dl_network = 'LTE' then 3 when dl_network = 'Non-LTE' then 4 end
'''
df_dl_5g_curr = pd.read_sql_query(dl_5g_curr, con=os.getenv('RSR_SVC_CONN'))
print(df_dl_5g_curr)

dl_5g_comp = f'''
WITH best_network_type AS (
    SELECT 
        pro.product_period, 
        c.friendly_name AS carrier, 
        md2.fn_get_best_network_type(
            ts.test_type_id, 
            ts.net_types, 
            tea.network_types, 
            tea.call_network_type, 
            tea.nr_status_filtered, 
            tea.nr_bearer_status_filtered,
            tea.nr_bearer_allocation_status_filtered,
            ','
        ) AS best_network_type,
        dsd_access_speed_median, 
        dsd_effective_download_test_speed, 
        dsd_throughput_max, 
        percentage_access_success, 
        percentage_task_success
    FROM prod_rsr_partitions.test_event_aggr_{comp_csid} tea
    JOIN prod_ms_partitions.test_summary_{comp_csid} ts USING (test_event_id)
    JOIN md2.carriers c ON (c.carrier_id = ts.carrier_id)
    JOIN md2.product_periods pro USING (product_period_id)
    WHERE c.friendly_name NOT IN ('Dish') 
      AND ts.blacklisted = FALSE 
      AND ts.flag_valid = TRUE 
      AND ts.collection_set_period_id IS NOT NULL 
      AND ts.test_type_id IN (20)
),
data_network_category AS (
    SELECT 
        product_period, 
        carrier, 
        dsd_access_speed_median, 
        dsd_effective_download_test_speed, 
        dsd_throughput_max, 
        percentage_access_success, 
        percentage_task_success,
        CASE 
            WHEN best_network_type IN ('NR SA', 'NR NSA') THEN '5G' 
            WHEN best_network_type IN ('NR SA, LTE', 'NR NSA, LTE') THEN 'Mixed-5G' 
            WHEN best_network_type IN ('LTE') THEN 'LTE'
            ELSE 'Non-LTE' 
        END AS dl_network
    FROM best_network_type
)
SELECT product_period,
        carrier,
        dl_network,
        COUNT(*) AS count,
        ROUND(100 * count(*) / sum(count(*)) over (partition by carrier),2) as dl_pct,
        ROUND(median(dsd_access_speed_median)::numeric,0) as access_spd,
        ROUND(cast(median(dsd_effective_download_test_speed)/1000 as numeric),1) as med_tput,
        ROUND(cast(max(dsd_effective_download_test_speed)/1000 as numeric),1) as max_tput,
        ROUND(cast(max(dsd_throughput_max)/1000 as numeric),1) as burst_tput,
        ROUND(cast(avg(percentage_access_success) * 100 as numeric),1) as access,
        ROUND(cast(avg(percentage_task_success) * 100 as numeric),1) as task
FROM data_network_category
Where carrier not in ('Dish')
GROUP BY product_period, carrier, dl_network
ORDER BY carrier, case when dl_network = '5G' then 1 when dl_network = 'Mixed-5G' then 2 when dl_network = 'LTE' then 3 when dl_network = 'Non-LTE' then 4 end
'''
df_dl_5g_comp = pd.read_sql_query(dl_5g_comp, con=os.getenv('RSR_SVC_CONN'))
print(df_dl_5g_comp)



#########################################################################################################################
#### Data Network Category (Download, Upload, LDRs)
### The queries bellow are the same for both markets, US and UK
### Create logic to run the query according to the market US or UK 
network_category_curr = f'''
WITH best_network_type AS (
  SELECT pro.product_period, c.friendly_name AS carrier,
  md2.fn_get_best_network_type(ts.test_type_id, ts.net_types, 
                              tea.network_types, 
                              tea.call_network_type, 
                              tea.nr_status_filtered, 
                              tea.nr_bearer_status_filtered,
                              tea.nr_bearer_allocation_status_filtered,',') best_network_type
  FROM prod_rsr_partitions.test_event_aggr_{curr_csid} tea
  JOIN prod_ms_partitions.test_summary_{curr_csid} ts using (test_event_id)
  JOIN md2.carriers c on (c.carrier_id = ts.carrier_id)
  JOIN md2.product_periods pro using(product_period_id)
  WHERE c.friendly_name NOT IN ('Dish') 
  AND  ts.blacklisted = false 
  AND ts.flag_valid = true 
  AND ts.collection_set_period_id is not null 
  AND ts.test_type_id in (20,19,26)
),
data_network_category as (
SELECT product_period, carrier,
    CASE WHEN best_network_type in ('NR SA','NR NSA') then '5G'
    WHEN best_network_type in ('NR SA, LTE','NR NSA, LTE') then 'Mixed-5G'
    WHEN best_network_type in ('LTE') then 'LTE'
    ELSE 'Non-LTE' end as network
FROM best_network_type
)
SELECT product_period, 
      carrier, 
      network, 
      count(*) as count, 
      round(100 * count(*) / sum(count(*)) over (partition by carrier),1) as percent
FROM data_network_category
GROUP BY product_period, carrier, network
ORDER BY carrier, CASE WHEN network = '5G' then 1 when network = 'Mixed-5G' then 2 when network = 'LTE' then 3 when network = 'Non-LTE' then 4 end
'''

df_network_category_curr = pd.read_sql_query(network_category_curr, con=os.getenv('RSR_SVC_CONN'))
# print(df_network_category_curr)

network_category_comp = f'''
with best_network_type as (
  SELECT pro.product_period, c.friendly_name as carrier,
  md2.fn_get_best_network_type(ts.test_type_id, 
                              ts.net_types, 
                              tea.network_types, 
                              tea.call_network_type, 
                              tea.nr_status_filtered, 
                              tea.nr_bearer_status_filtered,
                              tea.nr_bearer_allocation_status_filtered,',') best_network_type
  FROM prod_rsr_partitions.test_event_aggr_{comp_csid} tea
  JOIN prod_ms_partitions.test_summary_{comp_csid} ts using (test_event_id)
  JOIN md2.carriers c on (c.carrier_id = ts.carrier_id)
  JOIN md2.product_periods pro using(product_period_id)
  WHERE c.friendly_name NOT IN ('Dish') 
  AND  ts.blacklisted = false 
  AND ts.flag_valid = true 
  AND ts.collection_set_period_id is not null 
  AND ts.test_type_id in (20,19,26)
),
data_network_category as (
SELECT product_period, carrier,
    CASE WHEN best_network_type in ('NR SA','NR NSA') then '5G'
    WHEN best_network_type in ('NR SA, LTE','NR NSA, LTE') then 'Mixed-5G'
    WHEN best_network_type in ('LTE') then 'LTE'
    ELSE 'Non-LTE' end as network
FROM best_network_type
)
SELECT product_period, 
      carrier, 
      network, 
      count(*) as count, 
      round(100 * count(*) / sum(count(*)) over (partition by carrier),1) as percent
FROM data_network_category
GROUP BY product_period, carrier, network
ORDER BY carrier, CASE WHEN network = '5G' then 1 when network = 'Mixed-5G' then 2 when network = 'LTE' then 3 when network = 'Non-LTE' then 4 end
'''
df_network_category_comp = pd.read_sql_query(network_category_comp, con=os.getenv('RSR_SVC_CONN'))
# print(df_network_category_comp)



#######################################################################################################

#### Download Network Technology
###### DL LTE Current market Daily
# Logic computed from dataframe ts_curr and ts_comp
# outputs: dl_lte_curr, lte2, 


##### DL LTE Overall
#current market
# Logic computed from dataframe created in Download Network Technology

# PLOT GRAPH

#compute the EOM table

#### M2M VoLTE/VoNR/EPS Fallback

###### VoLTE Comparison market Daily

##### DL LTE Overall
#current market

#comparison market

# PLOT GRAPH

#compute the EOM table


#### Mobile-to-Mobile Call Block
###### M2M Block Rate Current market Daily

###### M2M Block Rate Comparison market Daily

#M2M Block Rate Overall

#current market

#comparison market

# PLOT GRAPH

#compute the M2M block EOM table



#### Mobile-to-Mobile Call Drop
###### M2M Drop Rate Current market

###### M2M drop Rate Comparison market


###### M2M drop Rate Comparison market Overall
#current market

#comparison market

# PLOT GRAPH

# Compute the EOM table

# Create the table


#### Download Throughput
# PLOT GRAPH
# EOM TABLE

#### Upload Throughput
# PLOT GRAPH
#compute the EOM table

#### Download Access Success
# PLOT GRAPH
#compute the EOM table

#### Download Task Success
# PLOT GRAPH
#compute the EOM table


### Notable Insights:
# *	DL 5G
# *	M2M block/drop 
# *	Median DL/UL throughput 


### Other Observations:
# *	Access success 
# *	Task Success rates 


### Data Exclusion/Tracking Items:
# * Excluded test(s) due to device issue: None
# *	Other manual exclusions performed: None 


### DQ Test Counts:


#### File Location: [DQ-CSID](http://jira.rootmetrics.net:8080/browse/DQ-CSID)