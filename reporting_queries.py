from dotenv import load_dotenv
import pandas as pd
import os
from reporting_config import *
load_dotenv()

def run_sql_query(sql_query):
   
   return pd.read_sql_query(sql_query, con=os.getenv('RSR_SVC_CONN')) 

def get_test_summary_curr_comp(curr_csid, comp_csid):
    ts_curr_comp = f'''
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
    FROM prod_ms_partitions.test_summary_$VAR$ as ts
    LEFT JOIN md2.collection_areas ca on (ts.collection_area_id = ca.collection_area_id)
    LEFT JOIN md2.vi_collection_sets vcs on (ts.collection_set_id = vcs.collection_set_id)
    LEFT JOIN md2.carriers c on (ts.carrier_id = c.carrier_id)
    LEFT JOIN prod_rsr_partitions.test_event_aggr_$VAR$ tea on (ts.collection_set_id = tea.collection_set_id and ts.test_event_id = tea.test_event_id)
    LEFT JOIN md2.kit_types kt on (kt.kit_type_id = ts.kit_type_id)
    LEFT JOIN md2.report_set_collectors rsc on (rsc.report_set_collector_id = ts.report_set_collector_id)
    WHERE ts.test_type_id IN (19, 20, 23) 
    AND ts.collection_set_period_id IS NOT NULL 
    AND ts.blacklisted = FALSE 
    AND ts.flag_valid = TRUE 
    AND ts.carrier_id <> 478
    '''

    df_ts_curr  = run_sql_query(ts_curr_comp.replace("$VAR$", str(curr_csid)))
    df_ts_comp  = run_sql_query(ts_curr_comp.replace("$VAR$", str(comp_csid)))
    # print(f" CURRENT {df_ts_curr}, COMPARISON {df_ts_comp}")
    return df_ts_curr, df_ts_comp

def get_test_count(curr_csid):
    
    test_count = f'''
    SELECT * 
    FROM analytic.fn_dq_test_counts({curr_csid}) 
    '''
    df_test_count = run_sql_query(test_count)
    return df_test_count

def get_dl_5g_curr_comp(curr_csid, comp_csid):
    dl_5g_curr_comp = f'''
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
        FROM prod_rsr_partitions.test_event_aggr_$VAR$ tea
        JOIN prod_ms_partitions.test_summary_$VAR$ ts USING (test_event_id)
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
    df_dl_5g_curr = run_sql_query(dl_5g_curr_comp.replace("$VAR$", str(curr_csid)))
    df_dl_5g_comp = run_sql_query(dl_5g_curr_comp.replace("$VAR$", str(comp_csid)))
    # print(f" CURRENT {df_dl_5g_curr}, COMPARISON {df_dl_5g_comp}")
    return df_dl_5g_curr, df_dl_5g_comp

def get_network_category_curr_comp(curr_csid, comp_csid):
    network_category_curr_comp = f'''
    with best_network_type as (
    select pro.product_period, c.friendly_name as carrier,
    md2.fn_get_best_network_type(ts.test_type_id, ts.net_types, tea.network_types, tea.call_network_type, tea.nr_status_filtered, tea.nr_bearer_status_filtered,tea.nr_bearer_allocation_status_filtered,',') best_network_type
    from prod_rsr_partitions.test_event_aggr_$VAR$ tea
    join prod_ms_partitions.test_summary_$VAR$ ts using (test_event_id)
    join md2.carriers c on (c.carrier_id = ts.carrier_id)
    join md2.product_periods pro using(product_period_id)
    where c.friendly_name NOT IN ('Dish') AND  ts.blacklisted = false and ts.flag_valid = true and ts.collection_set_period_id is not null and ts.test_type_id in (20,19,26)
    ),
    data_network_category as (
    select product_period, carrier,
        case when best_network_type in ('NR SA','NR NSA') then '5G'
        when best_network_type in ('NR SA, LTE','NR NSA, LTE') then 'Mixed-5G'
        when best_network_type in ('LTE') then 'LTE'
        else 'Non-LTE' end as network
    from best_network_type
    )
    select product_period, carrier, network, count(*) as count, round(100 * count(*) / sum(count(*)) over (partition by carrier),1) as percent
    from data_network_category
    group by product_period, carrier, network
    order by carrier, case when network = '5G' then 1 when network = 'Mixed-5G' then 2 when network = 'LTE' then 3 when network = 'Non-LTE' then 4 end
    '''

    df_network_category_curr = run_sql_query(network_category_curr_comp.replace("$VAR$", str(curr_csid)))
    df_network_category_comp = run_sql_query(network_category_curr_comp.replace("$VAR$", str(comp_csid)))
    # print(f" CURRENT {df_network_category_curr}, COMPARISON {df_network_category_comp}")
    return df_network_category_curr, df_network_category_comp


if __name__ == "__main__":
    
    get_test_summary_curr_comp(curr_csid, comp_csid)
    print("Test summary current data fetched successfully.")
    get_test_count(curr_csid)
    print("Test count data fetched successfully.")
    get_dl_5g_curr_comp(curr_csid, comp_csid)
    print("Download 5G data fetched successfully.")
    get_network_category_curr_comp(curr_csid, comp_csid)
    print("Network category data fetched successfully.")
   