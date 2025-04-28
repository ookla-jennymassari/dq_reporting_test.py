import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()


df_metadata = f'''select
   date_trunc('seconds',ts.start_time)::timestamp without time zone as start_time,
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
from prod_ms_partitions.test_summary_?curr_csid ts
left join md2.collection_areas ca on (ts.collection_area_id = ca.collection_area_id)
left join md2.vi_collection_sets vcs on (ts.collection_set_id = vcs.collection_set_id)
left join md2.carriers c on (ts.carrier_id = c.carrier_id)
left join prod_rsr_partitions.test_event_aggr_?curr_csid tea on (ts.collection_set_id = tea.collection_set_id and ts.test_event_id = tea.test_event_id)
left join md2.kit_types kt on (kt.kit_type_id = ts.kit_type_id)
left join md2.report_set_collectors rsc on (rsc.report_set_collector_id = ts.report_set_collector_id)
where ts.test_type_id IN (19, 20, 23) and ts.collection_set_period_id IS NOT NULL and ts.blacklisted = FALSE and ts.flag_valid = TRUE and ts.carrier_id <> 478'''

df_metadata = pd.read_sql_query(df_metadata, con=os.getenv('DATABASE_URL'))

print(df_metadata)

