with today_dag as (
   select 
          dag_id
        , next_dagrun_data_interval_start
        , next_dagrun_data_interval_end
     from dag 
    where is_paused = false 
      and is_active  = true 
      and schedule_interval not in ('null', '"Dataset"')
      and (date(next_dagrun_data_interval_start) between current_date -1 and current_date 
      or date(next_dagrun_data_interval_end ) between current_date -1 and current_date )
)
, today_dagrun as (
   select 
          dag_id
        , count(1) as run_cnt
        , count(case when state ='success' then 'success' end) as success_cnt
        , count(case when state ='failed'  then 'failed'  end) as failed_cnt
        , count(case when state ='running' then 'running' end) as running_cnt
        , max(case when state ='failed'    then data_interval_end end ) as last_failed_date
        , max(case when state ='success'   then data_interval_end end ) as last_success_date
    from dag_run 
   where date(data_interval_end) between current_date -1 and current_date 
   group by dag_id 
)
select 
       d.dag_id
     , r.run_cnt
     , r.success_cnt
     , r.failed_cnt
     , r.last_failed_date
     , r.last_success_date
     , next_dagrun_data_interval_start 
     , next_dagrun_data_interval_end 
  from today_dag d
  left outer join today_dagrun r 
    on d.dag_id = r.dag_id
