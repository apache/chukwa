drop table if exists node_activity_template;
drop table if exists switch_data_template;
drop table if exists system_metrics_template;
drop table if exists disk_template;
drop table if exists cluster_disk_template;
drop table if exists cluster_system_metrics_template;
drop table if exists dfs_namenode_template;
drop table if exists dfs_datanode_template;
drop table if exists dfs_fsnamesystem_template;
drop table if exists dfs_throughput_template;
drop table if exists hadoop_jvm_template;
drop table if exists hadoop_mapred_template;
drop table if exists hadoop_rpc_template;
drop table if exists cluster_hadoop_rpc_template;
drop table if exists hadoop_rpc_calls_template;
drop table if exists mr_job_template;
drop table if exists mr_task_template;
drop table if exists mr_job_timeline_template;
drop table if exists hod_machine_template;
drop table if exists HodJob_template;
drop table if exists hod_job_digest_template;
drop table if exists user_util_template;
drop table if exists hdfs_usage_template;
drop table if exists QueueInfo;
drop table if exists mapreduce_fsm_template;
drop table if exists user_job_summary_template;
drop table if exists filesystem_fsm_template;

create table if not exists node_activity_template (
    timestamp  timestamp default CURRENT_TIMESTAMP,
    used int(11) default NULL,
    usedMachines text,
    free int(11) default NULL,
    freeMachines text,
    down int(11) default NULL,
    downMachines text,
    primary key(timestamp),
    index (Timestamp)
) ENGINE=InnoDB;

create table if not exists switch_data_template (
    timestamp timestamp default CURRENT_TIMESTAMP,
    host varchar(40),
    port varchar(10),
    poller varchar(40),
    metricName varchar(20),
    value double,
    primary key(timestamp, host, port),
    index (Timestamp)
) ENGINE=InnoDB;

create table if not exists system_metrics_template (
    timestamp  timestamp default CURRENT_TIMESTAMP,
    host varchar(40),
    load_15 double, 
    load_5 double,
    load_1 double,
    task_total double,
    task_running double,
    task_sleep double,
    task_stopped double,
    task_zombie double,
    mem_total double,
    mem_buffers double,
    mem_cached double,
    mem_used double,
    mem_free double,
    eth0_rxerrs double,
    eth0_rxbyts double,
    eth0_rxpcks double,
    eth0_rxdrops double,
    eth0_txerrs double,
    eth0_txbyts double,
    eth0_txpcks double,
    eth0_txdrops double,
    eth1_rxerrs double,
    eth1_rxbyts double,
    eth1_rxpcks double,
    eth1_rxdrops double,
    eth1_txerrs double,
    eth1_txbyts double,
    eth1_txpcks double,
    eth1_txdrops double,
    sda_rkbs double,
    sda_wkbs double,
    sdb_rkbs double,
    sdb_wkbs double,
    sdc_rkbs double,
    sdc_wkbs double,
    sdd_rkbs double,
    sdd_wkbs double,
    cpu_idle_pcnt float,
    cpu_nice_pcnt float,
    cpu_system_pcnt float,
    cpu_user_pcnt float,
    cpu_hirq_pcnt float,
    cpu_sirq_pcnt float,
    iowait_pcnt float,
    mem_buffers_pcnt float,
    mem_used_pcnt float,
    eth0_busy_pcnt float,
    eth1_busy_pcnt float,
    sda_busy_pcnt float,
    sdb_busy_pcnt float,
    sdc_busy_pcnt float,
    sdd_busy_pcnt float,
    swap_used_pcnt float,
    primary key(host, timestamp),
    index (timestamp)
) ENGINE=InnoDB;

create table if not exists disk_template (
    timestamp  timestamp default CURRENT_TIMESTAMP,
    host varchar(40),
    mount varchar(40),
    used double,
    available double,
    used_percent double,
    fs varchar(40),
    primary key(timestamp,host,mount),
    index (timestamp)
) ENGINE=InnoDB;

create table if not exists cluster_disk_template (
    timestamp  timestamp default CURRENT_TIMESTAMP,
    mount varchar(40),
    used double,
    available double,
    used_percent double,
    primary key(timestamp,mount),
    index (timestamp)
) ENGINE=InnoDB;

create table if not exists cluster_system_metrics_template (
    timestamp  timestamp default CURRENT_TIMESTAMP,
    host int,
    load_15 double, 
    load_5 double,
    load_1 double,
    task_total double,
    task_running double,
    task_sleep double,
    task_stopped double,
    task_zombie double,
    mem_total double,
    mem_buffers double,
    mem_cached double,
    mem_used double,
    mem_free double,
    eth0_rxerrs double,
    eth0_rxbyts double,
    eth0_rxpcks double,
    eth0_rxdrops double,
    eth0_txerrs double,
    eth0_txbyts double,
    eth0_txpcks double,
    eth0_txdrops double,
    eth1_rxerrs double,
    eth1_rxbyts double,
    eth1_rxpcks double,
    eth1_rxdrops double,
    eth1_txerrs double,
    eth1_txbyts double,
    eth1_txpcks double,
    eth1_txdrops double,
    sda_rkbs double,
    sda_wkbs double,
    sdb_rkbs double,
    sdb_wkbs double,
    sdc_rkbs double,
    sdc_wkbs double,
    sdd_rkbs double,
    sdd_wkbs double,
    cpu_idle_pcnt float,
    cpu_nice_pcnt float,
    cpu_system_pcnt float,
    cpu_user_pcnt float,
    cpu_hirq_pcnt float,
    cpu_sirq_pcnt float,
    iowait_pcnt float,
    mem_buffers_pcnt float,
    mem_used_pcnt float,
    eth0_busy_pcnt float,
    eth1_busy_pcnt float,
    sda_busy_pcnt float,
    sdb_busy_pcnt float,
    sdc_busy_pcnt float,
    sdd_busy_pcnt float,
    swap_used_pcnt float,
    primary key(timestamp),
    index (timestamp)
) ENGINE=InnoDB;

create table if not exists dfs_namenode_template (
    timestamp timestamp default 0,
    host varchar(80),
    add_block_ops double,
    blocks_corrupted double,
    create_file_ops double,
    delete_file_ops double,
    files_created double,
    files_renamed double,
    files_deleted double,
    get_block_locations double,
    get_listing_ops double,
    safe_mode_time double,
    syncs_avg_time double,
    syncs_num_ops double,
    transactions_avg_time double,
    transactions_num_ops double,
    block_report_avg_time double,
    block_report_num_ops double,
    fs_image_load_time double,
    primary key(timestamp, host),
    index(timeStamp)
) ENGINE=InnoDB;

create table if not exists dfs_datanode_template (
    timestamp timestamp default 0,
    host varchar(80),
    block_reports_avg_time double,
    block_reports_num_ops double,
    block_verification_failures double,
    blocks_read double,
    blocks_removed double,
    blocks_replicated double,
    blocks_verified double,
    blocks_written double,
    bytes_read double,
    bytes_written double,
    copy_block_op_avg_time double,
    copy_block_op_num_ops double,
    heart_beats_avg_time double,
    heart_beats_num_ops double,
    read_block_op_avg_time double,
    read_block_op_num_ops double,
    read_metadata_op_avg_time double,
    read_metadata_op_num_ops double,
    reads_from_local_client double,
    reads_from_remote_client double,
    replace_block_op_avg_time double,
    replace_block_op_num_ops double,
    session_id double,
    write_block_op_avg_time double,
    write_block_op_num_ops double,
    writes_from_local_client double,
    writes_from_remote_client double,
    primary key(timestamp, host),
    index(timestamp)
) ENGINE=InnoDB;

create table if not exists dfs_fsnamesystem_template (
    timestamp timestamp default 0,
    host VARCHAR(80),
    blocks_total double,
    capacity_remaining_gb double,
    capacity_total_gb double,
    capacity_used_gb double,
    files_total double,
    pending_replication_blocks double,
    scheduled_replication_blocks double,
    total_load double,
    under_replicated_blocks double,
    primary key(timestamp, host),
    index(timestamp)
) ENGINE=InnoDB;

create table if not exists dfs_throughput_template (
    timestamp timestamp default 0,
    host int,
    block_reports_avg_time double,
    block_reports_num_ops double,
    block_verification_failures double,
    blocks_read double,
    blocks_removed double,
    blocks_replicated double,
    blocks_verified double,
    blocks_written double,
    bytes_read double,
    bytes_written double,
    copy_block_op_avg_time double,
    copy_block_op_num_ops double,
    heart_beats_avg_time double,
    heart_beats_num_ops double,
    read_block_op_avg_time double,
    read_block_op_num_ops double,
    read_metadata_op_avg_time double,
    read_metadata_op_num_ops double,
    reads_from_local_client double,
    reads_from_remote_client double,
    replace_block_op_avg_time double,
    replace_block_op_num_ops double,
    session_id double,
    write_block_op_avg_time double,
    write_block_op_num_ops double,
    writes_from_local_client double,
    writes_from_remote_client double,
    primary key(timestamp),
    index(timestamp)
) ENGINE=InnoDB;

create table if not exists hadoop_jvm_template (
    timestamp timestamp default 0,
    host VARCHAR(80),
    process_name VARCHAR(80),
    gc_timemillis double,
    gc_count double,
    log_error double,
    log_fatal double,
    log_info double,
    log_warn double,
    mem_heap_committed_m double,
    mem_heap_used_m double,
    mem_non_heap_committed_m double,
    mem_non_heap_used_m double,
    threads_blocked double,
    threads_new double,
    threads_runnable double,
    threads_terminated double,
    threads_timed_waiting double,
    threads_waiting double,
    primary key (timestamp, host, process_name),
    index(timestamp)
) ENGINE=InnoDB;

create table if not exists hadoop_mapred_template (
    timestamp timestamp default 0,
    host VARCHAR(80),
    jobs_completed double,
    jobs_submitted double,
    maps_completed double,
    maps_launched double,
    reduces_completed double,
    reduces_launched double,
    primary key (timestamp, host),
    index(timestamp)
) ENGINE=InnoDB;

create table if not exists hadoop_rpc_template (
    timestamp timestamp default 0,
    host VARCHAR(80),
    rpc_processing_time_avg_time double,
    rpc_processing_time_num_ops double,
    rpc_queue_time_avg_time double,
    rpc_queue_time_num_ops double,
    get_build_version_avg_time double,
    get_build_version_num_ops double,
    get_job_counters_avg_time double,
    get_job_counters_num_ops double,
    get_job_profile_avg_time double,
    get_job_profile_num_ops double,
    get_job_status_avg_time double,
    get_job_status_num_ops double,
    get_new_job_id_avg_time double,
    get_new_job_id_num_ops double,
    get_protocol_version_avg_time double,
    get_protocol_version_num_ops double,
    get_system_dir_avg_time double,
    get_system_dir_num_ops double,
    get_task_completion_events_avg_time double,
    get_task_completion_events_num_ops double,
    get_task_diagnostics_avg_time double,
    get_task_diagnostics_num_ops double,
    heartbeat_avg_time double,
    heartbeat_num_ops double,
    killJob_avg_time double,
    killJob_num_ops double,
    submit_job_avg_time double,
    submit_job_num_ops double,
    primary key (timestamp, host),
    index(timestamp)
) ENGINE=InnoDB;

create table if not exists cluster_hadoop_rpc_template (
    timestamp timestamp default 0,
    host int,
    rpc_processing_time_avg_time double,
    rpc_processing_time_num_ops double,
    rpc_queue_time_avg_time double,
    rpc_queue_time_num_ops double,
    get_build_version_avg_time double,
    get_build_version_num_ops double,
    get_job_counters_avg_time double,
    get_job_counters_num_ops double,
    get_job_profile_avg_time double,
    get_job_profile_num_ops double,
    get_job_status_avg_time double,
    get_job_status_num_ops double,
    get_new_job_id_avg_time double,
    get_new_job_id_num_ops double,
    get_protocol_version_avg_time double,
    get_protocol_version_num_ops double,
    get_system_dir_avg_time double,
    get_system_dir_num_ops double,
    get_task_completion_events_avg_time double,
    get_task_completion_events_num_ops double,
    get_task_diagnostics_avg_time double,
    get_task_diagnostics_num_ops double,
    heartbeat_avg_time double,
    heartbeat_num_ops double,
    killJob_avg_time double,
    killJob_num_ops double,
    submit_job_avg_time double,
    submit_job_num_ops double,
    primary key (timestamp),
    index(timestamp)
) ENGINE=InnoDB;

create table if not exists hadoop_rpc_calls_template (
    timestamp timestamp default 0,
    method varchar(80),
    calls double,
    primary key(timestamp, method),
    index(timestamp)
) ENGINE=InnoDB;

create table if not exists mr_job_template (
    job_id varchar(80),
    user varchar(32),
    queue varchar(100),
    status varchar(10),
    submit_time timestamp default 0,
    launch_time timestamp default 0,
    finish_time timestamp default 0,
    hdfs_bytes_read bigint default 0,
    hdfs_bytes_written bigint default 0,
    local_bytes_read bigint default 0,
    local_bytes_written bigint default 0,
    launched_map_tasks bigint default 0,
    launched_reduce_tasks bigint default 0,
    data_local_map_tasks bigint default 0,
    data_local_reduce_tasks bigint default 0,
    map_input_bytes bigint default 0,
    map_output_bytes bigint default 0,
    map_input_records bigint default 0,
    map_output_records bigint default 0,
    combine_input_records bigint default 0,
    combine_output_records bigint default 0,
    spilled_records bigint default 0,
    reduce_input_groups bigint default 0,
    reduce_output_groups bigint default 0,
    reduce_input_records bigint default 0,
    reduce_output_records bigint default 0,
    jobconf  text, 
    finished_maps bigint default 0,
    finished_reduces bigint default 0,
    failed_maps bigint default 0,
    failed_reduces bigint default 0,
    total_maps bigint default 0,
    total_reduces bigint default 0,
    reduce_shuffle_bytes bigint default 0,
    primary key(job_id),
    index(submit_time, finish_time, user, queue)
) ENGINE=InnoDB;

create table if not exists mr_task_template (
    job_id VARCHAR(80),
    task_id VARCHAR(80),
    start_time timestamp default 0,
    finish_time timestamp default 0,
    status varchar(10) default 0,
    attempts tinyint default 0,
    hdfs_bytes_read bigint default 0,
    hdfs_bytes_written bigint default 0,
    local_bytes_read bigint default 0,
    local_bytes_written bigint default 0,
    map_input_bytes bigint default 0,
    map_output_bytes bigint default 0,
    map_input_records bigint default 0,
    map_output_records bigint default 0,
    combine_input_records bigint default 0,
    combine_output_records bigint default 0,
    spilled_records bigint default 0,
    reduce_input_groups bigint default 0,
    reduce_output_groups bigint default 0,
    reduce_input_records bigint default 0,
    reduce_output_records bigint default 0,
    reduce_input_bytes bigint default 0,
    reduce_output_bytes bigint default 0,
    type VARCHAR(20),
    reduce_shuffle_bytes bigint default 0,
    hostname VARCHAR(80),
    shuffle_finished timestamp default 0,
    sort_finished timestamp default 0,
    spilts bigint default 0,
    primary key(task_id),
    index(start_time, finish_time, job_id)
) ENGINE=InnoDB;

create table if not exists mr_job_timeline_template (
    timestamp timestamp default CURRENT_TIMESTAMP,
    job_id varchar(80),
    task_type varchar(20),
    task_id double,
    status varchar(20),
    primary key(timestamp, job_id),
    index(timestamp, job_id, task_id, task_type)
) ENGINE=InnoDB;

create table if not exists hod_machine_template (
    hodid varchar(20) not null, 
    host varchar(40) not null,
    index(HodId)
) ENGINE=InnoDB;

create table if not exists HodJob_template (
    HodID varchar(20), 
    UserID varchar(20), 
    Status  smallint,
    JobTracker varchar(40), 
    TimeQueued mediumint unsigned,
    StartTime timestamp default CURRENT_TIMESTAMP, 
    EndTime timestamp default 0,  
    NumOfMachines smallint unsigned,  
    SlotLimitPerTracker smallint unsigned,
    LogProcessStatus varchar(20),
    primary key(HodId),
    index(StartTime, EndTime)
) ENGINE=InnoDB;

create table if not exists hod_job_digest_template (
    timestamp timestamp default 0,
    HodID VARCHAR(20),
    UserID VARCHAR(20),
    host int,
    load_15 double, 
    load_5 double,
    load_1 double,
    task_total double,
    task_running double,
    task_sleep double,
    task_stopped double,
    task_zombie double,
    mem_total double,
    mem_buffers double,
    mem_cached double,
    mem_used double,
    mem_free double,
    eth0_rxerrs double,
    eth0_rxbyts double,
    eth0_rxpcks double,
    eth0_rxdrops double,
    eth0_txerrs double,
    eth0_txbyts double,
    eth0_txpcks double,
    eth0_txdrops double,
    eth1_rxerrs double,
    eth1_rxbyts double,
    eth1_rxpcks double,
    eth1_rxdrops double,
    eth1_txerrs double,
    eth1_txbyts double,
    eth1_txpcks double,
    eth1_txdrops double,
    sda_rkbs double,
    sda_wkbs double,
    sdb_rkbs double,
    sdb_wkbs double,
    sdc_rkbs double,
    sdc_wkbs double,
    sdd_rkbs double,
    sdd_wkbs double,
    cpu_idle_pcnt float,
    cpu_nice_pcnt float,
    cpu_system_pcnt float,
    cpu_user_pcnt float,
    cpu_hirq_pcnt float,
    cpu_sirq_pcnt float,
    iowait_pcnt float,
    mem_buffers_pcnt float,
    mem_used_pcnt float,
    eth0_busy_pcnt float,
    eth1_busy_pcnt float,
    sda_busy_pcnt float,
    sdb_busy_pcnt float,
    sdc_busy_pcnt float,
    sdd_busy_pcnt float,
    swap_used_pcnt float,
    primary key(HodId, timestamp),
    index(timestamp)
) ENGINE=InnoDB; 

create table if not exists user_util_template (
    timestamp timestamp default CURRENT_TIMESTAMP,
    user VARCHAR(20),
    node_total int default 0,
    cpu_unused double default 0,
    cpu_used double default 0,
    cpu_used_pcnt float default 0,
    disk_unused double default 0,
    disk_used double default 0,
    disk_used_pcnt float default 0,
    network_unused double default 0,
    network_used double default 0,
    network_used_pcnt float default 0,
    memory_unused double default 0,
    memory_used double default 0,
    memory_used_pcnt float default 0,
    primary key(user, timestamp),
    index(timestamp)
) ENGINE=InnoDB;

create table if not exists hdfs_usage_template (
    timestamp timestamp default CURRENT_TIMESTAMP,
    user VARCHAR(20),
    bytes bigint default 0,
    files bigint default 0,
    primary key(timestamp, user),
    index(timestamp)
) ENGINE=InnoDB;

create table if not exists util_template (
    timestamp timestamp default CURRENT_TIMESTAMP,
    user VARCHAR(20),
    queue VARCHAR(20),
    bytes bigint,
    slot_hours double,
    primary key(user, timestamp),
    index(queue)
) ENGINE=InnoDB;

create table if not exists ClientTrace_template (
    Timestamp timestamp default 0,
    local_hdfs_read double,
    intra_rack_hdfs_read double,
    inter_rack_hdfs_read double,
    local_hdfs_write double,
    intra_rack_hdfs_write double,
    inter_rack_hdfs_write double,
    local_mapred_shuffle double,
    intra_rack_mapred_shuffle double,
    inter_rack_mapred_shuffle double,
    primary key(timestamp),
     index(timestamp)
) ENGINE=InnoDB;

create table if not exists chunkqueue_template (
    chukwa_timestamp timestamp default CURRENT_TIMESTAMP,
    hostname varchar(50),
    recordname varchar(32),
    contextname varchar(32),
	removedchunk bigint,
	queuesize int,
	removedchunk_raw bigint,
	datasize bigint,
	fullqueue tinyint,
	addedchunk_rate int,
	addedchunk_raw bigint,
	period bigint,
	addedchunk bigint,
	removedchunk_rate int,
    primary key(chukwa_timestamp,hostname,recordname,contextname),
    index (chukwa_timestamp)
) ENGINE=InnoDB;

create table if not exists chukwaagent_template (
    chukwa_timestamp timestamp default CURRENT_TIMESTAMP,
    hostname varchar(50),
    recordname varchar(32),
    contextname varchar(32),
	addedadaptor_rate int,
	addedadaptor_raw int,
	removedadaptor_rate int,
	removedadaptor int,
	period int,
	adaptorcount int,
	removedadaptor_raw int,
	process int,
	addedadaptor int,
    primary key(chukwa_timestamp,hostname,recordname,contextname),
    index (chukwa_timestamp)
) ENGINE=InnoDB;

create table if not exists chukwahttpsender_template (
    chukwa_timestamp timestamp default CURRENT_TIMESTAMP,
    hostname varchar(50),
    recordname varchar(32),
    contextname varchar(32),
	httppost_rate int,
	httpthrowable_raw int,
	httpexception_rate int,
	httpthrowable int,
	httpthrowable_rate int,
	collectorrollover_rate int,
	httppost_raw int,
	period int,
	httpexception_raw int,
	httppost int,
	httptimeoutexception int,
	httptimeoutexception_raw int,
	collectorrollover_raw int,
	collectorrollover int,
	httptimeoutexception_rate int,
	httpexception int,
    primary key(chukwa_timestamp,hostname,recordname,contextname),
    index (chukwa_timestamp)
) ENGINE=InnoDB;

create table if not exists mr_job_conf_template (
	ts  timestamp default CURRENT_TIMESTAMP,
    job_id varchar(80),
	mr_output_key_cls varchar(128) null,
	mr_runner_cls varchar(128) null,
	mr_output_value_cls varchar(128) null,
	mr_input_fmt_cls varchar(128) null,
	mr_output_fmt_cls varchar(128) null,
	mr_reducer_cls varchar(128) null,
	mr_mapper_cls varchar(128) null,
    primary key(job_id),
    index (ts)
) ENGINE=InnoDB;

create table if not exists mapreduce_fsm_template (
    job_id VARCHAR(80),
    unique_id VARCHAR(80),
    friendly_id VARCHAR(80),
    state_name VARCHAR(80),
    hostname VARCHAR(80),
    other_host VARCHAR(80),
    start_time timestamp default 0,
    finish_time timestamp default 0,
    start_time_millis bigint default 0,
    finish_time_millis bigint default 0,
    status varchar(10) default 0,
    file_bytes_read bigint default 0,
    file_bytes_written bigint default 0,
    combine_input_records bigint default 0,
    combine_output_records bigint default 0,
    input_records bigint default 0,
    output_records bigint default 0,
    input_bytes bigint default 0,
    output_bytes bigint default 0,
    input_groups bigint default 0,
    spilled_records bigint default 0,
    primary key(unique_id),
    index(start_time, finish_time, job_id)
) ENGINE=InnoDB;

create table if not exists user_job_summary_template (
    timestamp timestamp default CURRENT_TIMESTAMP,
    userid varchar(32),
    totalJobs double null, 
    dataLocalMaps double null, 
    rackLocalMaps double null, 
    remoteMaps double null, 
    mapInputBytes double null, 
    reduceOutputRecords double null, 
    mapSlotHours double null, 
    reduceSlotHours double null, 
    totalMaps double null, 
    totalReduces double null,
    primary key(userid, timestamp)
) ENGINE=InnoDB;


create table if not exists filesystem_fsm_template (
    block_id VARCHAR(80),
    unique_id VARCHAR(80),
    client_id VARCHAR(80),
    state_name VARCHAR(80),
    hostname VARCHAR(80),
    other_host VARCHAR(80),
    start_time timestamp default 0,
    finish_time timestamp default 0,
    start_time_millis bigint default 0,
    finish_time_millis bigint default 0,
    status varchar(10) default 0,
    bytes bigint default 0,
    primary key(unique_id),
    index(start_time, finish_time, unique_id)
) ENGINE=InnoDB;
