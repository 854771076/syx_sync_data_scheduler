import datetime
import os
# 设置当前路径为项目根目录
os.chdir(os.path.dirname(os.path.abspath(__file__)))
false=False
TABLES=[{
	"name": "采集三级节点状态详情表",
	"db": "rpa_gather_ota_trip",
	"table": "ota_spider_collect_details_info",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_ota_spider_collect_details_info",
	"update_column": "end_date",
	"other": false,
	"partition_column": "partition_date"
}, {
	"name": "文旅酒店平台基本信息表",
	"db": "rpa_gather_ota_trip",
	"table": "ota_trip_basic_info",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_ota_trip_basic_info",
	"update_column": "",
	"other": false,
	"partition_column": "partition_date"
}, {
	"name": "回调数据记录表",
	"db": "rpa_gather_ota_trip",
	"table": "ota_trip_callback_log",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_ota_trip_callback_log",
	"update_column": "update_time",
	"other": false,
	"partition_column": "partition_date"
}, {
	"name": "渠道管理表",
	"db": "rpa_gather_ota_trip",
	"table": "ota_trip_channel_app_id",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_ota_trip_channel_app_id",
	"update_column": "",
	"other": false,
	"partition_column": "partition_date"
}, {
	"name": "电子税局详细信息表",
	"db": "rpa_gather_ota_trip",
	"table": "ota_trip_collection_node_status",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_ota_trip_collection_node_status",
	"update_column": "",
	"other": false,
	"partition_column": "partition_date"
}, {
	"name": "解析索引信息",
	"db": "rpa_gather_ota_trip",
	"table": "ota_trip_data_index_info",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_ota_trip_data_index_info",
	"update_column": "finish_time",
	"other": false,
	"partition_column": "partition_date"
}, {
	"name": "钉钉消息类型表",
	"db": "rpa_gather_ota_trip",
	"table": "ota_trip_dingding_type",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_ota_trip_dingding_type",
	"update_column": "",
	"other": false,
	"partition_column": "partition_date"
}, {
	"name": "登录参数记录表",
	"db": "rpa_gather_ota_trip",
	"table": "ota_trip_login_success_info",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_ota_trip_login_success_info",
	"update_column": "",
	"other": false,
	"partition_column": "partition_date"
}, {
	"name": "登录方式表",
	"db": "rpa_gather_ota_trip",
	"table": "ota_trip_login_type_info",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_ota_trip_login_type_info",
	"update_column": "",
	"other": false,
	"partition_column": "partition_date"
}, {
	"name": "税区采集节点状态码表",
	"db": "rpa_gather_ota_trip",
	"table": "ota_trip_node_code",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_ota_trip_node_code",
	"update_column": "update_time",
	"other": false,
	"partition_column": "partition_date"
}, {
	"name": "税局节点渠道关联表",
	"db": "rpa_gather_ota_trip",
	"table": "ota_trip_node_code_channel_association",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_ota_trip_node_code_channel_association",
	"update_column": "",
	"other": false,
	"partition_column": "partition_date"
}, {
	"name": "税区任务平均执行时间记录表",
	"db": "rpa_gather_ota_trip",
	"table": "ota_trip_operate_bureau_info",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_ota_trip_operate_bureau_info",
	"update_column": "",
	"other": false,
	"partition_column": "partition_date"
},
	{
	"name": "微风企任务平均执行时间记录表",
	"db": "rpa_gather_ota_trip",
	"table": "ota_trip_operate_tax_record_time_info",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_ota_trip_operate_tax_record_time_info",
	"update_column": "",
	"other": false,
	"partition_column": "partition_date"
},
	{
	"name": "订单税区节点详情快照表",
	"db": "rpa_gather_ota_trip",
	"table": "ota_trip_order_id_snapshot",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_ota_trip_order_id_snapshot",
	"update_column": "update_time",
	"other": false,
	"partition_column": "partition_date"
}, {
	"name": "人员基本信息表",
	"db": "rpa_gather_ota_trip",
	"table": "ota_trip_people_basic_info",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_ota_trip_people_basic_info",
	"update_column": "",
	"other": false,
	"partition_column": "partition_date"
}, {
	"name": "人员钉钉消息关联表",
	"db": "rpa_gather_ota_trip",
	"table": "ota_trip_people_dingding",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_ota_trip_people_dingding",
	"update_column": "",
	"other": false,
	"partition_column": "partition_date"
}, {
	"name": "登录三级节点状态详情表",
	"db": "rpa_gather_ota_trip",
	"table": "ota_trip_spider_login_details_info",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_ota_trip_spider_login_details_info",
	"update_column": "update_time",
	"other": false,
	"partition_column": "partition_date"
}, {
	"name": "解析进度信息",
	"db": "rpa_gather_ota_trip",
	"table": "ota_trip_spider_parse_details_info",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_ota_trip_spider_parse_details_info",
	"update_column": "end_date",
	"other": false,
	"partition_column": "partition_date"
}, {
	"name": "任务状态概览表",
	"db": "rpa_gather_ota_trip",
	"table": "ota_trip_spider_task_info",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_ota_trip_spider_task_info",
	"update_column": "update_time",
	"other": false,
	"partition_column": "partition_date"
}, {
	"name": "回调数据示例表",
	"db": "rpa_gather_ota_trip",
	"table": "ota_trip_spider_task_schedule_callback_ge",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_ota_trip_spider_task_schedule_callback_ge",
	"update_column": "create_time",
	"other": false,
	"partition_column": "partition_date"
}, {
	"name": "任务节点状态表",
	"db": "rpa_gather_ota_trip",
	"table": "ota_trip_spider_task_schedule_info",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_ota_trip_spider_task_schedule_info",
	"update_column": "update_time",
	"other": false,
	"partition_column": "partition_date"
}, {
	"name": "商家违约信息",
	"db": "rpa_gather_ota_trip",
	"table": "tb_hotel_ota_breach_info",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_tb_hotel_ota_breach_info",
	"update_column": "create_time",
	"other": false,
	"partition_column": "partition_date"
}, {
	"name": "收款记录",
	"db": "rpa_gather_ota_trip",
	"table": "tb_hotel_ota_collection_record",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_tb_hotel_ota_collection_record",
	"update_column": "create_time",
	"other": false,
	"partition_column": "partition_date"
}, {
	"name": "收款明细",
	"db": "rpa_gather_ota_trip",
	"table": "tb_hotel_ota_collection_record_detail",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_tb_hotel_ota_collection_record_detail",
	"update_column": "create_time",
	"other": false,
	"partition_column": "partition_date"
}, {
	"name": "酒店设施服务",
	"db": "rpa_gather_ota_trip",
	"table": "tb_hotel_ota_facility_service_info",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_tb_hotel_ota_facility_service_info",
	"update_column": "create_time",
	"other": false,
	"partition_column": "partition_date"
}, {
	"name": "财务信息",
	"db": "rpa_gather_ota_trip",
	"table": "tb_hotel_ota_financial_info",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_tb_hotel_ota_financial_info",
	"update_column": "create_time",
	"other": false,
	"partition_column": "partition_date"
}, {
	"name": "酒店基础信息",
	"db": "rpa_gather_ota_trip",
	"table": "tb_hotel_ota_info",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_tb_hotel_ota_info",
	"update_column": "create_time",
	"other": false,
	"partition_column": "partition_date"
}, {
	"name": "酒店标签信息",
	"db": "rpa_gather_ota_trip",
	"table": "tb_hotel_ota_label_info",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_tb_hotel_ota_label_info",
	"update_column": "create_time",
	"other": false,
	"partition_column": "partition_date"
}, {
	"name": "订单信息",
	"db": "rpa_gather_ota_trip",
	"table": "tb_hotel_ota_order_info",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_tb_hotel_ota_order_info",
	"update_column": "create_time",
	"other": false,
	"partition_column": "partition_date"
}, {
	"name": "服务质量分",
	"db": "rpa_gather_ota_trip",
	"table": "tb_hotel_ota_psi_info",
	"db_start": 0,
	"db_num": 0,
	"t_start": 0,
	"t_num": 0,
	"hive_db": "ods_comps",
	"hive_table": "ods_b22_db_rpa_gather_ota_trip_t_tb_hotel_ota_psi_info",
	"update_column": "create_time",
	"other": false,
	"partition_column": "partition_date"
}]

# 表结构模型参照datax_executor/executors/models.py 中的Task
def generate_config_sql(task_config):
    """根据表结构模型生成对应配置"""
    if task_config.get('suffix_list'):
        print(task_config)
    if task_config.get("update_column_type")=="milli_second":
        task_config["update_column"]=f'DATE_FORMAT(FROM_UNIXTIME( {task_config["update_column"]} / 1000), "%Y-%m-%d %H:%i:%s")'
    base_sql = f"""
    replace INTO `executors_task` (
        `is_active`, `name`, `description`, `created_at`, `updated_at`, 
        `project_id`, `is_delete`, `split_config_id`, `datax_json`, `config`,
        `update_column`, `data_source_id`, `data_target_id`, `source_db`, `source_table`, 
        `target_db`, `target_table`, `columns`, `exclude_columns`, `is_add_sync_time`, 
        `is_partition`, `partition_column`, `reader_transform_columns`, `sync_time_column`,`is_custom_script`
    ) VALUES (
        1,  -- is_active
        '{task_config["db"]}.{task_config["table"]}',  -- name
        '{task_config.get("name", "")}',  -- description
        NOW(),  -- created_at
        NOW(),  -- updated_at
        {project_id},  -- project_id
        0,  -- is_delete (模型默认False，但示例数据为0)
        {split_config_id},  -- split_config_id
        '{{}}',  -- datax_json (模型默认空字典)
        '{{}}',  -- config (模型默认空字典)
        {f"'{task_config['update_column']}'" if task_config.get('update_column') else "NULL"},  -- update_column
        {data_source_id},  -- data_source_id
        {data_target_id},  -- data_target_id
        '{task_config["hive_db"]}',  -- source_db
        '{task_config["hive_table"]}',  -- source_table
        '{task_config["hive_db"]}',  -- target_db
        '{task_config["hive_table"]}',  -- target_table
        '[]',  -- columns (模型默认空列表)
        '["cdc_sync_date", "partition_date", "rw"]',  -- exclude_columns (模型默认空列表)
        0,  -- is_add_sync_time
        {1 if task_config.get('partition_column') else 0},  -- is_partition
        {f"'{task_config['partition_column']}'" if task_config.get('partition_column') else "NULL"},  -- partition_column
        '{{}}',  -- reader_transform_columns (模型默认空字典)
        '',  -- sync_time_column
        {1 if task_config.get('is_custom_script') else 0}  -- is_custom_script

        
    );
    """
    return base_sql

if __name__ == "__main__":
    data_source_id=4
    data_target_id=8
    project_id=35
    split_config_id=2
    sql_file="酒店采集和酒店解析.sql"
    with open(sql_file, 'w') as f:
        for table_config in TABLES:
            
            sql = generate_config_sql(table_config)
            f.write(sql + "\n")
    print(f"SQL文件已生成：{sql_file}")