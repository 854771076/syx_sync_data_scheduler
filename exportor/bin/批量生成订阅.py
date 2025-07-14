import datetime
import os
from TableEnum import TableEnum
os.chdir(os.path.dirname(os.path.abspath(__file__)))


# 配置参数（根据实际需求修改）
params = {
    'customer_id': 1,
    'topic': 'fix_*',  # 通配符保留，可根据表名调整
    'db': 'dwd_ggzx',
    'full_partition': '20250710',
    'start_partition': '20250710',
    'is_full': '0',
    'is_increment': '1',
    'sync_config': 'upsert',
    'output_file': './batch_insert.sql'
}


def generate_sql(tb_names):
    sql_template = """INSERT INTO `syx_export_data_file`.`syx_export_customer_subscribe` 
( `customer_id`, `topic`, `db`, `tb`, `full_partition`, `start_partition_date`, 
`is_full_syncing`, `is_increment_syncing`, `sync_config`, `filter`, `is_valid`) 
VALUES ({customer_id}, '{topic}', '{db}', '{tb}', '{full_partition}', 
'{start_partition}', '{is_full}', '{is_increment}', '{sync_config}', NULL, 1);\n"""
    
    with open(params['output_file'], 'w') as f:
        for idx, tb in enumerate(tb_names, 1):
            f.write(sql_template.format(
                customer_id=params['customer_id'],
                topic=params['topic'],  # 替换通配符
                db=params['db'],
                tb=tb.strip(),
                full_partition=params['full_partition'],
                start_partition=params['start_partition'],
                is_full=params['is_full'],
                is_increment=params['is_increment'],
                sync_config=params['sync_config']
            ))

if __name__ == '__main__':
    tables=[
       table.name for table in TableEnum
    ]
    generate_sql(tables)
    print(f"已生成 {len(tables)} 条SQL到 {params['output_file']}")