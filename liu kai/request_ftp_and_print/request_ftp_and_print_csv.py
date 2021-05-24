import os
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
# from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow.models import BaseOperator
from airflow.utils.dates import days_ago


import pandas as pd
from ftplib import FTP


user_name = 'KaiLiu'
pass_word = 'wllk123456.'
host = '113.54.221.50'
remote_path = 'hello_airflow'
local_path = 'local_hello_airflow'


def dag_fail_handler():
    raise Exception('Dag failed...')


def valid_path_and_create(path:str):
    # 验证本地文件路径是否存在，否则创建该目录
    exist = os.path.exists(local_path)

    if not exist:
        os.mkdir(local_path)


# FTPHook 开发参考文档：http://airflow.apache.org/docs/apache-airflow-providers-ftp/stable/_api/airflow/providers/ftp/hooks/ftp/index.html
'''
def extract():
    # 基于apache-airflow-providers-ftp提供的FTPHook访问ftp服务器，其底层本质使用了库ftplib
    # ftp_conn_id 是在Airflow Web UI 界面下admin->connection 菜单下添加的连接
    # FTPHook 在Airflow 2.0 下才支持

    ftp_hoop = FTPHook(ftp_conn_id='ftp_test_conn')
    # 从ftp获取文件到本地
    ftp_hoop.retrieve_file(remote_full_path=remote_path, local_full_path_or_buffer=local_path)
'''


# --------------------------------------------------------------------------------------------
# 模块ftplib开发参考文档： https://docs.python.org/3/library/ftplib.html
# ftplib.FTP 使用示例：https://www.cnblogs.com/xiao-apple36/p/9675185.html

def extract_v2(file_path, **args):
    # 基于库ftplib 实现访问ftp服务器，并获取指定的文件{file_path}

    local_ftp = FTP(host=args['host'], user=args['user_name'], passwd=args['pass_word'])
    # 从FTP服务器获取文件的命令
    retr_cmd = 'RETR ' + file_path

    with open(local_path, 'wb') as fp:
        res = local_ftp.retrbinary(cmd=retr_cmd, callback=fp.write)

        if res.find('226') != -1:
            print('success get file')
        else:
            raise Exception("fail get file")

    # 关闭ftp连接
    local_ftp.quit()


def load(header=None):
    # 显示从FTP服务器获取的文件数据

    df = pd.read_csv('local_hello_airflow', header=header)
    row, col = df.shape

    for r in range(row):
        for c in range(col):
            print(df.iloc[r, c])


default_args = {
    'owner': 'kailiu',
    'depends_on_past': False,
    'email_on_fail': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2022, 1, 1),
    # 'wait_for_downstream': False,
    # 'on_fail_callback': dag_fail_handler,
}

request_and_print_dag = DAG(dag_id='request_ftp_and_print_csv',
                            default_args=default_args,
                            schedule_interval=timedelta(hours=1), # 每小时调度该任务一次
                            start_date= days_ago(2),

                            )



# FTP 连接参数
# CLI或Web UI方式下运行Dag时传递参数的使用方法：https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
# CLI或Web UI方式下运行Dag时传递参数默认由Airflow引擎传递，在所有Dag中都可用
# 由Airflow引擎传递的参数有如下：http://airflow.apache.org/docs/apache-airflow/stable/macros-ref.html#macros

extract_task_op_kwargs = {
    # 'file_path':remote_path,
    # 'host':host,
    # 'user_name':user_name,
    # 'pass_word':pass_word

    'file_path': "{{dag_run.conf['remote_path']}}",
    'host': "{{dag_run.conf['host']}}",
    'user_name': "{{dag_run.conf['user_name']}}",
    'pass_word': "{{dag_run.conf['pass_word']}}"
}

extract_task = PythonOperator(task_id="extract_task", dag= request_and_print_dag, python_callable=extract_v2,
                              op_kwargs=extract_task_op_kwargs)

load_task = PythonOperator(task_id="load_task", dag=request_and_print_dag, python_callable=load)

extract_task >> load_task

