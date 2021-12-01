import os
import json
import datetime
import configparser
import logging
import airflow
import uuid
from contextlib import closing
from fastavro import writer
from airflow.models import DAG
from airflow.models.variable import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.jdbc_hook import JdbcHook
from airflow.hooks.webhdfs_hook import WebHDFSHook
from airflow.hooks.hive_hooks import HiveCliHook
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from af_etl_log import *
from select import select
from base64 import b64encode


def get_parameters(ini_file: str) -> dict:
    config = configparser.ConfigParser()
    with open(ini_file, "r", encoding="utf-8") as fp:
        config.read_file(fp)
    params = {}
    for section, proxy in config.items():
        ini_section = config.items(section)
        common_keys = set(params.keys()) & {p[0] for p in ini_section}
        assert len(common_keys) == 0, "Duplicated keys: {}".format(common_keys)
        params.update(dict(config.items(section)))
    return params


def get_webhdfs_hook(webhdfs_conn_id: str, destination_path: str) -> (WebHDFSHook, None):
    webhdfs_conn_id_list = webhdfs_conn_id.split(",")
    for conn in webhdfs_conn_id_list:
        logging.info(f" Trying connect: {conn}...")
        try:
            wh = WebHDFSHook(webhdfs_conn_id=conn)
            wh.check_for_path(destination_path)
            return wh
        except Exception as ex:
            logging.warning(ex)
            logging.warning(f" Fail connection to WebHDFS by conn_id: {conn} ")
    return None


job_name = "afdag_naumen_ods"
dag_folder = Variable.get("dag_folder")
import_files_path = os.path.join(dag_folder, job_name, "import_files")
config_file_full_path = os.path.join(import_files_path, "config", "config.ini")
global_params = get_parameters(config_file_full_path)


def run_bush_command(command: str, connection_id: str):
    '''
        Функция для вызова bash команды из python оператора.
    '''
    ssh_hook = SSHHook(connection_id)
    with ssh_hook.get_conn() as ssh_client:
        print(f"Running command: {command}")
        stdin, stdout, stderr = ssh_client.exec_command(command=command)
        channel = stdout.channel
        stdin.close()
        channel.shutdown_write()
        agg_stdout = b''
        agg_stderr = b''
        readq, _, _ = select([channel], [], [], 10)
        while not channel.closed or \
                channel.recv_ready() or \
                channel.recv_stderr_ready():
            readq, _, _ = select([channel], [], [], 10)
            for c in readq:
                if c.recv_ready():
                    line = stdout.channel.recv(len(c.in_buffer))
                    line = line
                    agg_stdout += line
                    print(line.decode('utf-8').strip('\n'))
                if c.recv_stderr_ready():
                    line = stderr.channel.recv_stderr(len(c.in_stderr_buffer))
                    line = line
                    agg_stderr += line
                    print(line.decode('utf-8').strip('\n'))
            if stdout.channel.exit_status_ready() \
                    and not stderr.channel.recv_stderr_ready() \
                    and not stdout.channel.recv_ready():
                stdout.channel.shutdown_read()
                stdout.channel.close()
                break
        stdout.close()
        stderr.close()
        exit_status = stdout.channel.recv_exit_status()
        if exit_status == 0:
            return b64encode(agg_stdout).decode('utf-8')
        else:
            error_msg = agg_stderr.decode('utf-8')
            print(f"error running cmd: {error_msg}")
            raise AirflowException("error running error: {0}".format(error_msg))


class ObjectDataStorage(object):
    def __init__(self, table_name: str):
        self._table_name = table_name
        self._paths = {
            'compute_delta': os.path.join(import_files_path, "hql_files", self._table_name, "compute_delta.hql"),
            'compute_mirror': os.path.join(import_files_path, "hql_files", self._table_name, "compute_mirror.hql"),
            'avro_to_hive': os.path.join(import_files_path, "hql_files", self._table_name, "truncate_src.hql"),
            'schema_avro': os.path.join(import_files_path, "avsc_files", f"{self._table_name}.avsc"),
            'data_avro': os.path.join(import_files_path, "avro_files", f"{self._table_name}.avro"),
            'get_data_naumen_pg': os.path.join(import_files_path, "sql_files", f"get_{self._table_name}.sql"),
            'get_count_naumen_pg': os.path.join(import_files_path, "sql_files", f"count_{self._table_name}.sql"),
        }

    def get_avro_data_path(self) -> str:
        return self._paths["data_avro"]

    def get_avro_schema_path(self) -> str:
        return self._paths["schema_avro"]

    def get_table_name(self) -> str:
        return self._table_name

    def __records_to_avro(self, fields: list, pg_hook: any, batch_size: int) -> dict:
        logging.info(" Start receiving data from PosgreSQL")
        base_sql_get_data = self.__read_file(self._paths['get_data_naumen_pg'])
        sql_count_data = self.__read_file(self._paths['get_count_naumen_pg'])
        with closing(pg_hook.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                print(" Using hook for connection PG - Naumen")
                cursor.execute(sql_count_data)
                print(sql_count_data)
                _count = int(cursor.fetchall()[0][0])
                logging.info(f" Storage contains {_count} records in PosgreSQL.")
                for offset in range(0, _count, batch_size):
                    sql_get_data = base_sql_get_data
                    sql_get_data = sql_get_data.replace("{offset}", str(offset))
                    sql_get_data = sql_get_data.replace("{limit}", str(batch_size))
                    print(sql_get_data)
                    cursor.execute(sql_get_data)
                    dataset = cursor.fetchall()
                    _saved = offset + batch_size
                    if _count < (offset + batch_size):
                        _saved = _count
                    logging.info(f" Done receiving {_saved} of {_count} records from PostgreSQL.")
                    for item in dataset:
                        record = {}
                        for idx in range(0, len(fields), 1):
                            record[fields[idx]['name']] = item[idx]
                        yield record

    def __read_file(self, filename: str) -> str:
        with open(filename, 'r') as f:
            data = f.read()
        return data.replace('\r\n', '\n').replace("\n", " ")

    def __read_avsc(self, filename: str) -> dict:
        with open(filename, 'r') as f:
            data = json.load(f)
        return data

    def pg_to_avro(self, pg_hook: any, batch_size: int) -> str:
        logging.info(" Start save data to avro to file")
        avsc = self.__read_avsc(self._paths['schema_avro'])
        if os.path.exists(self._paths['data_avro']):
            os.remove(self._paths['data_avro'])

        with open(self._paths['data_avro'], 'wb') as out:
            writer(out, avsc, self.__records_to_avro(avsc['fields'], pg_hook, batch_size))

        logging.info(f" Success save data to file {self._paths['data_avro']}")
        return self._paths['data_avro']

    def truncate(self, hive_schema_name: str, hdfs_full_path: str) -> str:
        avro_to_hive = self.__read_file(self._paths['avro_to_hive']).replace("{$hdfs_full_path}", f"{hdfs_full_path}")
        avro_to_hive = avro_to_hive.replace("${ods_schema_name}", hive_schema_name)
        return avro_to_hive

    def compute_delta(self, hive_schema_name: str, dws_job: str, insert_date: datetime, date: str) -> str:
        delta = self.__read_file(self._paths['compute_delta'])
        delta = delta.replace("${dws_job}", dws_job) \
            .replace("${insert_date}", insert_date.strftime('%Y-%m-%d %H:%M:%S.%f')) \
            .replace("${ods_schema_name}", hive_schema_name) \
            .replace("${date}", date)

        return delta

    def compute_mirror(self, hive_schema_name: str, dws_job: str, insert_date: datetime, date: str) -> str:
        mirror = self.__read_file(self._paths['compute_mirror'])
        mirror = mirror.replace("${ods_schema_name}", hive_schema_name)
        mirror = mirror.replace("${dws_job}", dws_job) \
            .replace("${insert_date}", insert_date.strftime('%Y-%m-%d %H:%M:%S.%f')) \
            .replace("${insert_date_delta}", date)
        return mirror


@step_log("Get job parameters")
def start_stage(**kwargs):
    print(kwargs)
    parameters = get_parameters(config_file_full_path)
    parameters['afdag_naumen_ods_job_id'] = str(xcom_pull_job_id(**kwargs))
    parameters['afdag_job_date'] = str(now_moscow_time().strftime('%Y-%m-%d'))
    return parameters


@step_log()
def avro_stage(*args, **context):
    table = ObjectDataStorage(args[0])
    parameters = xcom_pull_job_parameters(**context)
    hook = JdbcHook(parameters['pg_naumen_conn_id'])
    table.pg_to_avro(hook, int(parameters['batch_size']))
    return "Data saved in avro format."


@step_log()
def hdfs_stage(*args, **context):
    table = ObjectDataStorage(args[0])
    parameters = xcom_pull_job_parameters(**context)
    hdfs = get_webhdfs_hook(parameters['hdfs_ak_conn_id'], parameters['hdfs_path'])
    if hdfs is None:
        logging.warning("No servers available to upload files.")
        raise Exception(" No servers available to upload files.")
    hdfs.load_file(table.get_avro_data_path(),
                   f"{parameters['hdfs_path'] + table.get_table_name()}/{table.get_table_name()}.avro")
    return "Data transferred on HDFS"


@step_log()
def clear_src(*args, **kwargs):
    table = ObjectDataStorage(args[0])
    parameters = xcom_pull_job_parameters(**kwargs)
    path = f"{parameters['hdfs_path']}"
    hook = HiveCliHook(parameters['hive_cli_conn_id'])
    hook.run_cli(table.truncate(parameters['hive_schema_name'], path))
    return "Success"


@step_log()
def delta_stage(*args, **context):
    table = ObjectDataStorage(args[0])
    parameters = xcom_pull_job_parameters(**context)
    hook = HiveCliHook(parameters['hive_cli_conn_id'])
    job_id = str(xcom_pull_step_id(**context))
    insert_datetime = now_moscow_time()
    job_date = parameters['afdag_job_date']
    hook.run_cli(table.compute_delta(parameters['hive_schema_name'], job_id, insert_datetime, job_date))
    return "Success"


@step_log()
def mirror_stage(*args, **kwargs):
    table = ObjectDataStorage(args[0])
    parameters = xcom_pull_job_parameters(**kwargs)
    job_date = parameters['afdag_job_date']
    job_id = str(xcom_pull_step_id(**kwargs))
    insert_datetime = now_moscow_time()
    hook = HiveCliHook(parameters['hive_cli_conn_id'])
    hook.run_cli(table.compute_mirror(parameters['hive_schema_name'], job_id, insert_datetime, job_date))
    return "Success"


@step_log()
def run_sqoop_loading(*args, **context):
    table = ObjectDataStorage(args[0])
    parameters = xcom_pull_job_parameters(**context)
    max_page = int(parameters['count_pages'])
    for page in range(0, max_page, 1):
        bash_command = sqoop(table.get_table_name(), parameters, max_page, page)
        run_bush_command(bash_command, parameters['ssh_edge_conn_id'])
    return "Success"


def sqoop(table: str, parameters: dict, page_count: int, current_page: int) -> str:
    bash_file = os.path.join(import_files_path, "bash_files", f"{table}_sqoop_import.sh")
    with open(bash_file, 'r') as f:
        data = f.read()
    connection = BaseHook.get_connection(parameters['pg_naumen_conn_id'])
    command = data.replace('\r\n', '\n') \
        .replace("\n", " ") \
        .replace("{$file_path}", f"{parameters['hdfs_path']}{table}") \
        .replace("{$sqoop_naumen_jdbc}", connection.host) \
        .replace("{$sqoop_naumen_user}", connection.login) \
        .replace("{$sqoop_page_count}", str(page_count)) \
        .replace("{$sqoop_current_page}", str(current_page)) \
        .replace("{$sqoop_naumen_password}", connection.password) \
        .replace("{$src_full_table_name}", parameters['hive_schema_name'] + "." + table + "_src") \
        .replace("{$hadoop_database}", parameters['hive_schema_name']) \
        .replace("{$hadoop_table_name}", table + "_src") \
        .replace("{$creation_date}", parameters['afdag_job_date'])
    return command


default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime.strptime(global_params["start_date_time"], '%Y-%m-%d %H:%M:%S'),
    'depends_on_past': False,
    'email': [global_params['on_failure_email_recipients']],
    'email_on_failure': True,
    'provide_context': True
}

with DAG(dag_id=job_name,
         default_args=default_args,
         concurrency=int(global_params['concurrency']),
         on_success_callback=job_success_callback,
         on_failure_callback=job_error_callback,
         catchup=False,
         schedule_interval=global_params['cron_expression']
         ) as dag:
    job_init = InitJobOperator(get_params_callback=start_stage, dag=dag)

    for table in [
        "tbl_servicecall",
        "tbl_servicec_configit",
        "tbl_servicec_analytic",
        "tbl_objectba_analytic",
        "tbl_analyticalcat"
    ]:
        stage_src = PythonOperator(
            task_id=f'stage_{table}_hive_clear_src_ods_naumen',
            python_callable=clear_src,
            op_args=[table],
            dag=dag)

        stage_sqoop_cli = PythonOperator(
            task_id=f'stage_{table}_sqoop_avro_ods_naumen',
            python_callable=run_sqoop_loading,
            op_args=[table],
            dag=dag)

        stage_delta = PythonOperator(
            task_id=f'stage_{table}_hive_compute_delta_ods_naumen',
            python_callable=delta_stage,
            op_args=[table],
            dag=dag)

        stage_mirror = PythonOperator(
            task_id=f'stage_{table}_hive_compute_mirror_ods_naumen',
            python_callable=mirror_stage,
            op_args=[table],
            dag=dag)

        job_init >> stage_src >> stage_sqoop_cli >> stage_delta >> stage_mirror

    for table in [
        "tbl_catalogs",
        "tbl_employee",
        "tbl_impact",
        "tbl_location",
        "tbl_mark",
        "tbl_objectbase",
        "tbl_ou",
        "tbl_priority",
        "tbl_service",
        "tbl_servicetime",
        "tbl_stdonechildsc",
        "tbl_team",
        "tbl_timezone",
        "tbl_urgency",
        "tbl_wayadressing"
    ]:
        stage_avro = PythonOperator(
            task_id=f'stage_{table}_create_avro_ods_naumen',
            python_callable=avro_stage,
            op_args=[table],
            dag=dag)

        stage_src = PythonOperator(
            task_id=f'stage_{table}_hive_clear_src_ods_naumen',
            python_callable=clear_src,
            op_args=[table],
            dag=dag)

        stage_hdfs = PythonOperator(
            task_id=f'stage_{table}_upload_to_hdfs_avro_ods_naumen',
            python_callable=hdfs_stage,
            op_args=[table],
            dag=dag)

        stage_delta = PythonOperator(
            task_id=f'stage_{table}_hive_compute_delta_ods_naumen',
            python_callable=delta_stage,
            op_args=[table],
            dag=dag)

        stage_mirror = PythonOperator(
            task_id=f'stage_{table}_hive_compute_mirror_ods_naumen',
            python_callable=mirror_stage,
            op_args=[table],
            dag=dag)

        job_init >> stage_avro >> stage_src >> stage_hdfs >> stage_delta >> stage_mirror
