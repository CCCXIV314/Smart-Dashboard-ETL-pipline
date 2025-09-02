
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
# from airflow.sdk import Param
from datetime import datetime, timedelta
from airflow import DAG
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import hashlib
import os
import tempfile
from airflow.sensors.filesystem import FileSensor
from sqlalchemy import (create_engine, MetaData, Table, Column, String, Integer, Date, Numeric, Boolean, insert)
from sqlalchemy.schema import CreateSchema
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import MetaData, Table, Column, String
import io
import csv
from sqlalchemy import inspect, text
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException
import logging
from airflow.models.param import Param
from urllib.parse import urlparse, quote_plus, unquote
import re



DATA_DIR = '/opt/airflow/input'
CLIENTS_TABLE = 'clients'
TABLE_PREFIX = 'c_'  # Префикс для клиентских таблиц
# CLIENT_CONN_ID = 'client_db_conn_1'
OUR_CONN_ID = 'postgres_connect'



cols_map = {
    # госномер
    'ГосНомер': 'gov_number', 'госномер': 'gov_number', 'гос_номер': 'gov_number',
    'Гос номер': 'gov_number', 'гос номер': 'gov_number', 'gos_number': 'gov_number',
    'reg_number': 'gov_number', 'registration_number': 'gov_number', 'car_id': 'gov_number',
    'gov_number': 'gov_number',
    # водитель
    'Водитель': 'driver', 'водитель': 'driver', 'drivers': 'driver', 'driver': 'driver',
    'driver_name': 'driver', 'driver_fullname': 'driver', 'водители': 'driver',
    # тип ТС
    'ТипТС': 'vehicle_type', 'Тип ТС': 'vehicle_type', 'типтс': 'vehicle_type',
    'тип тс': 'vehicle_type', 'vehicle type': 'vehicle_type',
    'vehicle_type': 'vehicle_type', 'тип_ТС': 'vehicle_type',
    # год выпуска
    'ГодВыпуска': 'release_year', 'Год выпуска': 'release_year',
    'год выпуска': 'release_year', 'release_year': 'release_year',
    'year_of_release': 'release_year', 'год_выпуска': 'release_year',
    'гв': 'release_year', 'born_year': 'release_year',
    # пробег
    'Пробег': 'mileage', 'пробег': 'mileage', 'mileage': 'mileage',
    'mileage_km': 'mileage', 'пробег_км': 'mileage', 'total_mileage': 'mileage',
    'общий пробег': 'mileage', 'общий_пробег': 'mileage',
    # дата последнего ремонта
    'ДатаПоследнегоРемонта': 'last_repair_date', 'Дата последнего ремонта': 'last_repair_date',
    'last_repair_date': 'last_repair_date', 'дата_последнего_ремонта': 'last_repair_date',
    'дата ремонта': 'last_repair_date', 'repair_date': 'last_repair_date',
    # отрасль
    'Отрасль': 'industry', 'отрасль': 'industry', 'industry': 'industry',
    # расход топлива на 100 км
    'РасходТоплива': 'fuel_consumption', 'Расход топлива': 'fuel_consumption',
    'Расход на 100км': 'fuel_consumption', 'Расход_на_100км': 'fuel_consumption',
    'расход л/100км': 'fuel_consumption', 'fuel_per_100km': 'fuel_consumption',
    'fuel_consumption': 'fuel_consumption', 'avg_fuel_consumption': 'fuel_consumption',
    # прицеп
    'ЕстьПрицеп': 'trailer', 'Есть прицеп': 'trailer', 'trailer': 'trailer',
    'has_trailer': 'trailer', 'прицеп': 'trailer',
    # дата операции
    'Дата': 'date', 'дата': 'date', 'Date': 'date', 'date': 'date',
    'expense_date': 'date', 'transaction_date': 'date',
    'operation_date': 'date', 'date_of_expense': 'date',
    # топливо (литры или сумма)
    'Топливо': 'fuel', 'топливо': 'fuel', 'fuel': 'fuel',
    'fuel_amount': 'fuel', 'fuel_volume': 'fuel',
    'liters': 'fuel', 'л топлива': 'fuel',
    # ремонт (стоимость)
    'Ремонт': 'repair', 'ремонт': 'repair', 'repair': 'repair',
    'repair_cost': 'repair', 'cost_of_repair': 'repair',
    # страховка
    'Страховка': 'insurance', 'страховка': 'insurance',
    'insurance': 'insurance', 'insurance_cost': 'insurance',
    # штрафы
    'Штрафы': 'fines', 'штрафы': 'fines', 'fines': 'fines',
    'fines_cost': 'fines', 'cost_of_fines': 'fines', 'fine': 'fines',
    # техобслуживание
    'Техобслуживание': 'maintenance', 'техобслуживание': 'maintenance',
    'maintenance': 'maintenance', 'maintenance_cost': 'maintenance',
    'cost_of_maintenance': 'maintenance',
    # шиномонтаж / шины
    'Шины': 'tire_service', 'Шиномонтаж': 'tire_service',
    'tire_service': 'tire_service', 'tire_cost': 'tire_service',
    'cost_of_tires': 'tire_service',
    # мойка
    'Мойка': 'carwash', 'Автомойка': 'carwash',
    'carwash': 'carwash', 'car_wash': 'carwash',
    # парковка
    'Стоянка': 'parking', 'parking': 'parking', 'parking_cost': 'parking',
    'cost_of_parking': 'parking',
    # платные дороги (транзит)
    'ПлатныеДороги': 'flat_roads', 'Платные дороги': 'flat_roads',
    'tolls_cost': 'flat_roads', 'flat_roads': 'flat_roads',
    'tolls': 'flat_roads',
    # дневное расстояние
    'ДневноеРасстояние': 'daily_distance', 'Дневное расстояние': 'daily_distance',
    'daily_distance': 'daily_distance', 'distance_per_day': 'daily_distance',
    # зарплата
    'ЗарплатаРасчетная': 'calculated_salary', 'Зарплата': 'calculated_salary',
    'salary_cost': 'calculated_salary', 'calculated_salary': 'calculated_salary',
    'salary': 'calculated_salary',
    # антифриз
    'НезамерзающиеЖидкости': 'antifreeze_liquid', 'Незамерзающие жидкости': 'antifreeze_liquid',
    'antifreeze_liquid': 'antifreeze_liquid', 'antifreeze': 'antifreeze_liquid',
    # моторное масло
    'МоторноеМасло': 'engine_oil', 'Моторное масло': 'engine_oil',
    'engine_oil': 'engine_oil', 'oil_change': 'engine_oil',
    # тормозная жидкость
    'ТормознаяЖидкость': 'brake_fluid', 'Тормозная жидкость': 'brake_fluid',
    'brake_fluid': 'brake_fluid', 'brake_liquid': 'brake_fluid',
    # свечи зажигания
    'СвечиЗажигания': 'spark_plugs', 'Свечи зажигания': 'spark_plugs',
    'spark_plugs': 'spark_plugs', 'plugs': 'spark_plugs',
    # топливные фильтры
    'ТопливныеФильтры': 'fuel_filters', 'Топливные фильтры': 'fuel_filters',
    'fuel_filters': 'fuel_filters', 'filters_fuel': 'fuel_filters',
    # фильтры (общие)
    'Фильтры': 'filters', 'filters': 'filters', 'filter': 'filters',
    # ГРМ ремни
    'ГРМ_ремни': 'timing_belts', 'ГРМремни': 'timing_belts',
    'грм_ремни': 'timing_belts', 'Ремни_ГРМ': 'timing_belts',
    'timing_belts': 'timing_belts', 'belt_timing': 'timing_belts',
    'timing_belt': 'timing_belts',
    # тормозные колодки
    'ТормозныеКолодки': 'brake_pads', 'Тормозные колодки': 'brake_pads',
    'brake_pads': 'brake_pads', 'pads_brake': 'brake_pads',
    # прочие/другие расходы
    'ДругиеРасходы': 'other_cost', 'Другие_расходы': 'other_cost',
    'Другие расходы': 'other_cost', 'ПрочиеРасходы': 'other_cost',
    'Прочие расходы': 'other_cost', 'Прочие_расходы': 'other_cost',
    'other_cost': 'other_cost', 'misc_cost': 'other_cost',
    'miscellaneous_expenses': 'other_cost', 'Другое': 'other_cost',
    'Прочие': 'other_cost', 'Другие': 'other_cost',
    # Клиент
    'Клиент': 'client', 'клиент': 'client', 'client': 'client',
    'client_name': 'client',
    # is_rental
    'is_rental': 'is_rental',
    # rental_cost
    'rental_cost': 'rental_cost',
    # purchase_price
    'purchase_price': 'purchase_price',
    # has_casco
    'has_casco': 'has_casco',
    # start_date
    'start_date': 'start_date',
    # rental_expense
    'rental_expense': 'rental_expense',
    # amortization
    'amortization': 'amortization',
    # transport_tax
    'transport_tax': 'transport_tax',
    # casco
    'casco': 'casco',
    # fire_extinguisher
    'fire_extinguishers': 'fire_extinguishers',
    # first_aid_kit
    'first_aid_kits': 'first_aid_kits',
    # hydraulic_oil
    'hydraulic_oil': 'hydraulic_oil',
    # lubrication
    'special_lubricants': 'special_lubricants',
    # customs_fees
    'customs_fees': 'customs_fees',
    # cargo_insurance
    'cargo_insurance': 'cargo_insurance',
    # transloading_fees
    'transloading_fees': 'transloading_fees',
    # temp_control_maintenance
    'temp_control_maintenance': 'temp_control_maintenance',
    # sterilization_costs
    'sterilization_costs': 'sterilization_costs',
    # pharma_licenses
    'pharma_licenses': 'pharma_licenses',
    # bucket_parts
    'bucket_parts': 'bucket_parts',

}


TRANSPORT_COLS = [
    'gov_number', 'driver', 'vehicle_type',
    'release_year', 'mileage', 'last_repair_date',
    'industry', 'fuel_consumption', 'trailer',
    'is_rental', 'rental_cost', 'purchase_price', 'has_casco', 'start_date'
]

EXPENSES_COLS = [
    'date', 'gov_number', 'driver', 'vehicle_type', 'industry',
    'fuel', 'repair', 'insurance', 'fines', 'maintenance',
    'tire_service', 'carwash', 'parking',
    'flat_roads', 'daily_distance', 'calculated_salary', 'trailer',
    'antifreeze_liquid', 'engine_oil', 'brake_fluid', 'spark_plugs',
    'fuel_filters', 'filters', 'timing_belts', 'brake_pads', 'other_cost',
    'is_rental', 'rental_expense', 'amortization', 'transport_tax', 'casco',
    'fire_extinguishers', 'first_aid_kits', 'hydraulic_oil', 'special_lubricants',
    'customs_fees', 'cargo_insurance', 'transloading_fees', 'temp_control_maintenance',
    'sterilization_costs', 'pharma_licenses', 'bucket_parts',
]

TYPE_MAP = {
    'client_id': String(50),
    'gov_number': String(20),
    'driver':     String(255),
    'vehicle_type': String(30),
    'release_year': Integer(),
    'mileage':      Numeric(12,2),
    'last_repair_date': Date(),
    'industry':     String(50),
    'fuel_consumption': Numeric(10,2),
    'trailer':        Boolean(),
    'is_rental':      Boolean(),
    'rental_cost':    Numeric(12,2),
    'purchase_price': Numeric(12,2),
    'start_date':     Date(),
    'has_casco':      Boolean(),
    # для expenses:
    'date':        Date(),
    'fuel':        Numeric(12,2),
    'repair':      Numeric(12,2),
    'insurance':   Numeric(12,2),
    'fines':       Numeric(12,2),
    'maintenance': Numeric(12,2),
    'tire_service': Numeric(12,2),
    'carwash':     Numeric(12,2),
    'parking':     Numeric(12,2),
    'flat_roads':  Numeric(12,2),
    'daily_distance':Numeric(12,2),
    'calculated_salary': Numeric(12,2),
    'antifreeze_liquid': Numeric(12,2),
    'engine_oil': Numeric(12,2),
    'brake_fluid': Numeric(12,2),
    'spark_plugs': Numeric(12,2),
    'fuel_filters': Numeric(12,2),
    'filters': Numeric(12,2),
    'timing_belts': Numeric(12,2),
    'brake_pads': Numeric(12,2),
    'other_cost':  Numeric(12,2),
    'rental_expense': Numeric(12,2),
    'amortization': Numeric(12,2),
    'transport_tax': Numeric(12,2),
    'casco': Numeric(12,2),
    'fire_extinguishers': Numeric(12,2),
    'first_aid_kits': Numeric(12,2),
    'hydraulic_oil': Numeric(12,2),
    'special_lubricants': Numeric(12,2),
    'customs_fees': Numeric(12,2),
    'cargo_insurance': Numeric(12,2),
    'transloading_fees': Numeric(12,2),
    'temp_control_maintenance': Numeric(12,2),
    'sterilization_costs': Numeric(12,2),
    'pharma_licenses': Numeric(12,2),
    'bucket_parts': Numeric(12,2),
}

def get_engine():
    hook = PostgresHook(postgres_conn_id=OUR_CONN_ID)
    return hook.get_sqlalchemy_engine()

def get_raw_conn():
    hook = PostgresHook(postgres_conn_id=OUR_CONN_ID)
    return hook.get_conn()


def init_client_table_dynamic(table_name: str, cols: list, engine):
    meta = MetaData()
    columns = []
    for c in cols:
        col_type = TYPE_MAP.get(c, String(255))  # fallback на String
        # считаем, что первичные ключи — это пересечение с pk_list
        is_pk = c in (['client_id', 'gov_number']
                     if table_name == 'transport'
                     else ['client_id', 'date', 'gov_number'])
        columns.append(Column(c, col_type, primary_key=is_pk))
    Table(table_name, meta, *columns)
    meta.create_all(engine)


# В начало файла после импортов
def process_client(**context):
    client_id = context['dag_run'].conf.get('client_id')
    if not client_id:
        raise ValueError("client_id not provided in DAG configuration")
    # Ваша логика обработки для конкретного client_id
    print(f"Processing client: {client_id}")
    ti = context['ti']
    ti.xcom_push(key='client_id', value=client_id)


def parse_postgres_url(connection_url: str) -> dict:
    # url = connection_url.strip()
    parsed = urlparse(connection_url)
    print(parsed.scheme)
    # print(url)
    # Проверка корректности схемы
    # if not url.startswith("postgresql://"):
    #     raise ValueError("Invalid scheme. Expected 'postgresql'")

    if not connection_url.startswith("postgresql://"):
        raise ValueError("Invalid scheme. Expected 'postgresql://'")

    # Ручной разбор для обработки '@' в пароле
    try:
        # Отбрасываем схему
        parts = connection_url.split("://", 1)[1]

        # Разделяем на учётные данные и остаток (по последнему '@')
        auth, hostinfo = parts.rsplit("@", 1)

        # Разделяем user и password (по первому ':')
        user, password = auth.split(":", 1)

        # Декодируем user и password
        user = unquote(user)
        password = unquote(password)
        print("user: ", user)
        print("password: ", password)
        # Разделяем hostinfo на хост/порт и путь (по первому '/')
        host_port, dbname = hostinfo.split("/", 1)
        print("dbname: ", dbname)
        # Разделяем хост и порт (если есть ':')
        if ":" in host_port:
            host, port = host_port.split(":", 1)
            port = int(port)  # Преобразуем в число
        else:
            host = host_port
            port = 5432  # Порт по умолчанию
        print("host: ", host)
        print("port: ", port)
    except Exception as e:
        raise ValueError(f"Failed to parse URL: {str(e)}")




    # pattern = r"postgresql:\/\/(?P<user>[^:]+):(?P<password>[^|]+)|(?P<host>[^\/]+):?(?P<port>\d+)?\/(?P<dbname>.+)"
    # match = re.match(pattern, url)
    # if not match:
    #     raise ValueError(f"Cannot parse PostgreSQL URL: {url}")
    # Извлекаем компоненты
    # user = unquote(match.group("user"))
    # print("user: ", user)
    # password = unquote(match.group("password"))
    # print("password: ", password)
    # host = match.group("host")
    # print("host: ", host)
    # port = int(match.group("port")) or 5432 # Порт по умолчанию
    # print("port: ", port)
    # dbname = match.group("dbname")
    # print("dbname: ", dbname)

    # # Извлечение компонентов
    # user = unquote(parsed.username) if parsed.username else None
    # print("user: ", user)
    # password = unquote(parsed.password) if parsed.password else None
    # print("password: ", password)
    # host = parsed.hostname
    # print("host: ", host)
    # port = parsed.port if parsed.port else 5432  # Порт по умолчанию для PostgreSQL
    # print("port: ", port)
    # dbname = parsed.path.lstrip('/')  # Убираем первый символ '/' из пути
    # print("dbname: ", dbname)

    print(
        "host: ", host,
        "port: ", port,
        "dbname: ", dbname,
        "user: ", user,
        "password: ", password
    )

    return {
        "host": host,
        "port": port,
        "dbname": dbname,
        "user": user,
        "password": password
    }

def get_client_db_params(client_id: str):
    """Получает параметры подключения к БД клиента из таблицы users"""
    our_hook = PostgresHook(postgres_conn_id=OUR_CONN_ID)
    conn = our_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(
        'SELECT "postgresData" '
        'FROM web.users WHERE companyid = %s',
        (client_id,)
    )

    result = cursor.fetchone()
    cursor.close()
    conn.close()

    if not result:
        raise ValueError(f"DB connection params not found for client: {client_id}")

    print('result[0]: ', result[0])

    params = parse_postgres_url(result[0])

    # safe_password = quote_plus(params["password"])
    # params["password"] = safe_password

    return params

def get_client_id_and_schema(engine, **context):
    ti = context['ti']
    client_id = ti.xcom_pull(task_ids='process_client_data', key='client_id')
    if not client_id:
        raise ValueError("client_id not found in XCom")

    return client_id


def normalize_and_save_temp_csv(df: pd.DataFrame, expected_columns, client_id, return_temp_path=False):

    df = df.rename(columns=cols_map)

    df['client_id'] = client_id

    if 'client' in df.columns:
        df = df.drop(columns=['client'])

    provided = set(df.columns)
    keep_cols = [c for c in expected_columns if c in provided]

    keep_cols = ['client_id'] + keep_cols

    df = df[keep_cols]

    for col in df.columns:
        if col in ('date', 'last_repair_date', 'start_date'):
            df[col] = pd.to_datetime(df[col], errors='coerce')
        elif col in ('release_year'):
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        elif col in ('trailer', 'has_casco', 'is_rental'):
            s = df[col].astype(str).str.strip().str.lower()
            df[col] = s.map({'да': True, 'true': True, '1': True, 'TRUE': True, 'нет': False, 'false': False, '0': False, 'FALSE': False}).astype('boolean')
        elif col in ('gov_number', 'driver', 'vehicle_type', 'industry', 'client_id'):
            df[col] = df[col].astype(str).fillna('')
        else:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    print('df: ', df.head())

    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmpfile:
        df.to_csv(tmpfile.name, index=False, encoding='utf-8')
        temp_csv_path = tmpfile.name

    print('temp_csv: ', pd.read_csv(temp_csv_path).head())
    print('keep_cols: ', keep_cols)

    if return_temp_path:
        return temp_csv_path, keep_cols

def load_local_file(**context):

    transport_path = None
    expenses_path = None

    t_cols = []
    e_cols = []

    ti = context['ti']
    client_id = ti.xcom_pull(task_ids='process_client_data', key='client_id')

    print('client_id: ', client_id)
    print('type of client_id: ', type(client_id))
    for fname in os.listdir(DATA_DIR):
        if not fname.endswith('.csv') and not fname.endswith('.xlsx') and not fname.endswith('.json'):
            continue
        path = os.path.join(DATA_DIR, fname)
        try:
            df0 = pd.DataFrame()
            print('fname: ', fname)
            print('startswith: ', fname.startswith(client_id))
            if fname.startswith(client_id) and fname.endswith('.csv'):
                df0 = pd.read_csv(path)
            elif fname.startswith(client_id) and fname.endswith('.xlsx'):
                df0 = pd.read_excel(path, engine='openpyxl')
            elif fname.startswith(client_id) and fname.endswith('.json'):
                df0 = pd.read_json(path)

            df0 = df0.rename(columns=cols_map)

            cols = set(df0.columns)

            if {'date'}.issubset(cols) and {'gov_number'}.issubset(cols):
                de_df = df0
                expenses_path, e_cols = normalize_and_save_temp_csv(de_df, EXPENSES_COLS, client_id, return_temp_path=True)
            elif {'gov_number'}.issubset(cols) and not {'date'}.issubset(cols):
                tp_df = df0
                transport_path, t_cols = normalize_and_save_temp_csv(tp_df, TRANSPORT_COLS, client_id, return_temp_path=True)

            ti.xcom_push(key='load_success', value=True)

        except Exception as e:
            logging.error(f"Ошибка обработки файла {fname}: {e}")
            continue

    if not client_id:
        raise ValueError('Не удалось определить client_id')

    ti = context['ti']
    ti.xcom_push(key='transport_path_f', value=transport_path)
    ti.xcom_push(key='expenses_path_f', value=expenses_path)
    ti.xcom_push('t_cols', t_cols)
    ti.xcom_push('e_cols', e_cols)



def extract_from_db(**context):

    ti = context['ti']
    client_id = ti.xcom_pull(task_ids='process_client_data', key='client_id')

    db_params = get_client_db_params(client_id)

    hook = PostgresHook(
        host=db_params['host'],
        port=db_params['port'],
        database=db_params['dbname'],
        user=db_params['user'],
        password=quote_plus(db_params['password']) # db_params['password']
    )

    conn_str = f"postgresql://{db_params['user']}:{quote_plus(db_params['password'])}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    eng = create_engine(conn_str)
    if eng:
        print('eng: ', eng)
    # eng = hook.get_sqlalchemy_engine()
    insp = inspect(eng)
    conn = eng.connect()
    if conn:
        print('conn: ', conn)
    src_tp_cols = [c['name'] for c in insp.get_columns('transport_park')]
    sel_tp_src = []
    for src in src_tp_cols:
        if src in cols_map:
            sel_tp_src.append(src)
        elif src in EXPENSES_COLS:
            sel_tp_src.append(src)
    df_tp = pd.read_sql(f"SELECT {','.join(sel_tp_src)} FROM transport_park", conn)
    df_tp = df_tp.rename(columns=cols_map)
    print(df_tp.head())

    with eng.connect() as conn:

        src_de_cols = [c['name'] for c in insp.get_columns('transport_daily_expenses')]
        sel_de_src = []
        for src in src_de_cols:
            if src in cols_map:
                sel_de_src.append(src)
            elif src in EXPENSES_COLS:
                sel_de_src.append(src)

        df_de = pd.DataFrame()
        chunk_iter = pd.read_sql(f"SELECT {','.join(sel_de_src)} FROM transport_daily_expenses", conn, chunksize=50000)
        for i, chunk in enumerate(chunk_iter):
            chunk = chunk.rename(columns=cols_map)
            df_de = pd.concat([df_de, chunk], ignore_index=True)
    # df_de = df_de.rename(columns=cols_map)
    print(df_de.head())
    tp_path, t_cols = normalize_and_save_temp_csv(df_tp, TRANSPORT_COLS, client_id, return_temp_path=True)
    de_path, e_cols = normalize_and_save_temp_csv(df_de, EXPENSES_COLS, client_id, return_temp_path=True)

    ti = context['ti']
    ti.xcom_push('transport_path_db', tp_path)
    ti.xcom_push('expenses_path_db', de_path)
    ti.xcom_push('t_cols', t_cols)
    ti.xcom_push('e_cols', e_cols)


def update_schema(**context):
    ti = context['ti']
    engine = get_engine()
    inspector = inspect(engine)

    # Получаем списки колонок из XCom (БД и файл)
    tcols_db = ti.xcom_pull(task_ids='extract_from_db', key='t_cols') or []
    ecols_db = ti.xcom_pull(task_ids='extract_from_db', key='e_cols') or []
    tcols_f = ti.xcom_pull(task_ids='load_local_file',   key='t_cols') or []
    ecols_f = ti.xcom_pull(task_ids='load_local_file',   key='e_cols') or []

    client_id = get_client_id_and_schema(engine, **context)

    new_transport = list(set(tcols_db) | set(tcols_f))
    new_expense = list(set(ecols_db) | set(ecols_f))

    if not inspector.has_table('transport'):
        init_client_table_dynamic('transport', new_transport, engine)

    else:
        existing = {c['name'] for c in inspector.get_columns('transport')}
        to_add = [c for c in new_transport if c not in existing and c in TRANSPORT_COLS]
        for col in to_add:
            sql_type = TYPE_MAP[col]
            type_str = str(sql_type.compile(dialect=engine.dialect))

            engine.execute(text(
                f'ALTER TABLE transport '
                f'ADD COLUMN IF NOT EXISTS "{col}" {type_str}'
            ))
            logging.info(f"[update_schema] Added column {col} to transport")

    if not inspector.has_table('expenses'):
        init_client_table_dynamic('expenses', new_expense, engine)
    else:
        existing = {c['name'] for c in inspector.get_columns('expenses')}
        to_add = [c for c in new_expense if c not in existing and c in EXPENSES_COLS]
        for col in to_add:
            col_type = TYPE_MAP[col].compile(dialect=engine.dialect)
            engine.execute(text(
                f'ALTER TABLE expenses '
                f'ADD COLUMN IF NOT EXISTS "{col}" {col_type}'
            ))
            logging.info(f"[update_schema] Added column {col} to expenses")


def bulk_upsert(table_name, temp_csv, pk_cols, client_id, pg_conn):
    cursor = pg_conn.cursor()

    df0 = pd.read_csv(temp_csv, nrows=0)
    mapped_cols = list(df0.columns)

    cols_ddl = ', '.join(f'{c} TEXT' for c in mapped_cols)

    cursor.execute(
        f"CREATE TEMP TABLE staging_{table_name} ({cols_ddl}) ON COMMIT DROP;"
    )

    cols_list = ','.join(f'{c}' for c in mapped_cols)

    with open(temp_csv, 'r', encoding='utf-8') as f:
        cursor.copy_expert(
            f"COPY staging_{table_name} ({cols_list}) FROM STDIN WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')",
            f
        )

    select_list = []
    for o in mapped_cols:
        # если поле 'date' — приводим к типу date
        if o in ('date', 'last_repair_date', 'start_date'):
            cast = '::date'
        elif o == 'release_year':
            cast = '::integer'
        elif o in ('trailer', 'has_casco', 'is_rental'):
            cast = '::boolean'
        elif o in ('gov_number', 'driver', 'vehicle_type', 'industry', 'client_id'):
            cast = '::text'
        else:
            cast = '::numeric(10,2)'
        select_list.append(f'"{o}"{cast} AS {o}')

    select_sql = ', '.join(select_list)

    on_cond = ' AND '.join(f"t.{c} = s.{c}" for c in pk_cols)
    set_list = ', '.join(f"{u} = s.{u}" for u in [c for c in mapped_cols if c not in pk_cols])
    all_cols = ', '.join(f'"{c}"' for c in mapped_cols)
    all_src = ', '.join(f's."{c}"' for c in mapped_cols)

    cursor.execute(f"""
        MERGE INTO {table_name} AS t
        USING (
            SELECT {select_sql}
            FROM staging_{table_name}
            WHERE client_id = '{client_id}'
        ) AS s ON ({on_cond})
        WHEN MATCHED THEN UPDATE SET {set_list}
        WHEN NOT MATCHED THEN INSERT ({all_cols}) VALUES ({all_src});
    """)
    pg_conn.commit()

    print(f"staging {table_name} columns:", mapped_cols)
    print(pd.read_csv(temp_csv).head())


def upsert_data(**context):

    engine = get_engine()
    pg_conn = get_raw_conn()

    ti = context['ti']

    transport_db = ti.xcom_pull(key='transport_path_db', task_ids='extract_from_db') or []
    expenses_db = ti.xcom_pull(key='expenses_path_db', task_ids='extract_from_db') or []
    tcols_db = ti.xcom_pull(key='t_cols', task_ids='extract_from_db') or []
    ecols_db = ti.xcom_pull(key='e_cols', task_ids='extract_from_db') or []

    transport_f = ti.xcom_pull(key='transport_path_f', task_ids='load_local_file') or []
    expenses_f = ti.xcom_pull(key='expenses_path_f', task_ids='load_local_file') or []
    tcols_f = ti.xcom_pull(key='t_cols', task_ids='load_local_file') or []
    ecols_f = ti.xcom_pull(key='e_cols', task_ids='load_local_file') or []

    if not any([transport_db, expenses_db, transport_f, expenses_f]):
        raise ValueError("Не удалось получить данные ни из БД, ни из файлов")

    client_id = get_client_id_and_schema(engine, **context)

    def _maybe_upsert(table_name, csv_path, pk):
        if not csv_path:
            return
        bulk_upsert(table_name, csv_path, pk_cols=pk, client_id=client_id, pg_conn=pg_conn)

    _maybe_upsert('transport', transport_db, pk=['client_id', 'gov_number'])
    _maybe_upsert('expenses', expenses_db, pk=['client_id', 'date', 'gov_number'])

    _maybe_upsert('transport', transport_f, pk=['client_id', 'gov_number'])
    _maybe_upsert('expenses', expenses_f, pk=['client_id', 'date', 'gov_number'])

    for p in [transport_db, expenses_db, transport_f, expenses_f]:
        if p and os.path.exists(p):
            os.remove(p)


def clean_input(**context):
    ti = context["ti"]
    load_ok = ti.xcom_pull(task_ids='load_local_file', key='load_success')
    client_id = ti.xcom_pull(task_ids='process_client_data', key='client_id')
    if not load_ok:
        print("clean_input: пропускаем удаление — load_local_file не отработал успешно")
        return
    for fn in os.listdir(DATA_DIR):
        if fn.startswith(client_id):
            os.remove(os.path.join(DATA_DIR, fn))
    print("clean_input: все файлы удалены после успешной загрузки")


default_args = {
    "owner"          : "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "start_date"     : datetime(2025, 1, 1),
    "retries"        : 1,
    "retry_delay"    : timedelta(seconds=25),
}

with DAG(
    dag_id="etl_crm_erp_pipeline_sqlalchemy_COPY_no_limits", # etl_crm_erp_pipeline_3_tables
    description="ETL per-client, для больших таблиц",
    schedule_interval=None,
    catchup=False,
    max_active_runs=2,
    default_args=default_args,
    params={"client_id": Param(default="", type="string")},
) as dag:

    process_task = PythonOperator(
        task_id="process_client_data",
        python_callable=process_client,
        provide_context=True,
    )

    extract_db = PythonOperator(
        task_id="extract_from_db",
        python_callable=extract_from_db,
        provide_context=True,
    )

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        fs_conn_id="fs_default",
        filepath=DATA_DIR,
        poke_interval=20,
        timeout=300,
    )

    load_file = PythonOperator(
        task_id="load_local_file",
        python_callable=load_local_file,
        provide_context=True,
    )

    update_schema_task = PythonOperator(
        task_id="update_schema",
        python_callable=update_schema,
        provide_context=True,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    upsert = PythonOperator(
        task_id="upsert_data",
        python_callable=upsert_data,
        provide_context=True,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    cleanup = PythonOperator(
        task_id="clean_input",
        python_callable=clean_input,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED,
    )


process_task >> [extract_db, wait_for_file]
wait_for_file >> load_file >> update_schema_task
extract_db >> update_schema_task
update_schema_task >> upsert
load_file >> cleanup
