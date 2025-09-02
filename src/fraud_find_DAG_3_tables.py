from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import tempfile
from airflow.sensors.filesystem import FileSensor
from sqlalchemy import (create_engine, MetaData, Table, Column, String, Integer, Date, Numeric, Boolean, insert, Text)
from sqlalchemy.schema import CreateSchema
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import MetaData, Table, Column, String
import io
import csv
from sqlalchemy import inspect, text
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
import numpy as np
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import os
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
import logging
import pandas as pd
from prophet import Prophet
import json
from datetime import datetime
import numpy as np
import psutil
import gc
from fraud_finder import fraud_finder
from PlotDataMaker import PlotDataMaker


logger = logging.getLogger(__name__)

OUR_CONN_ID = 'postgres_connect'
RESULT_TABLE_NAME = 'fraud_json' # detection_results


def get_all_clients(**kwargs):

    hook = PostgresHook(postgres_conn_id=OUR_CONN_ID)
    query = 'SELECT companyid, "yearBudget", "companyType" FROM web.users'
    clients_df = hook.get_pandas_df(query)

    if clients_df.empty:
        raise AirflowSkipException("No clients found in the database")

    clients = clients_df.to_dict('records')
    kwargs['ti'].xcom_push(key='clients', value=clients)
    return clients


def check_client_tables(client_id):

    hook = PostgresHook(postgres_conn_id=OUR_CONN_ID)

    engine = hook.get_sqlalchemy_engine()
    inspector = inspect(engine)

    try:
        # if not inspector.has_table(RESULT_TABLE_NAME):
        #     raise AirflowSkipException(f"{RESULT_TABLE_NAME} doesn't exist")

        tables = inspector.get_table_names(schema='public')
        required_tables = {'expenses', 'transport'}
        if not required_tables.issubset(tables):
            missing = required_tables - set(tables)
            raise AirflowSkipException(f"Missing required tables: {missing}")

        clients_df = pd.read_sql(
            f"SELECT DISTINCT client_id FROM expenses",
            engine)
        print(clients_df)
        existing_ids = clients_df['client_id'].astype(str).tolist()
        print(existing_ids)
        if str(client_id) not in existing_ids:
            raise AirflowSkipException(f"No data for client ID: {client_id} in expenses table")

        return True

    except Exception as e:
        raise AirflowSkipException(f"Error checking tables: {str(e)}")


def create_result_table(client_id):

    hook = PostgresHook(postgres_conn_id=OUR_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    metadata = MetaData(schema='public')

    inspector = inspect(engine)
    existing_tables = inspector.get_table_names(schema='public')

    if RESULT_TABLE_NAME in existing_tables:
        logger.info(f"Table public.{RESULT_TABLE_NAME} already exists")
        return

    if RESULT_TABLE_NAME not in existing_tables:
        try:
            table = Table(
                RESULT_TABLE_NAME,
                metadata,
                Column('client_id', String(50), primary_key=True),
                Column('analysis_type', String(50), primary_key=True),  # Название анализа
                Column('result_json', Text),  # Результат в формате JSON
                Column('created_at', Date),  # Дата создания
            )
            table.create(engine)
            logger.info(f"Created table {client_id}.{RESULT_TABLE_NAME}")
        except Exception as e:
            logger.error(f"Failed to create table {client_id}.{RESULT_TABLE_NAME}: {str(e)}")
            raise


def save_results_to_db(client_id, results):

    hook = PostgresHook(postgres_conn_id=OUR_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    metadata = MetaData(schema='public')

    try:
        table = Table(RESULT_TABLE_NAME, metadata, autoload_with=engine)
        created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        records = []
        for analysis_type, result_data in results.items():
            records.append({
                'client_id': client_id,
                'analysis_type': analysis_type,
                'result_json': json.dumps(result_data, ensure_ascii=False, indent=4),
                'created_at': created_at,
            })

        with engine.begin() as conn:
            conn.execute(
                table.delete().where(table.c.client_id == client_id)
            )

            conn.execute(table.insert(), records)

        logger.info(f"Saved {len(records)} analysis results for {client_id} to {RESULT_TABLE_NAME}")

    except Exception as e:
        logger.error(f"Failed to save results for {client_id} to {RESULT_TABLE_NAME}: {str(e)}")
        raise


def process_client_data(**kwargs):

    client = kwargs['client']
    client_id = client['companyid']
    budget = client['yearBudget']
    industry = client['companyType']

    print('client_id: ', client_id)
    print('yearBudget: ', budget)
    print('companyType: ', industry)

    print(psutil.Process().memory_info().rss / 1024 ** 2, "MB")

    try:
        if not check_client_tables(client_id):
            logger.warning(f"Skipping client {client_id} - his data doesn't exist")
            return False

        create_result_table(client_id)

        hook = PostgresHook(postgres_conn_id=OUR_CONN_ID)

        expenses_df = pd.read_sql(f"SELECT * FROM (SELECT * FROM expenses WHERE client_id = '{client_id}' ORDER BY date DESC LIMIT 100000) AS last_rows ORDER BY date ASC", hook.get_sqlalchemy_engine()) # SELECT * FROM expenses WHERE client_id = '{client_id}' ORDER BY date DESC LIMIT 100000

        gc.collect()

        transport_df = hook.get_pandas_df(f"SELECT * FROM transport WHERE client_id = '{client_id}' ")

        print(psutil.Process().memory_info().rss / 1024 ** 2, "MB")

        ff = fraud_finder(
            expenses=expenses_df,
            transport=transport_df,
            budget=budget,
            industry=industry
        )

        pdm = PlotDataMaker(
            expenses=expenses_df,
            transport=transport_df,
            budget=budget,
            industry=industry
        )

        print(psutil.Process().memory_info().rss / 1024 ** 2, "MB")

        results = {}

        if industry == 'Строительство':
            results = {
                'high_salary_data': ff.find_high_salary(),
                'expenses_data': ff.budget_deviations(),
                'expenses_with_market_avg': ff.deviation_from_market_indicators(),
                'fuel_effectiveness': ff.fuel_effectivness(),
                'salary_effectiveness': ff.salary_effectivness(),
                'month_drivers_fine': ff.month_drivers_fine(),
                'year_drivers_fine': ff.year_drivers_fine(),
                'analyze_and_plot': ff.analyze_and_plot(),
                'recommendations': ff.generate_recommendations(),
                'salary_data': pdm.salary_data(),
                'fuel_km_per_month': pdm.fuel_km_per_month(),
                'building_expenses_by_month': pdm.building_expenses_by_month(),
                # 'downtime': pdm.downtime_counter(),
                'downtime_find': pdm.downtime_finder(),
                'aggregated_expenses_by_month_year': pdm.aggregate_expenses_by_month_year(),
                'vehicle_efficiency_per_year': pdm.vehicle_efficiency_per_year(),
                'amortization_by_month_year': pdm.amortization_by_month_year(),
                'repair_expenses_by_month_year': pdm.repair_expenses_by_month_year(),
                'top_drivers_fines_by_month': pdm.top_drivers_fines_by_month(),
                'top_drivers_salary_efficiency': pdm.top_drivers_salary_efficiency_by_month(),
                'seasonal_fines': pdm.seasonal_fines(),
            }
        elif industry == 'Логистика':
            results = {
                'high_salary_data': ff.find_high_salary(),
                'expenses_data': ff.budget_deviations(),
                'expenses_with_market_avg': ff.deviation_from_market_indicators(),
                'fuel_effectiveness': ff.fuel_effectivness(),
                'salary_effectiveness': ff.salary_effectivness(),
                'month_drivers_fine': ff.month_drivers_fine(),
                'year_drivers_fine': ff.year_drivers_fine(),
                'analyze_and_plot': ff.analyze_and_plot(),
                'recommendations': ff.generate_recommendations(),
                'salary_data': pdm.salary_data(),
                'fuel_km_per_month': pdm.fuel_km_per_month(),
                'logistic_expenses_by_month': pdm.logistic_expenses_by_month(),
                # 'downtime': pdm.downtime_counter(),
                'downtime_find': pdm.downtime_finder(),
                'aggregated_expenses_by_month_year': pdm.aggregate_expenses_by_month_year(),
                'vehicle_efficiency_per_year': pdm.vehicle_efficiency_per_year(),
                'amortization_by_month_year': pdm.amortization_by_month_year(),
                'repair_expenses_by_month_year': pdm.repair_expenses_by_month_year(),
                'top_drivers_fines_by_month': pdm.top_drivers_fines_by_month(),
                'top_drivers_salary_efficiency': pdm.top_drivers_salary_efficiency_by_month(),
                'seasonal_fines': pdm.seasonal_fines(),
            }
        elif industry == 'Фармацевтика':
            results = {
                'high_salary_data': ff.find_high_salary(),
                'expenses_data': ff.budget_deviations(),
                'expenses_with_market_avg': ff.deviation_from_market_indicators(),
                'fuel_effectiveness': ff.fuel_effectivness(),
                'salary_effectiveness': ff.salary_effectivness(),
                'month_drivers_fine': ff.month_drivers_fine(),
                'year_drivers_fine': ff.year_drivers_fine(),
                'analyze_and_plot': ff.analyze_and_plot(),
                'recommendations': ff.generate_recommendations(),
                'salary_data': pdm.salary_data(),
                'fuel_km_per_month': pdm.fuel_km_per_month(),
                'pharma_expenses_by_month': pdm.pharma_expenses_by_month(),
                # 'downtime': pdm.downtime_counter(),
                'downtime_find': pdm.downtime_finder(),
                'aggregated_expenses_by_month_year': pdm.aggregate_expenses_by_month_year(),
                'vehicle_efficiency_per_year': pdm.vehicle_efficiency_per_year(),
                'amortization_by_month_year': pdm.amortization_by_month_year(),
                'repair_expenses_by_month_year': pdm.repair_expenses_by_month_year(),
                'top_drivers_fines_by_month': pdm.top_drivers_fines_by_month(),
                'top_drivers_salary_efficiency': pdm.top_drivers_salary_efficiency_by_month(),
                'seasonal_fines': pdm.seasonal_fines(),
            }
        elif industry == 'Ритейл':
            results = {
                'high_salary_data': ff.find_high_salary(),
                'expenses_data': ff.budget_deviations(),
                'expenses_with_market_avg': ff.deviation_from_market_indicators(),
                'fuel_effectiveness': ff.fuel_effectivness(),
                'salary_effectiveness': ff.salary_effectivness(),
                'month_drivers_fine': ff.month_drivers_fine(),
                'year_drivers_fine': ff.year_drivers_fine(),
                'analyze_and_plot': ff.analyze_and_plot(),
                'recommendations': ff.generate_recommendations(),
                'salary_data': pdm.salary_data(),
                'fuel_km_per_month': pdm.fuel_km_per_month(),
                # 'downtime': pdm.downtime_counter(),
                'downtime_find': pdm.downtime_finder(),
                'aggregated_expenses_by_month_year': pdm.aggregate_expenses_by_month_year(),
                'vehicle_efficiency_per_year': pdm.vehicle_efficiency_per_year(),
                'amortization_by_month_year': pdm.amortization_by_month_year(),
                'repair_expenses_by_month_year': pdm.repair_expenses_by_month_year(),
                'top_drivers_fines_by_month': pdm.top_drivers_fines_by_month(),
                'top_drivers_salary_efficiency': pdm.top_drivers_salary_efficiency_by_month(),
                'seasonal_fines': pdm.seasonal_fines(),
            }
        elif industry == 'Такси':
            results = {
                'high_salary_data': ff.find_high_salary(),
                'expenses_data': ff.budget_deviations(),
                'expenses_with_market_avg': ff.deviation_from_market_indicators(),
                'fuel_effectiveness': ff.fuel_effectivness(),
                'salary_effectiveness': ff.salary_effectivness(),
                'month_drivers_fine': ff.month_drivers_fine(),
                'year_drivers_fine': ff.year_drivers_fine(),
                'analyze_and_plot': ff.analyze_and_plot(),
                'recommendations': ff.generate_recommendations(),
                'salary_data': pdm.salary_data(),
                'fuel_km_per_month': pdm.fuel_km_per_month(),
                # 'downtime': pdm.downtime_counter(),
                'downtime_find': pdm.downtime_finder(),
                'aggregated_expenses_by_month_year': pdm.aggregate_expenses_by_month_year(),
                'vehicle_efficiency_per_year': pdm.vehicle_efficiency_per_year(),
                'amortization_by_month_year': pdm.amortization_by_month_year(),
                'repair_expenses_by_month_year': pdm.repair_expenses_by_month_year(),
                'top_drivers_fines_by_month': pdm.top_drivers_fines_by_month(),
                'top_drivers_salary_efficiency': pdm.top_drivers_salary_efficiency_by_month(),
                'seasonal_fines': pdm.seasonal_fines(),
            }
        elif industry == 'Доставка':
            results = {
                'high_salary_data': ff.find_high_salary(),
                'expenses_data': ff.budget_deviations(),
                'expenses_with_market_avg': ff.deviation_from_market_indicators(),
                'fuel_effectiveness': ff.fuel_effectivness(),
                'salary_effectiveness': ff.salary_effectivness(),
                'month_drivers_fine': ff.month_drivers_fine(),
                'year_drivers_fine': ff.year_drivers_fine(),
                'analyze_and_plot': ff.analyze_and_plot(),
                'recommendations': ff.generate_recommendations(),
                'salary_data': pdm.salary_data(),
                'fuel_km_per_month': pdm.fuel_km_per_month(),
                # 'downtime': pdm.downtime_counter(),
                'downtime_find': pdm.downtime_finder(),
                'aggregated_expenses_by_month_year': pdm.aggregate_expenses_by_month_year(),
                'vehicle_efficiency_per_year': pdm.vehicle_efficiency_per_year(),
                'amortization_by_month_year': pdm.amortization_by_month_year(),
                'repair_expenses_by_month_year': pdm.repair_expenses_by_month_year(),
                'top_drivers_fines_by_month': pdm.top_drivers_fines_by_month(),
                'top_drivers_salary_efficiency': pdm.top_drivers_salary_efficiency_by_month(),
                'seasonal_fines': pdm.seasonal_fines(),
            }
        elif industry == 'Каршеринг':
            results = {
                'high_salary_data': ff.find_high_salary(),
                'expenses_data': ff.budget_deviations(),
                'expenses_with_market_avg': ff.deviation_from_market_indicators(),
                'fuel_effectiveness': ff.fuel_effectivness(),
                # 'salary_effectiveness': ff.salary_effectivness(),
                # 'month_drivers_fine': ff.month_drivers_fine(),
                # 'year_drivers_fine': ff.year_drivers_fine(),
                'analyze_and_plot': ff.analyze_and_plot(),
                'recommendations': ff.generate_recommendations(),
                # 'salary_data': pdm.salary_data(),
                'fuel_km_per_month': pdm.fuel_km_per_month(),
                'vehicle_usage_by_month': pdm.vehicle_usage_by_month(),
                # 'downtime': pdm.downtime_counter(),
                'downtime_find': pdm.downtime_finder(),
                'aggregated_expenses_by_month_year': pdm.aggregate_expenses_by_month_year(),
                'vehicle_efficiency_per_year': pdm.vehicle_efficiency_per_year(),
                'amortization_by_month_year': pdm.amortization_by_month_year(),
                'repair_expenses_by_month_year': pdm.repair_expenses_by_month_year(),
                # 'top_drivers_fines_by_month': pdm.top_drivers_fines_by_month(),
                # 'top_drivers_salary_efficiency': pdm.top_drivers_salary_efficiency_by_month(),
                # 'seasonal_fines': pdm.seasonal_fines(),
            }
        elif industry == 'Другое':
            results = {
                'high_salary_data': ff.find_high_salary(),
                'expenses_data': ff.budget_deviations(),
                'expenses_with_market_avg': ff.deviation_from_market_indicators(),
                'fuel_effectiveness': ff.fuel_effectivness(),
                'salary_effectiveness': ff.salary_effectivness(),
                'month_drivers_fine': ff.month_drivers_fine(),
                'year_drivers_fine': ff.year_drivers_fine(),
                'analyze_and_plot': ff.analyze_and_plot(),
                'recommendations': ff.generate_recommendations(),
                'salary_data': pdm.salary_data(),
                'fuel_km_per_month': pdm.fuel_km_per_month(),
                # 'downtime': pdm.downtime_counter(),
                'downtime_find': pdm.downtime_finder(),
                'aggregated_expenses_by_month_year': pdm.aggregate_expenses_by_month_year(),
                'vehicle_efficiency_per_year': pdm.vehicle_efficiency_per_year(),
                'amortization_by_month_year': pdm.amortization_by_month_year(),
                'repair_expenses_by_month_year': pdm.repair_expenses_by_month_year(),
                'top_drivers_fines_by_month': pdm.top_drivers_fines_by_month(),
                'top_drivers_salary_efficiency': pdm.top_drivers_salary_efficiency_by_month(),
                'seasonal_fines': pdm.seasonal_fines(),
            }

        print(psutil.Process().memory_info().rss / 1024 ** 2, "MB")

        save_results_to_db(client_id, results)

        print(psutil.Process().memory_info().rss / 1024 ** 2, "MB")

        return True

    except AirflowSkipException as e:
        logger.warning(f"Skipping client {client_id}: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Error processing client {client_id}: {str(e)}")
        return False
    finally:

        if 'expenses_df' in locals():
            del expenses_df
        if 'transport_df' in locals():
            del transport_df

        gc.collect()

def process_all_clients(**kwargs):

    ti = kwargs['ti']
    clients = ti.xcom_pull(task_ids='get_all_clients', key='clients')

    success_count = 0
    skipped_count = 0
    failed_count = 0

    for client in clients:
        try:
            print(psutil.Process().memory_info().rss / 1024 ** 2, "MB")
            result = process_client_data(client=client)
            if result:
                success_count += 1
            else:
                skipped_count += 1
        except Exception as e:
            logger.error(f"Unexpected error processing client {client['companyid']}: {str(e)}")
            failed_count += 1

    logger.info(f"Processing summary: {len(clients)} total, {success_count} successful, "
                f"{skipped_count} skipped, {failed_count} failed")

    return success_count > 0


default_args = {
    "owner"          : "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "start_date"     : datetime(2025, 1, 1),
    "retries"        : 1,
    "retry_delay"    : timedelta(seconds=25),
}

with DAG(
    dag_id="fraud_find_dag_3_tables",
    description="находит аномалии, делает рекомендации, складывает в таблицу",
    schedule_interval="*/3 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
) as dag:

    get_clients_task = PythonOperator(
        task_id="get_all_clients",
        python_callable=get_all_clients,
        provide_context=True,
    )

    process_clients_task = PythonOperator(
        task_id="process_all_clients",
        python_callable=process_all_clients,
        provide_context=True,
    )


get_clients_task >> process_clients_task
