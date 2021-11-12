import logging
import os
from enum import Enum, auto

import investpy
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from domain.company_information import get_company_information
from domain.get_historical_data import get_historical_data
from utils.build_directories import build_directories


logger = logging.getLogger(__name__)


class Tasks(Enum):
    build_directories = auto()
    get_stocks = auto()
    historical_stocks = auto()
    information_companies = auto()
    next_stock = auto()


class Xcoms(Enum):
    """
    name:  ref key
    value: ref task Xcom
    """

    stock_get_with_success = Tasks.historical_stocks.name
    error_get_stock = Tasks.historical_stocks.name
    stock_information = Tasks.information_companies.name
    list_next_stock = Tasks.next_stock.name


def get_stocks(**kwargs):
    country = kwargs["params"]["country"]
    shares_list = investpy.get_stocks_list(country=country)
    kwargs["ti"].xcom_push(key=f"stocks_{country}", value=shares_list)


def get_next_stocks(**kwargs):
    ti = kwargs["ti"]
    country = kwargs["params"]["country"]
    stocks = ti.xcom_pull(key=f"stocks_{country}", task_ids=Tasks.get_stocks.name)
    stock_with_success = os.listdir("/opt/airflow/data/bronze/stocks")
    if stock_with_success:
        stock_with_success = [stock.replace(".csv", "") for stock in stock_with_success]
    logger.info("stock with success %s", stock_with_success)
    next_stocks = [s for s in stocks if s not in stock_with_success]
    limit_stock = int(Variable.get("limit_stock"))
    ti.xcom_push(key=Xcoms.list_next_stock.name, value=next_stocks[0:limit_stock])


def get_information_companies(**kwargs):
    countries = kwargs["params"]["country"]
    ti = kwargs["ti"]
    stocks = ti.xcom_pull(
        task_ids=Tasks.next_stock.name,
        key=Xcoms.list_next_stock.name,
    )
    logger.info("Get information company: %s", stocks)
    stock_information_success = []
    error_get_information = []
    for stock in stocks:
        try:
            path_information = get_company_information(stock, [countries])
            stock_information_success.append(path_information)
        except RuntimeError:
            logger.error("Error get quotes  to stock %s", stock)
            error_get_information.append(stock)
    stock_information = {
        "error_get_information": error_get_information,
        "stock_information_success": stock_information_success,
    }
    ti.xcom_push(key=Xcoms.stock_information.name, value=stock_information)


def get_historical_stocks(**kwargs):
    # stocks_brazil
    country = kwargs["params"]["country"]
    ti = kwargs["ti"]
    stocks = ti.xcom_pull(
        task_ids=Tasks.next_stock.name, key=Xcoms.list_next_stock.name
    )
    logger.info("get share with  key %s country %s", stocks, country)
    from_date = Variable.get("start_date")
    from_date = pendulum.parse(from_date, exact=True).format("DD/MM/YYYY")
    to_date = pendulum.now("UTC").format("DD/MM/YYYY")
    stock_get_with_success = []
    for stock in stocks:
        try:
            get_historical_data(
                country="brazil",
                stock=stock,
                to_date=to_date,
                from_date=from_date,
                **kwargs,
            )
            stock_get_with_success.append(stock)
        except Exception as error:
            logger.error("Error get stock %s", stock, exc_info=error)
            ti.xcom_push(key="error_get_stock", value=stocks[stocks.index(stock)])
            ti.xcom_push(key="stock_get_with_success", value=stock_get_with_success)
            raise error
    ti.xcom_push(key=Xcoms.stock_get_with_success.name, value=stock_get_with_success)


dag = DAG(
    dag_id="get_stock_brazil",
    start_date=pendulum.datetime(2021, 10, 1),
    catchup=False,
    schedule_interval="@daily",
    tags=["get_stock_brazil", "brazil", "stock"],
)

build_directories = PythonOperator(
    task_id=Tasks.build_directories.name, python_callable=build_directories
)
get_stock = PythonOperator(
    task_id=Tasks.get_stocks.name,
    python_callable=get_stocks,
    provide_context=True,
    params={"country": "brazil"},
    dag=dag,
)
next_stock = PythonOperator(
    task_id=Tasks.next_stock.name,
    python_callable=get_next_stocks,
    provide_context=True,
    params={"country": "brazil"},
    dag=dag,
)
historical_stocks = PythonOperator(
    task_id=Tasks.historical_stocks.name,
    python_callable=get_historical_stocks,
    provide_context=True,
    params={"country": "brazil"},
    dag=dag,
)

information_companies = PythonOperator(
    task_id=Tasks.information_companies.name,
    python_callable=get_information_companies,
    provide_context=True,
    params={"country": "brazil"},
    dag=dag,
)

finish_job = BashOperator(task_id="finish_dag", bash_command=" echo finish")
(
    build_directories
    >> get_stock
    >> next_stock
    >> historical_stocks
    >> information_companies
    >> finish_job
)
