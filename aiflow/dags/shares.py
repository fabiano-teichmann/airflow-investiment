import logging
import os

import investpy
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from app.domain.get_shares import GetShares

share_brazil = GetShares("brazil")

logger = logging.getLogger(__name__)


def get_stocks(**kwargs):
    country = kwargs["params"]["country"]
    shares_list = investpy.get_stocks_list(country=country)
    print(shares_list)
    kwargs["ti"].xcom_push(key=f"stocks_{country}", value=shares_list)


def get_historical_stocks_brazil(**kwargs):
    # stocks_brazil
    ti = kwargs["ti"]
    stocks = ti.xcom_pull(task_ids="get_stock_in_B3", key="stocks_brazil")
    logger.info("get share with  key %s", stocks)
    from_date = Variable.get("start_date")
    from_date = pendulum.parse(from_date, exact=True).format("DD/MM/YYYY")
    to_date = pendulum.now("UTC").format("DD/MM/YYYY")
    limit_stock = int(Variable.get("limit_stock"))
    stock_get_with_success = ti.xcom_pull(task_ids="stock_get_with_success")
    if not stock_get_with_success:
        stock_get_with_success = []

    for stock in stocks[len(stock_get_with_success) + 1 : limit_stock]:
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
            ti.xcom_push(key="error_get_stock", value=stocks[stocks.index(stock):])
            # ti.xcom_push(key="stock_get_with_success", value=stock_get_with_success)
            if stock_get_with_success:
                return
             # raise error
    ti.xcom_push(key="stock_get_with_success", value=stock_get_with_success)


def get_historical_data(country, stock: str, to_date: str, from_date: str, **kwargs):
    logger.info("Get stock historical %s start : %s to : %s", stock, from_date, to_date)
    try:
        parameters = dict(
            stock=stock,
            country=country,
            from_date=from_date,
            to_date=to_date,
            as_json=False,
            order="ascending",
        )
        logger.info("Get share with parameters %s", parameters)
        df = investpy.get_stock_historical_data(**parameters)
    except IndexError as error:
        logger.critical("Not found stock %s", stock, exc_info=error)
        return
    except ConnectionError as error:
        logger.error(error)
        raise error

    path_csv = f"/opt/airflow/data/bronze/{stock}.csv"
    df.to_csv(path_csv, index=False)
    logger.info("Save in: %s", path_csv)
    kwargs["ti"].xcom_push(key=f"stock_{stock}", value=path_csv)


def build_directories():
    logger.info("List directories %s", os.listdir("/opt/airflow"))
    dir_bronze = "/opt/airflow/data/bronze"
    dir_silver = "/opt/airflow/data/silver"
    dir_gold = "/opt/airflow/data/gold"
    if not os.path.exists(dir_bronze):
        logger.info("create dir %s", dir_bronze)
        os.mkdir(dir_bronze)
    if not os.path.exists(dir_silver):
        logger.info("create dir %s", dir_silver)
        os.mkdir(dir_silver)
    if not os.path.exists(dir_gold):
        logger.info("create dir %s", dir_gold)
    logger.info("finished build directories")


dag = DAG(
    dag_id="get_shares",
    start_date=pendulum.datetime(2021, 10, 1),
    catchup=False,
    schedule_interval="@daily",
    tags=["get_shares", "get_stock"],
)

build_directories = PythonOperator(
    task_id="build_directories", python_callable=build_directories
)
get_stock_in_B3 = PythonOperator(
    task_id="get_stock_in_B3",
    python_callable=get_stocks,
    provide_context=True,
    params={"country": "brazil"},
    dag=dag,
)
historical_stocks_brazil = PythonOperator(
    task_id="historical_stocks_brazil",
    python_callable=get_historical_stocks_brazil,
    provide_context=True,
    dag=dag,
)

finish_job = BashOperator(task_id="finish_dag", bash_command=" echo finish")
build_directories >> get_stock_in_B3 >> historical_stocks_brazil >> finish_job
