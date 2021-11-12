import logging
import os

logger = logging.getLogger(__name__)


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
    stocks = os.path.join(dir_bronze, "stocks")
    info_stocks = os.path.join(dir_bronze, "info_stocks")
    if not os.path.exists(stocks):
        os.mkdir(stocks)
    if not os.path.exists(info_stocks):
        os.mkdir(info_stocks)
    logger.info("finished build directories")
