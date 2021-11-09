import logging

import investpy

logger = logging.getLogger(__name__)


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
