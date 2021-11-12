import logging
from typing import List, Optional

import investpy

from utils.save_to_file import save_json

logger = logging.getLogger(__name__)


def get_company_information(
    company: str, countries=Optional[List], n_results: int = 1
) -> str:
    """

    :param company: example WEGE3 to company WEG
    :param countries:
    :param n_results:
    :return:
    """
    if countries is None:
        countries = ["brazil"]
    parameters = dict(
        text=company, products=["stocks"], countries=countries, n_results=n_results
    )
    search_result = investpy.search_quotes(**parameters)
    information = search_result.retrieve_information()
    path_information = f"/opt/airflow/data/bronze/information/{company}.json"
    information.update(
        {
            "name": search_result.name,
            "retrieve_currency": search_result.retrieve_currency(),
            "symbol": search_result.symbol,
        }
    )
    save_json(path_information, information)
    return path_information
