import investpy
import pendulum


class GetShares:
    def __init__(self, country: str = 'brazil'):

        self.country = country
        self.now = pendulum.now('UTC').format('DD/MM/YYYY')

    def get_list_shares(self):
        shares_list = investpy.get_stocks_list(country=self.country)
        print(shares_list)
        return shares_list

    def get_historical_data(self, share: str, from_date: str):
        df = investpy.get_stock_historical_data(stock=share, country=self.country, from_date=from_date,
                                                to_date=self.now, as_json=False, order='ascending')
        return df

    def get_recent_historical(self, share: str):
        df = investpy.get_stock_recent_data(stock=share, country=self.country, as_json=False, order='ascending')
        return df

    def company_profile(self, share):
        profile = investpy.get_stock_company_profile(stock=share, country=self.country, language='english')
        if profile.get('desc'):
            return profile['desc']
        else:
            return ""