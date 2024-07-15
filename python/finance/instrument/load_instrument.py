import requests_cache
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from finance.core import VaultDB
# from vaultdb import VaultDB

# Set up the logger
import logging

logger = logging.getLogger()

yesterday = datetime.today() - timedelta(days=1)


class InstrumentFinancial(VaultDB):

    symbol: str

    def __init__(self, database_name: str, symbol: str, **kwargs) -> None:
        super().__init__(database_name, **kwargs)
        self.symbol = symbol

    def extract(self) -> yf.Ticker:
        session = requests_cache.CachedSession(
            f"{self.database_name}.{self.symbol}.instruments.yfinance.cache",
        )
        session.headers["User-agent"] = "my-program/1.0"
        ticker = yf.Ticker(self.symbol.upper(), session=session)
        logger.debug(ticker.isin)
        return ticker

    def transform_and_insert(
        self,
        df: pd.DataFrame,
        table_name: str,
        symbol: str,
        primary_keys: list[str],
        partition_by: str = None,
        reset_index=True,
    ):
        if not df.empty:
            if isinstance(df, pd.Series):
                df = df.to_frame()
            if symbol:
                df["symbol"] = symbol.upper()
            if reset_index:
                df.reset_index(inplace=True)
                if df.columns[0] == "index":
                    df = df.rename(columns={"index": "Date"})
            self.sync_load_and_merge(df, table_name, primary_keys, partition_by)

    def try_transform_and_insert(
        self, df, table_name: str, symbol: str, primary_keys: list[str], partition_by: str = None, reset_index=True
    ):
        """ """
        try:
            if isinstance(df, yf.Ticker):
                df = getattr(df, table_name)
            self.transform_and_insert(df, table_name, symbol, primary_keys, partition_by, reset_index=reset_index)
        except Exception as ex:
            logger.error(ex)

    def load(self):
        ticker = self.extract()
        # show holders
        try:
            major_holders = ticker.major_holders
            major_holders["Date Reported"] = datetime.today()
            self.transform_and_insert(major_holders, "major_holders", self.symbol, [], "symbol", reset_index=False)
        except Exception as ex:
            logger.error(ex)

        self.try_transform_and_insert(ticker, "institutional_holders", self.symbol, [], "symbol", reset_index=False)
        self.try_transform_and_insert(ticker, "mutualfund_holders", self.symbol, [], "symbol", reset_index=False)
        self.try_transform_and_insert(ticker, "insider_transactions", self.symbol, [], "symbol", reset_index=False)

        try:
            insider_purchases = ticker.insider_purchases
            insider_purchases["Date Reported"] = datetime.today()
            self.try_transform_and_insert(
                insider_purchases, "insider_purchases", self.symbol, [], "symbol", reset_index=False
            )
        except Exception as ex:
            logger.error(ex)

        try:
            insider_roster_holders = ticker.insider_roster_holders
            insider_roster_holders["Date Reported"] = datetime.today()
            # Partion this table no need of primary key
            self.try_transform_and_insert(
                insider_roster_holders, "insider_roster_holders", self.symbol, [], "symbol", reset_index=False
            )
        except Exception as ex:
            logger.error(ex)

        # show actions (dividends, splits, capital gains)
        self.try_transform_and_insert(ticker, "actions", self.symbol, ["symbol", "date"])
        self.try_transform_and_insert(ticker, "dividends", self.symbol, ["symbol", "date"])
        self.try_transform_and_insert(
            ticker, "capital_gains", self.symbol, ["symbol", "date"]
        )  # only for mutual funds & etfs
        self.try_transform_and_insert(ticker, "splits", self.symbol, ["symbol", "date"])

        # show financials:
        # - income statement
        # see `Ticker.get_income_stmt()` for more options
        try:
            self.try_transform_and_insert(ticker.income_stmt.transpose(), "income_statement", self.symbol, [], "symbol")
        except Exception as ex:
            logger.error(ex)

        try:
            self.try_transform_and_insert(
                ticker.quarterly_income_stmt.transpose(), "quarterly_income_statement", self.symbol, [], "symbol"
            )
        except Exception as ex:
            logger.error(ex)
        # - balance sheet
        try:
            self.try_transform_and_insert(ticker.balance_sheet.transpose(), "balance_sheet", self.symbol, [], "symbol")
        except Exception as ex:
            logger.error(ex)
        try:
            self.try_transform_and_insert(
                ticker.quarterly_balance_sheet.transpose(), "quarterly_balance_sheet", self.symbol, [], "symbol"
            )
        except Exception as ex:
            logger.error(ex)
        # - cash flow statement
        try:
            self.try_transform_and_insert(ticker.cashflow.transpose(), "cashflow", self.symbol, [], "symbol")
        except Exception as ex:
            logger.error(ex)
        try:
            self.try_transform_and_insert(
                ticker.quarterly_cashflow.transpose(), "quarterly_cashflow", self.symbol, [], "symbol"
            )
        except Exception as ex:
            logger.error(ex)

        # show recommendations
        try:
            recommendations = ticker.recommendations
            recommendations["Date Reported"] = datetime.today()
            self.try_transform_and_insert(
                recommendations, "recommendations", self.symbol, [], "symbol", reset_index=False
            )
        except Exception as ex:
            logger.error(ex)

        try:
            recommendations_summary = ticker.recommendations_summary
            recommendations_summary["Date Reported"] = datetime.today()
            self.try_transform_and_insert(
                recommendations_summary, "recommendations_summary", self.symbol, [], "symbol", reset_index=False
            )
        except Exception as ex:
            logger.error(ex)

        self.try_transform_and_insert(ticker, "upgrades_downgrades", self.symbol, [], "symbol", reset_index=False)

        # show share count
        try:
            shares_full = ticker.get_shares_full(start="1901-01-01", end=datetime.today().strftime("%Y-%m-%d"))
            shares_full = shares_full.to_frame()
            shares_full.reset_index(inplace=True)
            shares_full = shares_full.rename(columns={0: "shares", "index": "Date"})
            shares_full = shares_full.drop_duplicates(subset=["Date"])
            self.try_transform_and_insert(
                shares_full, "shares_outstanding", self.symbol, ["symbol", "date"], "symbol", reset_index=False
            )
        except Exception as ex:
            logger.error(ex)

        # Show future and historic earnings dates, returns at most next 4 quarters and last 8 quarters by default.
        # Note: If more are needed use msft.get_earnings_dates(limit=XX) with increased limit argument.
        try:
            earnings_dates = ticker.earnings_dates
            earnings_dates = earnings_dates.rename(columns={"Surprise(%)": "Surprise_Percent"})
            self.try_transform_and_insert(earnings_dates, "earnings_dates", self.symbol, ["symbol", "earnings_date"])
        except Exception as ex:
            logger.error(ex)

    def load_news(self):
        """ """
        ticker = self.extract()
        # show news
        news = pd.DataFrame(ticker.news)
        news["Date Reported"] = datetime.today()
        self.try_transform_and_insert(news, "news_links", self.symbol, [], "symbol", reset_index=False)

    def load_options_and_quotes(self, period: str):
        """ """
        try:
            ticker = self.extract()
            # get option chain for specific expiration
            for expiry_date in ticker.options:
                opt = ticker.option_chain(expiry_date)
                self.load_option_chain(opt.calls, expiry_date, "call", period)
                self.load_option_chain(opt.puts, expiry_date, "put", period)
        except Exception as ex:
            logger.error(ex)

    def load_option_chain(
        self, options: pd.DataFrame, expiry_date: str, option_type: str, option_quote_period: str = "1d"
    ):
        """ """
        try:
            contracts = options[["contractSymbol", "strike", "currency"]]
            contracts = contracts.rename(columns={"contractSymbol": "symbol"})
            contracts["type"] = option_type
            contracts["underlying"] = self.symbol
            contracts["expiry_date"] = datetime.strptime(str(expiry_date), "%Y-%m-%d")
            self.sync_load_and_merge(contracts, "option_chain", ["symbol"])
            from finance.quotes import load_historical_quotes

            contract_price = options[["contractSymbol", "openInterest", "impliedVolatility"]]
            for row in contract_price.itertuples(index=False):
                load_historical_quotes.load(
                    self.connection,
                    self.database_name,
                    symbol=row.contractSymbol,
                    period=option_quote_period,
                    openinterest=row.openInterest,
                    impliedvolatility=row.impliedVolatility,
                )
        except Exception as ex:
            logger.error(ex)


if __name__ == "__main__":
    database_name = "test"
    instr = InstrumentFinancial(database_name, "msft")
    instr.login("vaultdb", "test123")
    # instr.connection.execute(f"DROP TABLE insider_purchases;")
    # instr.load()
    # instr.load_news()
    instr.load_options_and_quotes(period="max")
