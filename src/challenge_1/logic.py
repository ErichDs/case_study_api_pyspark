from handlers import HandlerUtils
import requests
import time
import datetime
import pandas as pd
import numpy as np


## Exchanges 
class Exchanges:
    def fetchExchangeListData(base_urls)->list:
        """
        Fetches Exchanges data from the CoinGecko API.

        Args:
        base_urls: dict containing URL.

        Returns:
        A list containing the parsed response object of Exchange Data.
        """
        headers = {"accept": "application/json"}
        base_url_exchange = base_urls.get("exchange") + "/list"
        response = HandlerUtils.sendApiCall(base_url_exchange, headers=headers)
        return response.json()


    def fetchExchangeDetails(base_urls: dict[str, str], exchange_id: str)->list:
        """
        Fetches an Exchange details data from the CoinGecko API.

        Args:
        base_urls: A dict cointaining URLs.
        exchange_id: An exchange identifier (e.g.: "binance").

        Returns:
        A list containing the parsed response object of Exchange Details Data.
        """

        headers = {"accept": "application/json"}
        base_url_exchange = base_urls.get("exchange") + "/" + exchange_id
        response = HandlerUtils.sendApiCall(base_url_exchange, headers=headers)

        exchangeDetails = []
        data = response.json()
        exchangeDetails.append({
            "name": data.get("name"),
            "year_established": data.get("year_established"),
            "country": data.get("country"),
            "trust_score": data.get("trust_score"),
            "trust_score_rank": data.get("trust_score_rank")
        })

        return exchangeDetails
    

    def getExchangeDetails(base_urls: dict[str, str], exchanges: list)->list:
        """
        Fetches Exchanges details and applies a pre-processing operation.

        Args:
        base_urls: A dict cointaining URLs.
        exchanges: a list of exchanges ids.

        Returns:
        A list containing the pre-processed data from API call.
        """
        exchanges_details = []
        for id in exchanges:
            exchanges_details.append(
                Exchanges.fetchExchangeDetails(base_urls=base_urls, exchange_id=id)
            )
            # simplifies the control for API call and avoid 429
            time.sleep(15)

        return [item for sublist in exchanges_details for item in sublist]


## Tickers
class Tickers:
    def retrieveCoingeckoTickersExchange(base_urls: dict[str, str], exchange_id: str)->list:
      """
      Fetches ticker data for an specific exchange from the CoinGecko API.

      Args:
        base_urls: A dict cointaining URLs.
        exchange_id: The ID (str) of the exchange (e.g., "binance", "coinbase").

      Returns:
        A list containing the ticker data.
      """
      base_url = base_urls.get("exchange_ticker").format(**{"exchange_id": exchange_id})
      headers = {"accept": "application/json"}
      params = {
          "page": 1,
          "order": "market_cap_desc"
      }

      all_tickers = []
      while True:
        try:
            response = HandlerUtils.sendApiCall(base_url, params=params, headers=headers) #response.json()

            # simplifies the wait for API call and avoid exception break
            if response.status_code == 429:
              time.sleep(15)
              continue

            data = response.json().get("tickers", {})
            if not data:  # Check if any data was returned
              break

            for ticker in data:
              all_tickers.append({
                "name": ticker.get("market", {}).get("name"),
                "base": ticker["base"],
                "target": ticker["target"],
                "market_id": ticker["market"]["identifier"]
              })

            # Increment page for the next request (until page 3, to simplify the case)
            params["page"] += 1
            if params["page"] > 3:
                break

        except requests.exceptions.RequestException as e:
          print(f"Error fetching data from CoinGecko: {e}")
          break

      return all_tickers


    def getTickersFromExchanges(base_urls: dict[str, str], exchanges: list)->list:
        """
        Fetches Tickers details and applies a pre-processing operation.

        Args:
        base_urls: A dict cointaining URLs.
        exchanges: a list of exchanges ids.

        Returns:
        A list containing the pre-processed data from API call.
        """
        tickers_details = []
        for id in exchanges:
            tickers_details.append(
                Tickers.retrieveCoingeckoTickersExchange(base_urls, id)
            )

        return [item for sublist in tickers_details for item in sublist]


## Coins
class Coins:
    def fetchCoinsListData(base_urls: dict[str, str])->list:
        """
        Fetches Coins data for an specific exchange from the CoinGecko API.

        Args:
        base_urls: A dict cointaining URLs.

        Returns:
        A list containing the parsed response of Coins Data.
        """

        headers = {"accept": "application/json"}
        base_url_coins = base_urls.get("coins") + "/list"
        response = HandlerUtils.sendApiCall(base_url_coins, headers=headers)
        return response.json()
    

    def getExchangeRates(base_urls: dict[str, str])->list:
        """
        Fetches Exchange Rates from the CoinGecko API.

        Args:
        base_urls: a dict containing the URLs to be used in API call.

        Returns:
        A list containing the parsed response of Exchange Rates Data.
        """

        headers = {"accept": "application/json"}
        base_url_rates = base_urls.get("exchange_rates")
        response = HandlerUtils.sendApiCall(base_url_rates, headers=headers)
        return response.json()


    def parseExchangeRates(exchange_rates_data: list)->list:
        """
        Parses the exchange rates data into a processed list.
    
        Args:
          exchange_rates_data: A list containing rates data.
    
        Returns:
          A list with processed data.
        """
        exchange_rates = []
        for rate_name, rate_info in exchange_rates_data.items():
            if isinstance(rate_info, dict): 
                for unit, value in rate_info.items():
                  if isinstance(value, dict):
                    exchange_rates.append({"rate_name": value.get("name"), "unit": unit, "value": value.get("value"), "type": value.get("type")})
                  else:
                    continue
    
        return exchange_rates

## Exchange Market Transactions Volume (BTC)
class MarketVolumes:
   def fetchExchangeVolumeData(base_urls: dict[str, str], exchange_id: str, days: int=30)->list:
    """
    Fetches market volume data for an specific exchange from the CoinGecko API.
    
    Args:
    base_urls: A dict cointaining URLs.
    exchange_id: An str for exchange id.
    days: int, data up to number of days ago, default 30.
    
    Returns:
    A list containing the parsed response of Coins Data.
    """

    headers = {"accept": "application/json"}
    params = {"days":days}
    base_url_volume = base_urls.get("exchange_markets").format(**{"exchange_id": exchange_id})
    response = HandlerUtils.sendApiCall(base_url_volume, headers=headers, params=params)
    
    data = response.json()

    volume_data = []
    for item in data:
        if type(item[0]) == str:
          continue
        else:
          volume_data.append({
            "exchange_id": exchange_id,
            "date": datetime.datetime.fromtimestamp(item[0]/1000).strftime("%Y-%m-%d"),
            "volume_BTC": item[1]
        })
    return volume_data
   
   def getMarketVolume(base_urls: dict[str, str], exchanges: np.ndarray)->list:
    """
    Retrieve Market volume data.
    
    Args:
    base_urls: A dict cointaining URLs.
    exchanges: a list of exchanges(market) ids.
    
    Returns:
    A list containing the pre-processed data from API call.
    """

    volume_exchange_data = []
    for exchange in exchanges:
        volume_exchange_data.append(
            MarketVolumes.fetchExchangeVolumeData(base_urls, exchange)
        )
        # simplifies the control for API call and avoid 429
        time.sleep(15)
    
    return [item for sublist in volume_exchange_data for item in sublist]

## analysis
class Analysis:
   # Exchanges Table
   def mergeExchangeData(exchanges: pd.DataFrame, exchange_details: pd.DataFrame)->pd.DataFrame:
      return exchange_details.merge(exchanges, on="name", how="left")
   
   # Shared Markets
   def getBitsoMarkets(tickers: pd.DataFrame)->np.ndarray:
      return tickers["base"].loc[tickers["market_id"]=="bitso"].unique()
   
   def withSharedMarkets(tickers: pd.DataFrame, bitso_markets: np.ndarray)->pd.DataFrame:
      return tickers.loc[(tickers["market_id"] != "bitso") & (tickers["base"].isin(bitso_markets))]
   
   def withExchangesData(shared_markets: pd.DataFrame, exchanges: pd.DataFrame)->pd.DataFrame:
      return shared_markets.merge(exchanges, on="name", how="left")

   def convertBTCToUSD(df, exchangeRatesDF):
      cols = ["market_id", "date", "volume_USD"]
      usd_rate = float(exchangeRatesDF["value"].loc[exchangeRatesDF.unit == "usd"].values[0])
      df["volume_USD"] = df["volume_BTC"].multiply(usd_rate)
      return df[cols]