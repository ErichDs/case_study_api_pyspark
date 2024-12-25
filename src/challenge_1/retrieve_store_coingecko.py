from logic import Exchanges
from logic import Coins
from logic import Tickers
from logic import MarketVolumes
from logic import Analysis
from handlers import HandlerUtils
import datetime


def main():
    # For the sake of the case simplification
    # I'll be filtering the below exchanges for details/market volumes retrieval

    exchanges_to_filter = [
        "bitso",
        "binance",
        "crypto_com",
        "kraken",
        "foxbit",
        "bitfinex",
        "bybit",
        "coinbase_international",
    ]

    parquet_file_paths = {
        "exchanges_path": "resources/challenge_1/landing/exchanges/",
        "exchanges_details_path": "resources/challenge_1/landing/exchanges_details/",
        "tickers_path": "resources/challenge_1/landing/tickers/",
        "coins_path": "resources/challenge_1/landing/coins/",
        "exchange_rates_path": "resources/challenge_1/landing/exchange_rates/",
        "exchanges_table": "resources/challenge_1/serving/tb_exchanges/",
        "shared_market_table": "resources/challenge_1/serving/tb_shared_market/",
        "exchange_volume_table": "resources/challenge_1/serving/tb_exchange_volume/",
        "market_volume_table": "resources/challenge_1/serving/tb_market_volume/",
    }

    base_urls = {
        "exchange": "https://api.coingecko.com/api/v3/exchanges",
        "coins": "https://api.coingecko.com/api/v3/coins",
        "exchange_ticker": "https://api.coingecko.com/api/v3/exchanges/{exchange_id}/tickers",
        "exchange_rates": "https://api.coingecko.com/api/v3/exchange_rates",
        "exchange_markets": "https://api.coingecko.com/api/v3/exchanges/{exchange_id}/volume_chart",
    }

    print(
        "start processing exchanges: "
        + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z(%z)")
    )
    # exchanges
    exchanges_list = Exchanges.fetchExchangeListData(base_urls)
    exchangesDF = (
        HandlerUtils.createDF(exchanges_list)
        .pipe(HandlerUtils.withRenamedColumns, {"id": "exchange_id"})
        .pipe(HandlerUtils.withFilteredExchanges, exchanges_to_filter)
    ).pipe(HandlerUtils.convertTypes, {"exchange_id": str, "name": str})

    print(
        "start processing exchanges details: "
        + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z(%z)")
    )
    # exchanges details
    exchanges_details = Exchanges.getExchangeDetails(base_urls, exchanges_to_filter)
    exchangeDetailsDF = (
        HandlerUtils.createDF(exchanges_details)
        .pipe(HandlerUtils.withFillNA, "0", ["trust_score", "trust_score_rank"])
        .pipe(
            HandlerUtils.convertTypes,
            {
                "name": str,
                "year_established": str,
                "country": str,
                "trust_score": int,
                "trust_score_rank": int,
            },
        )
    )

    print(
        "start processing tickers: "
        + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z(%z)")
    )
    # tickers
    tickers = Tickers.getTickersFromExchanges(base_urls, exchanges_to_filter)
    tickersDF = HandlerUtils.createDF(tickers).pipe(
        HandlerUtils.convertTypes,
        {"name": str, "base": str, "target": str, "market_id": str},
    )

    print(
        "start processing coins: "
        + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z(%z)")
    )
    # coins
    coins_list = Coins.fetchCoinsListData(base_urls)
    coinsDF = HandlerUtils.createDF(coins_list).pipe(
        HandlerUtils.convertTypes, {"id": str, "symbol": str, "name": str}
    )

    print(
        "start processing exchanges rates: "
        + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z(%z)")
    )
    # exchanges rates
    exchange_rates = Coins.getExchangeRates(base_urls)
    exchange_rates_parsed = Coins.parseExchangeRates(exchange_rates)
    exchangeRatesDF = HandlerUtils.createDF(exchange_rates_parsed).pipe(
        HandlerUtils.convertTypes,
        {"rate_name": str, "unit": str, "value": float, "type": str},
    )

    print(
        "start creating exchanges table: "
        + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z(%z)")
    )
    # create exchanges table
    exchanges_table = (
        exchangesDF.pipe(Analysis.mergeExchangeData, exchangeDetailsDF)
        .pipe(HandlerUtils.withRenamedColumns, {"name": "exchange_name"})
        .pipe(
            HandlerUtils.convertTypes,
            {
                "exchange_id": str,
                "exchange_name": str,
                "year_established": int,
                "country": str,
                "trust_score": int,
                "trust_score_rank": int,
            },
        )
    )

    print(
        "start creating shared markets table: "
        + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z(%z)")
    )
    # create shared markets table
    bitso_markets = Analysis.getBitsoMarkets(tickersDF)
    shared_markets_table = (
        tickersDF.pipe(Analysis.withSharedMarkets, bitso_markets)
        .pipe(Analysis.withExchangesData, exchangesDF)
        .pipe(
            HandlerUtils.convertTypes,
            {
                "exchange_id": str,
                "market_id": str,
                "base": str,
                "target": str,
                "name": str,
            },
        )
    )

    print(
        "start creating exchanges volume table: "
        + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z(%z)")
    )
    # create rolling 30 day volume (in BTC) for exchanges
    exchanges = exchangesDF["exchange_id"].unique()
    exchange_volumes = MarketVolumes.getMarketVolume(base_urls, exchanges)
    exchange_volume_table = HandlerUtils.createDF(exchange_volumes).pipe(
        HandlerUtils.convertTypes,
        {"exchange_id": str, "date": str, "volume_BTC": float},
    )

    print(
        "start creating markets volume table: "
        + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z(%z)")
    )
    # create rolling 30 day volume (in USD) for shared market
    markets = shared_markets_table["market_id"].unique()
    market_volumes = MarketVolumes.getMarketVolume(base_urls, markets)
    market_volume_table = (
        HandlerUtils.createDF(market_volumes)
        .pipe(HandlerUtils.withRenamedColumns, {"exchange_id": "market_id"})
        .pipe(
            HandlerUtils.convertTypes,
            {"market_id": str, "date": str, "volume_BTC": float},
        )
        .pipe(Analysis.convertBTCToUSD, exchangeRatesDF)
    )

    # write data to parquet files
    # landing
    HandlerUtils.writeParquet(
        df=exchangesDF,
        filename="exchanges.parquet",
        path=parquet_file_paths.get("exchanges_path"),
        append=False,
    )
    HandlerUtils.writeParquet(
        df=exchangeDetailsDF,
        filename="exchanges_details.parquet",
        path=parquet_file_paths.get("exchanges_details_path"),
        append=False,
    )
    HandlerUtils.writeParquet(
        df=tickersDF,
        filename="tickers.parquet",
        path=parquet_file_paths.get("tickers_path"),
        append=False,
    )
    HandlerUtils.writeParquet(
        df=coinsDF,
        filename="coins.parquet",
        path=parquet_file_paths.get("coins_path"),
        append=False,
    )
    HandlerUtils.writeParquet(
        df=exchangeRatesDF,
        filename="exhange_rates.parquet",
        path=parquet_file_paths.get("exchange_rates_path"),
        append=False,
    )

    # serving
    HandlerUtils.writeParquet(
        df=exchanges_table,
        filename="tb_exchanges.parquet",
        path=parquet_file_paths.get("exchanges_table"),
        append=False,
    )
    HandlerUtils.writeParquet(
        df=shared_markets_table,
        filename="tb_shared_market.parquet",
        path=parquet_file_paths.get("shared_market_table"),
        append=False,
    )
    HandlerUtils.writeParquet(
        df=exchange_volume_table,
        filename="tb_exchange_volume.parquet",
        path=parquet_file_paths.get("exchange_volume_table"),
        append=False,
    )
    HandlerUtils.writeParquet(
        df=market_volume_table,
        filename="tb_market_volume.parquet",
        path=parquet_file_paths.get("market_volume_table"),
        append=False,
    )


if __name__ == "__main__":
    main()
