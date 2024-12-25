import requests
import pandas as pd
import os.path
import datetime

class HandlerUtils:
    def sendApiCall(url, params={}, headers={}):
        """
        trigger an API Call to CoinGecko API.

        Args:
          base_url: The base URL of the CoinGecko API (e.g., "https://api.coingecko.com/api/v3").
          path: The API endpoint path (e.g., "coins/bitcoin").
          params: A dictionary of query parameters.
          headers: A dictionary of headers for the API request.

        Returns:
          The response object from the CoinGecko API.

        Raises:
          requests.exceptions.RequestException: If an error occurs during the API request.
        """
        response = requests.get(url, params=params,headers=headers)
        # response.raise_for_status()  # Raise an exception for bad status codes
        return response


    def createDF(data: list) -> pd.DataFrame:
        """
        Creates a pandas DataFrame with Data received as list.

        Args:
            data: a list containing data as input to the DataFrame.

        Returns:
            A pandas DataFrame
        """
        return pd.DataFrame(data)

    def convertTypes(df: pd.DataFrame, types_dict: dict)->pd.DataFrame:
        """
        Update a DataFrame types.

        Args:
            df: a DataFrame to be updated
            types_dict: a Dict containing the colums and types to be updated

        Returns:
            A pandas DataFrame
        """
        return df.astype(types_dict)

    def withRenamedColumns(df: pd.DataFrame, cols: dict[str, str]) -> pd.DataFrame:
        """
        Rename DataFrame columns.

        Args:
            df: a Pandas DataFrame.
            cols: a dict of [str, str] containing the current and new name of columns.

        Returns:
            A pandas DataFrame
        """
        return df.rename(columns=cols)


    def withFilteredExchanges(exchangesDF: pd.DataFrame, exchangesFilter: list[str]) -> pd.DataFrame:
        """
        This function returns selected Exchanges based on filter parameter.

        Args:
            exchangesDF: a pandas DataFrame.
            exchangesFilter: a list of [str].

        Returns: 
        """
        return exchangesDF[exchangesDF.exchange_id.isin(exchangesFilter)].reset_index(drop=True)
    

    def withFillNA(df: pd.DataFrame, filler: any, columns: list) -> pd.DataFrame:
        """
        Fills NA values in specified columns of a DataFrame.

        Args:
          df: The pandas DataFrame.
          filler: The value to fill NA with.
          columns: A list of column names to fill NA values for.

        Returns:
          A new DataFrame with NA values filled in the specified columns.
        """
        df_filled = df.copy()  # Create a copy of the DataFrame to avoid modifying the original
        df_filled[columns] = df_filled[columns].fillna(filler)
        return df_filled


    def writeParquet(df: pd.DataFrame, filename: str, path: str, append: bool):
        """
        This function saves a pandas DataFrame to a parquet file.

        Args:
            df: a pandas DataFrame.
            filename: an str with the filename.
            path: an [str] with the path to save the file.
            append: [bool] flag that indicates if you want to overwrite or append data.
        """
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            filepath = path + filename
            if append:
                print("Appending data to: " + path + " started_at: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z(%z)"))
                df.to_parquet(path=filepath, compression="snappy", engine="fastparquet", append=append)
                print("Finished appending, time: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z(%z)"))
            else:
                print("Writing data to: " + path + " started_at: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z(%z)"))
                df.to_parquet(path=filepath, compression="snappy", engine="fastparquet")
                print("Finished writing, time: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z(%z)"))
            return True
        except FileNotFoundError as e:
            print(f"Error: Path not found: {e}")
            return False
        except (OSError, IOError) as e:
            print(f"Error writing to parquet file: {e}")
            return False