# Strategy 1 - Borrowing Rate Based Strategy

- Hypothesis: Increase in borrowing interest rates means higher demand in
  underlying coins, BTC and crypto in general.

## Get Started

1. Set up your Binance credentials

   Create `binance_secrets.py` in this folder. The file should look like this:

   ```python
   api_key = "...your api key..."
   api_secret = "...your api secret..."
   ```

   > Note: Do not commit this file.

2. Run [`231225-z-model-top-coins.ipynb`](231225-z-model-top-coins.ipynb) to see
   the performance of the strategy based on Z-scores of raw hourly borrowing
   interest rates on top 20 coins

## Deprecated Instructions Below

1. Set up your Binance credentials

   Create `binance_secrets.py` in this folder. The file should look like this:

   ```python
   api_key = "...your api key..."
   api_secret = "...your api secret..."
   ```

   > Note: Do not commit this file.

2. Run [`00_hourly_data_download.ipynb`](00_hourly_data_download.ipynb) to download
   the hourly data if needed

3. Run [`231221-z-model.ipynb`](231221-z-model.ipynb) to see the performance of the
   strategy based on Z-scores of raw USDT and BTC hourly borrowing interest rates
