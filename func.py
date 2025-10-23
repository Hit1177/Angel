import concurrent.futures
import pandas as pd
import time
from datetime import datetime, timedelta
import pytz
import requests
import json

def get_nifty50_tokens():
    nifty50_symbols = [
        "ADANIPORTS-EQ", "ASIANPAINT-EQ", "AXISBANK-EQ", "BAJAJ-AUTO-EQ", "BAJFINANCE-EQ",
        "BAJAJFINSV-EQ", "BPCL-EQ", "BHARTIARTL-EQ", "BRITANNIA-EQ", "CIPLA-EQ", "COALINDIA-EQ",
        "DIVISLAB-EQ", "DRREDDY-EQ", "EICHERMOT-EQ", "GRASIM-EQ", "HCLTECH-EQ", "HDFCBANK-EQ",
        "HDFCLIFE-EQ", "HEROMOTOCO-EQ", "HINDALCO-EQ", "HINDUNILVR-EQ", "ICICIBANK-EQ",
        "INDUSINDBK-EQ", "INFY-EQ", "ITC-EQ", "JSWSTEEL-EQ", "KOTAKBANK-EQ", "LT-EQ",
        "M&M-EQ", "MARUTI-EQ", "NESTLEIND-EQ", "NTPC-EQ", "ONGC-EQ", "POWERGRID-EQ", "RELIANCE-EQ",
        "SBILIFE-EQ", "SBIN-EQ", "SHREECEM-EQ", "SUNPHARMA-EQ", "TATACONSUM-EQ", "TATAMOTORS-EQ",
        "TATASTEEL-EQ", "TECHM-EQ", "TITAN-EQ", "TORNTPHARM-EQ", "ULTRACEMCO-EQ", "UPL-EQ",
        "WIPRO-EQ"
    ]
    nifty50_symbol_set = set(nifty50_symbols)
    url = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
    data = requests.get(url).json()
    nifty50_tokens = {}
    for d in data:
        if d.get("exch_seg") == "NSE":
            symbol = d.get("symbol")
            if symbol in nifty50_symbol_set:
                nifty50_tokens[int(d["token"])] = symbol
    return nifty50_tokens

# To use and pretty print:
tokens = get_nifty50_tokens()
# print(json.dumps(tokens, indent=4))

tz = pytz.timezone('Asia/Kolkata')

# --- Indicators here ---
def rsi(close, periods=14):
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    # Use EMA for smoothing
    avg_gain = gain.ewm(alpha=1/periods, min_periods=periods, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/periods, min_periods=periods, adjust=False).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def efi(df, price_col='Close', volume_col='Volume', span=5):
    efi_raw = (df[price_col] - df[price_col].shift(1)) * df[volume_col]
    return efi_raw.ewm(span=span, adjust=False).mean()

# --- Fetch functions ---
def fetch_candle_data_with_retry(obj, token, from_date, to_date, max_retries=5, sleep_sec=10):
    params = {
        "exchange": "NSE",
        "symboltoken": str(token),
        "interval": "TEN_MINUTE",
        "fromdate": from_date,
        "todate": to_date
    }
    for attempt in range(max_retries):
        try:
            api_response = obj.getCandleData(params)
            if api_response and api_response.get('status') and api_response.get('data'):
                df = pd.DataFrame(api_response['data'], columns=["DateTime", "Open", "High", "Low", "Close", "Volume"])
                df["DateTime"] = pd.to_datetime(df["DateTime"])
                df.set_index("DateTime", inplace=True)
                df['rsi'] = rsi(df['Close'])
                df['efi'] = efi(df)
                return df
            else:
                reason = api_response.get("message", api_response)
                print(f"Retry {attempt+1}/{max_retries} for token {token} failed, sleeping {sleep_sec}s. Reason: {reason}")
                time.sleep(sleep_sec)
        except Exception as e:
            print(f"Error for token {token}, attempt {attempt+1}: {e}")
            time.sleep(sleep_sec)
    print(f"All retries failed for token {token}")
    return pd.DataFrame()
def fetch_candle_data_with_retry_1(obj, token, from_date, to_date, max_retries=5, sleep_sec=10):
    params = {
        "exchange": "NSE",
        "symboltoken": str(token),
        "interval": "ONE_MINUTE",
        "fromdate": from_date,
        "todate": to_date
    }
    for attempt in range(max_retries):
        try:
            api_response = obj.getCandleData(params)
            if api_response and api_response.get('status') and api_response.get('data'):
                df = pd.DataFrame(api_response['data'], columns=["DateTime", "Open", "High", "Low", "Close", "Volume"])
                df["DateTime"] = pd.to_datetime(df["DateTime"])
                df.set_index("DateTime", inplace=True)
                df['rsi'] = rsi(df['Close'])
                df['efi'] = efi(df)
                return df
            else:
                reason = api_response.get("message", api_response)
                print(f"Retry {attempt+1}/{max_retries} for token {token} failed, sleeping {sleep_sec}s. Reason: {reason}")
                time.sleep(sleep_sec)
        except Exception as e:
            print(f"Error for token {token}, attempt {attempt+1}: {e}")
            time.sleep(sleep_sec)
    print(f"All retries failed for token {token}")
    return pd.DataFrame()
def fetch_all_candles_parallel(obj, nifty50_tokens, start_date, end_date, max_workers=4):
    results = {}
    from_date = start_date.strftime("%Y-%m-%d %H:%M")
    to_date = end_date.strftime("%Y-%m-%d %H:%M")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_symbol = {
            executor.submit(fetch_candle_data_with_retry, obj, token, from_date, to_date): symbol
            for token, symbol in nifty50_tokens.items()
        }
        for future in concurrent.futures.as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            try:
                df = future.result()
                results[symbol] = df
            except Exception as exc:
                print(f"{symbol} generated an exception: {exc}")
    return results
def fetch_all_candles_parallel_1(obj, nifty50_tokens, start_date, end_date, max_workers=4):
    results = {}
    from_date = start_date.strftime("%Y-%m-%d %H:%M")
    to_date = end_date.strftime("%Y-%m-%d %H:%M")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_symbol = {
            executor.submit(fetch_candle_data_with_retry_1, obj, token, from_date, to_date): symbol
            for token, symbol in nifty50_tokens.items()
        }
        for future in concurrent.futures.as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            try:
                df = future.result()
                results[symbol] = df
            except Exception as exc:
                print(f"{symbol} generated an exception: {exc}")
    return results

def get_stocks_with_high_rsi_and_efi(symbol_data, symbol_data_1, efi_quantile_df, rsi_threshold):
    high_rsi_and_efi_stocks = []
    efi_quantile_dict = efi_quantile_df.set_index('Symbol')['EFI_Quantile_0.99'].to_dict()

    for symbol, df in symbol_data.items():
        if not df.empty and 'rsi' in df.columns and 'efi' in df.columns:
            last_rsi = df['rsi'].iloc[-1]
            last_efi = df['efi'].iloc[-1]
            last_close = df['Close'].iloc[-1]
            if symbol in efi_quantile_dict:
                efi_99_percentile = efi_quantile_dict[symbol]
                # print(symbol)
                if (last_rsi > rsi_threshold 
                # and last_efi > efi_99_percentile 
                # and last_close<df['Close'].iloc[-4:-1].max()
                # and symbol_data_1[symbol]['Close'].iloc[-1]>df['High'].iloc[-1]
                   ):
                    high_rsi_and_efi_stocks.append(symbol)
            else:
                print(f"EFI quantile data not available for {symbol}")
        else:
            print(f"RSI or EFI data not available for {symbol}")
    return high_rsi_and_efi_stocks


def get_options_with_ltp(stocks_meeting_criteria, symbol_data_1, obj):
    """
    Get nearest call options with LTP for given stocks

    Parameters:
    - stocks_meeting_criteria: list of stock symbols
    - symbol_data_1: dict containing historical data with 'Close' prices
    - obj: SmartConnect API object

    Returns:
    - DataFrame with columns: symbol, token, strike, expiry, ltp
    """

    # --- Load and clean scrip master ---
    url = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
    df = pd.read_json(url)

    df["expiry"] = pd.to_datetime(df["expiry"], format="%d%b%Y", errors="coerce")
    df = df[df["instrumenttype"] == "OPTSTK"].copy()
    df["strike"] = pd.to_numeric(df["strike"], errors="coerce") / 100

    # --- Helper: Find nearest call token ---
    def find_nearest_call_token(symbol, spot_price):
        base_symbol = symbol.replace("-EQ", "").upper()
        calls = df[(df["name"] == base_symbol) &
                   (df["symbol"].str.endswith("CE"))]
        if calls.empty:
            return None, None, None
        valid_expiries = calls["expiry"].dropna().unique()
        if len(valid_expiries) == 0:
            return None, None, None
        nearest_expiry = sorted(valid_expiries)[0]
        calls = calls[calls["expiry"] == nearest_expiry]
        eligible = calls[calls["strike"] >= spot_price]
        if eligible.empty:
            return None, None, None
        nearest = eligible.sort_values("strike").iloc[0]
        return nearest["token"], nearest["strike"], nearest_expiry

    # --- Helper: Get LTP with retries ---
    def ltp(row):
        max_retries = 2
        for attempt in range(max_retries):
            try:
                api_response = obj.ltpData(
                    exchange='NSE',
                    tradingsymbol=row['symbol'],
                    symboltoken=row['token']
                )
                return api_response['data']['ltp']
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"Attempt {attempt + 1} failed for {row['symbol']}: {e}. Retrying...")
                    time.sleep(1)
                else:
                    print(f"All {max_retries} attempts failed for {row['symbol']}: {e}")
                    return None

    # --- Build output data ---
    output = []
    for symbol in stocks_meeting_criteria:
        spot_price = symbol_data_1[symbol]["Close"].iloc[-1]
        token, strike, expiry = find_nearest_call_token(symbol, spot_price)
        output.append({
            "symbol": symbol,
            "token": token,
            "strike": strike,
            "expiry": expiry
        })

    # Create DataFrame
    outdf = pd.DataFrame(output)

    # Add LTP column
    outdf['ltp'] = outdf.apply(ltp, axis=1)

    return outdf
