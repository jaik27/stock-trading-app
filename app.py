import os
import sys
import threading
import time
import signal
from dotenv import load_dotenv

load_dotenv()

import requests
import pandas as pd
from datetime import datetime

from data_collector import NSEDataCollector
from indicator_calculator import IndicatorCalculator
from pipeline_validator import PipelineValidator
from data_preparation import DataPreparation
from schema_design import ensure_schema, import_historical_csv

def get_auth_token(loginId, product, apikey, endpoint):
    url = f"{endpoint}?loginid={loginId}&product={product}&apikey={apikey}"
    res = requests.get(url)
    res.raise_for_status()
    return res.json()['AccessToken']

def get_historical_data(loginId, product, access_token, startdate, enddate, exch, inst, symbol, expiry=None, strike=None, optiontype=None):
    url = (
        f"http://qbase1.vbiz.in/directrt/gethistorical?loginid={loginId}&product={product}&accesstoken={access_token}"
        f"&startdate={startdate}&enddate={enddate}&exch={exch}&inst={inst}&symbol={symbol}"
    )
    if expiry:
        url += f"&expiry={expiry}"
    if strike:
        url += f"&strike={strike}"
    if optiontype:
        url += f"&optiontype={optiontype}"
    res = requests.get(url)
    res.raise_for_status()
    from io import StringIO
    csv_data = StringIO(res.text)
    df = pd.read_csv(csv_data)
    return df

def run_collector():
    collector = NSEDataCollector()
    collector.start()

def run_indicator_calculator():
    calc = IndicatorCalculator(mode="batch")
    calc.start()

def run_pipeline_validator():
    validator = PipelineValidator()
    validator.run_validation()

def run_data_preparation():
    prep = DataPreparation()
    prep.run()  # <-- fixed line

def main():
    ensure_schema()
    loginId = os.getenv("NSE_LOGIN_ID")
    product = os.getenv("NSE_PRODUCT")
    apikey = os.getenv("NSE_API_KEY")
    auth_endpoint = os.getenv("NSE_AUTH_ENDPOINT")
    access_token = get_auth_token(loginId, product, apikey, auth_endpoint)
    startdate = "21NOV2023"
    enddate = "23NOV2023"
    exch = "NSE"
    inst = "OPTIDX"
    symbol = "NIFTY"
    expiry = "23NOV2023"
    strike = "19700"
    optiontype = "CE"
    print("Fetching historical data from API...")
    hist_df = get_historical_data(loginId, product, access_token, startdate, enddate, exch, inst, symbol, expiry, strike, optiontype)
    hist_df.to_csv("historical_data.csv", index=False)
    print("Historical data exported to historical_data.csv")
    import_historical_csv("historical_data.csv", candle_type="1min")

    # Start data collector and indicator calculator in threads
    t_collector = threading.Thread(target=run_collector, daemon=True)
    t_calc = threading.Thread(target=run_indicator_calculator, daemon=True)
    t_collector.start()
    t_calc.start()

    # Wait for indicator calculator to finish initial calculation (or sleep for a period)
    # You may want to improve this with event signaling or health checks
    print("Waiting for indicator calculator to initialize...")
    time.sleep(10)

    # Run the pipeline validator synchronously (block until done)
    run_pipeline_validator()

    # Start data preparation in a separate thread
    t_prep = threading.Thread(target=run_data_preparation, daemon=True)
    t_prep.start()

    def signal_handler(sig, frame):
        print("Shutting down all components...")
        sys.exit(0)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    while True:
        time.sleep(2)

if __name__ == "__main__":
    main()