from datetime import date
from dateutil.relativedelta import relativedelta
from time import sleep
from json import dump, load
from os import makedirs

from pykrx import stock

import os
import pandas as pd


def string_to_date(string):
    return date(int(string[:4]), int(string[4:6]), int(string[6:]))

def date_to_string(dt):
    return f"{dt.year}{dt.month:02d}{dt.day:02d}"

class DataCollector:
    def __init__(self, log = None):
        if log:
            self.log = log
        else:
            self.log = print
            
        self.oldest_date = date(2023, 5, 1)
        self.oldest_date_string = date_to_string(self.oldest_date)
        self.today = date.today()
        self.today_string = date_to_string(self.today)
        
        self.log("""
                 ===============================================================
                 Data Collector
                 ===============================================================
                 """)
        
        if not os.path.exists("database"):
            self.log("generating ./database")
            makedirs("database")
            
        self.read_ticker()
        
    def read_ticker(self, indentation = ""):
        try:
            self.log(indentation + "trying to read database/tickers.json")
            with open("database/tickers.json", encoding="utf-8") as tkrs:
                self.tickers = load(tkrs)
        except:
            self.log(indentation + "failed to read")
            self.log(indentation + "generating database/tickers.json")
            self.tickers = dict()
            with open("database/tickers.json", "w", encoding="utf-8") as tkrs:
                dump(self.tickers, tkrs, indent=2, ensure_ascii=False)

        self.log(indentation + "done reading ticker")
        self.log(indentation)
        
    def update_ticker(self, indentation = ""):
        self.log(indentation + "collecting tickers")
        
        while True:
            try:
                tickers = stock.get_market_ticker_list(self.today_string)
                break
            
            except:
                self.log(f"""{indentation}
                         {indentation} + ===============================================================
                         {indentation} + Error occured
                         {indentation} + restarting
                         {indentation} + ===============================================================
                         {indentation}""")
                
        ticker_name = dict()
        self.log(indentation + "collecting tickers' name")
        for i in tickers:
            while True:
                try:
                    ticker_name[i] = stock.get_market_ticker_name(i)
                    break
                
                except:
                    self.log(f"""{indentation}
                             {indentation} + ===============================================================
                             {indentation} + Error occured
                             {indentation} + restarting
                             {indentation} + ===============================================================
                             {indentation}""")

        self.log(indentation + "saving tickers to database/tickers.json")
        with open("database/tickers.json", "w", encoding="utf-8") as tkrs:
            dump(ticker_name, tkrs, indent=2, ensure_ascii=False)

        self.log(indentation + "done updating ticker")
        self.read_ticker(indentation=indentation + " | ")
        
    def update_price(self, ticker, indentation = ""):
        self.log(indentation + f"updating prices of {ticker}({self.tickers[ticker]})")
        path = f"database/{ticker}.csv"
        has_file = os.path.exists(path)
        if has_file:
            df = pd.read_csv(path)
            y, m, d = map(int, df["날짜"][df.index[-1]].split("-"))
            ld = date(y, m, d)
            last_date_string = date_to_string(ld + relativedelta(days=1))
            last_date = string_to_date(last_date_string)
        else:
            df = pd.DataFrame()
            last_date_string = self.oldest_date_string
            last_date = string_to_date(last_date_string)
        
        while last_date <= self.today:
            next_date = last_date + relativedelta(months=1) - relativedelta(days=1)
            next_date_string = date_to_string(next_date)
            self.log(indentation + f"collecting prices of {ticker}({self.tickers[ticker]}) from {last_date} to {next_date}")
            
            try:
                prices = stock.get_market_ohlcv(last_date_string, next_date_string, ticker)
                
                if not prices.empty:
                    df = pd.concat([df, prices], ignore_index=False)
                    
                last_date += relativedelta(months=1)
                last_date_string = date_to_string(last_date)
                
            except:
                self.log(f"""{indentation}
                         {indentation} + ===============================================================
                         {indentation} + Error occured
                         {indentation} + restarting
                         {indentation} + ===============================================================
                         {indentation}""")
                
            sleep(1)
            
        df.to_csv(f"database/{ticker}.csv", index=(not has_file))
        self.log(indentation + f"done updating prices of {ticker}({self.tickers[ticker]})")
        self.log(indentation)
        
    def auto_update(self, indentation = ""):
        self.log(indentation + "updating all")
        self.update_ticker(indentation=indentation + " | ")
        
        self.log(indentation + "updating all prices")
        for idx, tkrs in enumerate(self.tickers):
            self.log(indentation + f"updating {tkrs}({self.tickers[tkrs]}) [{idx + 1}/{len(self.tickers)}]")
            self.update_price(tkrs, indentation=indentation + " | ")
            
        self.log(indentation + "done updating all prices")
        self.log(indentation)