# Creating a Trading Bot
## Objective:
Create a trading bot that detects anomalies from indicators before a price spike.
## Core Data Categories for Stock Market Analysis  
| Category                            | Key Data Points to Collect                                                                 | Purpose                                                         |
|-------------------------------------|---------------------------------------------------------------------------------------------|-----------------------------------------------------------------|
|  Price & Volume Data (Historical + Real-Time) | Open, High, Low, Close (OHLC), VWAP, Trading Volume, Number of Transactions                 | Detect price trends, volatility, and volume spikes.             |
|  Options Market Data              | Strike Prices, Open Interest, Implied Volatility (IV), Call & Put Volume, Unusual Large Option Trades | Detect institutional positioning before stock moves.            |
|  Dark Pool & Institutional Trading | Dark Pool Trade Volume, Price Impact, Large Block Trades, Unusual Buying Activity           | Identify hidden accumulation by hedge funds.                   |
|  SEC Filings & Insider Trading   | Insider Buys/Sells, 13F Hedge Fund Positions, Corporate Filings (10-K, 10-Q, 8-K), Earnings Reports | Track executive & hedge fund confidence in stocks.             |
|  Bid-Ask Spread & Liquidity Data | Real-Time Bid & Ask Prices, Spread Size, Order Book Depth                                   | Detect illiquid stocks or sudden liquidity shifts.              |
|  Social Media Sentiment & News   | Reddit/WSB Mentions, Twitter Mentions, Market News Headlines, Sentiment Score               | Identify meme stocks before they explode.                      |
|  Short Interest & Borrowing Data | Short Interest % of Float, Cost to Borrow, Utilization Rate, Days to Cover                  | Find potential short squeeze opportunities.                    |
|  Macro Market Indicators         | VIX (Volatility Index), US Treasury Bond Yields, Federal Reserve Announcements              | Understand broad market conditions impacting stocks.           |
