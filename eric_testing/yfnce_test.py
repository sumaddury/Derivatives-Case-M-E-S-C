import yfinance as yf
from datetime import datetime

sp = yf.Ticker("^GSPC")
# Get maximum period by the minute
hist = sp.history(period="max", interval="1d")

hist = hist.reset_index().rename(columns={"index": "Date"})
hist = hist[["Date", "Open", "High", "Low", "Close", "Volume"]]
hist["Date"] = hist["Date"].dt.tz_localize(None)
hist = hist[
    ((hist["Date"] >= datetime(1995, 1, 1)) & (hist["Date"] <= datetime(2006, 1, 1)))
    | (hist["Date"] >= datetime(2009, 1, 1))
]
hist.to_csv("data/daily_under.csv")
print(hist)
