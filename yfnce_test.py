import yfinance as yf
from datetime import datetime

sp = yf.Ticker("^SPX")
# Get maximum period by the minute
hist = sp.history(period="max", interval="day")
print(hist.size())
