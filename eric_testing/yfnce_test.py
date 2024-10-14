import yfinance as yf
from datetime import datetime

sp = yf.Ticker("^GSPC")
# Get maximum period by the minute
hist = sp.history(period="max", interval="1h")
print(hist)
