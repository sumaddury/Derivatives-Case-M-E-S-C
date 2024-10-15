import random
import pandas as pd
from datetime import datetime


class Strategy:

    def __init__(self, orders) -> None:
        self.orders = orders

    def generate_orders(self) -> pd.DataFrame:
        return pd.DataFrame(self.orders)
