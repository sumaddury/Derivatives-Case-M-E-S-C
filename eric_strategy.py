import random
import pandas as pd
from datetime import datetime


class Strategy:
    def __init__(self) -> None:
        self.capital = 100_000_000
        self.portfolio_value = 0

        self.start_date = datetime(2024, 1, 1)
        self.end_date = datetime(2024, 3, 30)

        self.options = pd.read_csv(
            r"Derivatives-Case-M-E-S-C\data\cleaned_options_data.csv"
        )
        self.options["datetime"] = pd.to_datetime(
            self.options["ts_recv"], format="%Y-%m-%dT%H:%M:%S.%fZ"
        )
        parsed_features = self.options["symbol"].apply(self.parse_option_symbol)
        # Create new columns by unpacking the parsed features
        (
            self.options["exp_date"],
            self.options["action"],
            self.options["strike_price"],
        ) = zip(*parsed_features)
        self.options = self.options[
            [
                "datetime",
                "bid_px_00",
                "ask_px_00",
                "bid_sz_00",
                "ask_sz_00",
                "exp_date",
                "action",
                "strike_price",
            ]
        ]
        self.options = self.options.rename(
            columns={
                "bid_px_00": "bidp",
                "ask_px_00": "askp",
                "bid_sz_00": "bid_sz",
                "ask_sz_00": "ask_sz",
            }
        )

        self.underlying = pd.read_csv(
            r"Derivatives-Case-M-E-S-C\data\underlying_data_hour.csv"
        )
        self.underlying.columns = self.underlying.columns.str.lower()
        self.underlying["date"] = pd.to_datetime(self.underlying["date"])
        print(self.underlying.columns)
        print(self.underlying.head(5))

    # Define the function to parse a single option symbol
    def parse_option_symbol(self, symbol):
        """
        example: SPX   240419C00800000
        """
        numbers = symbol.split(" ")[3]
        exp_date = datetime.strptime(
            numbers[:6], "%y%m%d"
        )  # Convert to datetime object
        action = numbers[6]  # Extract the action ('C' or 'P')
        strike_price = float(numbers[7:]) / 1000  # Extract and convert the strike price
        return [exp_date, action, strike_price]

    def generate_orders(self) -> pd.DataFrame:
        orders = []
        num_orders = 1000

        for _ in range(num_orders):
            row = self.options.sample(n=1).iloc[0]
            action = random.choice(["B", "S"])

            if action == "B":
                order_size = random.randint(1, int(row["ask_sz_00"]))
            else:
                order_size = random.randint(1, int(row["bid_sz_00"]))

            assert order_size <= int(row["ask_sz_00"]) or order_size <= int(
                row["bid_sz_00"]
            )

            order = {
                "datetime": row["ts_recv"],
                "option_symbol": row["symbol"],
                "action": action,
                "order_size": order_size,
            }
            orders.append(order)

        return pd.DataFrame(orders)


s = Strategy()
