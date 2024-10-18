import pandas as pd
import numpy as np
from datetime import datetime
import warnings

warnings.filterwarnings("ignore")


class Strategy:
    def __init__(self, start_date, end_date, options_data, underlying) -> None:
        self.capital: float = 100_000_000
        self.threshold = -150

        self.start_date: datetime = start_date
        self.end_date: datetime = end_date

        self.options: pd.DataFrame = pd.read_csv(options_data)
        self.options["datetime"] = pd.to_datetime(
            self.options["ts_recv"], format="%Y-%m-%dT%H:%M:%S.%fZ"
        )

        symbol_parts = self.options["symbol"].str.split(" ", expand=True)[1]
        self.options["exp_date"] = pd.to_datetime(symbol_parts.str[:8], format="%Y%m%d")
        self.options = self.options[self.options["exp_date"] <= self.end_date]
        self.options["action"] = symbol_parts.str[8]
        self.options["strike_price"] = symbol_parts.str[9:].astype(float) / 1000

        self.options["datetime"] = (
            pd.to_datetime(self.options["datetime"])
            # .dt.tz_localize("UTC")
            # .dt.tz_convert("US/Eastern")
            # .dt.tz_localize(None)
        )
        self.options["date"] = self.options["datetime"].dt.date

        self.underlying = pd.read_csv(underlying)
        self.underlying = self.underlying.loc[self.underlying["price"] > 0]
        self.underlying["date"] = pd.to_datetime(
            self.underlying["date"], format="%Y%m%d"
        )

        self.underlying_min = self.underlying.copy()

        self.underlying = (
            self.underlying.groupby("date")
            .agg(
                Open=("price", "first"),
                High=("price", "max"),
                Low=("price", "min"),
                Close=("price", "last"),
            )
            .reset_index()
        )
        self.underlying_min["date"] = pd.to_datetime(
            self.underlying_min["date"]
        ).dt.date
        time_part = pd.to_timedelta(
            self.underlying_min["ms_of_day"], unit="ms"
        ) + pd.Timedelta(hours=5)
        self.underlying_min["hour"] = time_part.dt.components.hours
        self.underlying_min["minute"] = time_part.dt.components.minutes

        self.underlying["date"] = self.underlying["date"].dt.date
        self.options = self.options.merge(self.underlying, how="left", on=["date"])

        self.options = self.options.rename(
            columns={
                "Close": "underlying",
                "bid_px_00": "bidp",
                "ask_px_00": "askp",
                "bid_sz_00": "bid_sz",
                "ask_sz_00": "ask_sz",
            }
        )[
            [
                "datetime",
                "strike_price",
                "underlying",
                "action",
                "bidp",
                "askp",
                "bid_sz",
                "ask_sz",
                "date",
                "exp_date",
                "symbol",
                "ts_recv",
                # "instrument_id",
            ]
        ]
        self.options["deviate"] = (
            self.options["underlying"] - self.options["strike_price"]
        )
        self.options.loc[self.options["action"] == "P", "deviate"] *= -1

    def generate_orders(self) -> pd.DataFrame:
        unique_exp_dates = self.options["exp_date"].unique()
        orders = pd.DataFrame(
            columns=["datetime", "option_symbol", "action", "order_size"]
        )

        for exp_date in unique_exp_dates:
            active_ord = pd.DataFrame(
                columns=[
                    "strike_price",
                    "order_size",
                    "datetime",
                    "exp_date",
                    "action",
                ]
            )
            day_before_exp = (exp_date - pd.Timedelta(days=1)).date()
            filtered_options = self.options[
                (self.options["date"] == day_before_exp)
                & (self.options["exp_date"] == exp_date)
            ]
            sorted_options = filtered_options.sort_values(by="deviate", ascending=True)
            sorted_options = sorted_options[sorted_options["bid_sz"] > 0]
            for col in [
                "datetime",
                "strike_price",
                "underlying",
                "symbol",
                "deviate",
                "bidp",
                "bid_sz",
            ]:
                print(sorted_options[col].head(5), flush=True)

            # Loop through sorted options, keep buying the minimum between spend limit remaining and the ask size, break if sort_by < threshold
            for _, option in sorted_options.iterrows():
                # General stuff
                if option["deviate"] > self.threshold:
                    break
                if option["bid_sz"] == 0 or option["bidp"] == 0:
                    continue
                margin = (
                    0.1 * option["strike_price"]
                    if option["action"] == "C"
                    else 0.1 * option["underlying"]
                )
                order_size = min(
                    option["bid_sz"],
                    (self.capital - margin) / (option["bidp"] * 100),
                )
                if order_size <= 0:
                    break

                earnings = order_size * 100 * option["bidp"]
                if earnings < 1:
                    continue

                if self.capital >= margin + earnings:
                    self.capital += earnings

                new_row = pd.DataFrame(
                    [
                        {
                            "datetime": option["ts_recv"],
                            "option_symbol": option["symbol"],
                            "action": "S",
                            "order_size": order_size,
                        }
                    ]
                )
                new_row2 = pd.DataFrame(
                    [
                        {
                            "strike_price": option["strike_price"],
                            "order_size": order_size,
                            "datetime": option["datetime"],
                            "exp_date": option["exp_date"],
                            "action": option["action"],
                        }
                    ]
                )
                orders = pd.concat(
                    [orders if not orders.empty else None, new_row],
                    ignore_index=True,
                )
                active_ord = pd.concat(
                    [active_ord if not active_ord.empty else None, new_row2],
                    ignore_index=True,
                )
            active_ord["date"] = pd.to_datetime(active_ord["exp_date"]).dt.date
            active_ord["datetime"] = pd.to_datetime(active_ord["datetime"])
            active_ord["hour"] = active_ord["datetime"].dt.hour
            active_ord["minute"] = active_ord["datetime"].dt.minute
            active_ord["hour"][active_ord["hour"] < 14] = 14
            active_ord["hour"][active_ord["hour"] > 21] = 21
            active_ord["minute"][active_ord["hour"] == 14] = 31
            active_ord["minute"][active_ord["hour"] == 21] = 0

            active_ord = active_ord.merge(
                self.underlying_min, how="left", on=["date", "hour", "minute"]
            )
            for _, option in active_ord.iterrows():
                diff = option["price"] - option["strike_price"]
                if option["action"] == "C":
                    if diff > 0:
                        self.capital -= option["order_size"] * 100 * diff
                else:
                    if diff < 0:
                        self.capital -= option["order_size"] * 100 * -diff
        return orders
