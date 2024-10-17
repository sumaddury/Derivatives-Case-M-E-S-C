import pandas as pd
import numpy as np
from datetime import datetime


class Strategy:
    def __init__(self, start_date, end_date) -> None:
        self.capital: float = 100_000_000
        self.portfolio_value: float = 0
        self.spend_lim_ratio = 0.9
        self.threshold = 20

        self.start_date: datetime = start_date
        self.end_date: datetime = end_date

        self.options: pd.DataFrame = pd.read_csv("data/cleaned_options_data.csv")
        self.options["datetime"] = pd.to_datetime(
            self.options["ts_recv"], format="%Y-%m-%dT%H:%M:%S.%fZ"
        )
        symbol_parts = self.options["symbol"].str.split(" ", expand=True)[3]
        self.options["exp_date"] = pd.to_datetime(symbol_parts.str[:6], format="%y%m%d")
        self.options = self.options[self.options["exp_date"] <= self.end_date]
        self.options["action"] = symbol_parts.str[6]
        self.options["strike_price"] = symbol_parts.str[7:].astype(float) / 1000
        self.options = self.options.rename(
            columns={
                "bid_px_00": "bidp",
                "ask_px_00": "askp",
                "bid_sz_00": "bid_sz",
                "ask_sz_00": "ask_sz",
            }
        )
        self.options["datetime"] = (
            pd.to_datetime(self.options["datetime"])
            .dt.tz_localize("UTC")
            .dt.tz_convert("US/Eastern")
            .dt.tz_localize(None)
        )
        self.options["date"] = self.options["datetime"].dt.date
        self.options["till_exp"] = (
            self.options["exp_date"] - self.options["datetime"]
        ).dt.days / 365.0
        self.options["fair_value"] = (self.options["bidp"] + self.options["askp"]) / 2.0

        self.underlying = pd.read_csv(r"data\spx_minute_level_data_jan_mar_2024.csv")
        print(self.underlying.columns, flush=True)
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
        time_part = pd.to_timedelta(self.underlying_min["ms_of_day"], unit="ms")
        self.underlying_min["hour"] = time_part.dt.components.hours
        self.underlying_min["minute"] = time_part.dt.components.minutes

        indices = self.underlying["date"].searchsorted(self.options["datetime"]) - 1
        indices = indices.clip(0, len(self.underlying) - 1)
        self.options["underlying"] = self.underlying["Close"].iloc[indices].values
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
            spend_limit = self.capital * self.spend_lim_ratio
            day_before_exp = (exp_date - pd.Timedelta(days=1)).date()
            filtered_options = self.options[
                (self.options["date"] == day_before_exp)
                & (self.options["exp_date"] == exp_date)
            ]
            filtered_options["sort_by"] = (
                filtered_options["deviate"] - filtered_options["askp"]
            )
            sorted_options = filtered_options.sort_values(by="sort_by", ascending=False)

            # Loop through sorted options, keep buying the minimum between spend limit remaining and the ask size, break if sort_by < threshold
            for _, option in sorted_options.iterrows():
                # General stuff
                if option["sort_by"] < self.threshold:
                    break
                if option["ask_sz"] == 0:
                    continue
                order_size = min(
                    (self.capital - spend_limit) / (option["askp"] * 100),
                    option["ask_sz"],
                )
                if order_size <= 0:
                    break
                options_cost = order_size * 100 * option["askp"]
                self.capital -= options_cost + 0.5

                new_row = pd.DataFrame(
                    [
                        {
                            "datetime": option["ts_recv"],
                            "option_symbol": option["symbol"],
                            "action": "B",
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
                    [orders, new_row],
                    ignore_index=True,
                )
                active_ord = pd.concat(
                    [active_ord, new_row2],
                    ignore_index=True,
                )
            active_ord["date"] = pd.to_datetime(active_ord["exp_date"]).dt.date
            active_ord["datetime"] = pd.to_datetime(active_ord["datetime"])
            active_ord["hour"] = active_ord["datetime"].dt.hour
            active_ord["minute"] = active_ord["datetime"].dt.minute
            active_ord.loc[(active_ord["hour"] < 9), "hour"] = 9
            active_ord.loc[
                (active_ord["hour"] == 9) & (active_ord["minute"] < 31),
                ["hour", "minute"],
            ] = [9, 31]
            active_ord.loc[(active_ord["hour"] > 16), ["hour", "minute"]] = [16, 0]

            active_ord = active_ord.merge(
                self.underlying_min, how="left", on=["date", "hour", "minute"]
            )
            for _, option in active_ord.iterrows():
                diff = option["price"] - option["strike_price"]
                if option["action"] == "C":
                    if diff > 0:
                        self.capital += option["order_size"] * 100 * diff
                else:
                    if diff < 0:
                        self.capital += option["order_size"] * 100 * -diff
        return orders
