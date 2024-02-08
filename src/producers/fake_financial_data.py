from pydantic import BaseModel
import random
from datetime import datetime


class FakeFinancialData(BaseModel):
    timestamp: str
    ticker: str
    open: float
    high: float
    low: float
    close: float

    @classmethod
    def generate(cls):
        open_price = round(random.uniform(100, 200), 2)
        high_price = round(random.uniform(open_price, open_price + 10), 2)
        low_price = round(random.uniform(open_price - 10, open_price), 2)
        close_price = round(random.uniform(low_price, high_price), 2)
        return cls(
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ticker="%TFM 1!-ICN",
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price
        )
