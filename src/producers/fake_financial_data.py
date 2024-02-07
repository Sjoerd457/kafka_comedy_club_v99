import json
import random
from datetime import datetime


class FakeFinancialData:

    # Define a list of predefined ticker symbols
    predefined_tickers = ["AAPL", "GOOG", "MSFT", "AMZN", "%TFM 1!-ICN"]

    def __init__(self):
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.ticker = self.generate_fake_ticker()
        self.open = round(random.uniform(100, 200), 2)
        self.high = round(random.uniform(self.open, self.open + 10), 2)
        self.low = round(random.uniform(self.open - 10, self.open), 2)
        self.close = round(random.uniform(self.low, self.high), 2)

    def generate_fake_ticker(self):
        """Randomly selects a ticker symbol from a predefined list."""
        return random.choice(self.predefined_tickers)

    def json(self):
        """Serializes the data into JSON format."""
        return json.dumps({
            "timestamp": self.timestamp,
            "ticker": self.ticker,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
        })

    def to_dict(self):
        """Converts the instance to a dictionary matching the Avro schema."""
        return {
            "timestamp": self.timestamp,
            "ticker": self.ticker,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
        }


# Example usage:
if __name__ == "__main__":
    fake_data = FakeFinancialData()
    print(fake_data.json())
