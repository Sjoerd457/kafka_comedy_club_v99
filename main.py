# main.py

import uvicorn
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

if __name__ == "__main__":

    # Import producer_app from src.producers.example_producer
    uvicorn.run("src.producers.example_producer:producer_app", host="0.0.0.0", port=8000, reload=True)
