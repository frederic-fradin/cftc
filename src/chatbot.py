import os
import pandas as pd
from pandasai import Agent
from dataset import load_parquet


df = load_parquet('cftc')

# By default, unless you choose a different LLM, it will use BambooLLM.
# You can get your free API key signing up at https://pandabi.ai (you can also configure it in your .env file)
os.environ["PANDASAI_API_KEY"] = ''

agent = Agent(df)
agent.chat('What is the value for managed money long + short position of the latest publication for CORN market?')