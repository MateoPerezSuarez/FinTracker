import os
import finnhub
import pandas as pd
from datetime import date, timedelta

# API key ya verificada
with open("api_key.txt", "r") as f:
    api_key = f.read().strip().split(" = ")[1]

finnhub_client = finnhub.Client(api_key=api_key)

to_ = date.today()
from_ = to_ - timedelta(days=120)

appl = finnhub_client.company_news('AAPL', _from=from_.isoformat(), to=to_.isoformat())
nvda = finnhub_client.company_news('NVDA', _from=from_.isoformat(), to=to_.isoformat())
msft = finnhub_client.company_news('MSFT', _from=from_.isoformat(), to=to_.isoformat())
amzn = finnhub_client.company_news('AMZN', _from=from_.isoformat(), to=to_.isoformat())
googl = finnhub_client.company_news('GOOGL', _from=from_.isoformat(), to=to_.isoformat())
tsla = finnhub_client.company_news('TSLA', _from=from_.isoformat(), to=to_.isoformat())
meta = finnhub_client.company_news('META', _from=from_.isoformat(), to=to_.isoformat())

news = appl + nvda + msft + amzn + googl + tsla + meta

df = pd.DataFrame(news).drop_duplicates(subset=['id']).reset_index(drop=True)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)

with open(os.path.join(os.path.dirname(__file__), '../data_processing/ticker_news.csv'), 'w', encoding='utf-8', newline='') as f:
    df.to_csv(f, index=False)

print(len(df))