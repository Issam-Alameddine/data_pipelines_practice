from polygon import RESTClient
import pandas as pd
from polygon.rest.models import (
    TickerNews,
)

client = RESTClient("ok9x8dCfl_dne7p0gNRpB4ziLdQJjLC4")

news = []
for n in client.list_ticker_news(
    ticker = "CRWD",
	order="asc",
	limit="10",
	sort="published_utc",
	):
    news.append(n)

df = pd.DataFrame(news)
df.to_csv('news.csv', index=False)

# print date + title
# for index, item in enumerate(news):
#     # verify this is an agg
#     if isinstance(item, TickerNews):
#         print("{:<25}{:<15}".format(item.published_utc, item.title))
#
#         if index == 20:
#             break
