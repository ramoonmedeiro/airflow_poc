import requests
import json
import pandas as pd
import numpy as np

r = requests.get('https://economia.awesomeapi.com.br/last/USD-BRL')
content = json.loads(r.text)
high = content["USDBRL"]["high"]
low = content["USDBRL"]["low"]
var = content["USDBRL"]["varBid"]

df_dolar = pd.DataFrame(np.array([[high, low, var]]), columns = ['high', 'low', 'variance'])
df_dolar.to_csv('teste-airflow.csv', index=False)