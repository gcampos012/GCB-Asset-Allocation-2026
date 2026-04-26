#%%
import pandas as pd
import datetime as dt
from datetime import date
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt

hoje = dt.date.today()
dez_anos_atras = hoje - relativedelta(years=10) 

#%%
url = (
    f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json"
    f"&dataInicial={dez_anos_atras.strftime('%d/%m/%Y')}"
    f"&dataFinal={hoje.strftime('%d/%m/%Y')}"
)
df = pd.read_json(url)

#%%
df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y")
df["valor"] = pd.to_numeric(df["valor"])

df["Acumulado"] = ((1+df["valor"]/100).cumprod())
df

# %%
df.tail(10)
df["Valor"].plot(title="Evolução Selic", figsize=(12,5))
plt.xlabel("Data")
plt.ylabel("Retorno Acumulado (%)")
plt.tight_layout()
plt.show()


