# %%
import os
import requests
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

# %%
# ===== Passo 1: Autenticação =====
url_auth = "https://data.maisretorno.com/auth"

credenciais = {
    "grant_type": "password",
    "username": os.getenv("MAISRETORNO_USERNAME"),
    "password": os.getenv("MAISRETORNO_PASSWORD"),
    "client_id": os.getenv("MAISRETORNO_CLIENT_ID"),
}

auth_response = requests.post(url_auth, data=credenciais)

print("\n--- Resposta da autenticação ---")
print("\nStatus code:", auth_response.status_code)
print("\nContent-Type:", auth_response.headers.get("Content-Type"))
print("\nCorpo:", auth_response.text[0:30])

# Garante que a autenticação foi bem-sucedida antes de continuar
auth_response.raise_for_status()

# Extrai o token do JSON
token = auth_response.json()["access_token"]
print(f"\nToken obtido: {token[:20]}...")  # mostra só o início, por segurança

# %%
# ===== Passo 2: Consulta usando o token =====
url_busca = "https://data.maisretorno.com/mr-data/v4/api/search/18599388000154"

headers = {
    "Authorization": f"Bearer {token}",  # ← agora usa a string do token, não o objeto
}

params = {}
params["has_quotes"] = "true"

search_response = requests.get(url_busca, headers=headers, params=params)

print("\n--- Resposta da busca ---")
print("Status code:", search_response.status_code)
print("Content-Type:", search_response.headers.get("Content-Type"))

search_response.raise_for_status()
dados = search_response.json()
print("\nDados retornados:")
dados

# %% 

url_quotes = f"https://data.maisretorno.com/mr-data/v4/api/quotes/{dados[0]["identifier"]}"

params={}

quotes_response = requests.get(url_quotes, headers=headers, params=params)

print(f"\n--- Resultado Encontrato ---")

cotacoes = quotes_response.json()

df = pd.DataFrame(cotacoes["quotes"])
df = df.rename(columns={
    "d": "data",
    "c": "cota",
    "p": "patrimonio_liquido",
    "q": "quantidade"
})
df["data"] = pd.to_datetime(df["data"])
df = df.set_index("data").sort_index()
df.head()

# %%
df["retorno_diario"] = df["cota"].pct_change()
df["retorno_acumulado"] = (1 + df["retorno_diario"]).cumprod() -1
df