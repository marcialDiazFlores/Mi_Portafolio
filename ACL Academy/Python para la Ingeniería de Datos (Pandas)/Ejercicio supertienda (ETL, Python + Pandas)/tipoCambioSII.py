import pandas as pd
import requests
from bs4 import BeautifulSoup

def valorDolarSII(url, ultimoValorConocido, anio):

    page = requests.get(url)
    page.encoding = 'utf-8'
    soup = BeautifulSoup(page.text, 'lxml')

    if anio >= 2013:
        table_data  = soup.find("table", attrs={"id": "table_export", "class":"table table-hover table-bordered"})
        table_headers_row = table_data.find("thead").find("tr")

    if anio < 2013:
        table_data  = soup.find("table", attrs={"class":"tabla"})
        table_headers_row = table_data.find("thead").find("tr")

    headers = []
    for i in table_headers_row.find_all('th'):
        title = i.text
        headers.append(title)

    headers2 = headers
    for i in range(1,len(table_headers_row.find_all('th'))):
        headers2[i] = str(i).zfill(2)

    df_dolar = pd.DataFrame(columns = headers)

    for j in table_data.find_all('tr')[1:]:
            row_data = j.find_all()
            row = [tr.text for tr in row_data]
            length = len(df_dolar)
            df_dolar.loc[length] = row

    df_dolar.drop(df_dolar.tail(1).index,inplace=True)

    df_dolar_unpivoted = df_dolar.melt(id_vars=['Día'], var_name='Mes', value_name='Valor')
    df_dolar_unpivoted["Año-Mes-Día"] = str(anio)+"-"+df_dolar_unpivoted["Mes"]+"-"+df_dolar_unpivoted["Día"].apply(lambda x: '{0:0>2}'.format(x))
    df_dolar_unpivoted['Año-Mes-Día'] = pd.to_datetime(df_dolar_unpivoted['Año-Mes-Día'], errors='coerce')
    df_dolar_unpivoted = df_dolar_unpivoted.dropna(subset=['Año-Mes-Día'])
    df_dolar_unpivoted = df_dolar_unpivoted.drop(['Día','Mes'], axis=1)
    df_dolar_unpivoted = df_dolar_unpivoted.sort_values(by=['Año-Mes-Día'], ascending=True).copy()

    df_dolar_unpivoted['Valor'].iloc[0] = ultimoValorConocido

    df_dolar_unpivoted['Valor'] = df_dolar_unpivoted['Valor'].replace("", pd.NA   )
    df_dolar_unpivoted['Valor'] = df_dolar_unpivoted['Valor'].replace(" ", pd.NA   )
    df_dolar_unpivoted['Valor'] = df_dolar_unpivoted['Valor'].replace('\xa0', pd.NA   )
    df_dolar_unpivoted['Valor'] = df_dolar_unpivoted['Valor'].fillna(method='ffill')
    df_dolar_unpivoted['Valor'] = df_dolar_unpivoted['Valor'].str.replace("." , "")
    df_dolar_unpivoted['Valor'] = df_dolar_unpivoted['Valor'].str.replace("," , ".")
    df_dolar_unpivoted['Valor'] = pd.to_numeric(df_dolar_unpivoted['Valor'])
    df_dolar_unpivoted.columns = ['USD_to_CLP', 'Fecha']
    return df_dolar_unpivoted


