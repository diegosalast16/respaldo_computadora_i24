import pandas as pd

data = pd.read_csv('C:\\Users\\diego.trejo\\Downloads\\tuvis_feb_2025.csv', dtype={"Datetime": str})
df = pd.DataFrame(data)

df['FechayHora'] = df['Datetime'].str.split(" ").str[0]
df['FechayHora'] = pd.to_datetime(df['FechayHora'], format='%d/%m/%Y', errors='coerce')
df['Fecha'] = df['FechayHora'].dt.date

df_dupl = df.drop(columns =['Datetime','FechayHora']).drop_duplicates(subset=['Tuvis Thread Message: Nombre del propietario','Related Account','Related Lead','Fecha'])
print(df.head(10))

df_dupl.to_csv('C:\\Users\\diego.trejo\\Downloads\\TuvisClean_feb_2025.csv', index=False)
