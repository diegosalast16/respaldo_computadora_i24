import pandas as pd
import re
import unicodedata

dataNoPublicadores = pd.read_csv('C:\\Users\\diego.trejo\\Downloads\\No_publicadores.csv') #Esta data se descargó en xlsx, se convirtió a csv en python y se limipió antes en sheets
dataPublicadores = pd.read_csv('C:\\Users\\diego.trejo\\Downloads\\Publicadores_5a.csv')
dataCuentas = pd.read_csv('C:\\Users\\diego.trejo\\Downloads\\cuentas.csv')
dataContactos = pd.read_csv('C:\\Users\\diego.trejo\\Downloads\\contactos.csv')
dataContactos = dataContactos[dataContactos['Organizaciones de venta'].str.contains('MX', na=False)]
dataContactos2 = pd.read_csv('C:\\Users\\diego.trejo\\Downloads\\contactos2.csv')
dataListings = pd.read_csv('C:\\Users\\diego.trejo\\Downloads\\listings_vigentes_5a.csv')
dataListingsVigentes = pd.read_csv('C:\\Users\\diego.trejo\\Downloads\\listing_vigentes_agrupado_5a.csv')
dataListingsTotal = pd.read_csv('C:\\Users\\diego.trejo\\Downloads\\listing_total_agrupado_5a.csv')
dataListasPrecios = pd.read_csv('C:\\Users\\diego.trejo\\Downloads\\listas_precios.csv')

dfnp = pd.DataFrame(dataNoPublicadores)
dfp = pd.DataFrame(dataPublicadores)
dfcuentas = pd.DataFrame(dataCuentas)   
dfcontactos = pd.DataFrame(dataContactos)
dfcontactos2 = pd.DataFrame(dataContactos2)
dflistings = pd.DataFrame(dataListings)
dflistings2 = pd.DataFrame(dataListingsVigentes)
dflistingsTotal = pd.DataFrame(dataListingsTotal)
df_listas = pd.DataFrame(dataListasPrecios)

delta_avisos = 5
delta_fecha = 90

def ordenar_competencia(valor):
    if pd.isna(valor):
        return valor
    return ", ".join(sorted(valor.split(", ")))

for df in [dfp, dfnp]:
    df['Donde Publica Competencia'] = df['Donde Publica Competencia'].apply(ordenar_competencia)

def limpiar_ciudad(df, columns):
    for col in columns:
        if col in df.columns:
            df[col] = df[col].fillna('').astype(str).str.lower()
            df[col] = df[col].apply(lambda x: re.sub(r'[^a-záéíóúüñ\s]', '', x))  
            df[col] = df[col].apply(lambda x: unicodedata.normalize('NFKD', x).encode('ascii', 'ignore').decode('utf-8'))
            df[col] = df[col].apply(lambda x: re.sub(r'\s+', ' ', x).strip())  
    return df

df_ciudades = [
    (dflistings2, ['ciudad', 'provincia']),
    (dflistingsTotal, ['ciudad', 'provincia']),
    (dfnp, ['ciudad', 'provincia']),
    (dfp, ['ciudad', 'provincia'])
    ]

for df, col_ciudad in df_ciudades:
    limpiar_ciudad(df, col_ciudad)

def limpiar_publicador(df, columns):
    for col in columns:
        if col in df.columns:
            df['id_publicador'] = df[col].astype(str).str.extract(r"(\d+)")
            df['name_publicador'] = df[col].astype(str).str.extract(r"([a-zA-Z\s]+)")
    return df

df_publicador = [(dfp, ['Publicador']), (dfnp, ['Publicador'])]

for df, col_publicador in df_publicador:
    limpiar_publicador(df, col_publicador)

def limpiar_telefono(df_tel, columns):
    for col in columns:
        if col in df_tel.columns:
            df_tel[col] = df_tel[col].astype(str).str.replace(r'[^0-9]', '', regex=True).str[-10:]
    return df_tel

df_telefonos = [
    (dfnp, ['Telefono', 'Telefono_2']),
    (dfp, ['Telefono', 'Telefono_2']),
    (dfcuentas, ['Telefono', 'ID Empresa portal']),
    (dfcontactos, ['Telefono', 'Celular']),
    (dfcontactos2, ['Telefono', 'Telefono_2', 'Celular']),
    (dflistings, ['telefono_principal', 'telefono_salesforce', 'telefono_os', 'telefono_portal'])
    ]

for df_tel, cols_tel in df_telefonos:
    limpiar_telefono(df_tel, cols_tel)
    
def verificar_telefonos(df_tel, cols):
    checks = df_tel[cols].applymap(lambda x: pd.notna(x) and x.strip() != '' and x in telefonos)
    df_tel['check'] = checks.any(axis=1)  #Al menos una columna tiene coincidencia
    return df_tel
    
sets_telefonos = [
    set(dfcuentas['Telefono'].dropna()),
    set(dfcontactos['Telefono'].dropna()),
    set(dfcontactos['Celular'].dropna()),
    set(dfcontactos2['Telefono'].dropna()),
    set(dfcontactos2['Telefono_2'].dropna()),
    set(dfcontactos2['Celular'].dropna()),
    set(dflistings['telefono_principal'].dropna()),
    set(dflistings['telefono_salesforce'].dropna()),
    set(dflistings['telefono_os'].dropna()),
    set(dflistings['telefono_portal'].dropna())
    ]

telefonos = set().union(*sets_telefonos)
empresas_set = set(dflistings['empresaid'].dropna())

for df_tel, cols_tels in df_telefonos:
    verificar_telefonos(df_tel, cols_tels)

dfp['empresa_check'] = dfp['ID Empresa Activadora'].map(lambda x: (pd.notna(x) and str(x).strip() != '' and (x in empresas_set)))
dfp['check'] = dfp['check'] | dfp['empresa_check']

dfnp = dfnp.drop(columns=['Publicador Tipo', 'ID Empresa Publicador', 'Tipo Servicio', 'Donde Publica 5A', 'CRECI'])
dfp = dfp.drop(columns=['Publicador','Publicador Tipo', 'ID Empresa Publicador', 'Tipo Servicio', 'CRECI', 'empresa_check', 'id_publicador'])

df_totales = dfp.merge(dflistingsTotal, 
                    left_on=['ID Empresa Activadora', 'provincia', 'ciudad'], 
                    right_on=['empresaid', 'provincia', 'ciudad'], 
                    how='left')

df_prov_totales = dfp.merge(dflistingsTotal, 
                    left_on=['ID Empresa Activadora', 'provincia'], 
                    right_on=['empresaid', 'provincia'],
                    how="inner").rename(columns={'ciudad_x': 'ciudad'})

df_ciudad_totales = dfp.merge(dflistingsTotal, 
                    left_on=['ID Empresa Activadora', 'ciudad'], 
                    right_on=['empresaid', 'ciudad'], 
                    how="inner").rename(columns={'provincia_x': 'provincia'})

df_general = dfp.merge(dflistingsTotal, 
                    left_on=['ID Empresa Activadora'], 
                    right_on=['empresaid'], 
                    how="inner")

dfp_agrupado = dfp.groupby(['ID Empresa Activadora']).agg(
                    listings_competencia = ('Listings en Competencia', 'sum'),
                    tel_1 = ('Telefono', 'max'),
                    tel_2 = ('Telefono_2', 'max'),
                    donde_publica = ('Donde Publica Competencia', lambda x: ', '.join(sorted(set(x.dropna().astype(str)))))
                    ).dropna().reset_index()

dfListings_agrupado = dflistingsTotal.groupby(['empresaid', 'tipo_cliente', 'flag_bloqueado']).agg(
                    suma_listings = ('listings', 'sum'), 
                    suma_stock = ('stock', 'max'), 
                    fila = ('fila', 'max')
                    ).dropna().reset_index()

df_agrupado_final = dfp_agrupado.merge(dfListings_agrupado, 
                    left_on=['ID Empresa Activadora'], 
                    right_on=['empresaid'], 
                    how='left')

dfcuentas['Telefono'] = dfcuentas['Telefono'].replace("", None) 
dfcuentas['Telefono'] = dfcuentas['Telefono'].fillna('-')

dfcuentas_agrupado = dfcuentas.groupby(['ID Empresa portal']).agg(
                    tel_1 = ('Telefono', 'max')
                    ).dropna().reset_index()

df_agrupado_final['listings_locales'] = df_agrupado_final['suma_stock']
df_agrupado_final['dif_total'] = df_agrupado_final['listings_competencia'] - df_agrupado_final['listings_locales']

for df in [df_totales, df_prov_totales, df_ciudad_totales, df_general, dfListings_agrupado, df_agrupado_final]:
    df['fila'] = pd.to_datetime(df['fila'], errors='coerce')
    
df_agrupado_final['dif_fecha'] = (pd.Timestamp.now() - df_agrupado_final['fila']).dt.days

df_upsell = df_agrupado_final[(df_agrupado_final['dif_total'] >= delta_avisos) & 
                              (df_agrupado_final['dif_fecha'] < delta_fecha) & 
                              (df_agrupado_final['flag_bloqueado'] == False)
                              ]

df_upsell['ID Empresa Activadora'] = pd.to_numeric(df_upsell['ID Empresa Activadora'], errors='coerce')
dfcuentas_agrupado['ID Empresa portal'] = pd.to_numeric(dfcuentas_agrupado['ID Empresa portal'], errors='coerce')
df_listas['idempresa'] = pd.to_numeric(df_listas['idempresa'] , errors='coerce')

df_upsell = df_upsell.merge(dfcuentas_agrupado,
                    left_on=['ID Empresa Activadora'], 
                    right_on=['ID Empresa portal'], 
                    how="left").rename(columns={'tel_1_x': 'tel_1', 'tel_1_y': 'tel_3'})
df_upsell['Telefono_Final'] = df_upsell['tel_3'].fillna(df_upsell['tel_1']).fillna(df_upsell['tel_2'])
df_upsell = df_upsell.merge(df_listas,
                            left_on=['ID Empresa Activadora'],
                            right_on=['idempresa'],
                            how='left')
df_upsell = df_upsell.drop(columns=['empresaid', 'flag_bloqueado', 'suma_listings', 'suma_stock', 'fila', 'dif_fecha', 'ID Empresa portal', 'tel_1', 'tel_2', 'tel_3', 'idempresa'])
df_upsell_final = df_upsell[df_upsell['region'].notna()]
df_upsell_final.to_csv('C:\\Users\\diego.trejo\\Downloads\\upsell.csv', index=False)

df_recovery = df_agrupado_final[(df_agrupado_final['dif_fecha'] >= delta_fecha) & 
                                (df_agrupado_final['flag_bloqueado'] == False)
                                ]

df_recovery = df_recovery.drop(columns=['empresaid', 'flag_bloqueado', 'suma_listings', 'suma_stock', 'fila', 'dif_fecha'])
bloqueados = df_agrupado_final[(df_agrupado_final['flag_bloqueado'] == True)]
bloqueados = bloqueados['empresaid'].nunique()

def analizar(df, group_by, col_name):
    listas = {"stock": [], "listings": []}

    for valor in df[group_by].unique():
        df_filtrado = df[df[group_by] == valor]
        group = df_filtrado.groupby(['ID Empresa Activadora', group_by]).agg(
            fecha_max=('fila', 'max'),
            listings_competencia=('Listings en Competencia', 'sum'),
            listings=('listings', 'sum'),
            stock=('stock', 'max')
            ).reset_index()

        for tipo, key in [("stock", "delta_stock"), ("listings", "delta_listings")]:
            group[key] = group['listings_competencia'] - group[tipo].replace(0, pd.NA)
            datos = pd.Series(sorted(set(group[key].dropna())))
            if not datos.empty:
                listas[tipo].append({"Categoria": valor, 
                                     "Q1": datos.quantile(0.25), 
                                     "Q2": datos.median(), 
                                     "Q3": datos.quantile(0.75),
                                     "Min": datos.min(), 
                                     "Max": datos.max(), 
                                     "Media": datos.mean()})

    return pd.DataFrame(listas["stock"]), pd.DataFrame(listas["listings"])

df_competencia_stock, df_competencia_listings = analizar(df_totales, 'Donde Publica Competencia', 'Competencia')
df_provincia_stock, df_provincia_listings = analizar(df_prov_totales, 'provincia', 'Provincia')
df_ciudad_stock, df_ciudad_listings = analizar(df_ciudad_totales, 'ciudad', 'Ciudad')

for nombre, df in {"stock_vs_competencia": df_competencia_stock, 
                   "listings_vs_competencia": df_competencia_listings,
                   "stock_vs_provincia": df_provincia_stock, 
                   "listings_vs_provincia": df_provincia_listings,
                   "stock_vs_ciudad": df_ciudad_stock, 
                   "listings_vs_ciudad": df_ciudad_listings}.items():
    df.to_csv(f'C:\\Users\\diego.trejo\\Downloads\\{nombre}.csv', index=False)

dfp_grouped_empresas = df_totales.groupby(['ID Empresa Activadora']).agg(
                        fecha_max = ('fila', 'max'), 
                        listings_competencia = ('Listings en Competencia', 'sum'), 
                        listings = ('listings', 'sum'), 
                        stock = ('stock','max')
                        ).reset_index()

lista_np = dfnp['Publicador'].dropna().nunique()
df_np = dfnp[(dfnp['check'] == False)]
df_np['Telefono_final'] = dfnp['Telefono'].fillna(dfnp['Telefono_2']).replace('',None).fillna('-')
df_np = df_np[(df_np['Telefono_final'] != '-')]

dfnp_provincia_principal = df_np.groupby('id_publicador')['provincia'].apply(lambda x: x.mode()[0]).reset_index(name='provincia_principal')
dfnp_ciudad_principal = df_np.groupby('id_publicador')['ciudad'].apply(lambda x: x.mode()[0]).reset_index(name='ciudad_principal')

df_np_grouped = df_np.groupby(['id_publicador','name_publicador', 'Telefono_final']).agg(
    listings=('Listings en Competencia', 'sum'),
    donde_publica = ('Donde Publica Competencia', lambda x: ', '.join(sorted(set(x.dropna().astype(str)))))
    ).reset_index()

df_np_grouped = df_np_grouped.merge(dfnp_provincia_principal,
                                    on='id_publicador',
                                    how='left')
df_np_grouped = df_np_grouped.merge(dfnp_ciudad_principal,
                                    on='id_publicador',
                                    how='left') 
df_np_grouped.to_csv('C:\\Users\\diego.trejo\\Downloads\\venta_nueva.csv', index=False)
