import re
import unicodedata
import heapq
import pandas as pd

venta_nueva = pd.read_csv('C:\\Users\\diego.trejo\\Downloads\\venta_nueva.csv')
ejecutivos = pd.read_csv('C:\\Users\\diego.trejo\\Downloads\\Ejecutivos.csv')

leads_df = pd.DataFrame(venta_nueva)
ejecutivos_df = pd.DataFrame(ejecutivos)

def limpiar_ciudad(df, columns):
    for col in columns:
        if col in df.columns:
            df[col] = df[col].fillna('').astype(str).str.lower()
            df[col] = df[col].apply(lambda x: re.sub(r'[^a-záéíóúüñ\s]', '', x))  
            df[col] = df[col].apply(lambda x: unicodedata.normalize('NFKD', x).encode('ascii', 'ignore').decode('utf-8'))
            df[col] = df[col].apply(lambda x: re.sub(r'\s+', ' ', x).strip())  
    return df

df_ciudades = [
    (leads_df, ['provincia principal']),
    (ejecutivos_df, ['Estado'])
    ]

for df, col_ciudad in df_ciudades:
    limpiar_ciudad(df, col_ciudad)

def asignar_leads_por_provincia(leads_df, ejecutivos_df):
    asignacion = {}
    
    ejecutivos_por_provincia = ejecutivos_df.groupby('Estado')['Ejecutivo'].apply(list).to_dict()
    
    leads_por_provincia = leads_df.groupby('provincia_principal')
    
    for provincia, leads in leads_por_provincia:
        if provincia not in ejecutivos_por_provincia:
            continue  # Si no hay ejecutivos para esta provincia, se omite
        
        ejecutivos = ejecutivos_por_provincia[provincia]
        carga_ejecutivos = [(0, ejecutivo) for ejecutivo in ejecutivos]
        heapq.heapify(carga_ejecutivos)
        
        asignacion[provincia] = {ejecutivo: [] for ejecutivo in ejecutivos}
        
        leads_sorted = leads.sort_values(by='listings', ascending=False)
        
        for _, row in leads_sorted.iterrows():
            lead_id, cantidad_listings = row['id_publicador'], row['listings']
            carga_actual, ejecutivo = heapq.heappop(carga_ejecutivos)
            asignacion[provincia][ejecutivo].append((lead_id, cantidad_listings))
            heapq.heappush(carga_ejecutivos, (carga_actual + cantidad_listings, ejecutivo))
    
    return asignacion

asignacion_resultado = asignar_leads_por_provincia(leads_df, ejecutivos_df)

# Mostrar resultado
for provincia, ejecutivos in asignacion_resultado.items():
    print(f"Provincia: {provincia}")
    for ejecutivo, leads_asignados in ejecutivos.items():
        total_listings = sum(l[1] for l in leads_asignados)
        print(f"  {ejecutivo} - Total Listings: {total_listings}\n  Leads: {leads_asignados}\n")
