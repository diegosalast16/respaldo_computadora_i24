import re
import unicodedata
import heapq
import pandas as pd

venta_nueva = pd.read_csv('C:\\Users\\diego.trejo\\Downloads\\leads.csv')
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
    (leads_df, ['provincia_principal']),
    (ejecutivos_df, ['Estado'])
    ]

for df, col_ciudad in df_ciudades:
    limpiar_ciudad(df, col_ciudad)

def asignar_leads_por_equipo(leads_df, ejecutivos_df):
    asignaciones = []
    
    # Agrupar ejecutivos por equipo, provincia y tipo de lead
    ejecutivos_por_grupo = ejecutivos_df.groupby(['Team', 'provincia_principal', 'tipo_lead'])['Ejecutivo'].apply(list).to_dict()
    
    # Agrupar leads por equipo y provincia
    leads_por_equipo = leads_df.groupby(['Team', 'provincia_principal'])
    
    for (equipo, provincia), leads in leads_por_equipo:
        
        # Filtrar leads en dos categorías
        leads_mayor_30 = leads[leads['listings'] > 30]
        leads_menor_30 = leads[leads['listings'] <= 30]
        
        for tipo_lead, leads_filtrados in [('Mayor a 30 propiedades', leads_mayor_30), ('Menor a 30 propiedades', leads_menor_30)]:
            if (equipo, provincia, tipo_lead) not in ejecutivos_por_grupo:
                continue  # Si no hay ejecutivos para este grupo, se omite
            
            ejecutivos = ejecutivos_por_grupo[(equipo, provincia, tipo_lead)]
            carga_ejecutivos = [(0, ejecutivo) for ejecutivo in ejecutivos]
            heapq.heapify(carga_ejecutivos)
            
            # Ordenar leads por cantidad de listings de mayor a menor
            leads_sorted = leads_filtrados.sort_values(by='listings', ascending=False)
            
            for _, row in leads_sorted.iterrows():
                lead_id, cantidad_listings = row['id_publicador'], row['listings']
                carga_actual, ejecutivo = heapq.heappop(carga_ejecutivos)
                asignaciones.append([equipo, provincia, tipo_lead, ejecutivo, lead_id, cantidad_listings])
                heapq.heappush(carga_ejecutivos, (carga_actual + cantidad_listings, ejecutivo))
    
    return pd.DataFrame(asignaciones, columns=['Team', 'provincia_principal', 'tipo_lead', 'Ejecutivo', 'id_publicador', 'listings'])

# Realizar asignación por equipo, provincia y tipo de lead
asignacion_df = asignar_leads_por_equipo(leads_df, ejecutivos_df)

# Guardar resultado en CSV
asignacion_df.to_csv('C:\\Users\\diego.trejo\\Downloads\\Asignacion_Leads.csv', index=False)

print("Asignación guardada en 'Asignacion_Leads.csv'")
