import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
import pandas as pd

dfs = []

for ano in range(2019, 2025):
    df = pd.read_excel(f"Planilhas baixadas/CMPF, Royalties e Beneficiários {ano}.xlsx")
    dfs.append(df)
df_final = pd.concat(dfs, ignore_index=True)


df_final['data_competencia'] = '01/' + df_final['AnoMesCompetencia'].astype(str).str[4:6] + '/' + df_final['AnoMesCompetencia'].astype(str).str[0:4]
df_final['data_distribuicao'] = '01/' + df_final['AnoMesDistribuicao'].astype(str).str[4:6] + '/' + df_final['AnoMesDistribuicao'].astype(str).str[0:4]


df_final['data_competencia'] = df_final['data_competencia'].replace('01//nan', None)
df_final['data_distribuicao'] = df_final['data_distribuicao'].replace('01//nan', None)


df_final['data_competencia'] = pd.to_datetime(df_final['data_competencia'], format='%d/%m/%Y', errors='coerce')
df_final['data_distribuicao'] = pd.to_datetime(df_final['data_distribuicao'], format='%d/%m/%Y', errors='coerce')


df_final = df_final.drop(columns=['VlrPgDolarEstado', 'VlrPgDolarDemaisEntes', 'VlrPgDolarTotal', 'VlrPgDolarMunicipio', 'AnoMesCompetencia', 'AnoMesDistribuicao'])

df_final['UsinaID'] = df_final['UsinaID'].str.replace(r"\s*\(.*?\)", "", regex=True)

df_final = df_final.iloc[:-1]
df_final = df_final[df_final['NomEstado'].notna()]
df_final = df_final[~df_final['NomEstado'].str.contains('Filtros aplicados:', na=False)]
print(df_final[['data_competencia', 'data_distribuicao']])
print(df_final.info())
print(df_final)


if __name__ == '__main__':
    from sql import executar_sql
    executar_sql()