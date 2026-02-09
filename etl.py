import polars as pl
from datetime import datetime
...

dfs = []
ano_atual = datetime.now().year
for ano in range(2019, ano_atual):
  try:
    df = pl.read_excel(f"Planilhas baixadas/CMPF, Royalties e Beneficiários {ano}.xlsx")
    print(f"Arquivo de {ano} lido com sucesso.")
    dfs.append(df)
  except Exception as e:
    print(f"Erro ao ler o arquivo de {ano}: {e}")

if not dfs:
  raise RuntimeError("Nenhum arquivo foi lido com sucesso.")

df_final = pl.concat(dfs)

# Criação das datas
df_final = df_final.with_columns([
  (pl.lit("01/") + pl.col("AnoMesCompetencia").cast(pl.Utf8).str.slice(4, 2) + "/" + pl.col("AnoMesCompetencia").cast(pl.Utf8).str.slice(0, 4)).alias("data_competencia"),
  (pl.lit("01/") + pl.col("AnoMesDistribuicao").cast(pl.Utf8).str.slice(4, 2) + "/" + pl.col("AnoMesDistribuicao").cast(pl.Utf8).str.slice(0, 4)).alias("data_distribuicao")
])

# Substitui '01//nan' por None
df_final = df_final.with_columns([
  pl.when(pl.col("data_competencia") == "01//nan").then(None).otherwise(pl.col("data_competencia")).alias("data_competencia"),
  pl.when(pl.col("data_distribuicao") == "01//nan").then(None).otherwise(pl.col("data_distribuicao")).alias("data_distribuicao")
])

df_final = df_final.with_columns([
    pl.col("data_competencia").str.strptime(pl.Date, "%d/%m/%Y", strict=False).alias("data_competencia"),
    pl.col("data_distribuicao").str.strptime(pl.Date, "%d/%m/%Y", strict=False).alias("data_distribuicao")
])

df_final = df_final.drop([
  "VlrPgDolarEstado",
  "VlrPgDolarDemaisEntes",
  "VlrPgDolarTotal",
  "VlrPgDolarMunicipio",
  "AnoMesCompetencia",
  "AnoMesDistribuicao"
])

# Limpeza e filtros adicionais
df_final = df_final.with_columns([
    pl.col("UsinaID")
    .str.replace_all(r"\s*\([^)]*\)", "", ) # remove todos os parênteses e conteúdo, global
    .str.replace(r"\s+", " ", literal=False) # normaliza espaços múltiplos
    .str.strip_chars() # remove espaços no início/fim
    .alias("UsinaID")
])
df_final = df_final.slice(0, df_final.height - 1)
df_final = df_final.filter(pl.col("NomEstado").is_not_null())
df_final = df_final.filter(~pl.col("NomEstado").str.contains("Filtros aplicados:"))

print(df_final.select(["data_competencia", "data_distribuicao"]))
print(df_final)
# df_final.write_excel("df_final.xlsx")

if __name__ == '__main__':
  from sql import executar_sql
  executar_sql()