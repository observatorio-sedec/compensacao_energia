from etl import df_final
import psycopg2
from conexão import conexao

def executar_sql():
    cursor = conexao.cursor()
    cursor.execute('SET search_path TO energia, public')
    
    criando_tabela_estadual = '''
        CREATE TABLE IF NOT EXISTS energia.compensacao_energia (
            NomEstado          VARCHAR,
            NomMunicipio       VARCHAR,
            CodMunicipio       NUMERIC,
            UsinaID            VARCHAR,
            TipoCFURH          VARCHAR,
            UsinaProrrogada    VARCHAR,
            VlrPgMunicipio     NUMERIC,
            VlrPgEstado        NUMERIC,
            VlrPgDemaisEntes   NUMERIC,
            VlrPgANA           NUMERIC,
            VlrPgTotal         NUMERIC,
            data_competencia   DATE,
            data_distribuicao  DATE
        );
    '''
    cursor.execute(criando_tabela_estadual)

    # Opcional: limpar a tabela antes de inserir
    cursor.execute('TRUNCATE energia.compensacao_energia;')

    inserindo_dados = '''
        INSERT INTO energia.compensacao_energia (
            NomEstado, NomMunicipio, CodMunicipio, UsinaID, TipoCFURH,
            UsinaProrrogada, VlrPgMunicipio, VlrPgEstado, VlrPgDemaisEntes,
            VlrPgANA, VlrPgTotal, data_competencia, data_distribuicao
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    '''
    try:
        for idx, i in df_final.iterrows():
            dados = (
                i['NomEstado'],
                i['NomMunicipio'],
                i['CodMunicipio'],
                i['UsinaID'],
                i['TipoCFURH'],
                i['UsinaProrrogada'],
                i['VlrPgMunicipio'],
                i['VlrPgEstado'],
                i['VlrPgDemaisEntes'],
                i['VlrPgANA'],
                i['VlrPgTotal'],
                i['data_competencia'],
                i['data_distribuicao'],
            )
            cursor.execute(inserindo_dados, dados)

        conexao.commit()

    except psycopg2.Error as e:
        print(f"Erro ao inserir dados: {e}")
        conexao.rollback()

    conexao.close()
