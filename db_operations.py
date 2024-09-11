import logging
import pandas as pd

# Configura o logger
logger = logging.getLogger(__name__)

def get_existing_data_from_db(conn):
    """Recupera os dados existentes no banco de dados para validação de dados já inseridos"""
    try:
        query = "SELECT numero_nota, codigo_produto FROM sales_data_with_dates"

        # Executa a consulta
        with conn.cursor() as cursor:
            cursor.execute(query)

        # Recupera todos os resultados da consulta
            rows = cursor.fetchall()    
        #existing_data = pd.read_sql(query, conn)
        existing_data = pd.DataFrame(rows, columns=['numero_nota', 'codigo_produto'])
        logger.info("Dados existentes no banco de dados carregados para validacao com sucesso.")
        return existing_data
    except Exception as e:
        logger.error(f"Erro ao carregar dados para validação do banco de dados: {e}")
        raise
    
def validate_database(df, existing_data, conn):
    # Filtra as linhas que não estão no banco de dados
    #if count != 0:
    merged_df = pd.merge(df, existing_data, on=['numero_nota', 'codigo_produto'], how='left', indicator=True)
    df = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])
    
    #new_data = pd.DataFrame()   
    #df = new_data
    
    if df.empty:
        logger.info("Nenhum dado novo encontrado para ser inserido no banco.")
        
        #return pd.DataFrame()  # Retorna DataFrame vazio se não houver novos dados
    else:
        logger.info(f'Foram encontrados {len(df)} dados que nao estavam no banco de dados')
    # Realiza a validação dos dados filtrados
        
    # Verifica se há duplicatas no novo DataFrame
    duplicates = df[df.duplicated(subset=['numero_nota', 'codigo_produto'], keep=False)]
    if not duplicates.empty:
        logging.warning(f"Registros duplicados encontrados e serão ignorados:\n{duplicates}")
        df = df.drop_duplicates(subset=['numero_nota', 'codigo_produto'], keep='first')
    #df.to_excel('new_data.xlsx')
    return df
    
def insert_data(df, cursor):
  # Insere os dados não duplicados no banco de dados.
  #df.to_excel('fetch.xlsx')
  if df.empty:
      logger.warning('Dados ja estao no banco, e nao serao inseridos novos dados no banco. ')

  else:
      
      for index, row in df.iterrows():
        # Verifica se o registro já existe no banco de dados.
        cursor.execute("""
            SELECT 1 FROM sales_data_with_dates 
            WHERE numero_nota = %s AND codigo_produto = %s
        """, (row['numero_nota'], row['codigo_produto']))
  
      try:
         # Se o registro não existir, insere o df no banco de dados.     
         if cursor.fetchone() is None:
             for index, row in df.iterrows():
                 cursor.execute("""
                INSERT INTO sales_data_with_dates (data_venda, numero_nota, codigo_produto, descricao_produto, 
                                    codigo_cliente, descricao_cliente, valor_unitario_produto, 
                                    quantidade_vendida_produto, valor_total, custo_da_venda, 
                                    valor_tabela_de_preco_do_produto)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (row['data_venda'], row['numero_nota'], row['codigo_produto'], row['descricao_produto'],
                row['codigo_cliente'], row['descricao_cliente'], row['valor_unitario_produto'],
                row['quantidade_vendida_produto'], row['valor_total'], row['custo_da_venda'],
                row['valor_tabela_de_preco_do_produto']
              ))
        
             logging.info(f'{len(df)} foram inseridos no banco com sucesso') 
         else:
           logging.info(f"Foram encontrados {len(df)} registros existententes, que nao serao inseridos no banco.")
      except Exception as e:
         raise e 