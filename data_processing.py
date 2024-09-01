import pandas as pd
import logging

logger = logging.getLogger(__name__)

def read_excel(file_path):
    try:
        df = pd.read_excel(file_path)
        logger.info("Planilha lida com sucesso.")
        return df
    except Exception as e:
        logger.error(f"Erro ao ler a planilha: {e}")
        raise

def validate_data(df):
    #Verifica se todas as colunas estão presente
    required_columns = [
        'data_venda', 'numero_nota', 'codigo_produto', 'descricao_produto',
        'codigo_cliente', 'descricao_cliente', 'valor_unitario_produto',
        'quantidade_vendida_produto', 'valor_total', 'custo_da_venda',
        'valor_tabela_de_preco_do_produto'
    ]

    for col in required_columns:
        if col not in df.columns:
            logger.error(f"Coluna ausente: {col}")
            raise ValueError(f"Coluna ausente: {col}")
    
    df['data_venda'] = pd.to_datetime(df['data_venda'].str.strip(), format='%d/%m/%Y', errors='coerce')
    numeric_columns = [
        'numero_nota', 'valor_unitario_produto', 'quantidade_vendida_produto', 
        'valor_total', 'custo_da_venda', 'valor_tabela_de_preco_do_produto'
    ]
    #Validação das colunas númericas
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Verifica duplicatas e valores nulos
    duplicates = df[df.duplicated(subset=['numero_nota', 'codigo_produto'], keep=False)]
    if not duplicates.empty:
        logger.warning(f"Registros duplicados encontrados e serao ignorados:\n {duplicates}")
        df = df.drop_duplicates(subset=['numero_nota', 'codigo_produto'], keep='first')

    if df.isnull().any().any():
        null_rows = df[df.isnull().any(axis=1)]
        logger.warning(f"Registros com valores nulos encontrados e serao ignorados:\n{null_rows}")
        df = df.dropna()

        
    logger.info(f"Validacao concluída. Total de registros apos validacao: {len(df)}")
    
    
    return df

def transform_data(df):
    #Tranformando o formato data para ser inserida no banco de dados
    df['data_venda'] = pd.to_datetime(df['data_venda'], format='%Y-%m-%d', errors='coerce')
    logger.info("Transformacao dos dados concluida.")
    
    return df
