from config import get_db_connection
from data_processing import read_excel, validate_data, transform_data
from db_operations import insert_data, validate_database, get_existing_data_from_db
from loggin_setup import setup_logging

# Configura os logs
logger = setup_logging()

file_path = 'sales_data_with_dates.xlsx' #Informar o caminho do arquivo

def main():
    conn = None
    cursor = None
    try:
        # Ler e processar os dados da planilha
        df = read_excel(file_path)
        df = validate_data(df)
        df = transform_data(df)
        

        # Conectar ao banco de dados
        conn, cursor = get_db_connection()

        #Verifica se há dados que já foram inseridos no banco
        get_existing_data_from_db(conn)

        #Compara os dados do dataframe com o banco de dados
        existing_data = get_existing_data_from_db(conn)
        df = validate_database(df, existing_data, conn)

        

        # Inserir os dados no banco
        insert_data(df, cursor)

        # Confirmar as mudanças
        conn.commit()
        
    except Exception as e:
        logger.error(f'Erro durante a execucao: {e}')
       
        if conn:
            conn.rollback()
    finally:
         if cursor:
             cursor.close()
         if conn:
             conn.close()

if __name__ == "__main__":
    main()
