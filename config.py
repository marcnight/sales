import psycopg2

def get_db_connection():

    user = input('Informe seu usuario do banco: ')
    password = input('Informe sua senha do banco: ')
    
    
    conn = psycopg2.connect(
        host = "localhost", #Informar o host
        port = "5432",      #Informar a porta do banco
        database = "postgres", #Informar o banco de dados
        user = user,
        password = password

    )

    return conn, conn.cursor()