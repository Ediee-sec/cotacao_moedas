import psycopg2
from Transform import TransformData
import configparser

class Credentials:
    """
    Classe para obter as credenciais do banco de dados
    """
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('config/db.ini')
        self.host = self.config['postgresql']['host']
        self.port = self.config['postgresql']['port']
        self.database = self.config['postgresql']['database']
        self.user = self.config['postgresql']['user']
        self.password = self.config['postgresql']['password']


class LoadData(Credentials):
    """
    Classe para carregar os dados no banco de dados
    """
    def __init__(self):
        super().__init__()
        self.df = TransformData().main()

    def create_connection(self):
        """
        Cria uma conexão com o banco de dados
        Retorna: Conexão e cursor
        """
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )
        cursor = conn.cursor()
        return conn, cursor
    
    def create_table(self):
        """
        Cria a tabela no banco de dados
        """
        conn, cursor = self.create_connection()
        cursor.execute("CREATE TABLE IF NOT EXISTS USD_BRL (moeda_origem VARCHAR(10), moeda_destino VARCHAR(10), nome VARCHAR(100), maior_cotacao FLOAT, menor_cotacao FLOAT, variacao FLOAT, percentual_variacao FLOAT, cotacao_compra FLOAT, cotacao_venda FLOAT, data_hora TIMESTAMP, data_criacao TIMESTAMP, data_processamento TIMESTAMP, PRIMARY KEY (moeda_origem, moeda_destino, data_hora))")
        conn.commit()
        cursor.close()
        conn.close()

    def insert_data(self):
        """
        Insere os dados na tabela
        """
        conn, cursor = self.create_connection()
        cursor.executemany("INSERT INTO USD_BRL (moeda_origem, moeda_destino, nome, maior_cotacao, menor_cotacao, variacao, percentual_variacao, cotacao_compra, cotacao_venda, data_hora, data_criacao, data_processamento) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", self.df.values.tolist())
        conn.commit()
        cursor.close()
        conn.close()

    def main(self):
        """
        Método principal para carregar os dados no banco de dados
        """
        self.create_table()
        self.insert_data()
    
