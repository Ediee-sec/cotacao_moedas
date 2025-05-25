import psycopg2
from Transform import TransformData
import os
from logger import ETLLogger

class Credentials:
    """
    Classe para obter as credenciais do banco de dados a partir de uma conexão do Airflow
    """
    def __init__(self):
        try:
            self.host = os.environ.get('POSTGRES_HOST')
            self.port = os.environ.get('POSTGRES_PORT')
            self.database = os.environ.get('POSTGRES_DB')
            self.user = os.environ.get('POSTGRES_USER')
            self.password = os.environ.get('POSTGRES_PASSWORD')
        except Exception as e:
            if not all([self.host, self.port, self.database, self.user, self.password]):
                raise ValueError(f"Não foi possível obter as credenciais do banco de dados. Erro original: {str(e)}")


class LoadData(Credentials):
    """
    Classe para carregar os dados no banco de dados
    """
    def __init__(self):
        super().__init__()
        self.logger = ETLLogger()
        self.df = None

    def create_connection(self):
        """
        Cria uma conexão com o banco de dados
        Retorna: Conexão e cursor
        """
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            cursor = conn.cursor()
            self.logger.log_info("Conexão com o banco de dados estabelecida com sucesso")
            return conn, cursor
        except Exception as e:
            self.logger.log_error(f"Erro ao conectar ao banco de dados: {str(e)}")
            raise
    
    def create_table(self):
        """
        Cria a tabela no banco de dados
        """
        try:
            conn, cursor = self.create_connection()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS USD_BRL (
                    moeda_origem VARCHAR(10),
                    moeda_destino VARCHAR(10),
                    nome VARCHAR(100),
                    maior_cotacao FLOAT,
                    menor_cotacao FLOAT,
                    variacao FLOAT,
                    percentual_variacao FLOAT,
                    cotacao_compra FLOAT,
                    cotacao_venda FLOAT,
                    data_hora TIMESTAMP,
                    data_criacao TIMESTAMP,
                    data_processamento TIMESTAMP,
                    PRIMARY KEY (moeda_origem, moeda_destino, data_hora)
                )
            """)
            conn.commit()
            self.logger.log_info("Tabela USD_BRL criada/verificada com sucesso")
            cursor.close()
            conn.close()
        except Exception as e:
            self.logger.log_error(f"Erro ao criar tabela: {str(e)}")
            raise

    def insert_data(self):
        """
        Insere os dados na tabela
        """
        try:
            self.logger.log_load_start()
            conn, cursor = self.create_connection()
            
            # Verificar registros existentes
            for _, row in self.df.iterrows():
                cursor.execute("""
                    SELECT COUNT(*) FROM USD_BRL 
                    WHERE moeda_origem = %s 
                    AND moeda_destino = %s 
                    AND data_hora = %s
                """, (row['moeda_origem'], row['moeda_destino'], row['data_hora']))
                
                count = cursor.fetchone()[0]
                if count > 0:
                    self.logger.log_info(f"Atualizando registro existente para {row['moeda_origem']}/{row['moeda_destino']} em {row['data_hora']}")
                else:
                    self.logger.log_info(f"Inserindo novo registro para {row['moeda_origem']}/{row['moeda_destino']} em {row['data_hora']}")
            
            cursor.executemany("""
                INSERT INTO USD_BRL (
                    moeda_origem, moeda_destino, nome, maior_cotacao,
                    menor_cotacao, variacao, percentual_variacao,
                    cotacao_compra, cotacao_venda, data_hora,
                    data_criacao, data_processamento
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (moeda_origem, moeda_destino, data_hora) DO UPDATE SET
                    nome = EXCLUDED.nome,
                    maior_cotacao = EXCLUDED.maior_cotacao,
                    menor_cotacao = EXCLUDED.menor_cotacao,
                    variacao = EXCLUDED.variacao,
                    percentual_variacao = EXCLUDED.percentual_variacao,
                    cotacao_compra = EXCLUDED.cotacao_compra,
                    cotacao_venda = EXCLUDED.cotacao_venda,
                    data_criacao = EXCLUDED.data_criacao,
                    data_processamento = EXCLUDED.data_processamento
            """, self.df.values.tolist())
            
            conn.commit()
            self.logger.log_load_complete(len(self.df))
            cursor.close()
            conn.close()
        except Exception as e:
            self.logger.log_load_error(str(e))
            raise

    def main(self):
        """
        Método principal para carregar os dados no banco de dados
        """
        try:
            self.logger.log_etl_start()
            
            # Obter dados transformados
            self.df = TransformData().main()
            
            # Executar processo de carga
            self.create_table()
            self.insert_data()
            
            self.logger.log_etl_complete()
        except Exception as e:
            self.logger.log_etl_error(str(e))
            raise

if __name__ == "__main__":
    try:
        load = LoadData()
        load.main()
    except Exception as e:
        print(f"Erro na execução do ETL: {str(e)}")
    
