import pandas as pd
from extract import ExtractData
import json
from datetime import datetime
from logger import ETLLogger

class TransformData:
    def __init__(self):
        self.logger = ETLLogger()
        self.extract = ExtractData().get_data()

    def create_dataframe(self):
        try:
            # Pega apenas os valores do dicionário e cria o DataFrame
            df = pd.DataFrame([self.extract['USDBRL']])
            df.columns = ['code', 'codein', 'name', 'high', 'low', 'varBid', 'pctChange', 'bid', 'ask', 'timestamp', 'create_date']
            return df
        except Exception as e:
            self.logger.log_transform_error(f"Erro ao criar DataFrame: {str(e)}")
            raise
    
    def trasnform_data_type(self):
        try:
            df = self.create_dataframe()
            df['high'] = df['high'].astype(float)
            df['low'] = df['low'].astype(float)
            df['varBid'] = df['varBid'].astype(float)
            df['pctChange'] = df['pctChange'].astype(float)
            # Converter timestamp de Unix para datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'].astype(int), unit='s')
            df['create_date'] = pd.to_datetime(df['create_date'])
            return df
        except Exception as e:
            self.logger.log_transform_error(f"Erro na conversão de tipos: {str(e)}")
            raise
    
    def transform_data_rename_columns(self):
        try:
            with open('json/columns_df.json', 'r') as file:
                columns_rename_df = json.load(file)

            df = self.trasnform_data_type()
            df = df.rename(columns=columns_rename_df)
            return df
        except Exception as e:
            self.logger.log_transform_error(f"Erro ao renomear colunas: {str(e)}")
            raise
    
    def add_columns_dt_process(self):
        try:
            df = self.transform_data_rename_columns()
            df['data_processamento'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            return df
        except Exception as e:
            self.logger.log_transform_error(f"Erro ao adicionar coluna de processamento: {str(e)}")
            raise
    
    def main(self):
        try:
            self.logger.log_transform_start()
            df = self.add_columns_dt_process()
            # Garantir a ordem correta das colunas para inserção no banco
            columns_order = ['moeda_origem', 'moeda_destino', 'nome', 'maior_cotacao', 'menor_cotacao', 
                           'variacao', 'percentual_variacao', 'cotacao_compra', 'cotacao_venda', 
                           'data_hora', 'data_criacao', 'data_processamento']
            df = df[columns_order]
            self.logger.log_transform_complete(len(df))
            return df
        except Exception as e:
            self.logger.log_transform_error(str(e))
            raise
    