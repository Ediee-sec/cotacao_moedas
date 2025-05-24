import pandas as pd
from extract import ExtractData
import json
from datetime import datetime

class TransformData:
    def __init__(self):
        self.extract = ExtractData().get_data()

    def create_dataframe(self):
        # Pega apenas os valores do dicionário e cria o DataFrame
        df = pd.DataFrame([self.extract['USDBRL']])
        df.columns = ['code', 'codein', 'name', 'high', 'low', 'varBid', 'pctChange', 'bid', 'ask', 'timestamp', 'create_date']
        return df
    
    def trasnform_data_type(self):
        df = self.create_dataframe()
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['varBid'] = df['varBid'].astype(float)
        df['pctChange'] = df['pctChange'].astype(float)
        df['timestamp'] = pd.to_datetime(df['timestamp'].astype(int), unit='s')
        df['create_date'] = pd.to_datetime(df['create_date'])
        return df
    
    def transform_data_rename_columns(self):
        with open('json/columns_df.json', 'r') as file:
            columns_rename_df = json.load(file)

        df = self.trasnform_data_type()
        df = df.rename(columns=columns_rename_df)
        return df
    
    def add_columns_dt_process(self):
        df = self.transform_data_rename_columns()
        df['data_processamento'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return df
    
    def main(self):
        df = self.add_columns_dt_process()
        # Garantir a ordem correta das colunas para inserção no banco
        columns_order = ['moeda_origem', 'moeda_destino', 'nome', 'maior_cotacao', 'menor_cotacao', 
                        'variacao', 'percentual_variacao', 'cotacao_compra', 'cotacao_venda', 
                        'data_hora', 'data_criacao', 'data_processamento']
        return df[columns_order]
    