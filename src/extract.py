import requests
import configparser

class Credentials:
    """
    Classe para obter as credenciais da API
    Atributos:
        config: ConfigParser para ler o arquivo de configuração
        api_key: Chave da API
    """
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('config/api_key.ini')
        self.api_key = self.config['API_KEY']['key']

class ExtractData(Credentials):
    """
    Classe para extrair os dados da API
    Atributos:
        api_key: Chave da API
    """
    def __init__(self):
        super().__init__()
        self.api_key = self.api_key

    def get_data(self):
        """
        Método para obter os dados da API
        Retorna: Dicionário com os dados da API
        """
        url = f"https://economia.awesomeapi.com.br/json/last/USD-BRL?token={self.api_key}"
        response = requests.get(url)
        return response.json()