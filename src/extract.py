import requests
import configparser
from logger import ETLLogger

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
        self.logger = ETLLogger()
        self.currency_pair = 'USD-BRL'
        self.url = f'https://economia.awesomeapi.com.br/json/last/{self.currency_pair}'

    def get_data(self):
        """
        Método para obter os dados da API
        Retorna: Dicionário com os dados da API
        """
        try:
            self.logger.log_extract_start(self.currency_pair)
            
            response = requests.get(self.url)
            response.raise_for_status()
            data = response.json()
            
            self.logger.log_extract_complete(self.currency_pair, len(data))
            return data
            
        except requests.exceptions.RequestException as e:
            self.logger.log_extract_error(self.currency_pair, str(e))
            raise
        except Exception as e:
            self.logger.log_extract_error(self.currency_pair, str(e))
            raise