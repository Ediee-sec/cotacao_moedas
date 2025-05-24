import requests
import configparser

class Credentials:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('config/api_key.ini')
        self.api_key = self.config['API_KEY']['key']

class ExtractData(Credentials):
    def __init__(self):
        super().__init__()
        self.api_key = self.api_key

    def get_data(self):
        url = f"https://economia.awesomeapi.com.br/json/last/USD-BRL?token={self.api_key}"
        response = requests.get(url)
        return response.json()