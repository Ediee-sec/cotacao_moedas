import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler

class ETLLogger:
    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ETLLogger, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._initialized:
            # Criar diretório de logs se não existir
            self.log_dir = 'logs'
            if not os.path.exists(self.log_dir):
                os.makedirs(self.log_dir)

            # Nome do arquivo de log com data
            self.log_file = os.path.join(
                self.log_dir, 
                f'etl_{datetime.now().strftime("%Y%m%d")}.log'
            )

            # Configurar o logger
            self.logger = logging.getLogger('ETLLogger')
            self.logger.setLevel(logging.INFO)

            # Remover handlers existentes
            if self.logger.handlers:
                self.logger.handlers.clear()

            # Criar formato do log
            formatter = logging.Formatter(
                '%(asctime)s | %(levelname)s | %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )

            # Handler para arquivo com rotação (máximo 5MB por arquivo, mantém 5 backups)
            file_handler = RotatingFileHandler(
                self.log_file,
                maxBytes=5*1024*1024,  # 5MB
                backupCount=5
            )
            file_handler.setFormatter(formatter)

            # Handler para console
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)

            # Adicionar handlers ao logger
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

            self._initialized = True

    def log_extract_start(self, currency_pair):
        self.logger.info(f"Iniciando extração de dados para o par {currency_pair}")

    def log_extract_complete(self, currency_pair, data_size):
        self.logger.info(f"Extração completa para {currency_pair}. Tamanho dos dados: {data_size}")

    def log_extract_error(self, currency_pair, error):
        self.logger.error(f"Erro na extração de {currency_pair}: {str(error)}")

    def log_transform_start(self):
        self.logger.info("Iniciando transformação dos dados")

    def log_transform_complete(self, rows_processed):
        self.logger.info(f"Transformação completa. Linhas processadas: {rows_processed}")

    def log_transform_error(self, error):
        self.logger.error(f"Erro na transformação: {str(error)}")

    def log_load_start(self):
        self.logger.info("Iniciando carregamento dos dados no banco")

    def log_load_complete(self, rows_loaded):
        self.logger.info(f"Carregamento completo. Linhas inseridas: {rows_loaded}")

    def log_load_error(self, error):
        self.logger.error(f"Erro no carregamento: {str(error)}")

    def log_etl_start(self):
        self.logger.info("=== Iniciando processo ETL ===")

    def log_etl_complete(self):
        self.logger.info("=== Processo ETL finalizado com sucesso ===")

    def log_etl_error(self, error):
        self.logger.error(f"=== Processo ETL finalizado com erro: {str(error)} ===")

    def log_info(self, message):
        self.logger.info(message)

    def log_error(self, message):
        self.logger.error(message)

    def log_warning(self, message):
        self.logger.warning(message) 