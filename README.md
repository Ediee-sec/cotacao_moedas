# Currency Quotes - Cotação de Moedas

Este projeto é um sistema ETL (Extract, Transform, Load) que coleta dados de cotações de moedas da API de câmbio, processa os dados e armazena em um banco de dados PostgreSQL.

## Estrutura do Projeto

```
currency_quotes/
├── json/
│   └── columns_df.json
├── src/
│   ├── extract.py
│   ├── transform.py
│   └── load.py
└── README.md
```

## Componentes

### Extract (extract.py)
Responsável por extrair os dados da API de cotações de moedas. 
- Realiza a requisição HTTP para a API
- Obtém dados da cotação USD/BRL (Dólar/Real)
- Retorna os dados em formato JSON

### Transform (transform.py)
Realiza o processamento e transformação dos dados obtidos.
- Converte os dados JSON em DataFrame
- Realiza conversões de tipos de dados
- Renomeia colunas para o formato do banco de dados
- Adiciona timestamp de processamento

### Load (load.py)
Gerencia a conexão com o banco de dados e a inserção dos dados.
- Estabelece conexão com PostgreSQL
- Cria a tabela se não existir
- Insere os dados processados no banco

## Configuração

### Requisitos
- Python 3.x
- PostgreSQL
- Bibliotecas Python (requirements.txt):
  - pandas
  - psycopg2
  - requests

### Variáveis de Ambiente
O projeto utiliza variáveis de ambiente para configuração. Configure as seguintes variáveis:

Para o banco de dados PostgreSQL:
```bash
export POSTGRES_HOST=seu_host
export POSTGRES_PORT=sua_porta
export POSTGRES_DB=seu_banco
export POSTGRES_USER=seu_usuario
export POSTGRES_PASSWORD=sua_senha
```

Para a API de cotações:
```bash
export API_KEY=sua_chave_api_aqui
```

Você pode adicionar essas variáveis ao seu arquivo `.env` ou configurá-las diretamente no seu ambiente.

### Estrutura da Tabela
```sql
CREATE TABLE USD_BRL (
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
);
```

## Como Usar

1. Clone o repositório:
```bash
git clone [url-do-repositorio]
cd currency_quotes
```

2. Configure o ambiente:
- Configure as variáveis de ambiente necessárias (conforme seção "Variáveis de Ambiente")
- Instale as dependências:
```bash
pip install -r requirements.txt
```

3. Execute o projeto:
```bash
python src/load.py
```

## Dados Coletados

O sistema coleta as seguintes informações:
- Moeda de origem (USD)
- Moeda de destino (BRL)
- Nome da cotação
- Maior cotação do dia
- Menor cotação do dia
- Variação
- Percentual de variação
- Cotação de compra
- Cotação de venda
- Data/hora da cotação
- Data de criação
- Data de processamento

## Contribuição

Para contribuir com o projeto:
1. Faça um fork do repositório
2. Crie uma branch para sua feature (`git checkout -b feature/nova-feature`)
3. Commit suas mudanças (`git commit -m 'Adiciona nova feature'`)
4. Push para a branch (`git push origin feature/nova-feature`)
5. Abra um Pull Request

## Licença

Este projeto está sob a licença MIT. Veja o arquivo LICENSE para mais detalhes.
