import cx_Oracle
from pymongo import MongoClient
import json
from datetime import datetime
import pytz


# Conectar ao MongoDB
client = MongoClient('mongodb+srv://... insira sua conexao aqui')

# Conectar ao banco de dados Oracle
connection = cx_Oracle.connect('user/password@127.0.0.1/database')

# Carrega Fuso Horario do Brasil
fuso_horario_brasil = pytz.timezone('America/Sao_Paulo')

def carregaTabela(nomeSchema, nomeTabela, connection):
    
    cursor = connection.cursor()
    
    try:
        print(f"{datetime.now(fuso_horario_brasil).strftime('%Y-%m-%d %H:%M:%S')} -> ORACLE-DB - Tabela: {nomeSchema}.{nomeTabela} | Obtendo dados...")

        # Executar uma consulta
        cursor.execute(f"SELECT * FROM {nomeSchema}.{nomeTabela} WHERE ROWNUM <= 10000")

        # Obter os nomes das colunas
        colunas = [col[0] for col in cursor.description]

        # Obter os resultados e transformá-los em uma lista de dicionários
        resultados = []
        for linha in cursor.fetchall():
            documento = dict(zip(colunas, linha))
            resultados.append(documento)

        # Classe personalizada para serializar datetime
        class DateTimeEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()  # Converte datetime para string no formato ISO 8601
                return super(DateTimeEncoder, self).default(obj)

        # Converter a lista de dicionários em JSON usando o encoder personalizado
        json_resultado = json.dumps(resultados, indent=4, cls=DateTimeEncoder)

        print(f"{datetime.now(fuso_horario_brasil).strftime('%Y-%m-%d %H:%M:%S')} -> ORACLE-DB - Tabela: {nomeSchema}.{nomeTabela} | Dados Obtidos com Sucesso!")

        # Exibir o JSON
        return json_resultado
    
    except cx_Oracle.DatabaseError as e:
        print(f"{datetime.now(fuso_horario_brasil).strftime('%Y-%m-%d %H:%M:%S')} -> Erro ao acessar o Oracle: {e}")
        return []
    
    finally:
        # Fechar o cursor, mas a conexão continua aberta para o chamador
        cursor.close()


# Inserir os documentos no MongoDB
def insereMongoDB(nomeDatabaseMongo, Nomecollection, documentos, client):

    print(f'{datetime.now(fuso_horario_brasil).strftime('%Y-%m-%d %H:%M:%S')} -> MONGO-DB - Collection: {Nomecollection} | Inserindo Documentos...')

    db = client[f'{nomeDatabaseMongo}']  # Database MongoDB
    collection = db[f'{Nomecollection}']  # CollectionName do MongoDB


    if isinstance(documentos, list):
        # Se for uma lista de documentos, insira todos de uma vez
        collection.insert_many(documentos)
    else:
        # Se for um único documento, insira um
        collection.insert_one(documentos)
    
    print(f'{datetime.now(fuso_horario_brasil).strftime('%Y-%m-%d %H:%M:%S')} -> MONGO-DB - Collection: {Nomecollection} | Dados Inseridos com Sucesso.')
           

# Chamada das funcoes que executam a migracao de tabelas
def oracleToMongoDB(oracleNomeSchema, oracleNomeTabela, mongoDatabase, mongoCollection, fl_loop = 0):

    # Tras o resultado do select na tabela ja em formato de documento (json) 
    json_resultado = carregaTabela(oracleNomeSchema, oracleNomeTabela, connection)

    # Converter o JSON string de volta para dicionários
    documentos = json.loads(json_resultado)  

    insereMongoDB(mongoDatabase, mongoCollection, documentos, client)

    if fl_loop == 0:
        # Fecha a conexao com o Oracle e o MongoDB
        connection.close()
        client.close() 
    


# Processo que faz a migracao de varias tabelas do oracle para collections do mongoDB
def oracleToMongoEmMassa(arquivoConfig):

    # lendo o arquivo de configuracao que contem o nome das tabelas e collections que serao migrados
    with open(arquivoConfig, "r") as file:
        tabelas = json.load(file)

    for i in tabelas:

        oracleToMongoDB(\
            mongoCollection= i['target_nomecollection'],\
            mongoDatabase= i['target_nomeDatabaseMongo'],\
            oracleNomeSchema= i['source_nomeSchema'],\
            oracleNomeTabela= i['source_nomeTabela'],\
            fl_loop = 1
            )
    
    # Fecha a conexao com o Oracle e o MongoDB
    connection.close()
    client.close() 
