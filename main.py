from datetime import datetime
from funcoes import oracleToMongoEmMassa
import pytz


# Carrega Variaveis
fuso_horario_brasil = pytz.timezone('America/Sao_Paulo')
arquivo_configuracao = 'config.json'

# Obter a data e hora atual no inicio do processamento
inicio = datetime.now(fuso_horario_brasil)
print(inicio.strftime('%Y-%m-%d %H:%M:%S'), " -> Início do processamento:")


oracleToMongoEmMassa(arquivo_configuracao)

# Obter a data e hora atual no final do processamento
fim = datetime.now(fuso_horario_brasil)
print(fim.strftime('%Y-%m-%d %H:%M:%S'), " -> Fim do processamento:")


# Calcular a duração do processamento
duracao = fim - inicio
print("Duração do processamento:", duracao)