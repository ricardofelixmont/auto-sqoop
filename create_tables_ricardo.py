#!/usr/bin/env python3

# =================COMO USAR==========================
# |||---GERAR CREATE TABLES---|||
# Precisamos gerar 1 arquivo .txt para o Scheema de cada tabela.
# Esses arquivo devem ser colocados no mesmo diretório do 'create_tables.py'
#   exemplo: rezadod01.dbo.Agent.txt
#
# No arquivo.ini:
#   -path = 'diretorio onde o modulo create_tables.py está'
#
# O programa cria um diretório 'saida'
# Trata os arquivos da entrada e joga na saida como .sql
# O programa retira o cabeçalho das tabelas, portanto as tabelas precisam estar com o cabecalho
#
# CUIDADOS:
# 1 - Toda vez que o programa roda, ele da um append no arquivo de saida,
# por esse movito é importante apagar o arquivo para gerar novos 'Create Tables'.
# 2 - O arquivo .txt precisa ser o mesmo do nome da tabela no SQL server.
#   ex: rezadod01.dbo.Agent.txt
# pois esse nome é utilizado para preencher o comando create_table

#
# |||---GERAR SQOOP JOBS---|||
# Preencher o arquivo 'campos_importantes.csv' com:
#   Estes campos são utilizado para gerar os sqoop jobs
#   1 - nome da tabela
#   2 - chave primaria
#   3 - campo assistido
#   4 - particionar_campo_por
#   
# Depois precisamos gerar 1 arquivo .txt para o Scheema de cada tabela.
# Esses arquivo devem ser colocados no mesmo diretório do 'create_tables.py'
#   exemplo: rezadod01.dbo.Agent.txt
# 
# Agora é só executar o programa e escolher a opção 3 do Menu:  
# [3] - Formatar colunas SqoopJOB
# - Na pasta saida se encontra o arquivo 'newSkies-append-job.txt', que contem todos
# os sqoop jobs gerados.
# 
# CUIDADOS: 
# 1 - Toda vez que o programa roda, ele da um append no arquivo 'newSkies-append-job.txt', 
# por esse movito é importante apagar o arquivo para gerar novos sqoop jobs.
# 2 - O arquivo .txt precisa ser o mesmo do nome da tabela no SQL server. 
#   ex: rezadod01.dbo.Agent.txt
# pois esse nome é utilizado para preencher o sqoop job

import os
import configparser
import csv


conteudo = ''       # Guarda o conteudo de cada linha da tabela, que sera gravada na saida
separador = '\n'    # Separa os conteudos com \n 
# Acessando o caminho do arquivo 'p.ini'
conf_file = "create_table_ricardo.ini"
config = configparser.ConfigParser()
config.read(conf_file, encoding='utf8')

path = config.get('path', 'path_name') # Variavel path contem o caminho

os.system(f'ls *.txt > lista') # Cria um arquivo lista com o nome dos arquivos que tem final '.txt' no diretorio de entrada
os.system('mkdir saida')      # Cria um diretorio 'saida' 
 
# Função para retirar os espaços e colocar a vírgula:
# Essa função vai gravar os sqoop jobs em uma tabela.
def retira_espacos(conteudo):
    count = 1
    string = ''
   
    for pos,linha in enumerate(conteudo.split()):
        if not pos % 2 != 0:
            string += f"{linha}"+','
            count += 1
    string = string[0:-1] # retira a última virgula
    return string

# Formata o corpo da tabela para o create table
def formata_tabela(conteudo):
    string = ''
    for pos, linha in enumerate(conteudo.split()):
        if pos == 0 or pos == 1: # Retira o cabeçalho
            continue
        string += f'{linha} '
        if pos % 2 != 0:
            string += '\n'
    return string

# Create Table
# nome_da_tabela vem do arquivo 'lista' onde estão os nomes de todas as tabelas que
# que estamos tratando
def create_table_trunc_insert(conteudo, nome_da_tabela, conjunto_substituidos):
    substituidos = ''

    # Pega o nome da tabela e o nome do banco, a partir do nome do arquivo que 'campos_importantes.csv'
    single_table_name = nome_da_tabela.split('.')[2]
    single_db_name = nome_da_tabela.split('.')[0]

    for substituido in conjunto_substituidos:
        substituidos += '--Esta tabela possui o tipo ' + substituido + '\n'
    create_table = f"--{nome_da_tabela}\n--work\n{substituidos}\nCREATE TABLE IF NOT EXISTS {single_db_name}_work.{single_table_name}(\n\n{formata_tabela(conteudo)})\n\n ROW format DELIMITED FIELDS TERMINATED BY '|'STORED AS TEXTFILE;\n\n"
    with open(path+'saida/Create_table_trunc_insert.sql', 'a+') as arquivo:
        arquivo.write(create_table)
        print('Create_table_trunc_insert gravado com sucesso...')
    return create_table

def create_table_append_raw(conteudo, nome_da_tabela, conjunto_substituidos):
    substituidos = ''
    
    # Pega o nome da tabela e o nome do banco, a partir do nome do arquivo que 'campos_importantes.csv'
    single_table_name = nome_da_tabela.split('.')[2]
    single_db_name = nome_da_tabela.split('.')[0]

    for substituido in conjunto_substituidos:
        substituidos += '--Esta tabela possui o tipo ' + substituido + '\n'
    create_table = f"--{nome_da_tabela}\n--raw\n{substituidos}\nCREATE TABLE IF NOT EXISTS {single_db_name}_raw.{single_table_name}(\n\n{formata_tabela(conteudo)})\n\nROW format DELIMITED FIELDS TERMINATED BY '|'STORED AS TEXTFILE;\n\n"
    with open(path+'saida/Create_table_append.sql', 'a+') as arquivo:
        arquivo.write(create_table)
        print('Create_table_append_raw gravado com sucesso...')
    return create_table

def create_table_append_work(conteudo, nome_da_tabela, conjunto_substituidos):
    substituidos = ''
    
    # Pega o nome da tabela e o nome do banco, a partir do nome do arquivo que 'campos_importantes.csv'
    single_table_name = nome_da_tabela.split('.')[2]
    single_db_name = nome_da_tabela.split('.')[0]

    campo_assistido = ''
    with open('campos_importantes.csv', 'r') as f:
        reader = csv.DictReader(f) # Lista de dicionarios
        for tabela in reader:
            if tabela['nome_tabela'] == nome_da_tabela:
                campo_assistido = tabela['campo_assistido']

    for substituido in conjunto_substituidos:
        substituidos += '--Esta tabela possui o tipo ' + substituido + '\n'
        create_table = f"--{nome_da_tabela}\n--work\n{substituidos}\nCREATE TABLE IF NOT EXISTS {single_db_name}_work.{single_table_name}(\n\n{formata_tabela(conteudo)})\n\npartitioned by ({campo_assistido})\nSTORED AS PARQUET;\n\n"
        # O campo assistido ainda é pego do arquivo .ini
    with open(path+'saida/Create_table_append.sql', 'a+') as arquivo:
        arquivo.write(create_table)
        print('Create_table_Append_work gravado com sucesso...')
    return create_table

# AUTO-SQOOP JOB GENERATOR
def auto_sqoop_job_append(conteudo, nome_da_tabela, colunas_de_substituidos):

    # Acessando o csv com a 'Primary_key' e 'Campo_assistido'
    campo_assistido = ''
    chave_primaria = ''
    with open('campos_importantes.csv', 'r') as f:
        reader = csv.DictReader(f) # Lista de dicionarios
        for tabela in reader:
            if tabela['nome_tabela'] == nome_da_tabela:
                chave_primaria = tabela['chave_primaria']
                campo_assistido = tabela['campo_assistido']
    

    # Pega o nome da tabela e o nome do banco, a partir do nome do arquivo que 'campos_importantes.csv'
    single_table_name = nome_da_tabela.split('.')[2]
    single_db_name = nome_da_tabela.split('.')[0]
    
    # Campo map-column 
    map_col = map_column(colunas_de_substituidos)
    # Criando a string que armazena o sqoop JOB
    string_sqoop_job = f"--{nome_da_tabela}\n\nsqoop job \ \n   -Dhadoop.security.credential.provider.path=jceks://hdfs/credenciais/newskies.mssql.password.jceks \ \n   --create import_{single_db_name}_{single_table_name.lower()} \ \n   --meta-connect jdbc:hsqldb:hsql://clderenutb01p:16000/sqoop \ \n   -- import \ \n   --driver com.microsoft.sqlserver.jdbc.SQLServerDriver  \ \n   --connect jdbc:sqlserver://149.122.13.148:52900 \ \n   --username ADODSUSER13 \ \n   --password-alias newskies.mssql.password.alias \ \n   --table {nome_da_tabela} \ \n   --columns {retira_espacos(conteudo)} \ \n{map_col}   --hive-database {single_db_name}_raw \ \n   --hive-table {single_table_name.lower()} \ \n   --incremental append \ \n   --check-column {campo_assistido} \ \n   --delete-target-dir \ \n   --target-dir /user/hive/warehouse/{single_db_name}_raw.db/{single_table_name.lower()} \ \n   --split-by {chave_primaria} -m 20 \ \n   --direct \ \n   --verbose \ \n   --fields-terminated-by '|' \n\nsqoop job --meta-connect jdbc:hsqldb:hsql://clderenutb01p:16000/sqoop --exec import_{single_db_name}_{single_table_name.lower()}\n\n\n"
    
    # Gravar o Sqoop Job no arquivo newSkies-append-job.txt
    with open(path+'saida/newSkies-append-job.txt', 'a+') as f:
        f.write(string_sqoop_job)
           
    return string_sqoop_job

def map_column(colunas_de_substituidos):
    if len(colunas_de_substituidos) == 0:
        return ''
    else:
        return f'   --map-column-java {substituidos(colunas_de_substituidos)} \ \n'

def substituidos(colunas_de_substituidos):
    substituidos = ''
    for substituido in colunas_de_substituidos:
        substituidos += f'{substituido.strip()}=Integer,'
        """ 
        Retorna o campo column map do sqoop JOB
        """
    return substituidos[:-1]

# MAIN
with open(path+'lista', 'r') as f:  # Abre a lista de arquivos

    # MENU
    print('-=' * 15)
    print('MENU')
    print('-=' * 15)
    try: 
        op = int(input('[1] - Gerar - Trunc/Insert\n[2] - Gerar - Append\n[3] - Gerar Sqoop-job\nDigite uma opção: '))
    except Exception as e:
        print(f'Erro: {e}')

    # Fim do Menu    

    for linha in f:                             # Cada linha é o nome de arquivo .txt que será aberto
        with open(path+linha[:-1],"r") as f:    # Abre o arquivo .txt e guarda no objeto f
            nome_da_tabela = linha[:-5]
            data = f.readlines()                # Cria uma lista 'data' com o conteudo de f
                
        # Começo do tratamento (Regras de Negócio)
        conteudo = '' # Esvazia a lista
        conjunto_substituidos = set()
        # vai ser utilizada para fazer o map column do sqoop JOB
        colunas_de_substituidos = set()

        for d in data:
            a = d.split('|')
            table = a[1:6]
            if len(table) != 0:
                if table[2].strip() == '(null)' and table[1].strip() == "numeric":
                    conteudo += f'{table[0].strip()} {table[1].strip()}({table[3].strip()},{table[4].strip()}),'+separador

                elif table[1].strip() == "varchar" or table[1] == "char":
                    conteudo += f'{table[0].strip()} {table[1].strip()}({table[2].strip()}),'+separador
                # As seguintes substituições devem ser feitas:
                # nvarchar ->  string
                # datetime -> timestamp
                # datetime2 -> timestamp
                # bit -> tinyint
                # money -> decimal
                elif table[1].strip() == 'nvarchar':
                    conteudo += f'{table[0].strip()} string,' + '\n'
                    conjunto_substituidos.add('nvarchar')
                elif table[1].strip() == 'datetime':
                    conteudo += f'{table[0].strip()} timestamp,' + '\n'
                    conjunto_substituidos.add('datatime')
                elif table[1].strip() == 'datetime2':
                    conteudo += f'{table[0].strip()} timestamp,' + '\n'
                    conjunto_substituidos.add('datetime2')
                elif table[1].strip() == 'bit':
                    conteudo += f'{table[0].strip()} tinyint,'+ '\n'
                    conjunto_substituidos.add('bit')
                    # Armazendo o nome das colunas que tem o tipo bit, para preencher o map reduce
                    colunas_de_substituidos.add(table[0])
                elif table[1].strip() == 'money':
                    conteudo += f'{table[0].strip()} decimal,' + '\n'
                    conjunto_substituidos.add('money')
                # Fim das substituições
                else:
                    conteudo += f'{table[0].strip()} {table[1].strip()},' + '\n'
        while op == 1:
            create_table_trunc_insert(conteudo[:-2], nome_da_tabela, conjunto_substituidos)
            break
        while op == 2:
            create_table_append_raw(conteudo[:-2], nome_da_tabela, conjunto_substituidos) 
            create_table_append_work(conteudo[:-2], nome_da_tabela, conjunto_substituidos)
            break
        # Apenas retira os espaços da tabela gerada pelo get schema
        while op == 3:
            auto_sqoop_job_append(conteudo, nome_da_tabela, colunas_de_substituidos)
            print(f'Sqoop Job "{nome_da_tabela}" gerado com sucesso')
            break
        # A partir daqui não é mais necessário
        # Cria um arquivo com o nome da tabela(ex: dim_accounts), grava o conteudo e fecha o arquivo
        # ref_arquivo = open(path+'saida/'+'tabela_'+linha[:-1], 'w')
        # ref_arquivo.write(conteudo)
        # print('Gravado com sucesso...')
        # ref_arquivo.close()

