#!/usr/bin/env python3

# =================COMO USAR==========================
# Colocar os .txt que devem ser tratados na mesma pasta do programa 'create_tables.py'
# O nome dos arquivos.txt tem que ser o mesmo nome da tabela a ser tratada, exemplo: CustomerProgramSettingType.txt
# No arquivo.ini:
#   -path = 'diretorio onde o modulo create_tables.py está'
#   -nome_do_banco = 'rezad01'
#   -campo_assistido = 'ano_mes int'
#
# Programa cria um diretório 'saida'
# Trata os arquivos da entrada e joga na saida como .sql
# O programa retira o cabeçalho das tabelas, portanto as tabelas precisam estar com o cabecalho

import os
import configparser


conteudo = ''       # Guarda o conteudo de cada linha da tabela, que sera gravada na saida
separador = '\n'    # Separa os conteudos com \n 
# Acessando o caminho do arquivo 'p.ini'
conf_file = "create_table_ricardo.ini"
config = configparser.ConfigParser()
config.read(conf_file, encoding='utf8')

path = config.get('path', 'path_name') # Variavel path contem o caminho
nome_do_banco = config.get('path', 'nome_do_banco')
campo_assistido = config.get('path', 'campo_assistido')

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
    for substituido in conjunto_substituidos:
        substituidos += '--Esta tabela possui o tipo ' + substituido + '\n'
    create_table = f"--{nome_do_banco}.dbo.{nome_da_tabela}\n--work\n{substituidos}\nCREATE TABLE IF NOT EXISTS {nome_do_banco}_work.{nome_da_tabela}(\n\n{formata_tabela(conteudo)})\n\n ROW format DELIMITED FIELDS TERMINATED BY '|'STORED AS TEXTFILE;\n\n"
    with open(path+'saida/Create_table_trunc_insert.sql', 'a+') as arquivo:
        arquivo.write(create_table)
        print('Create_table_trunc_insert gravado com sucesso...')
    return create_table

def create_table_append_raw(conteudo, nome_da_tabela, conjunto_substituidos):
    substituidos = ''
    for substituido in conjunto_substituidos:
        substituidos += '--Esta tabela possui o tipo ' + substituido + '\n'
    create_table = f"--{nome_do_banco}.dbo.{nome_da_tabela}\n--raw\n{substituidos}\nCREATE TABLE IF NOT EXISTS {nome_do_banco}_raw.{nome_da_tabela}(\n\n{formata_tabela(conteudo)})\n\nROW format DELIMITED FIELDS TERMINATED BY '|'STORED AS TEXTFILE;\n\n"
    with open(path+'saida/Create_table_append.sql', 'a+') as arquivo:
        arquivo.write(create_table)
        print('Create_table_append_raw gravado com sucesso...')
    return create_table

def create_table_append_work(conteudo, nome_da_tabela, conjunto_substituidos):
    substituidos = ''
    for substituido in conjunto_substituidos:
        substituidos += '--Esta tabela possui o tipo ' + substituido + '\n'
        create_table = f"--{nome_do_banco}.dbo.{nome_da_tabela}\n--work\n{substituidos}\nCREATE TABLE IF NOT EXISTS {nome_do_banco}_work.{nome_da_tabela}(\n\n{formata_tabela(conteudo)})\n\npartitioned by ({campo_assistido})\nSTORED AS PARQUET;\n\n"
    with open(path+'saida/Create_table_append.sql', 'a+') as arquivo:
        arquivo.write(create_table)
        print('Create_table_Append_work gravado com sucesso...')
    return create_table

# MAIN
with open(path+'lista', 'r') as f:  # Abre a lista de arquivos

    # MENU
    print('-=' * 15)
    print('MENU')
    print('-=' * 15)
    try: 
        op = int(input('[1] - Gerar - Trunc/Insert\n[2] - Gerar - Append\n[3] - Formatar colunas SqoopJOB\nDigite uma opção: '))
    except Exception as e:
        print(f'Erro: {e}')
    else:
        print('Opção invalida, tente novamente')
    # Fim do Menu    

    for linha in f:                             # Cada linha é o nome de arquivo .txt que será aberto
        with open(path+linha[:-1],"r") as f:    # Abre o arquivo .txt e guarda no objeto f
            nome_da_tabela = linha[:-5]
            data = f.readlines()                # Cria uma lista 'data' com o conteudo de f
                
        # Começo do tratamento (Regras de Negócio)
        conteudo = '' # Esvazia a lista
        conjunto_substituidos = set()
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
        if op == 3:
            print(retira_espacos(conteudo))
        # A partir daqui não é mais necessário
        # Cria um arquivo com o nome da tabela(ex: dim_accounts), grava o conteudo e fecha o arquivo
        # ref_arquivo = open(path+'saida/'+'tabela_'+linha[:-1], 'w')
        # ref_arquivo.write(conteudo)
        # print('Gravado com sucesso...')
        # ref_arquivo.close()

