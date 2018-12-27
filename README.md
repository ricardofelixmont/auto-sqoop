# auto-sqoop

COMO USAR:

	|||---GERAR CREATE TABLES---|||
	Precisamos gerar 1 arquivo .txt para o Scheema de cada tabela.
	Esses arquivo devem ser colocados no mesmo diretório do 'create_tables.py'
	exemplo: rezadod01.dbo.Agent.txt
	
	No arquivo.ini:
	-path = 'diretorio onde o modulo create_tables.py está'
	
	O programa cria um diretório 'saida'
	Trata os arquivos da entrada e joga na saida como .sql
	O programa retira o cabeçalho das tabelas, portanto as tabelas precisam estar com o cabecalho
	
	CUIDADOS:
	1 - Toda vez que o programa roda, ele da um append no arquivo de saida,
	por esse movito é importante apagar o arquivo para gerar novos 'Create Tables'.
	2 - O arquivo .txt precisa ser o mesmo do nome da tabela no SQL server.
	ex: rezadod01.dbo.Agent.txt
	pois esse nome é utilizado para preencher o comando create_table
	
	
	|||---GERAR SQOOP JOBS---|||
	Preencher o arquivo 'campos_importantes.csv' com:
	Estes campos são utilizado para gerar os sqoop jobs
	1 - nome da tabela
	2 - chave primaria
	3 - campo assistido
	4 - particionar_campo_por
	
	Depois precisamos gerar 1 arquivo .txt para o Scheema de cada tabela.
	Esses arquivo devem ser colocados no mesmo diretório do 'create_tables.py'
	exemplo: rezadod01.dbo.Agent.txt
	
	Agora é só executar o programa e escolher a opção 3 do Menu:  
	[3] - Formatar colunas SqoopJOB
	- Na pasta saida se encontra o arquivo 'newSkies-append-job.txt', que contem todos
	os sqoop jobs gerados.
	
	CUIDADOS: 
	1 - Toda vez que o programa roda, ele da um append no arquivo 'newSkies-append-job.txt', 
	por esse movito é importante apagar o arquivo para gerar novos sqoop jobs.
	2 - O arquivo .txt precisa ser o mesmo do nome da tabela no SQL server. 
	ex: rezadod01.dbo.Agent.txt
	pois esse nome é utilizado para preencher o sqoop job