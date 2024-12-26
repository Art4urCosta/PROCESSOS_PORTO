import openpyxl
import pyodbc
import os
import pandas as pd
from datetime import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import schedule

def criar_pasta():
    # Definindo as pastas
    pasta_base = 'Analitico_Porto'
    pasta_ano = datetime.now().strftime("%Y")
    pasta_mes = datetime.now().strftime("%m")
    pasta_dia = datetime.now().strftime("%d")

    # Construindo o caminho hierárquico
    caminho_ano = os.path.join(pasta_base, pasta_ano)
    caminho_mes = os.path.join(caminho_ano, pasta_mes)
    caminho_dia = os.path.join(caminho_mes, pasta_dia)

    # Criando as pastas se não existirem
    for caminho in [pasta_base, caminho_ano, caminho_mes, caminho_dia]:
        if not os.path.exists(caminho):
            os.makedirs(caminho)
            print(f"Pasta {caminho} criada com sucesso")
    
    # Sempre retorna o caminho final
    return caminho_dia

def conectar_db():
	try:
		SERVER = '192.168.1.9'
		DATABASE = 'MIS'
		USERNAME = 'sistemas_treo'
		PASSWORD = 'sistemas_2023'
		connectionString = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes'
		global conn 
		conn = pyodbc.connect(connectionString) 
		print(conn)
		print("Conectado com sucesso")
	except Exception as e:
		print(e)

def executar_query(query):
	try:
		with conn.cursor() as cursor:
			cursor.execute(query)
			conn.commit()
			print("Query executada com sucesso")
	except Exception as e:
		print(e)

def criar_dataframe(query):
	try:
		df = pd.read_sql(query, conn)
		return df
	except Exception as e:
		print(f'Erro ao criar Df: {e}')

def analitico_entradas_porto():
		query_analy_1 = """
			IF OBJECT_ID('tempdb..#CARTEIRA_PORTO') IS NOT NULL
				DROP TABLE #CARTEIRA_PORTO;

		SELECT *
			INTO #CARTEIRA_PORTO
		FROM OPENQUERY(LINKEDPLAN,'
			SELECT DISTINCT*
			FROM CARTEIRA
			WHERE ID_CARTEIRA BETWEEN ''1150'' AND ''1154''');"""
		
		query_analy_2 = """
			IF OBJECT_ID('tempdb..#ENTRADAS_PORTO') IS NOT NULL 
				DROP TABLE #ENTRADAS_PORTO;

		SELECT *
			INTO #ENTRADAS_PORTO
		FROM OPENQUERY(LINKEDPLAN,'
		SELECT DISTINCT 
			ID_CONTR
			,NUM_CONTRATO
			,ID_CARTEIRA
			,CAST(DATA_CARGA AS DATE) AS DATA_CARGA
		FROM CONTRATO
			WHERE ID_CARTEIRA BETWEEN ''1150'' AND ''1154''
			AND CAST(DATA_CARGA AS DATE) = CURRENT_DATE()')"""
		
		query_analy_3 ="""	
			SELECT 
				DATA_CARGA
				,A.ID_CARTEIRA
				,B.NOME AS CARTEIRA
				,NUM_CONTRATO
				,ID_CONTR
			FROM #ENTRADAS_PORTO AS A JOIN #CARTEIRA_PORTO AS B ON A.ID_CARTEIRA = B.ID_CARTEIRA
			ORDER BY A.ID_CARTEIRA,NUM_CONTRATO;
			"""
		try:
			executar_query(query_analy_1)
			executar_query(query_analy_2)
			arquivo = criar_dataframe(query_analy_3)
			
			caminho_pasta = criar_pasta()
			print(f"Caminho da pasta criado: {caminho_pasta}")
			if caminho_pasta is None:
				raise ValueError("Caminho da pasta não foi criado corretamente.")
			
			nome_arquivo = os.path.join(caminho_pasta, f"ENTRADAS_PORTO_{datetime.now().strftime('%d_%m_%Y')}.xlsx")
			print(f"Caminho completo do arquivo: {nome_arquivo}")

			# gera o arquivo
			arquivo.to_excel(nome_arquivo, index=False,engine='openpyxl')
			print(f"Arquivo criado com sucesso: {nome_arquivo}")
			return nome_arquivo

		except Exception as e:
			print(f"Erro na criação do arquivo: {e}")

def entradas_porto():
	query_1 = """
		IF OBJECT_ID('tempdb..#CARTEIRA_PORTO') IS NOT NULL
			DROP TABLE #CARTEIRA_PORTO;
		SELECT *
		INTO #CARTEIRA_PORTO
		FROM OPENQUERY(LINKEDPLAN,'
			SELECT DISTINCT*
			FROM CARTEIRA
			WHERE ID_CARTEIRA BETWEEN ''1150'' AND ''1154''');
	"""

	query_2 = """
		IF OBJECT_ID('tempdb..#ENTRADAS_PORTO') IS NOT NULL
			DROP TABLE #ENTRADAS_PORTO;
		SELECT *
		INTO #ENTRADAS_PORTO
		FROM OPENQUERY(LINKEDPLAN,'
		SELECT DISTINCT 
			ID_CONTR
			,ID_CARTEIRA
			,CAST(DATA_CARGA AS DATE) AS DATA_CARGA
		FROM CONTRATO
			WHERE ID_CARTEIRA BETWEEN ''1150'' AND ''1154''
			AND YEAR(DATA_CARGA) = YEAR(CURRENT_DATE())
			AND MONTH(DATA_CARGA) = MONTH(CURRENT_DATE())');
	"""

	query_3 ="""	
		SELECT 
			DATA_CARGA
			,A.ID_CARTEIRA
			,B.NOME AS CARTEIRA
			,COUNT(ID_CONTR) AS QTD_DE_ENTRADAS
	FROM #ENTRADAS_PORTO AS A JOIN #CARTEIRA_PORTO AS B 
		ON A.ID_CARTEIRA = B.ID_CARTEIRA
	GROUP BY 
		DATA_CARGA
		,A.ID_CARTEIRA
		,B.NOME
	ORDER BY 
		DATA_CARGA;
	"""

	try:
		executar_query(query_1)
		executar_query(query_2)
		df = criar_dataframe(query_3)
		return df
	except Exception as e:
		print(f"Erro na query: {e}")


def saidas_porto():
	query_1 = """
	IF OBJECT_ID('tempdb..#CARTEIRA_PORTO') IS NOT NULL
		DROP TABLE #CARTEIRA_PORTO;

	SELECT *
		INTO #CARTEIRA_PORTO
	FROM OPENQUERY(LINKEDPLAN,'
		SELECT DISTINCT*
		FROM CARTEIRA
		WHERE ID_CARTEIRA BETWEEN ''1150'' AND ''1154''');"""
	
	query_2 = """
		IF
		OBJECT_ID('tempdb..#SAIDAS_PORTO') IS NOT NULL 
			DROP TABLE #SAIDAS_PORTO;

		SELECT *
			INTO #SAIDAS_PORTO
		FROM OPENQUERY(LINKEDPLAN,'
		SELECT DISTINCT 
			ID_CONTR
			,ID_CARTEIRA
			,CAST(DATA_SAIDA_EFET AS DATE) AS DATA_SAIDA_EFET
		FROM CONTRATO
			WHERE ID_CARTEIRA BETWEEN ''1150'' AND ''1154''
			AND DATA_SAIDA_EFET IS NOT NULL
			AND YEAR(DATA_SAIDA_EFET) = YEAR(CURRENT_DATE())
			AND MONTH(DATA_SAIDA_EFET) = MONTH(CURRENT_DATE())');"""
	
	query_3 ="""	
		SELECT 
		DATA_SAIDA_EFET
		,A.ID_CARTEIRA AS CARTEIRA
		,B.NOME
		,COUNT(ID_CONTR) AS QTD_DE_SAÍDAS
	FROM #SAIDAS_PORTO AS A JOIN #CARTEIRA_PORTO AS B ON A.ID_CARTEIRA = B.ID_CARTEIRA
	GROUP BY 
		DATA_SAIDA_EFET
		,A.ID_CARTEIRA
		,B.NOME
	ORDER BY DATA_SAIDA_EFET;"""
	
	try:
		executar_query(query_1)
		executar_query(query_2)
		df = criar_dataframe(query_3)
		return df
	except Exception as e:
		print(f"Erro na query: {e}")


def analitico_saidas_porto():
	query_analy_1 = """
	IF OBJECT_ID('tempdb..#CARTEIRA_PORTO') IS NOT NULL
		DROP TABLE #CARTEIRA_PORTO;

	SELECT *
		INTO #CARTEIRA_PORTO
	FROM OPENQUERY(LINKEDPLAN,'
		SELECT DISTINCT*
		FROM CARTEIRA
		WHERE ID_CARTEIRA BETWEEN ''1150'' AND ''1154''');"""
	
	query_analy_2 = """
	IF OBJECT_ID('tempdb..#SAIDAS_PORTO') IS NOT NULL 
		DROP TABLE #SAIDAS_PORTO;

	SELECT *
		INTO #SAIDAS_PORTO
	FROM OPENQUERY(LINKEDPLAN,'
	SELECT DISTINCT 
		ID_CONTR
		,NUM_CONTRATO
		,ID_CARTEIRA
		,CAST(DATA_SAIDA_EFET AS DATE) AS DATA_SAIDA_EFET
	FROM CONTRATO
		WHERE ID_CARTEIRA BETWEEN ''1150'' AND ''1154''
		AND CAST(DATA_SAIDA_EFET AS DATE) = CURRENT_DATE()')"""
	
	query_analy_3 ="""	
			SELECT 
				DATA_SAIDA_EFET
				,A.ID_CARTEIRA
				,B.NOME AS CARTEIRA
				,NUM_CONTRATO
				,ID_CONTR
			FROM #SAIDAS_PORTO AS A JOIN #CARTEIRA_PORTO AS B ON A.ID_CARTEIRA = B.ID_CARTEIRA
			ORDER BY A.ID_CARTEIRA,NUM_CONTRATO;"""
	
	try:
		executar_query(query_analy_1)
		executar_query(query_analy_2)
		arquivo = criar_dataframe(query_analy_3)

		caminho_pasta = criar_pasta()
		print(f"Caminho da pasta criado: {caminho_pasta}")
		if caminho_pasta is None:
			raise ValueError("Caminho da pasta não foi criado corretamente.")

		nome_arquivo = os.path.join(caminho_pasta, f"SAIDAS_PORTO_{datetime.now().strftime('%d_%m_%Y')}.xlsx")
		print(f"Caminho completo do arquivo: {nome_arquivo}")

		# gera o arquivo
		arquivo.to_excel(nome_arquivo, index=False,engine='openpyxl')
		print(f"Arquivo criado com sucesso: {nome_arquivo}")
		return nome_arquivo

	except Exception as e:
		print(f"Erro na criação do arquivo: {e}")

def enviar_email():
	df = entradas_porto()
	anexo = analitico_entradas_porto()
	remetente = 'automacao@treo.com.br'
	destinatario = [
		'jonas.amaro@dnr.com.br','flavia.reis@dnr.com.br',' sidney.lima@dnr.com.br',
		' alex.barboza@dnr.com.br','diego.rosa@dnr.com.br','controldesk@dnr.com.br',
		'planejamento.porto@dnr.com.br','tamyres.oliveira@dnr.com.br',
		'sistemas@treo.com.br','felipe.fecundes@dnr.com.br', 'nathally.silva@dnr.com.br']

	subject = f'(DNR) Relatório de processamentos ENTRADAS - PORTO SEGURO - {datetime.now().strftime("%d/%m/%Y")}'
	
	msg = MIMEMultipart()
	msg['From'] = remetente
	msg['To'] = ', '.join(destinatario)
	msg['Subject'] = subject
	
	# Corpo com DataFrame
	corpo =f"""
	<p>Olá!</p>

	<p>Importação de Porto Seguro finalizada com sucesso. </p>

	<p>Segue em anexo, relatório dos processamentos.</p>
	
	{df.to_html(index=False)}
	
	<p>Abs,</p>
	<p>Automação TREO</p>
	"""
	msg.attach(MIMEText(corpo, 'html'))
	with open(anexo,'rb') as arquivos:
		part = MIMEBase('application', 'octet-stream')
		part.set_payload(arquivos.read())
		encoders.encode_base64(part)
		part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(anexo)}"')
		msg.attach(part)
	try:
		with smtplib.SMTP('smtp.office365.com',587) as server:
			server.starttls()
			server.login(remetente, 'Treo@1979')
			server.sendmail(remetente, destinatario, msg.as_string())
			print("Email enviado com sucesso!")
	except Exception as e:
		print(e)

def enviar_email_saidas():
	df = saidas_porto()
	anexo = analitico_saidas_porto()
	remetente = 'automacao@treo.com.br'
	destinatario = [
		'jonas.amaro@dnr.com.br','flavia.reis@dnr.com.br',' sidney.lima@dnr.com.br',
		' alex.barboza@dnr.com.br','diego.rosa@dnr.com.br','controldesk@dnr.com.br',
		'planejamento.porto@dnr.com.br','tamyres.oliveira@dnr.com.br',
		'sistemas@treo.com.br','felipe.fecundes@dnr.com.br', 'nathally.silva@dnr.com.br']
	
	subject = f'(DNR) Relatório de processamentos SAIDAS - PORTO SEGURO -  {datetime.now().strftime("%d/%m/%Y")}'
	
	msg = MIMEMultipart()
	msg['From'] = remetente
	msg['To'] = ', '.join(destinatario)
	msg['Subject'] = subject
	
	# Corpo com DataFrame
	corpo =f"""
	<p>Olá!</p>

	<p>Importação de Porto Seguro finalizada com sucesso. </p>

	<p>Segue em anexo, relatório dos processamentos.</p>

	{df.to_html(index=False)}
	
	<p>Abs,</p>
	<p>Automação TREO</p>
	"""
	msg.attach(MIMEText(corpo, 'html'))
	with open(anexo,'rb') as arquivos:
		part = MIMEBase('application', 'octet-stream')
		part.set_payload(arquivos.read())
		encoders.encode_base64(part)
		part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(anexo)}"')
		msg.attach(part)
	try:
		with smtplib.SMTP('smtp.office365.com',587) as server:
			server.starttls()
			server.login(remetente, 'Treo@1979')
			server.sendmail(remetente, destinatario, msg.as_string())
			print("Email enviado com sucesso!")
	except Exception as e:
		print(e)

def exec_db():
	try:
		conectar_db()
		enviar_email()
		enviar_email_saidas()
	except Exception as e:
		print(f"Erro geral: {e}")

if __name__ == "__main__":
	exec_db()

#schedule.every().day().at("08:00").do(exec_db)