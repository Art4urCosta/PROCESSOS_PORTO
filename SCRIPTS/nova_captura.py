import pickle
import schedule
from ftplib import FTP_TLS
from ftplib import FTP
import ssl
import os
from datetime import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from textwrap import dedent
from zipfile import ZipFile, ZIP_DEFLATED
import shutil
import re
import calendar
import locale
import time
import pysftp
import pandas as pd
import requests
from SCRIPTS.parametros import *

def enviar_report(mensagem):
    remetente = 'arthur.costa@treo.com.br'
    destinaratio = remetente

    subject = f'DNR Report processamentos PORTO - {datetime.now().strftime("%d/%m/%Y")}'
    msg = MIMEMultipart()
    #msg['To'] = ', '.join(destinaratio)
    msg['To'] = destinaratio
    msg['Subject'] = subject

    corpo = f"""
    <p>Olá!</p>

    <p>{mensagem}. </p>

    <p>Abs,</p>
    <p>Automação TREO</p>
    """
    msg.attach(MIMEText(corpo, 'html'))

    # Enviar o email
    server = smtplib.SMTP('smtp.office365.com', 587)
    server.starttls()
    server.login(remetente, senha_email)
    server.sendmail(remetente, destinaratio, msg.as_string())
    print(f"E-mail envido com sucesso!")
    server.quit()

def criar_pasta_onedrive():
    # Definir o caminho base (substitua por seu caminho no OneDrive)
    onedrive_path = r"C:\\Users\\Arthur\\OneDrive - TREO\\00_AUTOMAÇÕES\\PROCESSOS_PORTO"  
    pasta_base = os.path.join(onedrive_path, 'LOTES_IMPORTAÇÃO')

    # Criar a estrutura de pastas baseada na data atual
    pasta_ano = datetime.now().strftime("%Y")
    pasta_mes = datetime.now().strftime("%m")
    pasta_dia = f'LOTE_{datetime.now().strftime("%d_%Y")}'

    # Caminhos completos
    caminho_ano = os.path.join(pasta_base, pasta_ano)
    caminho_mes = os.path.join(caminho_ano, pasta_mes)
    caminho_dia = os.path.join(caminho_mes, pasta_dia)

    # Criar as pastas de forma hierárquica
    try:
        for caminho in [pasta_base, caminho_ano, caminho_mes, caminho_dia]:
            if not os.path.exists(caminho):
                os.makedirs(caminho)
                print(f"Pasta criada: {caminho}")
        print(f"Todas as pastas criadas com sucesso em: {caminho_dia}")
        return caminho_dia  
    except Exception as e:
        print(f"Erro ao criar pastas: {e}")

def criar_pasta_onedrive_zip():
    # Definir o caminho base (substitua por seu caminho no OneDrive)
    onedrive_path = r"C:\\Users\\Arthur\\OneDrive - TREO\\00_AUTOMAÇÕES\\PROCESSOS_PORTO"  
    pasta_base = os.path.join(onedrive_path, 'ZIP_IMPORTAÇÃO')

    # Criar a estrutura de pastas baseada na data atual
    pasta_ano = datetime.now().strftime("%Y")
    pasta_mes = datetime.now().strftime("%m")
    pasta_dia = f'LOTE_ZIP_{datetime.now().strftime("%d_%Y")}'

    # Caminhos completos
    caminho_ano = os.path.join(pasta_base, pasta_ano)
    caminho_mes = os.path.join(caminho_ano, pasta_mes)
    caminho_dia = os.path.join(caminho_mes, pasta_dia)

    # Criar as pastas de forma hierárquica
    try:
        for caminho in [pasta_base, caminho_ano, caminho_mes, caminho_dia]:
            if not os.path.exists(caminho):
                os.makedirs(caminho)
                print(f"Pasta criada: {caminho}")
        print(f"Todas as pastas criadas com sucesso em: {caminho_dia}")
        return caminho_dia  
    except Exception as e:
        print(f"Erro ao criar pastas: {e}")

def captura_ftp():
    host = host_ftp_porto
    port = 990
    usuario = usuario_ftp_porto
    senha = senha_ftp_porto
    caminho_pasta = criar_pasta_onedrive()
    caminho_remoto = '/sfpadimp/pub/DISTRIBUICAO'
    data_completa = datetime.now().date() 
    try:
        # Criar um contexto SSL/TLS com parâmetros ajustados
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.set_ciphers('DEFAULT@SECLEVEL=1')  # Define o nível de segurança mais baixo
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE  # Desativa a verificação do certificado
        
        # Conectar ao servidor FTPS usando o contexto personalizado
        ftp = FTP_TLS(context=context)
        ftp.connect(host, port)
        ftp.login(usuario, senha)
        
        # Começa a sessão de TLS
        ftp.prot_p()
        
        # Muda para o diretório desejado
        ftp.cwd(caminho_remoto)
        print("Conectado ao servidor FTPS com sucesso!")

        # Filtrando Arquivos com base na data do dia
        prefixo_arquivo = 'C000000'
        arquivos_modificados_hoje = []
        for nome_arquivo in ftp.nlst():
            data_mod = ftp.sendcmd(f'MDTM {nome_arquivo}')
            data_mod = datetime.strptime(data_mod[4:], '%Y%m%d%H%M%S').date()
            if data_mod == data_completa:
                caminho_destino = os.path.join(caminho_pasta, nome_arquivo)
                with open(caminho_destino, 'wb') as file:
                    ftp.retrbinary(f'RETR {nome_arquivo}', file.write, 1024)  # Ajuste aqui
                arquivos_modificados_hoje.append(nome_arquivo)
                print(f'Arquivo {nome_arquivo} baixado com sucesso para {os.path.join(os.getcwd(), caminho_pasta)}')
    except Exception as e:
        print(e)

def captura_ftp_cdc():
    host = host_ftp_porto
    port = 990
    usuario = usuario_ftp_porto
    senha = senha_ftp_porto
    caminho_pasta = criar_pasta_onedrive()
    caminho_remoto = '/sfpadimp/pub/CDCAMIGAVEL/DISTRIBUICAO'
    data_completa = datetime.now().date() 
    try:
        # Criar um contexto SSL/TLS com parâmetros ajustados
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.set_ciphers('DEFAULT@SECLEVEL=1')  # Define o nível de segurança mais baixo
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE  # Desativa a verificação do certificado
        
        # Conectar ao servidor FTPS usando o contexto personalizado
        ftp = FTP_TLS(context=context)
        ftp.connect(host, port)
        ftp.login(usuario, senha)
        
        # Começa a sessão de TLS
        ftp.prot_p()
        
        # Muda para o diretório desejado
        ftp.cwd(caminho_remoto)
        print("Conectado ao servidor FTPS com sucesso!")

        # Filtrando Arquivos com base na data do dia
        prefixo_arquivo = 'C000000'
        arquivos_modificados_hoje = []
        for nome_arquivo in ftp.nlst():
            data_mod = ftp.sendcmd(f'MDTM {nome_arquivo}')
            data_mod = datetime.strptime(data_mod[4:], '%Y%m%d%H%M%S').date()
            if data_mod == data_completa:
                caminho_destino = os.path.join(caminho_pasta, nome_arquivo)
                with open(caminho_destino, 'wb') as file:
                    ftp.retrbinary(f'RETR {nome_arquivo}', file.write, 1024)  # Ajuste aqui
                arquivos_modificados_hoje.append(nome_arquivo)
                print(f'Arquivo {nome_arquivo} baixado com sucesso para {os.path.join(os.getcwd(), caminho_pasta)}')
    except Exception as e:
        print(e)


def captura_fidic():
    host = host_ftp_porto
    port = 990
    usuario = usuario_ftp_porto
    senha = senha_ftp_porto
    caminho_pasta = criar_pasta_onedrive()
    caminho_remoto = '/sfpadimp/pub/FIDC/DISTRIBUICAO'
    data_completa = datetime.now().date() 
    try:
        # Criar um contexto SSL/TLS com parâmetros ajustados
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.set_ciphers('DEFAULT@SECLEVEL=1')  # Define o nível de segurança mais baixo
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE  # Desativa a verificação do certificado
        
        # Conectar ao servidor FTPS usando o contexto personalizado
        ftp = FTP_TLS(context=context)
        ftp.connect(host, port)
        ftp.login(usuario, senha)
        
        # Começa a sessão de TLS
        ftp.prot_p()
        
        # Muda para o diretório desejado
        ftp.cwd(caminho_remoto)
        print("Conectado ao servidor FTPS com sucesso!")

        # Filtrando Arquivos com base na data do dia
        prefixo_arquivo = 'C000000'
        arquivos_modificados_hoje = []
        for nome_arquivo in ftp.nlst():
            data_mod = ftp.sendcmd(f'MDTM {nome_arquivo}')
            data_mod = datetime.strptime(data_mod[4:], '%Y%m%d%H%M%S').date()
            if data_mod == data_completa:
                caminho_destino = os.path.join(caminho_pasta, nome_arquivo)
                with open(caminho_destino, 'wb') as file:
                    ftp.retrbinary(f'RETR {nome_arquivo}', file.write, 1024)  # Ajuste aqui
                arquivos_modificados_hoje.append(nome_arquivo)
                print(f'Arquivo {nome_arquivo} baixado com sucesso para {os.path.join(os.getcwd(), caminho_pasta)}')
    except Exception as e:
        print(e)

# Variáveis
layout_cdc = '45.TXT'
layout_cart = '51.TXT'
lay_cdc_amg = '50.TXT'
layout_fidc = '64.TXT'

def zip_files():
    captura_ftp()
    captura_ftp_cdc()
    captura_fidic()
    caminho_pasta = criar_pasta_onedrive()
    caminho = os.path.join(os.getcwd(), caminho_pasta)
    
    caminho_pasta_zip = criar_pasta_onedrive_zip()
    caminho_zip = os.path.join(os.getcwd(), caminho_pasta_zip)
    
    data_completa = datetime.now().date()

    # Lista os arquivos na pasta e faz a atribuição de cada variável
    for file in os.listdir(caminho):
        print(f"Arquivos na pasta: {file}")
        
        if file.endswith(layout_cdc):
            base_cdc = file
            print(f"Arquivo base para CDC: {base_cdc}")
            
        elif file.endswith(layout_cart):
            base_cart = file
            print(f"Arquivo base para Cartão: {base_cart}")
            
        elif file.endswith(lay_cdc_amg):
            base_cdc_amg = file
            print(f"Arquivo base para CDC Amigavel: {base_cdc_amg}")

        elif file.endswith(layout_fidc):
            base_fidic = file
            print(f"Arquivo base para FIDIC : {base_fidic}")

        else:
            print("Nenhum arquivo encontrado com o layout especificado.")
            break
        #Criando o zip
        try:
            if base_cdc:
                arquivo_cdc = os.path.join(caminho,base_cdc)
                nome_cdc = f'BASE_PORTO_{data_completa}_CDC_45.zip'
                base_cdc_zip = os.path.join(caminho_zip,f'{arquivo_cdc}') # Linha alterada para testar se vai dar erro na importação
                
                
                with ZipFile(base_cdc_zip,"w",compression=ZIP_DEFLATED) as zip:
                    zip.write(arquivo_cdc,arcname=os.path.basename(arquivo_cdc))
                    print(f'{base_cdc_zip} criado com sucesso')
            
            if base_cart:
                arquivo_cart = os.path.join(caminho,base_cart)
                nome_cart = f'BASE_PORTO_{data_completa}_CARTAO_51.zip'
                base_cart_zip = os.path.join(caminho_zip,f'{arquivo_cart}') # Linha alterada para testar se vai dar erro na importação

                with ZipFile(base_cart_zip,"w",compression=ZIP_DEFLATED) as zip:
                    zip.write(arquivo_cart,arcname=os.path.basename(arquivo_cart))
                    print(f'{base_cart_zip} criado com sucesso')
            
            if base_cdc_amg:
                arquivo_cdc_amg = os.path.join(caminho,base_cdc_amg)
                nome_cdc_amg = f'BASE_PORTO_{data_completa}_CDC_AMG_50.zip'
                base_cdc_amg_zip = os.path.join(caminho_zip,f'{arquivo_cdc_amg}')# Linha alterada para testar se vai dar erro na importação

                with ZipFile(base_cdc_amg_zip,"w",compression=ZIP_DEFLATED) as zip:
                    zip.write(arquivo_cdc_amg,arcname=os.path.basename(arquivo_cdc_amg))
                    print(f'{base_cdc_amg_zip} criado com sucesso')
            
            if base_fidic:
                arquivo_cdc_amg = os.path.join(caminho,base_cdc_amg)
                nome_cdc_amg = f'BASE_PORTO_{data_completa}_CDC_AMG_50.zip'
                base_fidic_zip = os.path.join(caminho_zip,f'{base_fidic}')# Linha alterada para testar se vai dar erro na importação

                with ZipFile(base_cdc_amg_zip,"w",compression=ZIP_DEFLATED) as zip:
                    zip.write(arquivo_cdc_amg,arcname=os.path.basename(base_fidic))
                    print(f'{base_fidic_zip} criado com sucesso')
            
            print("Todos os arquivos foram compactados com sucesso!")
        except Exception as e:
            print(f"Erro ao criar o arquivo ZIP: {e}")

#zip_files()

def sftp_cslog():
    sftp_host ="192.168.1.36"
    sftp_user = "carga"
    sftp_password = "czZhEu"  # Lembre-se de substituir pela senha real
    sftp_port = 22  # Porta padrão para SFTP
    sftp_directory = "/carga/importacao"  # Diretório no servidor SFTP
    # Envia o arquivo para o servidor SFTP
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    caminho_pasta_zip = criar_pasta_onedrive()

    with pysftp.Connection(host=sftp_host, username=sftp_user, password=sftp_password, port=sftp_port, cnopts=cnopts) as sftp:
        
        sftp.chdir(sftp_directory)
        
        for file in os.listdir(caminho_pasta_zip):
            print(f"Arquivos ZIP: {file}")
            sftp.put(os.path.join(caminho_pasta_zip, file), file)
            print(f"Arquivo {file} enviado com sucesso para o servidor SFTP")
            
        arquivos = sftp.listdir()  # Lista os arquivos no diretório atual
        
        #print("Arquivos 'PORTO':")
        
            # Filtra os arquivos que-BEGIN com 'BASE_PORTO'
        """
        arquivos_filtrados = [arquivo for arquivo in arquivos if arquivo.startswith('C00000')]
        # Exibe os arquivos filtrados
        if arquivos_filtrados:
            for arquivo in arquivos_filtrados:
                print(arquivo)

        else:
            print("Nenhum arquivo 'PORTO' encontrado.")"""

def obter_e_salvar_token():
    try:
        api_url = "http://10.192.80.156:8106/api/token?login=robo.treo"
        token_file = "token.pickle"
        response = requests.get(api_url)
        response.raise_for_status()  # Verificar se a solicitação foi bem-sucedida

        data = response.json()

        if "token" in data:
            token = data["token"]
            
            print (token)

            with open(token_file, "wb") as file:
                pickle.dump(data, file)# Salva o conteúdo retornado pelo JSON
                print('Salva arquivo Pickle')
                
            print("Token obtido com sucesso e salvo.")
        else:
            print("Erro ao obter o token:", data.get("erro", "Erro desconhecido"))
    except requests.exceptions.RequestException as e:
        print("Erro ao fazer a solicitação:", str(e))
    except Exception as e:
        print("Erro ao obter o token:", str(e))
    return token

# URL para importar o arquivo
url = "http://10.192.80.156:8106/api/importaArquivo" 

# Nome do arquivo para salvar o token
token_file = "token.pickle"

def obter_token():
    token_file = "token.pickle"
    try:
        if os.path.exists(token_file):
            with open(token_file, "rb") as file:
                token_data = pickle.load(file)
                return token_data.get("token")
        else:
            print("Arquivo do token não encontrado.")
            return None
    except Exception as e:
        print("Erro ao obter o token:", str(e))
        return None

def importar(arquivo,carteira,tipo):
    #zip_files()
    sftp_cslog()
    token = obter_token()
    empresa = "Porto Seguro"
    nFicha = 1
    if token:
        try:
            parametros = {
                "token": token,
                "arquivo": arquivo,
                "empresa": empresa,
                "carteira": carteira,
                "tipo": tipo,
                "nFicha": nFicha,
            }

            response = requests.post(url, params=parametros)
            response_data = response.json()

            if "ticket" in response_data:
                if response_data["ticket"]:
                    print(
                        "Importação realizada com sucesso. Ticket:",
                        response_data["ticket"],
                    )
                    ticket=response_data["ticket"]
                    return ticket
                else:
                    print(
                        "Erro ao importar o arquivo:",
                        response_data.get("erro"),
                    )
                    erro_import = response_data.get("erro")
                    
                    match = re.search(r'Id = (\d+)', erro_import)

                    #envio_erro_wpp('5511965042803',f"Erro na importação de {carteira} {tipo}.{response_data.get("erro")} - Mensagem automática")
                    #envio_erro_wpp('5511981333752',f"Erro na importação de {carteira} {tipo}.{response_data.get("erro")} - Mensagem automática")
                    #envio_erro_wpp('5511981333752',f"Erro na importação de {carteira} {tipo}.{response_data.get("erro")} - Mensagem automática")
                    # {response_data.get("erro")}
                    informacao = f"{response_data.get("erro")}"
                    enviar_report(informacao)
                    if match:
                        ticket = match.group(1)
                        print(f"Ticket:{ticket}")
                        ticket=ticket
                        return ticket

                    else:
                        print("Nenhum ID encontrado na mensagem de erro.")      
            else:
                print("Resposta inválida:", response_data)
        except requests.exceptions.RequestException as e:
            print("Erro ao fazer a solicitação:", str(e))
        except Exception as e:
            print("Erro na solicitação:", str(e))
    else:
        print("Token não disponível. Não foi possível realizar a importação.")

layout_cdc = '45.TXT'
layout_cart = '51.TXT'
lay_cdc_amg = '50.TXT'
layout_fidc = '64.TXT'

def processar_importacoes():
    captura_ftp()
    captura_ftp_cdc()
    captura_fidic()
    data_completa = datetime.now().date()
    path = criar_pasta_onedrive()
    all_files = os.listdir(path)
    
    cdc = [file for file in all_files if file.endswith(layout_cdc)]   
    
    cdc_amg = [file for file in all_files if file.endswith(lay_cdc_amg)]

    cart = [file for file in all_files if file.endswith(layout_cart)]

    fidc = [file for file in all_files if file.endswith(layout_fidc)]
    print(cdc,cdc_amg,cart,fidc)

    
    carteiras_arquivos = {
        "CDC": cdc,
        "CDC Amigável": cdc_amg,
        "Cartão Ws": cart,
        "FIDC Cartão": fidc
    }


    tipos = ["Pré-envio","Envio","Envio Acordo"]
    #tipos= []
    for carteira, arquivo in carteiras_arquivos.items():
        print(f"Iniciando para importação:{carteira}")
        for tipo in tipos:
                print(f"Iniciando:{tipo} para {arquivo}")
                token = obter_e_salvar_token()
                ticket_pre_envio = importar(arquivo=arquivo,carteira=carteira,tipo=tipo)
                while True:
                    
                    # URL da API para ver o status do arquivo
                    url = f"http://10.192.80.156:8106/api/statusImportacao?token={token}&ticket={ticket_pre_envio}"

                    
                    # Fazer a requisição à API
                    response = requests.get(url)

                    # Verificar o código de status da resposta
                    if response.status_code == 200:
                        try:
                            # Tentar converter a resposta para JSON
                            data = response.json()
                            # Fazer algo com os dados recebidos
                            print(data)
                        except requests.exceptions.JSONDecodeError as e:
                            print("Erro ao decodificar JSON:", e)
                    else:
                        print("Erro na requisição. Código de status:", response.status_code)

                    # Extrair informações do response
                    status = data.get("status")
                    info = data.get("info")
                    mensagem_erro = data.get("erro")


                    # Verificar o status e agir de acordo
                    if status == "Não Iniciado" or status == "Aguardando" or status == "Aguardando outras instancias" or status == "Testando" or status == "Processando" or status == "Processando em outra(s) instancia(s)":
                        print(f"Status: {status}, Info: {info}")
                        time.sleep(10)  # Esperar 10 segundos antes da próxima requisição
                    elif status == "Erro":
                        #info = {status} para {carteira} {tipo}
                        enviar_report(f"Importação de {carteira} {tipo} falhou com erro: {mensagem_erro}")
                        break  # Parar o loop em caso de erro
                    elif status == "Finalizado":
                        print("Importação finalizada com sucesso.")
                        def enviar_report_sucesso():
                            remetente = 'arthur.costa@treo.com.br'
                            destinaratio = [
                                'arthur.costa@treo.com.br','arthurgm66@gmail.com']
                            
                            subject = f'DNR Report processamentos {carteira} {tipo} - {datetime.now().strftime("%d/%m/%Y")}'
                            msg = MIMEMultipart()
                            remetente = remetente
                            msg['To'] = ', '.join(destinaratio)
                            msg['Subject'] = subject

                            corpo = f"""
                            <p>Olá!</p>

                            <p>Importação de {carteira}, {tipo} finalizada com sucesso. </p>

                            <p>Abs,</p>
                            <p>Automação TREO</p>
                            """
                            msg.attach(MIMEText(corpo, 'html'))

                            # Enviar o email
                            server = smtplib.SMTP('smtp.office365.com', 587)
                            server.starttls()
                            server.login(remetente, 'Trocarsenha@@5966')
                            server.sendmail(remetente, destinaratio, msg.as_string())
                            print(f"E-mail envido com sucesso!")
                            server.quit()
                        def envio_report_wpp(contato,mensagem):
                            # API details
                            api_url = "http://localhost:3000/api/sendText"
                            data = {
                                "chatId": f"{contato}@c.us",  #Coloque o número de telefone com 5511. MANTER O "@c.us"
                                "text": mensagem,
                                "session": "default"  # 
                            }

                            #Chama a requisição
                            response = requests.post(api_url, json=data)

                            # Verificando a resposta
                            if response.status_code == 201:
                                print("Mensagem enviada com sucesso!")
                                print("Resposta:", response.json())
                            else:
                                print("Falha ao enviar a mensagem.")
                                print("Código de Status:", response.status_code)
                                print("Resposta:", response.text)
                        #envio_report_wpp("5511965042803",f"Importação de {carteira} {tipo} falhou com erro: {mensagem_erro}")
                        enviar_report_sucesso()
                        break  # Parar o loop em caso de sucesso
                    elif status == "Cancelado":
                        mensagem_erro = data.get("erro")
                        status = data.get("status")
                        print(f"Importação cancelada: {mensagem_erro}")
                        print(f'Erro Detalhamento:{status}')
                        break  # Parar o loop em caso de cancelamento
                    elif mensagem_erro == "Token inválido: ":
                        mensagem_erro = data.get("erro")
                        print(f"Erro: {mensagem_erro}")
                        token=obter_e_salvar_token()

                    else:
                        print(f"Status desconhecido: {status}")
                        break  # Parar o loop se o status for desconhecido

try:
    processar_importacoes()
    #from SCRIPTS.conect_db import exec_db
    #exec_db()
except Exception as e:
    print(f"Ocorreu um erro: {str(e)}")