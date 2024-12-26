from ftplib import FTP_TLS
import ssl
import base64
from parametros import host,port,username,password

# Implemtar o acesso SSL

# Tenta conexão
try:
    ftps = FTP_TLS(host=host,user=username,passwd=password,context=ssl._create_unverified_context()) # Cria a Conexão TLS
    ftps.connect(host, port,timeout=100)
    ftps.set_pasv(True)
    ftps.login(user=username, passwd=password)
    ftps.prot_p()
    print('teste ok')
except Exception as e:
    print('Erro',e)