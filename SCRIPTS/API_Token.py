import requests
import pickle
import os

# URL da API para obter o token
api_url = "http://10.192.80.156:8106/api/token?login=robo.treo"

# Nome do arquivo para salvar o token
token_file = "token.pickle"


# Função para obter um novo token e salvá-lo em um arquivo
def obter_e_salvar_token():
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Verificar se a solicitação foi bem-sucedida

        data = response.json()

        if "token" in data:
            token = data["token"]
            print (token)

            with open(token_file, "wb") as file:
                pickle.dump(data, file)  # Salva o conteúdo retornado pelo JSON

            print("Token obtido com sucesso e salvo.")
        else:
            print("Erro ao obter o token:", data.get("erro", "Erro desconhecido"))
    except requests.exceptions.RequestException as e:
        print("Erro ao fazer a solicitação:", str(e))
    except Exception as e:
        print("Erro ao obter o token:", str(e))


# Verifique o conteúdo do arquivo pickle (token)
def verificar_conteudo_token():
    if os.path.exists(token_file):
        with open(token_file, "rb") as file:
            token_data = pickle.load(file)
            print(token_data)
            return token_data
    else:
        print("Arquivo do token não encontrado.")


# Verifique se o token já existe ou está expirado
obter_e_salvar_token()
verificar_conteudo_token()