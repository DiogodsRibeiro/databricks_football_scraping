import json
import os
import re
import unicodedata
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from datetime import datetime
import time
import random
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_CONTAINER_NAME = "calender"  

SCRIPT_DIR = Path(__file__).resolve().parent
URLS_FILE = SCRIPT_DIR / "../../../data/json/all_url.json"

def upload_para_azure(dados, nome_arquivo):
    """
    Faz upload do arquivo JSON para o Azure Blob Storage
    """
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        
        container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)
        
        try:
            container_client.create_container()
            print(f"✅ Container '{AZURE_CONTAINER_NAME}' criado")
        except Exception:
            print(f"ℹ️  Container '{AZURE_CONTAINER_NAME}' já existe")
        
        json_data = json.dumps(dados, ensure_ascii=False, indent=4)
        
        blob_name = nome_arquivo

        blob_client = blob_service_client.get_blob_client(
            container=AZURE_CONTAINER_NAME, 
            blob=blob_name
        )
        
        blob_client.upload_blob(json_data, overwrite=True)
        
        print(f"✅ Arquivo salvo no Azure: {blob_name}")
        return True
        
    except Exception as e:
        print(f"❌ Erro ao fazer upload para o Azure: {e}")
        return False


def limpar_nome_arquivo(nome):
    nome_sem_acentos = unicodedata.normalize('NFKD', nome).encode('ASCII', 'ignore').decode('ASCII')
    nome_formatado = re.sub(r'[^a-zA-Z0-9\s_-]', '', nome_sem_acentos)
    nome_formatado = re.sub(r'\s+', ' ', nome_formatado).strip()
    nome_formatado = nome_formatado.replace(' ', '_')
    return nome_formatado


def extrair_dados(driver, url_resultado, ano_atual):
    dados = []
    campeonato = None  
    season = None
    nacionalidade = None

    driver.get(url_resultado)

    try:
        WebDriverWait(driver, 15).until(EC.visibility_of_element_located((By.CLASS_NAME, "event--fixtures")))

        try:
            driver.find_element(By.ID, "no-match-found")
            print("Não existem mais partidas para essa competição. Campeonato Finalizado!")
            return [], campeonato, season, nacionalidade
        except NoSuchElementException:
            pass

        season = driver.find_element(By.CLASS_NAME, "heading__info").text.strip()
        heading = driver.find_element(By.CLASS_NAME, "heading__title") 
        campeonato = heading.find_element(By.CLASS_NAME, "heading__name").text.strip()

        links = driver.find_elements(By.CLASS_NAME, "breadcrumb__link")
        if len(links) > 1:
            nacionalidade = links[1].text.strip()
        else:
            nacionalidade = "Desconhecido"

        leagues = driver.find_element(By.CSS_SELECTOR, ".container__fsbody #live-table .event--fixtures .sportName.soccer")
        elements = leagues.find_elements(By.CSS_SELECTOR, ".event__round, .event__match")

        current_round = None
        for el in elements:
            class_list = el.get_attribute("class")

            if "event__round" in class_list:
                current_round = el.text.strip()

            elif "event__match" in class_list:
                try:
                    date_time_raw = el.find_element(By.CLASS_NAME, "event__time").text.strip()
                    home_team = el.find_element(By.CLASS_NAME, "event__homeParticipant").text.strip()
                    away_team = el.find_element(By.CLASS_NAME, "event__awayParticipant").text.strip()

                    dia_mes = date_time_raw.split()[0].rstrip('.')
                    
                    # Lógica de ano direta aqui
                    if '/' not in season:
                        # Temporada simples (ex: "2025")
                        anos_na_temporada = re.findall(r'\d{4}', season)
                        ano = int(anos_na_temporada[0]) if anos_na_temporada else ano_atual
                    else:
                        # Temporada cruzada (ex: "2025/2026")
                        data_hoje = datetime.now().date()
                        data_teste = datetime.strptime(f"{dia_mes}.{data_hoje.year}", "%d.%m.%Y").date()
                        ano = data_hoje.year + 1 if data_teste < data_hoje else data_hoje.year
                    
                    data_completa = f"{dia_mes}.{ano}"
                    data_final = datetime.strptime(data_completa, "%d.%m.%Y").strftime("%d/%m/%Y")

                    dados.append({
                        "origem": nacionalidade.capitalize(),
                        "Campeonato": campeonato,
                        "Temporada": season,
                        "Rodada": current_round,
                        "Data": data_final,
                        "Hora": date_time_raw.split()[1],
                        "Time da Casa": home_team,
                        "Time Visitante": away_team,
                        "id": f"{limpar_nome_arquivo(home_team)}_vs_{limpar_nome_arquivo(away_team)}_{data_final}".replace(" ", "")
                    })

                except Exception as e:
                    print(f"Erro ao extrair dados da partida: {e}")

    except Exception as e:
        print(f"Erro ao processar a URL: {url_resultado}\n{e}")

    return dados, campeonato, season, nacionalidade


# Carregar URLs usando caminho relativo
with open(URLS_FILE, "r", encoding="utf-8") as f:
    config = json.load(f)
    urls = [url.replace("{endpoint}", "calendario") for url in config["urls"]]
    #urls = ["https://www.flashscore.com.br/futebol/espanha/laliga/calendario/"]


def carga_calendario():
    ano_atual = datetime.now().year
    todos_os_dados = []

    driver = webdriver.Chrome() 
    try:
        for url_resultado in urls:
            print(f"\nProcessando: {url_resultado}")
            dados_partida, campeonato, season, nacionalidade = extrair_dados(driver, url_resultado, ano_atual)

            if not dados_partida:
                print(f"Sem dados para a URL: {url_resultado}. Continuando com a próxima...\n")
                continue

            print(f"📊 {len(dados_partida)} partidas encontradas para {campeonato} ({nacionalidade})")
            
            todos_os_dados.extend(dados_partida)

            time.sleep(random.uniform(6, 13))

        nome_arquivo = 'all_calendar.json'
        upload_para_azure(todos_os_dados, nome_arquivo)
        
        print(f"\n✅ Total de {len(todos_os_dados)} partidas salvas no Azure")

    finally:
        driver.quit()

    print("\n✅ Extração Finalizada")


carga_calendario()
