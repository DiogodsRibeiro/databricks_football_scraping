import json
import re
import csv
import time
from pathlib import Path
import io 
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

# Carregar variáveis do arquivo .env
load_dotenv()

# Configurações do Azure
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_CONTAINER_NAME = "statistics-urls"  # Nome do container para estatísticas

# Caminho relativo ao script
SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_FILE = "data/json/all_url.json"


def upload_para_azure(dados, nome_arquivo):
    """
    Faz upload do arquivo CSV para o Azure Blob Storage
    """
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)
        
        try:
            container_client.create_container()
            print(f"✅ Container '{AZURE_CONTAINER_NAME}' criado")
        except Exception:
            print(f"ℹ️  Container '{AZURE_CONTAINER_NAME}' já existe")
        
        # Converter para CSV
        csv_buffer = io.StringIO()
        csv_writer = csv.writer(csv_buffer)
        csv_writer.writerow(['url'])
        for url in dados:
            csv_writer.writerow([url])
        csv_data = csv_buffer.getvalue()
        
        blob_name = nome_arquivo
        blob_client = blob_service_client.get_blob_client(
            container=AZURE_CONTAINER_NAME, 
            blob=blob_name
        )
        
        blob_client.upload_blob(csv_data, overwrite=True)
        
        print(f"✅ Arquivo salvo no Azure: {blob_name}")
        return True
        
    except Exception as e:
        print(f"❌ Erro ao fazer upload para o Azure: {e}")
        return False


def coletar_urls_estatisticas():
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    urls_template = data["urls"]
    endpoint = "resultados"
    urls_input = [url.replace("{endpoint}", endpoint) for url in urls_template]

    driver = webdriver.Chrome()
    all_stats_links = []

    hoje = datetime.now().date()
    inicio_intervalo = hoje - timedelta(days=2)

    try:
        for URL in urls_input:
            if not URL.startswith("http"):
                print(f"URL inválida ignorada: {URL}")
                continue

            print(f"🌐 Acessando: {URL}")
            driver.get(URL)
            time.sleep(3)

            try:
                WebDriverWait(driver, 10).until(
                    EC.visibility_of_element_located((By.CLASS_NAME, "event--results"))
                )
            except:
                print("Página não carregou corretamente, pulando.")
                continue
            
            try:
                leagues = driver.find_element(By.CLASS_NAME, "container__fsbody") \
                                .find_element(By.ID, "live-table") \
                                .find_element(By.CLASS_NAME, "event--results") \
                                .find_element(By.CLASS_NAME, "sportName.soccer") 

                elements = leagues.find_elements(By.CSS_SELECTOR, ".event__round, .event__match")

                for match in elements:
                    try:
                        class_list = match.get_attribute("class")
                        if "event__match" not in class_list:
                            continue
                        
                        date_time_raw = match.find_element(By.CLASS_NAME, "event__time").text.strip()
                        dia_mes = date_time_raw.split()[0].rstrip('.')
                        dia = int(dia_mes.split('.')[0])
                        mes = int(dia_mes.split('.')[1])
                        ano = datetime.now().year
                        data_partida = datetime(ano, mes, dia).date()

                        if not (inicio_intervalo <= data_partida <= hoje):
                            continue

                        link = match.find_element(By.CSS_SELECTOR, "a.eventRowLink").get_attribute("href")
                        
                        link = re.sub(r'/\?mid=[^/]*', '', link)
                        
                        all_stats_links.append(link.rstrip("/") + "/resumo/estatisticas/0/")

                    except Exception as e:
                        pass
                        
            except Exception as e:
                print(f"Erro ao processar {URL}: {e}")
                continue

        nome_arquivo = 'statistics_urls.csv'
        upload_para_azure(all_stats_links, nome_arquivo)
        
        print(f"\n✅ {len(all_stats_links)} links (últimos 4 dias incluindo hoje) salvos no Azure")

    finally:
        driver.quit()

coletar_urls_estatisticas()