import json
import time
import re
import unicodedata
import csv
import io
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

load_dotenv()

AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_INPUT = "statistics-urls"  
CONTAINER_OUTPUT = "statistics"     

def limpar_nome_arquivo(nome):
    nome_sem_acentos = unicodedata.normalize('NFKD', nome).encode('ASCII', 'ignore').decode('ASCII')
    nome_formatado = re.sub(r'[^a-zA-Z0-9\s_-]', '', nome_sem_acentos)
    nome_formatado = re.sub(r'\s+', ' ', nome_formatado).strip()
    nome_formatado = nome_formatado.replace(' ', '_')
    return nome_formatado


def download_from_azure(container_name, blob_name):
    """
    Baixa um arquivo CSV do Azure Blob Storage e retorna os dados
    """
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        
        download_stream = blob_client.download_blob()
        conteudo = download_stream.readall().decode('utf-8')
        
        csv_reader = csv.DictReader(io.StringIO(conteudo))
        dados = [row['url'] for row in csv_reader]
        
        print(f"✅ Arquivo baixado do Azure: {blob_name}")
        return dados
    except Exception as e:
        print(f"❌ Erro ao baixar do Azure: {e}")
        return None


def upload_para_azure(dados, container_name, nome_arquivo):
    """
    Faz upload incremental do arquivo CSV para o Azure Blob Storage
    """
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(container_name)
        
        try:
            container_client.create_container()
            print(f"✅ Container '{container_name}' criado")
        except Exception:
            print(f"ℹ️  Container '{container_name}' já existe")
        
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=nome_arquivo)
        
        dados_existentes = []
        try:
            download_stream = blob_client.download_blob()
            conteudo_existente = download_stream.readall().decode('utf-8')
            csv_reader = csv.DictReader(io.StringIO(conteudo_existente))
            dados_existentes = list(csv_reader)
            print(f"📥 Arquivo existente carregado com {len(dados_existentes)} registros")
        except Exception:
            print(f"ℹ️  Arquivo não existe ainda, criando novo")
        
        ids_existentes = {item['id'] for item in dados_existentes}
        novos_dados = [item for item in dados if item['id'] not in ids_existentes]
        
        dados_finais = dados_existentes + novos_dados
        
        print(f"➕ Adicionando {len(novos_dados)} novos registros")
        print(f"📊 Total final: {len(dados_finais)} registros")
        
        # Coletar todas as colunas possíveis de todos os registros
        todas_colunas = set()
        for item in dados_finais:
            todas_colunas.update(item.keys())
        
        # Ordenar colunas: 'date' e 'id' primeiro, depois as outras em ordem alfabética
        colunas_fixas = ['date', 'id']
        outras_colunas = sorted([col for col in todas_colunas if col not in colunas_fixas])
        fieldnames = colunas_fixas + outras_colunas
        
        # Converter para CSV
        csv_buffer = io.StringIO()
        if dados_finais:
            csv_writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames, extrasaction='ignore')
            csv_writer.writeheader()
            
            # Preencher campos vazios com string vazia
            for item in dados_finais:
                row = {col: item.get(col, '') for col in fieldnames}
                csv_writer.writerow(row)
        
        csv_data = csv_buffer.getvalue()
        
        blob_client.upload_blob(csv_data, overwrite=True)
        
        print(f"✅ Arquivo salvo no Azure: {nome_arquivo}")
        return True
        
    except Exception as e:
        print(f"❌ Erro ao fazer upload para o Azure: {e}")
        return False


def esperar_todos_elementos(driver, classes, espera=15):
    for class_name in classes:
        WebDriverWait(driver, espera).until(EC.presence_of_element_located((By.CLASS_NAME, class_name)))


def coletar_estatisticas_partidas_incremental():

    print("📥 Baixando URLs do Azure...")
    urls = download_from_azure(CONTAINER_INPUT, "statistics_urls.csv")
    
    if not urls:
        print("❌ Não foi possível carregar as URLs do Azure")
        return

    options = Options()
    options.add_argument('--disable-logging')
    options.add_argument('--log-level=3')
    options.add_argument('--silent')
    options.add_argument('--disable-gpu-logging')
    options.add_argument('--disable-extensions-logging')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--no-sandbox')

    todos_os_jogos = []
    urls_com_falha = []
    driver = webdriver.Chrome(options=options)

    for i, url in enumerate(urls):
        max_tentativas = 3
        tentativas = 0
        sucesso = False
        
        print(f"🔄 Processando URL {i+1}/{len(urls)}: {url}")
        
        while tentativas < max_tentativas and not sucesso:
            try:
                driver.get(url)
                
                esperar_todos_elementos(driver, [
                    "wcl-row_2oCpS",
                    "wcl-awayValue_Y-QR1",
                    "wcl-category_6sT1J", 
                    "wcl-homeValue_3Q-7P",
                    "wcl-charts_UfKzp"
                ], espera=15)

                date = driver.find_element(By.CLASS_NAME, "duelParticipant__startTime").text.strip().split(" ")[0]
                data_formatada = datetime.strptime(date, "%d.%m.%Y").strftime("%d/%m/%Y")

                home_team = driver.find_element(By.CLASS_NAME, "duelParticipant__home").find_element(By.CSS_SELECTOR, ".participant__participantName a").text.strip()
                away_team = driver.find_element(By.CLASS_NAME, "duelParticipant__away").find_element(By.CSS_SELECTOR, ".participant__participantName a").text.strip()

                id_value = f"{limpar_nome_arquivo(home_team)}_vs_{limpar_nome_arquivo(away_team)}_{data_formatada}".replace(" ", "")

                estatisticas = {}
                
                row_selectors = ['.wcl-row_2oCpS', '.wcl-row_OFViZ']
                linhas = []
                
                for row_selector in row_selectors:
                    linhas = driver.find_elements(By.CSS_SELECTOR, row_selector)
                    if linhas:
                        break
                
                for linha in linhas:
                    try:
                       
                        home_selectors = [
                            '.wcl-homeValue_3Q-7P', '.wcl-homeValue_-iJBW', 
                            '[class*="homeValue"]'
                        ]
                        away_selectors = [
                            '.wcl-awayValue_Y-QR1', '.wcl-awayValue_rQvxs',
                            '[class*="awayValue"]'
                        ]
                        category_selectors = [
                            '.wcl-category_6sT1J', '.wcl-category_7qsgP',
                            '[class*="category"]'
                        ]
                        
                        home_value, away_value, category = None, None, None
                        
                     
                        for selector in home_selectors:
                            try:
                                home_value = linha.find_element(By.CSS_SELECTOR, selector).text.strip()
                                break
                            except:
                                continue
                        
                    
                        for selector in away_selectors:
                            try:
                                away_value = linha.find_element(By.CSS_SELECTOR, selector).text.strip()
                                break
                            except:
                                continue
                        
                        for selector in category_selectors:
                            try:
                                category = linha.find_element(By.CSS_SELECTOR, selector).text.strip()
                                break
                            except:
                                continue

                        if home_value and away_value and category:
                            estatisticas[category] = {
                                "home_team": home_value,
                                "away_team": away_value
                            }
                            
                    except Exception as e:
                        continue

                if not estatisticas:
                    raise Exception("Nenhuma estatística encontrada.")

                game_stats = {
                    "date": data_formatada,
                    "id": id_value,
                    **estatisticas
                }

                todos_os_jogos.append(game_stats)
                print(f"✅ Coletado: {id_value}")
                sucesso = True

                if len(todos_os_jogos) % 100 == 0:
                    upload_para_azure(todos_os_jogos, CONTAINER_OUTPUT, "incremental_games_statistics.csv")
                    print(f"💾 Progresso salvo com {len(todos_os_jogos)} jogos.")

            except Exception as e:
                tentativas += 1
                print(f"❌ Erro ao processar {url} (tentativa {tentativas}/{max_tentativas}): {e}")
                
                if tentativas >= max_tentativas:
                    urls_com_falha.append({
                        "url": url,
                        "erro": str(e),
                        "posicao": i + 1
                    })
                    print(f"🚫 URL falhou após {max_tentativas} tentativas: {url}")
                    break
                    
                try:
                    driver.quit()
                except:
                    pass
                print(f"🔄 Reiniciando navegador em 10 segundos... (tentativa {tentativas + 1}/{max_tentativas})")
                time.sleep(10)
                driver = webdriver.Chrome(options=options)

        if sucesso:
            time.sleep(3)

    upload_para_azure(todos_os_jogos, CONTAINER_OUTPUT, "incremental_games_statistics.csv")

    if urls_com_falha:
        upload_para_azure(urls_com_falha, CONTAINER_OUTPUT, "urls_com_falha.csv")
        print(f"⚠️  {len(urls_com_falha)} URLs falharam")

    print(f"\n✅ {len(todos_os_jogos)} jogos coletados com sucesso!")
    print(f"❌ {len(urls_com_falha)} URLs falharam")
    
    driver.quit()

coletar_estatisticas_partidas_incremental()