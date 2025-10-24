import sys
import os
import time
from datetime import datetime

# Adiciona o diretório raiz ao path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))


from src.data_processing.get_incremental_results import carga_incremental_results
from src.data_processing.get_all_data_calendar import carga_calendario
from src.data_processing.get_all_urls_statistics import coletar_urls_estatisticas
from src.data_processing.get_incremental_statistics import coletar_estatisticas_partidas_incremental


def main():
    """
    # Função principal que executa o pipeline de coleta de dados
    # """
    inicio = time.time()
    
    # print("Iniciando carga das rodadas finalizadas, dados serão inseridos na camada staging")
    # carga_incremental_results()
    
    # print("Aguardando 20 segundos antes de iniciar a carga do endpoint calendario")
    # time.sleep(20)
    # carga_calendario()
    # print("Aguardando 20 segundos antes de coletar as urls das estatisticas")
    # time.sleep(20)
    # coletar_urls_estatisticas()
    
    print("Aguardando 20 segundos antes de coletar as estatísticas das partidas")
    time.sleep(20)
    coletar_estatisticas_partidas_incremental()
    
    fim = time.time()
    tempo_total = fim - inicio
    
    print(f"\nProcesso concluído com sucesso!")
    print(f"Tempo total de execução: {tempo_total:.2f} segundos ({tempo_total/60:.2f} minutos)")


if __name__ == "__main__":
    main()