import dlt

@dlt.table(
    name="bronze.statistics",  
    comment="Ingestão de dados de estatísticas das partidas"
)
def bronze_statistics():
    return (
        spark.readStream
        .format("delta")
        .load("/Volumes/databricks_webscraping_football_ws/raw/statistics/all_statistics/")
        .select(
            # Identificadores
            "id",
            "date",
            
            # Assistências esperadas (xA)
            "assistencias_esperadas_xa_away",
            "assistencias_esperadas_xa_home",
            
            # Bolas na trave
            "bolas_na_trave_away",
            "bolas_na_trave_home",
            
            # Cartões
            "cartoes_amarelos_away",
            "cartoes_amarelos_home",
            
            # Chances claras
            "chances_claras_away",
            "chances_claras_home",
            
            # Cruzamentos
            "cruzamentos_away",
            "cruzamentos_home",
            
            # Defesas do goleiro
            "defesas_do_goleiro_away",
            "defesas_do_goleiro_home",
            
            # Desarmes
            "desarmes_away",
            "desarmes_home",
            
            # Duelos ganhos
            "duelos_ganhos_away",
            "duelos_ganhos_home",
            
            # Erros
            "erros_que_resultaram_em_finalizacao_away",
            "erros_que_resultaram_em_finalizacao_home",
            "erros_que_resultaram_em_gol_away",
            "erros_que_resultaram_em_gol_home",
            
            # Escanteios
            "escanteios_away",
            "escanteios_home",
            
            # Faltas
            "faltas_away",
            "faltas_cobradas_away",
            "faltas_cobradas_home",
            "faltas_home",
            
            # Finalizações
            "finalizacoes_bloqueadas_away",
            "finalizacoes_bloqueadas_home",
            "finalizacoes_de_dentro_da_area_away",
            "finalizacoes_de_dentro_da_area_home",
            "finalizacoes_de_fora_da_area_away",
            "finalizacoes_de_fora_da_area_home",
            "finalizacoes_no_alvo_away",
            "finalizacoes_no_alvo_home",
            "finalizacoes_para_fora_away",
            "finalizacoes_para_fora_home",
            "total_de_finalizacoes_away",
            "total_de_finalizacoes_home",
            
            # Gols
            "gols_de_cabeca_away",
            "gols_de_cabeca_home",
            "gols_esperados_xg_away",
            "gols_esperados_xg_home",
            "gols_evitados_away",
            "gols_evitados_home",
            
            # Impedimentos
            "impedimentos_away",
            "impedimentos_home",
            
            # Interceptações
            "interceptacoes_away",
            "interceptacoes_home",
            
            # Laterais
            "laterais_cobrados_away",
            "laterais_cobrados_home",
            
            # Passes
            "passes_away",
            "passes_em_profundidade_certos_away",
            "passes_em_profundidade_certos_home",
            "passes_home",
            "passes_longos_away",
            "passes_longos_home",
            "passes_no_terco_final_away",
            "passes_no_terco_final_home",
            
            # Posse de bola
            "posse_de_bola_away",
            "posse_de_bola_home",
            
            # Rebatidas
            "rebatidas_away",
            "rebatidas_home",
            
            # Toques
            "toques_dentro_da_area_adversaria_away",
            "toques_dentro_da_area_adversaria_home",
            
            # xG nas finalizações no alvo (xGOT)
            "xg_das_finalizacoes_no_alvo_xgot_away",
            "xg_das_finalizacoes_no_alvo_xgot_home",
            "xgot_enfrentado_away",
            "xgot_enfrentado_home"
        )
    )