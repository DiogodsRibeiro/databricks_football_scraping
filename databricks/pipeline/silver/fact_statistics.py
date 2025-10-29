import dlt
from pyspark.sql.functions import col

@dlt.table(
    name="silver.fact_statistics",
    comment="Estatísticas das partidas"
)
def silver_statistics():
    return (
        dlt.read_stream("bronze.statistics")
        
        .select(
            # Identificadores
            col("id").alias("match_id"),
            col("date").alias("match_date"),
            
            # Expected Assists (xA)
            col("assistencias_esperadas_xa_away").alias("expected_assists_xa_away"),
            col("assistencias_esperadas_xa_home").alias("expected_assists_xa_home"),
            
            # Woodwork
            col("bolas_na_trave_away").alias("woodwork_hits_away"),
            col("bolas_na_trave_home").alias("woodwork_hits_home"),
            
            # Cards
            col("cartoes_amarelos_away").alias("yellow_cards_away"),
            col("cartoes_amarelos_home").alias("yellow_cards_home"),
            
            # Big Chances
            col("chances_claras_away").alias("big_chances_away"),
            col("chances_claras_home").alias("big_chances_home"),
            
            # Crosses
            col("cruzamentos_away").alias("crosses_away"),
            col("cruzamentos_home").alias("crosses_home"),
            
            # Saves
            col("defesas_do_goleiro_away").alias("goalkeeper_saves_away"),
            col("defesas_do_goleiro_home").alias("goalkeeper_saves_home"),
            
            # Tackles
            col("desarmes_away").alias("tackles_away"),
            col("desarmes_home").alias("tackles_home"),
            
            # Duels Won
            col("duelos_ganhos_away").alias("duels_won_away"),
            col("duelos_ganhos_home").alias("duels_won_home"),
            
            # Errors
            col("erros_que_resultaram_em_finalizacao_away").alias("errors_leading_to_shot_away"),
            col("erros_que_resultaram_em_finalizacao_home").alias("errors_leading_to_shot_home"),
            col("erros_que_resultaram_em_gol_away").alias("errors_leading_to_goal_away"),
            col("erros_que_resultaram_em_gol_home").alias("errors_leading_to_goal_home"),
            
            # Corners
            col("escanteios_away").alias("corners_away"),
            col("escanteios_home").alias("corners_home"),
            
            # Fouls
            col("faltas_away").alias("fouls_committed_away"),
            col("faltas_cobradas_away").alias("fouls_drawn_away"),
            col("faltas_cobradas_home").alias("fouls_drawn_home"),
            col("faltas_home").alias("fouls_committed_home"),
            
            # Shots
            col("finalizacoes_bloqueadas_away").alias("blocked_shots_away"),
            col("finalizacoes_bloqueadas_home").alias("blocked_shots_home"),
            col("finalizacoes_de_dentro_da_area_away").alias("shots_inside_box_away"),
            col("finalizacoes_de_dentro_da_area_home").alias("shots_inside_box_home"),
            col("finalizacoes_de_fora_da_area_away").alias("shots_outside_box_away"),
            col("finalizacoes_de_fora_da_area_home").alias("shots_outside_box_home"),
            col("finalizacoes_no_alvo_away").alias("shots_on_target_away"),
            col("finalizacoes_no_alvo_home").alias("shots_on_target_home"),
            col("finalizacoes_para_fora_away").alias("shots_off_target_away"),
            col("finalizacoes_para_fora_home").alias("shots_off_target_home"),
            col("total_de_finalizacoes_away").alias("total_shots_away"),
            col("total_de_finalizacoes_home").alias("total_shots_home"),
            
            # Goals
            col("gols_de_cabeca_away").alias("headed_goals_away"),
            col("gols_de_cabeca_home").alias("headed_goals_home"),
            col("gols_esperados_xg_away").alias("expected_goals_xg_away"),
            col("gols_esperados_xg_home").alias("expected_goals_xg_home"),
            col("gols_evitados_away").alias("goals_prevented_away"),
            col("gols_evitados_home").alias("goals_prevented_home"),
            
            # Offsides
            col("impedimentos_away").alias("offsides_away"),
            col("impedimentos_home").alias("offsides_home"),
            
            # Interceptions
            col("interceptacoes_away").alias("interceptions_away"),
            col("interceptacoes_home").alias("interceptions_home"),
            
            # Throw-ins
            col("laterais_cobrados_away").alias("throw_ins_away"),
            col("laterais_cobrados_home").alias("throw_ins_home"),
            
            # Passes
            col("passes_away").alias("total_passes_away"),
            col("passes_em_profundidade_certos_away").alias("through_balls_away"),
            col("passes_em_profundidade_certos_home").alias("through_balls_home"),
            col("passes_home").alias("total_passes_home"),
            col("passes_longos_away").alias("long_passes_away"),
            col("passes_longos_home").alias("long_passes_home"),
            col("passes_no_terco_final_away").alias("final_third_passes_away"),
            col("passes_no_terco_final_home").alias("final_third_passes_home"),
            
            # Possession
            col("posse_de_bola_away").alias("possession_away"),
            col("posse_de_bola_home").alias("possession_home"),
            
            # Clearances
            col("rebatidas_away").alias("clearances_away"),
            col("rebatidas_home").alias("clearances_home"),
            
            # Touches in box
            col("toques_dentro_da_area_adversaria_away").alias("touches_in_opposition_box_away"),
            col("toques_dentro_da_area_adversaria_home").alias("touches_in_opposition_box_home"),
            
            # xGOT
            col("xg_das_finalizacoes_no_alvo_xgot_away").alias("xgot_away"),
            col("xg_das_finalizacoes_no_alvo_xgot_home").alias("xgot_home"),
            col("xgot_enfrentado_away").alias("xgot_faced_away"),
            col("xgot_enfrentado_home").alias("xgot_faced_home")
        )
        
        .filter(col("match_id").isNotNull())
        .filter(col("match_date").isNotNull())
    )