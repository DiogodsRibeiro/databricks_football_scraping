@dlt.table(
    name="gold.fact_attack_defense_metrics",
    comment="Métricas detalhadas de eficiência ofensiva e defensiva"
)
def gold_attack_defense_metrics():
    """
    Análise profunda de capacidade ofensiva e defensiva dos times
    """
    matches = dlt.read("gold.dim_matches_complete")
    
    # Métricas ofensivas (casa)
    attack_home = (
        matches
        .groupBy("home_team", "championship", "season")
        .agg(
            # Finalizações
            avg("total_shots_home").alias("avg_shots_home"),
            avg("shots_on_target_home").alias("avg_shots_on_target_home"),
            avg("shots_inside_box_home").alias("avg_shots_inside_box_home"),
            avg("big_chances_home").alias("avg_big_chances_home"),
            
            # Eficiência
            avg(when(col("total_shots_home") > 0,
                    (col("shots_on_target_home") / col("total_shots_home")) * 100)
               ).alias("avg_shot_accuracy_home"),
            
            # Criação
            avg("expected_assists_xa_home").alias("avg_xa_home"),
            avg("crosses_home").alias("avg_crosses_home"),
            avg("final_third_passes_home").alias("avg_final_third_passes_home"),
            avg("through_balls_home").alias("avg_through_balls_home"),
            
            # Métricas defensivas (sofrendo)
            avg("goalkeeper_saves_home").alias("avg_saves_made_home"),
            avg("tackles_home").alias("avg_tackles_home"),
            avg("interceptions_home").alias("avg_interceptions_home"),
            avg("clearances_home").alias("avg_clearances_home"),
            avg("away_score").alias("avg_goals_conceded_home")
        )
        .withColumnRenamed("home_team", "team")
        .withColumn("location", lit("home"))
    )
    
    # Métricas ofensivas (fora)
    attack_away = (
        matches
        .groupBy("away_team", "championship", "season")
        .agg(
            avg("total_shots_away").alias("avg_shots_away"),
            avg("shots_on_target_away").alias("avg_shots_on_target_away"),
            avg("shots_inside_box_away").alias("avg_shots_inside_box_away"),
            avg("big_chances_away").alias("avg_big_chances_away"),
            
            avg(when(col("total_shots_away") > 0,
                    (col("shots_on_target_away") / col("total_shots_away")) * 100)
               ).alias("avg_shot_accuracy_away"),
            
            avg("expected_assists_xa_away").alias("avg_xa_away"),
            avg("crosses_away").alias("avg_crosses_away"),
            avg("final_third_passes_away").alias("avg_final_third_passes_away"),
            avg("through_balls_away").alias("avg_through_balls_away"),
            
            avg("goalkeeper_saves_away").alias("avg_saves_made_away"),
            avg("tackles_away").alias("avg_tackles_away"),
            avg("interceptions_away").alias("avg_interceptions_away"),
            avg("clearances_away").alias("avg_clearances_away"),
            avg("home_score").alias("avg_goals_conceded_away")
        )
        .withColumnRenamed("away_team", "team")
        .withColumn("location", lit("away"))
    )
    
    return attack_home.union(attack_away)