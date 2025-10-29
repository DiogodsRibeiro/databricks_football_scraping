import dlt
from pyspark.sql.functions import (
    col, coalesce, when, lit, round as spark_round,
    sum as spark_sum, avg, count, max as spark_max, min as spark_min
)
from pyspark.sql.window import Window

@dlt.table(
    name="gold.fact_team_performance",
    comment="Métricas agregadas de performance por time, campeonato e temporada"
)
def gold_team_performance():
    """
    Estatísticas agregadas por time para análise de desempenho
    """
    matches = dlt.read("gold.dim_matches_complete")
    
    # Performance em casa
    home_stats = (
        matches
        .groupBy("home_team", "championship", "season", "country")
        .agg(
            count("*").alias("games_played_home"),
            spark_sum(when(col("match_result") == "Home Win", 1).otherwise(0)).alias("wins_home"),
            spark_sum(when(col("match_result") == "Draw", 1).otherwise(0)).alias("draws_home"),
            spark_sum(when(col("match_result") == "Away Win", 1).otherwise(0)).alias("losses_home"),
            spark_sum("home_score").alias("goals_scored_home"),
            spark_sum("away_score").alias("goals_conceded_home"),
            avg("possession_home").alias("avg_possession_home"),
            avg("expected_goals_xg_home").alias("avg_xg_home"),
            avg("shots_on_target_home").alias("avg_shots_on_target_home"),
            avg("total_shots_home").alias("avg_total_shots_home")
        )
        .withColumnRenamed("home_team", "team")
    )
    
    # Performance fora
    away_stats = (
        matches
        .groupBy("away_team", "championship", "season", "country")
        .agg(
            count("*").alias("games_played_away"),
            spark_sum(when(col("match_result") == "Away Win", 1).otherwise(0)).alias("wins_away"),
            spark_sum(when(col("match_result") == "Draw", 1).otherwise(0)).alias("draws_away"),
            spark_sum(when(col("match_result") == "Home Win", 1).otherwise(0)).alias("losses_away"),
            spark_sum("away_score").alias("goals_scored_away"),
            spark_sum("home_score").alias("goals_conceded_away"),
            avg("possession_away").alias("avg_possession_away"),
            avg("expected_goals_xg_away").alias("avg_xg_away"),
            avg("shots_on_target_away").alias("avg_shots_on_target_away"),
            avg("total_shots_away").alias("avg_total_shots_away")
        )
        .withColumnRenamed("away_team", "team")
    )
    
    # Combinar estatísticas
    return (
        home_stats
        .join(away_stats, ["team", "championship", "season", "country"], "outer")
        .select(
            col("team"),
            col("championship"),
            col("season"),
            col("country"),
            
            # Jogos Totais
            (coalesce("games_played_home", lit(0)) + coalesce("games_played_away", lit(0))).alias("total_games"),
            coalesce("games_played_home", lit(0)).alias("games_home"),
            coalesce("games_played_away", lit(0)).alias("games_away"),
            
            # Vitórias/Empates/Derrotas
            (coalesce("wins_home", lit(0)) + coalesce("wins_away", lit(0))).alias("total_wins"),
            (coalesce("draws_home", lit(0)) + coalesce("draws_away", lit(0))).alias("total_draws"),
            (coalesce("losses_home", lit(0)) + coalesce("losses_away", lit(0))).alias("total_losses"),
            
            # Pontos (3 por vitória, 1 por empate)
            ((coalesce("wins_home", lit(0)) + coalesce("wins_away", lit(0))) * 3 + 
             (coalesce("draws_home", lit(0)) + coalesce("draws_away", lit(0)))).alias("total_points"),
            
            # Gols
            (coalesce("goals_scored_home", lit(0)) + coalesce("goals_scored_away", lit(0))).alias("total_goals_scored"),
            (coalesce("goals_conceded_home", lit(0)) + coalesce("goals_conceded_away", lit(0))).alias("total_goals_conceded"),
            (coalesce("goals_scored_home", lit(0)) + coalesce("goals_scored_away", lit(0)) - 
             coalesce("goals_conceded_home", lit(0)) - coalesce("goals_conceded_away", lit(0))).alias("goal_difference"),
            
            # Médias
            spark_round(
                (coalesce("avg_possession_home", lit(0)) + coalesce("avg_possession_away", lit(0))) / 2, 
                2
            ).alias("avg_possession"),
            spark_round(
                (coalesce("avg_xg_home", lit(0)) + coalesce("avg_xg_away", lit(0))) / 2,
                2
            ).alias("avg_xg"),
            spark_round(
                (coalesce("avg_shots_on_target_home", lit(0)) + coalesce("avg_shots_on_target_away", lit(0))) / 2,
                2
            ).alias("avg_shots_on_target"),
            spark_round(
                (coalesce("avg_total_shots_home", lit(0)) + coalesce("avg_total_shots_away", lit(0))) / 2,
                2
            ).alias("avg_total_shots")
        )
    )