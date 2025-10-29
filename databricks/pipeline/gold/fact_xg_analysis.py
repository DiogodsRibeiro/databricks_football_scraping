import dlt
from pyspark.sql.functions import (
    col, coalesce, when, lit, round as spark_round,
    sum as spark_sum, avg, count, max as spark_max, min as spark_min
)
from pyspark.sql.window import Window

@dlt.table(
    name="gold.fact_xg_analysis",
    comment="Análise comparativa entre gols esperados (xG) e gols reais"
)
def gold_xg_analysis():
    """
    Comparação entre performance esperada (xG) e real para identificar 
    times sobre/sub-performando
    """
    matches = dlt.read("gold.dim_matches_complete")
    
    return (
        matches
        .select(
            col("match_id"),
            col("match_datetime"),
            col("championship"),
            col("season"),
            col("home_team"),
            col("away_team"),
            
            # Gols Reais
            col("home_score"),
            col("away_score"),
            
            # Expected Goals
            col("expected_goals_xg_home"),
            col("expected_goals_xg_away"),
            
            # Diferença: Real vs Esperado
            spark_round(col("home_score") - col("expected_goals_xg_home"), 2).alias("xg_overperformance_home"),
            spark_round(col("away_score") - col("expected_goals_xg_away"), 2).alias("xg_overperformance_away"),
            
            # Eficiência de conversão
            when(col("expected_goals_xg_home") > 0,
                 spark_round((col("home_score") / col("expected_goals_xg_home")) * 100, 2))
                .otherwise(None).alias("conversion_efficiency_home_pct"),
            when(col("expected_goals_xg_away") > 0,
                 spark_round((col("away_score") / col("expected_goals_xg_away")) * 100, 2))
                .otherwise(None).alias("conversion_efficiency_away_pct"),
            
            # Classificação da partida
            when((col("home_score") - col("expected_goals_xg_home")) > 1, "Home Overperformed")
                .when((col("away_score") - col("expected_goals_xg_away")) > 1, "Away Overperformed")
                .when((col("expected_goals_xg_home") - col("home_score")) > 1, "Home Underperformed")
                .when((col("expected_goals_xg_away") - col("away_score")) > 1, "Away Underperformed")
                .otherwise("As Expected").alias("performance_classification")
        )
        .filter(col("expected_goals_xg_home").isNotNull())
        .filter(col("expected_goals_xg_away").isNotNull())
    )