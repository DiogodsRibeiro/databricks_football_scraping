import dlt
from pyspark.sql.functions import (
    col, coalesce, when, lit, round as spark_round,
    sum as spark_sum, avg, count, max as spark_max, min as spark_min
)
from pyspark.sql.window import Window

@dlt.table(
    name="gold.fact_head_to_head",
    comment="Histórico de confrontos diretos entre times"
)
def gold_head_to_head():
    """
    Estatísticas de confrontos diretos para análise de rivalidades
    """
    matches = dlt.read("gold.dim_matches_complete")
    
    return (
        matches
        .groupBy("home_team", "away_team", "championship", "season")
        .agg(
            count("*").alias("total_matches"),
            spark_sum(when(col("match_result") == "Home Win", 1).otherwise(0)).alias("home_wins"),
            spark_sum(when(col("match_result") == "Away Win", 1).otherwise(0)).alias("away_wins"),
            spark_sum(when(col("match_result") == "Draw", 1).otherwise(0)).alias("draws"),
            spark_sum("home_score").alias("total_goals_home"),
            spark_sum("away_score").alias("total_goals_away"),
            avg("home_score").alias("avg_goals_home"),
            avg("away_score").alias("avg_goals_away"),
            spark_max("home_score").alias("max_goals_home"),
            spark_max("away_score").alias("max_goals_away")
        )
    )
