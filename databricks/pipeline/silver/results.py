import dlt
from pyspark.sql.functions import col, to_timestamp, trim, concat_ws

@dlt.table(
    name="silver.results",
    comment="Dados dos resultados limpos e padronizados"
)
def silver_calendar():
    return (
        dlt.read_stream("bronze.results")
        
        .select(
            col("id").alias("match_id"),
            col("Campeonato").alias("championship"),
            col("temporada").alias("season"),
            col("rodada").alias("round"),
            col("time_casa").alias("home_team"),
            col("time_visitante").alias("away_team"),
            col("origem").alias("country"),
            col("placar_casa").alias("home_score"),
            col("placar_visitante").alias("away_score")
        )
    )