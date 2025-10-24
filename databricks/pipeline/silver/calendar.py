import dlt
from pyspark.sql.functions import col, to_timestamp, trim, concat_ws

@dlt.table(
    name="silver.calendar",
    comment="Dados de calendário limpos e padronizados"
)
def silver_calendar():
    return (
        dlt.read_stream("bronze.calendar")
        
        .withColumn("match_datetime", 
            to_timestamp(
                concat_ws(" ", col("data"), col("hora")), 
                "dd/MM/yyyy HH:mm"
            )
        )
    
        .select(
            col("id").alias("match_id"),
            col("Campeonato").alias("championship"),
            col("temporada").alias("season"),
            col("rodada").alias("round"),
            col("time_casa").alias("home_team"),
            col("time_visitante").alias("away_team"),
            col("origem").alias("country"),
            col("match_datetime")  
        )
        .filter(col("home_team").isNotNull())
        .filter(col("away_team").isNotNull())
    )