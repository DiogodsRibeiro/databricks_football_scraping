import dlt

@dlt.table(
    name="bronze.results",  
    comment="Ingestão de dados dos resultados"
)
def bronze_results():
    return (
        spark.readStream
        .format("delta")
        .load("/Volumes/databricks_webscraping_football_ws/raw/results/all_results/")
        .select(
            "campeonato",
            "data",
            "hora",
            "id",
            "origem",
            "rodada",
            "temporada",
            "placar_casa",
            "placar_visitante",
            "time_casa",
            "time_visitante"
        )
    )