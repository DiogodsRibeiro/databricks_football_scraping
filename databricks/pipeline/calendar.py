import dlt

@dlt.table(
    name="bronze.calendar",  # ← Nome completo com schema
    comment="Ingestão de dados de calendário"
)
def bronze_calendar():
    return (
        spark.readStream
        .format("delta")
        .load("/Volumes/databricks_webscraping_football_ws/raw/calendar/all_calendar/")
        .select(
            "Campeonato",
            "data",
            "hora",
            "id",
            "origem",
            "rodada",
            "temporada",
            "time_casa",
            "time_visitante"
        )
    )