import dlt
from pyspark.sql.functions import (
    col, coalesce, when, lit, round as spark_round,
    sum as spark_sum, avg, count, max as spark_max, min as spark_min
)
from pyspark.sql.window import Window

@dlt.table(
    name="gold.dim_matches_complete",
    comment="Visão completa das partidas com resultados e estatísticas consolidadas"
)
def gold_matches_complete():
    """
    Tabela dimensional principal com todas as informações da partida
    """
    results = dlt.read("silver.fact_finished_matches")
    stats = dlt.read("silver.fact_statistics")
    calendar = dlt.read("silver.fact_calendar")
    
    return (
        results
        .join(stats, results.match_id == stats.match_id, "left")
        .join(calendar, results.match_id == calendar.match_id, "left")
        .select(
            results.match_id,
            coalesce(results.match_date, calendar.match_datetime).alias("match_datetime"),
            results.championship,
            results.season,
            results.round,
            results.country,
            
            results.home_team,
            results.away_team,
            
            results.home_score,
            results.away_score,
            when(results.home_score > results.away_score, "Home Win")
                .when(results.home_score < results.away_score, "Away Win")
                .otherwise("Draw").alias("match_result"),
            (results.home_score + results.away_score).alias("total_goals"),
            
            stats.expected_goals_xg_home,
            stats.expected_goals_xg_away,
            (stats.expected_goals_xg_home + stats.expected_goals_xg_away).alias("total_xg"),
            
            stats.possession_home,
            stats.possession_away,
            
            stats.total_shots_home,
            stats.total_shots_away,
            stats.shots_on_target_home,
            stats.shots_on_target_away,
            
            when(stats.total_shots_home > 0, 
                 spark_round((stats.shots_on_target_home / stats.total_shots_home) * 100, 2))
                .otherwise(0).alias("shot_accuracy_home_pct"),
            when(stats.total_shots_away > 0,
                 spark_round((stats.shots_on_target_away / stats.total_shots_away) * 100, 2))
                .otherwise(0).alias("shot_accuracy_away_pct"),
            
            stats.big_chances_home,
            stats.big_chances_away,
            stats.expected_assists_xa_home,
            stats.expected_assists_xa_away,
            
            stats.goalkeeper_saves_home,
            stats.goalkeeper_saves_away,
            stats.tackles_home,
            stats.tackles_away,
            stats.interceptions_home,
            stats.interceptions_away,
            
            stats.yellow_cards_home,
            stats.yellow_cards_away,
            stats.fouls_committed_home,
            stats.fouls_committed_away,
            
            stats.corners_home,
            stats.corners_away,
            stats.offsides_home,
            stats.offsides_away
        )
    )
