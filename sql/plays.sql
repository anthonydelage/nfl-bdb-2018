SELECT
  game_id,
  play_id,
  MAX(quarter) AS quarter,
  MAX(game_clock) AS game_clock,
  MAX(CAST(SUBSTR(game_clock, 1, 2) AS INT64) * 60 +
      CAST(SUBSTR(game_clock, 4, 2) AS INT64)) AS quarter_seconds,
  MAX((4 - quarter) * 15 * 60 +
      CAST(SUBSTR(game_clock, 1, 2) AS INT64) * 60 +
      CAST(SUBSTR(game_clock, 4, 2) AS INT64)) AS game_seconds,
  MAX(down) AS down,
  MAX(yards_to_go) AS yards_to_go,
  MAX(possession_team) AS possession_team,
  MAX(yardline_side) AS yardline_side,
  MAX(yardline_number) AS yardline_number,
  MAX(CASE WHEN yardline_side IS NULL THEN 50.0
      WHEN yardline_side = possession_team THEN 100.0 - yardline_number
      ELSE yardline_number END) AS yardline_true,
  MAX(offense_formation) AS offense_formation,
  MAX(personnel_offense) AS personnel_offense,
  MAX(defenders_in_the_box) AS defenders_in_the_box,
  MAX(number_of_pass_rushers) AS number_of_pass_rushers,
  MAX(personnel_defense) AS personnel_defense,
  MAX(home_score_before_play) AS home_score_before_play,
  MAX(visitor_score_before_play) AS visitor_score_before_play,
  MAX(pass_length) AS pass_length,
  MAX(pass_result) AS pass_result,
  MAX(yards_after_catch) AS yards_after_catch,
  MAX(play_result) AS play_result
FROM
  `ad-big-data-bowl.raw.plays`
WHERE
  pass_length > 0
    AND pass_result IS NOT NULL
    AND NOT is_penalty
GROUP BY 1, 2