SELECT
  *,
  COS(qb_pass_direction - throw_direction) * qb_pass_speed AS qb_pass_velo_par,
  SIN(qb_pass_direction - throw_direction) * qb_pass_speed AS qb_pass_velo_orth
FROM
  (
    SELECT
      game_id,
      play_id,
      MAX(CASE WHEN position = 'QB' THEN nfl_id END) AS qb_nfl_id,
      MAX(CASE WHEN position = 'QB' THEN display_name END) AS qb_name,
      MAX(CASE WHEN targeted_receiver_flag = 1 THEN nfl_id END) AS receiver_nfl_id,
      MAX(CASE WHEN targeted_receiver_flag = 1 THEN display_name END) AS receiver_name,
      MAX(pass_complete_flag) AS pass_complete_flag,
      MAX(play_epa) AS play_epa,
      MAX(play_air_epa) AS play_air_epa,
      MAX(play_yac_epa) AS play_yac_epa,
      MAX(play_comp_air_epa) AS play_comp_air_epa,
      MAX(play_comp_yac_epa) AS play_comp_yac_epa,
      MAX(time_to_pass) AS time_to_pass,
      MAX(CASE WHEN position = 'QB' THEN pass_speed END) AS qb_pass_speed,
      MAX(CASE WHEN position = 'QB' THEN pass_direction END) AS qb_pass_direction,
      MAX(throw_direction) AS throw_direction,
      MAX(throw_distance) AS throw_distance,
      MAX(arrived_endline_distance) AS arrived_endline_distance,
      MAX(arrived_sideline_distance) AS arrived_sideline_distance,
      MAX(FLOOR(arrived_ball_x)) AS arrived_x,
      MAX(FLOOR(arrived_ball_y)) AS arrived_y,
      MIN(CASE WHEN player_side = 'defense' THEN pass_ball_distance END) AS closest_pass_rusher_distance,
      MIN(CASE WHEN player_side = 'defense' THEN arrived_ball_distance END) AS closest_pass_defender_distance,
      MIN(CASE WHEN player_side = 'defense' THEN arrived_pred_ball_distance END) AS closest_pred_pass_defender_distance,
      COALESCE(SUM(CASE WHEN player_side = 'defense' AND pass_ball_distance < 1 THEN 1 END), 0) AS pass_rushers_1yd,
      COALESCE(SUM(CASE WHEN player_side = 'defense' AND pass_ball_distance < 2 THEN 1 END), 0) AS pass_rushers_2yd,
      COALESCE(SUM(CASE WHEN player_side = 'defense' AND pass_ball_distance < 3 THEN 1 END), 0) AS pass_rushers_3yd,
      COALESCE(SUM(CASE WHEN player_side = 'defense' AND arrived_ball_distance < 2 THEN 1 END), 0) AS pass_defenders_2yd,
      COALESCE(SUM(CASE WHEN player_side = 'defense' AND arrived_ball_distance < 4 THEN 1 END), 0) AS pass_defenders_4yd,
      COALESCE(SUM(CASE WHEN player_side = 'defense' AND arrived_ball_distance < 5 THEN 1 END), 0) AS pass_defenders_5yd
    FROM
      (
        SELECT
          *
        FROM
          `ad-big-data-bowl.workspace.passes_enhanced`
        WHERE
          personnel_offense_type = 'conventional'
            AND play_epa IS NOT NULL
      )
    GROUP BY 1, 2
  )
