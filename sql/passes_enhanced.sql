WITH
w_tracking AS (
  SELECT
    *,
    COALESCE((velocity_x - lag_velocity_x) / 0.1, 0) AS accel_x,
    COALESCE((velocity_y - lag_velocity_y) / 0.1, 0) AS accel_y
  FROM
    (
      SELECT
        *,
        LAG(velocity_x) OVER (PARTITION BY game_id, play_id, nfl_id ORDER BY frame_id) AS lag_velocity_x,
        LAG(velocity_y) OVER (PARTITION BY game_id, play_id, nfl_id ORDER BY frame_id) AS lag_velocity_y
      FROM
        (
          SELECT
            game_id,
            play_id,
            CAST(nfl_id AS INT64) AS nfl_id,
            time,
            frame_id,
            team,
            display_name,
            jersey_number,
            event,
            x,
            y,
            speed,
            direction,
            COS((direction + 90.0) * ACOS(-1) / 180) * speed AS velocity_x,
            SIN((direction + 90.0) * ACOS(-1) / 180) * speed AS velocity_y
          FROM
            `ad-big-data-bowl.raw.tracking`
          WHERE
            display_name != 'football'
        )
    )
)
SELECT
  *,
  (CASE WHEN min_arrived_ball_distance = arrived_ball_distance AND player_side = 'offense' THEN 1
    ELSE 0 END) AS targeted_receiver_flag,
  (CASE WHEN min_arrived_ball_distance = arrived_ball_distance AND player_side = 'defense' THEN 1
    ELSE 0 END) AS targeted_defender_flag
FROM
  (
    SELECT
      *,
      (CASE WHEN play_direction_multiplier > 0 THEN 120 - arrived_ball_x
        WHEN play_direction_multiplier < 0 THEN arrived_ball_x
        ELSE NULL END) AS arrived_endline_distance,
      LEAST(53.3 - arrived_ball_y, arrived_ball_y) AS arrived_sideline_distance,
      MIN(pass_ball_distance) OVER (PARTITION BY game_id, play_id, player_side) AS min_pass_ball_distance,
      MIN(arrived_ball_distance) OVER (PARTITION BY game_id, play_id, player_side) AS min_arrived_ball_distance,
      MIN(arrived_pred_ball_distance) OVER (PARTITION BY game_id, play_id, player_side) AS min_arrived_pred_ball_distance
    FROM
      (
        SELECT
          *,
          (CASE WHEN throw_direction_raw < 0 THEN throw_direction_raw + 360 ELSE throw_direction_raw END) AS throw_direction,
          (snap_x - snap_ball_x) * play_direction_multiplier AS snap_x_rel,
          (pass_x - pass_ball_x) * play_direction_multiplier AS pass_x_rel,
          (arrived_x - arrived_ball_x) * play_direction_multiplier AS arrived_x_rel,
          ROUND(SQRT(POW(arrived_pred_x - arrived_ball_x, 2) + POW(arrived_pred_y - arrived_ball_y, 2)), 2) AS arrived_pred_ball_distance
        FROM
          (
            SELECT
              *,
              (CASE WHEN snap_ball_x > pass_ball_x AND arrived_ball_x > pass_ball_x THEN 1
                WHEN snap_ball_x < pass_ball_x AND arrived_ball_x < pass_ball_x THEN -1
                ELSE NULL END) AS play_direction_multiplier,
              (CASE WHEN possession_team = home_team_abbr AND team = 'home' THEN 'offense'
                WHEN possession_team = home_team_abbr AND team = 'away' THEN 'defense'
                WHEN possession_team != home_team_abbr AND team = 'home' THEN 'defense'
                WHEN possession_team != home_team_abbr AND team = 'away' THEN 'offense'
                ELSE 'other' END) AS player_side,
              (CASE WHEN position = 'QB' THEN 'QB'
                WHEN position IN ('RB', 'WR', 'FB', 'TE') THEN 'SKILL'
                WHEN position IN ('T', 'G', 'C') THEN 'OL'
                WHEN position IN ('DT', 'DE', 'NT') THEN 'DL'
                WHEN position IN ('LB', 'MLB', 'OLB', 'ILB') THEN 'LB'
                WHEN position IN ('DB', 'CB', 'SS', 'FS') THEN 'DB'
                WHEN position IN ('K', 'P', 'LS') THEN 'ST'
                ELSE 'OTHER' END) AS position_group,
              (CASE WHEN REGEXP_CONTAINS(personnel_offense, r'(OL|DL|LB|DB)') THEN 'non-conventional'
                ELSE 'conventional' END) AS personnel_offense_type,
              (CASE WHEN pass_result = 'C' THEN 1 ELSE 0 END) AS pass_complete_flag,
              (pass_frame - snap_frame) / 10.0 AS time_to_pass,
              (arrived_frame - snap_frame) / 10.0 AS time_to_arrival,
              (arrived_frame - pass_frame) / 10.0 AS pass_air_time,
              ROUND(SQRT(POW(snap_x - snap_ball_x, 2) + POW(snap_y - snap_ball_y, 2)), 2) AS snap_ball_distance,
              ROUND(SQRT(POW(pass_x - pass_ball_x, 2) + POW(pass_y - pass_ball_y, 2)), 2) AS pass_ball_distance,
              ROUND(SQRT(POW(arrived_x - arrived_ball_x, 2) + POW(arrived_y - arrived_ball_y, 2)), 2) AS arrived_ball_distance,
              ROUND(SQRT(POW(arrived_ball_x - pass_ball_x, 2) + POW(arrived_ball_y - pass_ball_y, 2)), 2) AS throw_distance,
              ROUND(ATAN2(arrived_ball_y - pass_ball_y, arrived_ball_x - pass_ball_x) * 180 / ACOS(-1), 2) - 90.0 AS throw_direction_raw,
              ROUND(pass_x + pass_velocity_x * (arrived_frame - pass_frame) / 10 +
                pass_accel_x * POW((arrived_frame - pass_frame) / 10, 2)) arrived_pred_x,
              ROUND(pass_y + pass_velocity_y * (arrived_frame - pass_frame) / 10 +
                pass_accel_y * POW((arrived_frame - pass_frame) / 10, 2)) arrived_pred_y
            FROM
              (
                SELECT
                  CONCAT(CAST(t_play.game_id AS STRING), '-', CAST(t_play.play_id AS STRING)) AS event_id,
                  t_play.*,
                  t_game.* EXCEPT (game_id),
                  t_player.* EXCEPT (nfl_id),
                  t_snap.* EXCEPT (game_id, play_id),
                  t_pass.* EXCEPT (game_id, play_id, nfl_id),
                  t_arrived.* EXCEPT (game_id, play_id, nfl_id),
                  t_ball_snap.* EXCEPT (game_id, play_id, frame_id),
                  t_ball_pass.* EXCEPT (game_id, play_id, frame_id),
                  t_ball_arrived.* EXCEPT (game_id, play_id, frame_id),
                  t_epa.* EXCEPT (game_id, play_id)
                FROM
                  (
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
                  ) t_play
                    INNER JOIN
                  (
                    SELECT
                      *
                    FROM
                      `ad-big-data-bowl.raw.games`
                  ) t_game
                    ON (t_play.game_id = t_game.game_id)
                    INNER JOIN
                  (
                    SELECT
                      game_id,
                      play_id,
                      nfl_id,
                      MIN(jersey_number) AS jersey_number,
                      MIN(display_name) AS display_name,
                      MIN(team) AS team,
                      MIN(time) AS snap_time,
                      MIN(frame_id) AS snap_frame,
                      MIN(x) AS snap_x,
                      MIN(y) AS snap_y,
                      MIN(speed) AS snap_speed,
                      MIN(direction) AS snap_direction,
                      MIN(velocity_x) AS snap_velocity_x,
                      MIN(velocity_y) AS snap_velocity_y,
                      MIN(accel_x) AS snap_accel_x,
                      MIN(accel_y) AS snap_accel_y
                    FROM
                      w_tracking
                    WHERE
                      event = 'ball_snap'
                    GROUP BY 1, 2, 3
                  ) t_snap
                    ON (t_play.game_id = t_snap.game_id
                      AND t_play.play_id = t_snap.play_id
                    )
                    INNER JOIN
                  (
                    SELECT
                      game_id,
                      play_id,
                      nfl_id,
                      MIN(time) AS pass_time,
                      MIN(frame_id) AS pass_frame,
                      MIN(x) AS pass_x,
                      MIN(y) AS pass_y,
                      MIN(speed) AS pass_speed,
                      MIN(direction) AS pass_direction,
                      MIN(velocity_x) AS pass_velocity_x,
                      MIN(velocity_y) AS pass_velocity_y,
                      MIN(accel_x) AS pass_accel_x,
                      MIN(accel_y) AS pass_accel_y
                    FROM
                      w_tracking
                    WHERE
                      event = 'pass_forward'
                    GROUP BY 1, 2, 3
                  ) t_pass
                    ON (t_play.game_id = t_pass.game_id
                      AND t_play.play_id = t_pass.play_id
                      AND t_snap.nfl_id = t_pass.nfl_id
                    )
                    INNER JOIN
                  (
                    SELECT
                      game_id,
                      play_id,
                      nfl_id,
                      MIN(time) AS arrived_time,
                      MIN(frame_id) AS arrived_frame,
                      MIN(x) AS arrived_x,
                      MIN(y) AS arrived_y,
                      MIN(speed) AS arrived_speed,
                      MIN(direction) AS arrived_direction,
                      MIN(velocity_x) AS arrived_velocity_x,
                      MIN(velocity_y) AS arrived_velocity_y,
                      MIN(accel_x) AS arrived_accel_x,
                      MIN(accel_y) AS arrived_accel_y
                    FROM
                      w_tracking
                    WHERE
                      event = 'pass_arrived'
                    GROUP BY 1, 2, 3
                  ) t_arrived
                    ON (t_play.game_id = t_arrived.game_id
                      AND t_play.play_id = t_arrived.play_id
                      AND t_snap.nfl_id = t_arrived.nfl_id
                    )
                    INNER JOIN
                  (
                    SELECT
                      game_id,
                      play_id,
                      frame_id,
                      MIN(x) AS snap_ball_x,
                      MIN(y) AS snap_ball_y
                    FROM
                      `ad-big-data-bowl.raw.tracking`
                    WHERE
                      display_name = 'football'
                    GROUP BY 1, 2, 3
                  ) t_ball_snap
                    ON (t_play.game_id = t_ball_snap.game_id
                      AND t_play.play_id = t_ball_snap.play_id
                      AND t_snap.snap_frame = t_ball_snap.frame_id
                    )
                    INNER JOIN
                  (
                    SELECT
                      game_id,
                      play_id,
                      frame_id,
                      MIN(x) AS pass_ball_x,
                      MIN(y) AS pass_ball_y
                    FROM
                      `ad-big-data-bowl.raw.tracking`
                    WHERE
                      display_name = 'football'
                    GROUP BY 1, 2, 3
                  ) t_ball_pass
                    ON (t_play.game_id = t_ball_pass.game_id
                      AND t_play.play_id = t_ball_pass.play_id
                      AND t_pass.pass_frame = t_ball_pass.frame_id
                    )
                    INNER JOIN
                  (
                    SELECT
                      game_id,
                      play_id,
                      frame_id,
                      MIN(x) AS arrived_ball_x,
                      MIN(y) AS arrived_ball_y
                    FROM
                      `ad-big-data-bowl.raw.tracking`
                    WHERE
                      display_name = 'football'
                    GROUP BY 1, 2, 3
                  ) t_ball_arrived
                    ON (t_play.game_id = t_ball_arrived.game_id
                      AND t_play.play_id = t_ball_arrived.play_id
                      AND t_arrived.arrived_frame = t_ball_arrived.frame_id
                    )
                    INNER JOIN
                  (
                    SELECT
                      nfl_id,
                      MAX(position_abbr) AS position,
                      MAX(height) AS height,
                      MAX(CAST(SUBSTR(height, 1, 1) AS INT64) * 12 +
                          CAST(SUBSTR(height, 3, 2) AS INT64)) AS height_inches,
                      MAX(weight) AS weight
                    FROM
                      `ad-big-data-bowl.raw.players`
                    GROUP BY 1
                  ) t_player
                    ON (t_snap.nfl_id = t_player.nfl_id)
                    LEFT OUTER JOIN
                  (
                    SELECT
                      game_id,
                      play_id,
                      MAX(epa) AS play_epa,
                      MAX(air_epa) AS play_air_epa,
                      MAX(yac_epa) AS play_yac_epa,
                      MAX(comp_air_epa) AS play_comp_air_epa,
                      MAX(comp_yac_epa) AS play_comp_yac_epa
                    FROM
                      `ad-big-data-bowl.raw.nflscrapr`
                    GROUP BY 1, 2
                  ) t_epa
                    ON (t_play.game_id = t_epa.game_id
                      AND t_play.play_id = t_epa.play_id
                    )
              )
          )
        WHERE
          play_direction_multiplier IS NOT NULL
      )
  )