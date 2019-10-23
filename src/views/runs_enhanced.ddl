DROP VIEW IF EXISTS `ad-big-data-bowl.workspace.runs_enhanced`;
CREATE VIEW `ad-big-data-bowl.workspace.runs_enhanced` AS
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

FROM
  (
    SELECT
      CONCAT(CAST(t_play.game_id AS STRING), '-', CAST(t_play.play_id AS STRING)) AS event_id,
      t_play.*,
      t_game.* EXCEPT (game_id),
      t_player.* EXCEPT (nfl_id),
      t_snap.* EXCEPT (game_id, play_id),
      t_handoff.* EXCEPT (game_id, play_id, nfl_id),
      t_tackle.* EXCEPT (game_id, play_id, nfl_id),
      t_ball_snap.* EXCEPT (game_id, play_id, frame_id),
      t_ball_handoff.* EXCEPT (game_id, play_id, frame_id),
      t_ball_tackle.* EXCEPT (game_id, play_id, frame_id),
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
          MAX(number_of_handoff_rushers) AS number_of_handoff_rushers,
          MAX(personnel_defense) AS personnel_defense,
          MAX(home_score_before_play) AS home_score_before_play,
          MAX(visitor_score_before_play) AS visitor_score_before_play,
          MAX(play_result) AS play_result
        FROM
          `ad-big-data-bowl.raw.plays`
        WHERE
          number_of_handoff_rushers IS NULL
            AND handoff_result IS NULL
            AND down > 0
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
          MIN(time) AS handoff_time,
          MIN(frame_id) AS handoff_frame,
          MIN(x) AS handoff_x,
          MIN(y) AS handoff_y,
          MIN(speed) AS handoff_speed,
          MIN(direction) AS handoff_direction,
          MIN(velocity_x) AS handoff_velocity_x,
          MIN(velocity_y) AS handoff_velocity_y,
          MIN(accel_x) AS handoff_accel_x,
          MIN(accel_y) AS handoff_accel_y
        FROM
          w_tracking
        WHERE
          event = 'handoff'
        GROUP BY 1, 2, 3
      ) t_handoff
        ON (t_play.game_id = t_handoff.game_id
          AND t_play.play_id = t_handoff.play_id
          AND t_snap.nfl_id = t_handoff.nfl_id
        )
        INNER JOIN
      (
        SELECT
          game_id,
          play_id,
          nfl_id,
          MIN(time) AS tackled_time,
          MIN(frame_id) AS tackled_frame,
          MIN(x) AS tackled_x,
          MIN(y) AS tackled_y,
          MIN(speed) AS tackled_speed,
          MIN(direction) AS tackled_direction,
          MIN(velocity_x) AS tackled_velocity_x,
          MIN(velocity_y) AS tackled_velocity_y,
          MIN(accel_x) AS tackled_accel_x,
          MIN(accel_y) AS tackled_accel_y
        FROM
          w_tracking
        WHERE
          event = 'tackle'
        GROUP BY 1, 2, 3
      ) t_tackle
        ON (t_play.game_id = t_tackle.game_id
          AND t_play.play_id = t_tackle.play_id
          AND t_snap.nfl_id = t_tackle.nfl_id
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
          MIN(x) AS handoff_ball_x,
          MIN(y) AS handoff_ball_y
        FROM
          `ad-big-data-bowl.raw.tracking`
        WHERE
          display_name = 'football'
        GROUP BY 1, 2, 3
      ) t_ball_handoff
        ON (t_play.game_id = t_ball_handoff.game_id
          AND t_play.play_id = t_ball_handoff.play_id
          AND t_handoff.handoff_frame = t_ball_handoff.frame_id
        )
        INNER JOIN
      (
        SELECT
          game_id,
          play_id,
          frame_id,
          MIN(x) AS tackled_ball_x,
          MIN(y) AS tackled_ball_y
        FROM
          `ad-big-data-bowl.raw.tracking`
        WHERE
          display_name = 'football'
        GROUP BY 1, 2, 3
      ) t_ball_tackle
        ON (t_play.game_id = t_ball_tackle.game_id
          AND t_play.play_id = t_ball_tackle.play_id
          AND t_tackle.tackled_frame = t_ball_tackle.frame_id
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
;