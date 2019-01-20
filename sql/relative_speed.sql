SELECT
  *,
  ROUND(rec_pass_x_rel - def_pass_x_rel, 3) AS diff_pass_x_rel,
  ROUND(rec_pass_y - def_pass_y, 3) AS diff_pass_y,
  ROUND(rec_pass_speed - def_pass_speed, 3) AS diff_pass_speed,
  ROUND(GREATEST(rec_pass_direction_rel - def_pass_direction_rel,
                 def_pass_direction_rel - rec_pass_direction_rel), 3) AS diff_pass_direction_rel,
  ROUND(rec_pass_velocity_x_rel - def_pass_velocity_x_rel, 3) AS diff_pass_velocity_x_rel,
  ROUND(rec_pass_velocity_y - def_pass_velocity_y, 3) AS diff_pass_velocity_y,
  ROUND(rec_pass_accel_x_rel - def_pass_accel_x_rel, 3) AS diff_pass_accel_x_rel,
  ROUND(rec_pass_accel_y - def_pass_accel_y, 3) AS diff_pass_accel_y,
  ROUND(rec_arrived_x_rel - def_arrived_x_rel, 3) AS diff_arrived_x_rel,
  ROUND(rec_arrived_y - def_arrived_y, 3) AS diff_arrived_y,
  ROUND(rec_arrived_speed - def_arrived_speed, 3) AS diff_arrived_speed,
  ROUND(GREATEST(rec_arrived_direction_rel - def_arrived_direction_rel,
                 def_arrived_direction_rel - rec_arrived_direction_rel), 3) AS diff_arrived_direction_rel,
  ROUND(rec_arrived_velocity_x_rel - def_arrived_velocity_x_rel, 3) AS diff_arrived_velocity_x_rel,
  ROUND(rec_arrived_velocity_y - def_arrived_velocity_y, 3) AS diff_arrived_velocity_y,
  ROUND(rec_arrived_accel_x_rel - def_arrived_accel_x_rel, 3) AS diff_arrived_accel_x_rel,
  ROUND(rec_arrived_accel_y - def_arrived_accel_y, 3) AS diff_arrived_accel_y
FROM
  (
    SELECT
      t_rec.*,
      t_def.* EXCEPT (game_id, play_id)
    FROM
      (
        SELECT
          game_id,
          play_id,
          nfl_id AS rec_nfl_id,
          display_name AS rec_display_name,
          CAST(jersey_number AS INT64) AS rec_jersey_number,
          ROUND(pass_x_rel, 3) AS rec_pass_x_rel,
          ROUND(pass_y, 3) AS rec_pass_y,
          ROUND(pass_speed, 3) AS rec_pass_speed,
          ROUND(pass_direction_rel, 3) AS rec_pass_direction_rel,
          ROUND(pass_velocity_x_rel, 3) AS rec_pass_velocity_x_rel,
          ROUND(pass_velocity_y, 3) AS rec_pass_velocity_y,
          ROUND(pass_accel_x_rel, 3) AS rec_pass_accel_x_rel,
          ROUND(pass_accel_y, 3) AS rec_pass_accel_y,
          ROUND(arrived_x_rel, 3) AS rec_arrived_x_rel,
          ROUND(arrived_y, 3) AS rec_arrived_y,
          ROUND(arrived_speed, 3) AS rec_arrived_speed,
          ROUND(arrived_direction_rel, 3) AS rec_arrived_direction_rel,
          ROUND(arrived_velocity_x_rel, 3) AS rec_arrived_velocity_x_rel,
          ROUND(arrived_velocity_y, 3) AS rec_arrived_velocity_y,
          ROUND(arrived_accel_x_rel, 3) AS rec_arrived_accel_x_rel,
          ROUND(arrived_accel_y, 3) AS rec_arrived_accel_y,
          time_to_pass,
          pass_complete_flag,
          play_epa,
          play_air_epa,
          play_yac_epa,
          play_comp_air_epa,
          play_comp_yac_epa
        FROM
          `ad-big-data-bowl.workspace.passes_enhanced`
        WHERE
          targeted_receiver_flag = 1
      ) AS t_rec
        INNER JOIN
      (
        SELECT
          game_id,
          play_id,
          nfl_id AS def_nfl_id,
          display_name AS def_display_name,
          CAST(jersey_number AS INT64) AS def_jersey_number,
          ROUND(pass_x_rel, 3) AS def_pass_x_rel,
          ROUND(pass_y, 3) AS def_pass_y,
          ROUND(pass_speed, 3) AS def_pass_speed,
          ROUND(pass_direction_rel, 3) AS def_pass_direction_rel,
          ROUND(pass_velocity_x_rel, 3) AS def_pass_velocity_x_rel,
          ROUND(pass_velocity_y, 3) AS def_pass_velocity_y,
          ROUND(pass_accel_x_rel, 3) AS def_pass_accel_x_rel,
          ROUND(pass_accel_y, 3) AS def_pass_accel_y,
          ROUND(arrived_x_rel, 3) AS def_arrived_x_rel,
          ROUND(arrived_y, 3) AS def_arrived_y,
          ROUND(arrived_speed, 3) AS def_arrived_speed,
          ROUND(arrived_direction_rel, 3) AS def_arrived_direction_rel,
          ROUND(arrived_velocity_x_rel, 3) AS def_arrived_velocity_x_rel,
          ROUND(arrived_velocity_y, 3) AS def_arrived_velocity_y,
          ROUND(arrived_accel_x_rel, 3) AS def_arrived_accel_x_rel,
          ROUND(arrived_accel_y, 3) AS def_arrived_accel_y
        FROM
          `ad-big-data-bowl.workspace.passes_enhanced`
        WHERE
          targeted_defender_flag = 1
      ) AS t_def
        ON (t_rec.game_id = t_def.game_id
          AND t_rec.play_id = t_def.play_id
        )
  )