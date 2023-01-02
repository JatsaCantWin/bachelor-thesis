DROP VIEW IF EXISTS energy_balance;
CREATE VIEW energy_balance AS
    SELECT ec.cap_timeutc, ec.value - ep.value as difference
    FROM power_provided_to_the_grid ec
    JOIN power_taken_from_the_grid ep
    ON ec.cap_timeutc = ep.cap_timeutc;

DROP VIEW IF EXISTS frequency_range_check
CREATE VIEW frequency_range_check AS
SELECT
    COUNT(*) AS total_count,
    SUM(CASE WHEN value BETWEEN 49.5 AND 50.5 THEN 1 ELSE 0 END) / COUNT(*) AS within_range,
    SUM(CASE WHEN value BETWEEN 47 AND 52 THEN 1 ELSE 0 END) / COUNT(*) AS within_range_2
FROM frequency
HAVING within_range >= 0.995 AND within_range_2 = 1;

DROP VIEW IF EXISTS voltage_range_check;
CREATE VIEW voltage_range_check AS
SELECT
    COUNT(*) AS total_count_phase_1,
    SUM(CASE WHEN value BETWEEN 207 AND 253 THEN 1 ELSE 0 END) / COUNT(*) AS within_range
FROM phase_L1_voltage
UNION
SELECT
    COUNT(*) AS total_count_phase_2,
    SUM(CASE WHEN value BETWEEN 207 AND 253 THEN 1 ELSE 0 END) / COUNT(*) AS within_range_2
FROM phase_L1_voltage
UNION
SELECT
    COUNT(*) AS total_count_phase_3,
    SUM(CASE WHEN value BETWEEN 207 AND 253 THEN 1 ELSE 0 END) / COUNT(*) AS within_range_3
FROM phase_L1_voltage
HAVING within_range >= 0.95 OR within_range_2 >= 0.95 OR within_range_3 >= 0.95;

DROP VIEW IF EXISTS energy_consumed_delta;
CREATE VIEW energy_consumed_delta AS
  SELECT
    power_taken_from_the_grid.cap_timeutc,
    power_taken_from_the_grid.value - LAG(power_taken_from_the_grid.value, 1) OVER (ORDER BY power_taken_from_the_grid.cap_timeutc) -
    power_provided_to_the_grid.value - LAG(power_provided_to_the_grid.value, 1) OVER (ORDER BY power_provided_to_the_grid.cap_timeutc)
    AS difference

  FROM power_taken_from_the_grid JOIN power_provided_to_the_grid ON power_taken_from_the_grid.cap_timeutc = power_provided_to_the_grid.cap_timeutc;
SELECT * FROM energy_consumed_delta