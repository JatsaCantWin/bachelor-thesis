CREATE VIEW net_power AS
SELECT cap_timeutc,
       power_consumed.value - LAG(power_consumed.value, 1) OVER (ORDER BY cap_timeutc) -
       power_produced.value - LAG(power_produced.value, 1) OVER (ORDER BY cap_timeutc) AS net_power_consumed
FROM power_consumed
JOIN power_produced USING (cap_timeutc);

CREATE VIEW power_consumed_time_window AS
SELECT cap_timeutc,
       power_consumed.value - LAG(power_consumed.value, 1) OVER (ORDER BY cap_timeutc) AS power_consumed_time_window
FROM power_consumed;


