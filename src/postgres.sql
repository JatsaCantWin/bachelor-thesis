create or replace function public.net_power(start_time timestamp with time zone, end_time timestamp with time zone)
    returns TABLE(cap_timeutc timestamp with time zone, net_power_consumed numeric)
    language sql
as
$$
    SELECT cap_timeutc,
           power_consumed.value - LAG(power_consumed.value, 1) OVER (ORDER BY cap_timeutc) -
           power_produced.value - LAG(power_produced.value, 1) OVER (ORDER BY cap_timeutc) AS net_power_consumed
    FROM power_consumed
    JOIN power_produced USING (cap_timeutc)
    WHERE cap_timeutc >= start_time AND cap_timeutc <= end_time;
$$;

create or replace function public.contracted_power_exceedance(start_time timestamp with time zone, end_time timestamp with time zone, contracted_power numeric)
    returns TABLE(cap_timeutc timestamp with time zone, power_consumed_time_window numeric)
    language sql
as
$$
    SELECT * FROM (SELECT cap_timeutc,
           power_consumed.value - LAG(power_consumed.value, 1) OVER (ORDER BY cap_timeutc) AS power_consumed_time_window
    FROM power_consumed) as time_window_select
    WHERE cap_timeutc >= start_time AND cap_timeutc <= end_time AND
          time_window_select.power_consumed_time_window > contracted_power;
$$;

create or replace function public.power_balance(start_time timestamp with time zone, end_time timestamp with time zone)
    returns TABLE(cap_timeutc timestamp with time zone, power_consumed numeric, power_produced numeric, balance numeric)
    language sql
as
$$
    SELECT cap_timeutc, power_consumed, power_produced, power_consumed - power_produced AS balance FROM
    (SELECT cap_timeutc,
           power_produced.value - LAG(power_produced.value, 1) OVER (ORDER BY cap_timeutc) AS power_produced,
           power_consumed.value - LAG(power_consumed.value, 1) OVER (ORDER BY cap_timeutc) AS power_consumed
    FROM power_consumed
    JOIN power_produced USING (cap_timeutc)) as power_balance_select
    WHERE cap_timeutc >= start_time AND cap_timeutc <= end_time;
$$;

create or replace function public.frequency_anomalies(start_time timestamp with time zone, end_time timestamp with time zone)
    returns TABLE(cap_timeutc timestamp with time zone, value numeric)
    language sql
as
$$
    SELECT cap_timeutc, value
    FROM frequency
    WHERE value < 47 OR value > 52 OR
          (value < 49.5 OR value > 50.5) AND
          (SELECT COUNT(*) FROM frequency WHERE value BETWEEN 49.5 AND 50.5 AND cap_timeutc >= start_time AND cap_timeutc <= end_time) / (SELECT COUNT(*) FROM frequency WHERE cap_timeutc >= start_time AND cap_timeutc <= end_time) > 0.995;
$$;

create or replace function public.voltage_anomalies(start_time timestamp with time zone, end_time timestamp with time zone, rated_voltage numeric)
    returns TABLE(phase text, cap_timeutc timestamp with time zone, value numeric)
    language sql
as
$$
    SELECT 'L1' as phase, cap_timeutc, value
    FROM phase_l1_voltage
    WHERE value < rated_voltage*0.9 OR value > rated_voltage*1.1 AND
          (SELECT COUNT(*) FROM phase_l1_voltage WHERE cap_timeutc >= start_time AND cap_timeutc <= end_time AND value < rated_voltage*0.9 OR value > rated_voltage*1.1)
        / (SELECT COUNT(*) FROM phase_l1_voltage WHERE cap_timeutc >= start_time AND cap_timeutc <= end_time) > 0.95
    UNION
    SELECT 'L2' as phase, cap_timeutc, value
    FROM phase_l2_voltage
    WHERE value < rated_voltage*0.9 OR value > rated_voltage*1.1 AND
          (SELECT COUNT(*) FROM phase_l2_voltage WHERE cap_timeutc >= start_time AND cap_timeutc <= end_time AND value < rated_voltage*0.9 OR value > rated_voltage*1.1)
        / (SELECT COUNT(*) FROM phase_l2_voltage WHERE cap_timeutc >= start_time AND cap_timeutc <= end_time) > 0.95
    UNION
    SELECT 'L3' as phase, cap_timeutc, value
    FROM phase_l3_voltage
    WHERE value < rated_voltage*0.9 OR value > rated_voltage*1.1 AND
          (SELECT COUNT(*) FROM phase_l3_voltage WHERE cap_timeutc >= start_time AND cap_timeutc <= end_time AND value < rated_voltage*0.9 OR value > rated_voltage*1.1)
        / (SELECT COUNT(*) FROM phase_l3_voltage WHERE cap_timeutc >= start_time AND cap_timeutc <= end_time) > 0.95
$$;


