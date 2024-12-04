-- DROP TABLE IF EXISTS hosts_cumulated ;
CREATE TABLE hosts_cumulated
(
    host                   TEXT,
    month                  DATE,
    date                   DATE,
    host_activity_datelist DATE[],
    num_events_total       NUMERIC,
    num_events_list        NUMERIC[],
    unique_visitors_list   NUMERIC[],
    PRIMARY KEY (host, month, date)
);
