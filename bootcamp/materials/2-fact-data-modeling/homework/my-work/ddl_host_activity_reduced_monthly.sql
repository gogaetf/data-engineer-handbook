-- DROP TABLE IF EXISTS host_activity_reduced_monthly;
CREATE TABLE host_activity_reduced_monthly
(
    host                       TEXT,
    month                      DATE,
    host_activity_datelist     DATE[],
    host_activity_datelist_int BIT(32),
    num_events_list            NUMERIC[],
    unique_visitors_list       NUMERIC[],
    num_events_total           NUMERIC,
    PRIMARY KEY (host, month)
);