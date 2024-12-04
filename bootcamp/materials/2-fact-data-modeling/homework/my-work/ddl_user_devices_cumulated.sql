CREATE TABLE user_devices_cumulated
(
    user_id                  NUMERIC,
    device_id                NUMERIC,
    browser_type             TEXT,
    date                     DATE,
    device_activity_datelist DATE[],
    PRIMARY KEY (user_id, device_id, browser_type, date)
);

