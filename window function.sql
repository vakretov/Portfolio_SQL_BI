-- db1.vk_conversion_with_index исходный текст

CREATE VIEW db1.vk_conversion_with_index
(

    `branch` String,

    `event_type` String,

    `total_deals` UInt64,

    `conversion_rate` Float64,

    `event_index` Nullable(String),

    `final_event_index` Nullable(String)
)
AS SELECT
    f.branch,

    f.event_type,

    f.total_deals,

    f.conversion_rate,

    multiIf(f.event_type = 'Определенное событие',
 concat(f.branch,
 '-',
 toString(f.date)),
 NULL) AS event_index,

    max(multiIf(f.event_type = 'Определенное событие',
 concat(f.branch,
 '-',
 toString(f.date)),
 NULL)) OVER (PARTITION BY f.branch,
 f.date) AS final_event_index
FROM
(
    SELECT
        v.branch,

        v.event_type,

        COUNTDistinct(v.deal_id) AS total_deals,

        (COUNTDistinct(v.deal_id) * 100.) / total.total_deals AS conversion_rate,

        v.date
    FROM db1.view_vk_events AS v
    INNER JOIN
    (
        SELECT
            branch,

            COUNTDistinct(deal_id) AS total_deals
        FROM db1.view_vk_events
        GROUP BY branch
    ) AS total ON v.branch = total.branch
    WHERE v.date >= (today() - toIntervalDay(7))
    GROUP BY
        v.branch,

        v.event_type,

        total.total_deals,

        v.date
    ORDER BY
        v.branch ASC,

        conversion_rate DESC
) AS f;