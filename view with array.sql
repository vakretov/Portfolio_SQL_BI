-- db1.View_vk_branches_review исходный текст

CREATE VIEW db1.View_vk_branches_review
(

    `a.deal_id` Int32,

    `Теги источника сделки` String,

    `Филиал` String,

    `event_type` String,

    `event_date` Nullable(DateTime),

    `c.date` Nullable(Date),

    `tag` String,

    `quantity` Nullable(UInt64),

    `sum_per_tag` Nullable(Float64),

    `Cost` Nullable(Float64)
)
AS WITH
    tempo_tag AS
    (
        SELECT
            a.`Теги источника сделки` AS tag,

            toDate(created_at_deal) AS date,

            count(a.deal_id) AS quantity
        FROM db1.deals_cityarbitr_sales_daily AS a
        GROUP BY
            a.`Теги источника сделки`,

            toDate(created_at_deal)
    ),

    grouped_yandex_direct AS
    (
        SELECT
            dt.tag AS tag,

            sum(Cost) AS Cost,

            Date
        FROM db1.final_yandex_direct_es AS a
        LEFT JOIN db1.dict_tags AS dt ON a.ClientLogin = dt.login
        GROUP BY
            dt.tag,

            Date
    )
SELECT
    a.deal_id,

    a.`Теги источника сделки` AS `Теги источника сделки`,

    a.`Филиал` AS `Филиал`,

    a.event.1 AS event_type,

    a.event.2 AS event_date,

    c.date,

    arrayFilter(x -> (NOT (x IN ['КУ',
 'СВ',
 'КЛ',
 'ВН',
 'ВС',
 'ДП',
 'ПП',
 'УР',
 'Наз.Повторно',
 'принятопк',
 'передать_в_опк',
 'Не КАЧ.ЛИД',
 'Долг от 500',
 'КВАЛ',
 'Принят',
 'скорозвон',
 'Интерес',
 'СД',
 'ОШД',
 'НазДистВстреч'])),
 a.tags)[1] AS tag,

    quantity,

    if((a.event.1) = 'a_Новый лид',
 fyd.Cost / tempo_tag.quantity,
 0) AS sum_per_tag,

    Cost
FROM
(
    SELECT
        deal_id,

        deal_name,

        price,

        responsible_user,

        responsible_group,

        pipeline_name,

        status_name,

        status_sort,

        created_by_name,

        updated_by_name,

        created_at_deal,

        updated_at_deal,

        closed_at,

        is_deleted,

        utm_campaign,

        utm_source,

        utm_medium,

        utm_content,

        utm_referrer,

        utm_term,

        source,

        `Отметка маркетинга`,

        `Теги источника сделки`,

        `Филиал`,

        `Менеджер КЦ`,

        `Менеджер ОП`,

        `Место встречи`,

        `Группа тегов источника сделки`,

        tags,

        arrayJoin([('a_Новый лид',
 `a_Новый лид`),
 ('b_Приняли в работу',
 `b_Приняли в работу`),
 ('c_Прозвон недозвонов',
 `c_Прозвон недозвонов`),
 ('d_Закрыто после обзвона',
 `d_Закрыто после обзвона`),
 ('e_Контакт установлен',
 `e_Контакт установлен`),
 ('f_Квалифицирован',
 `f_Квалифицирован`),
 ('g_Квал.передать в ОПК',
 `g_Квал.передать в ОПК`),
 ('h_Квал. Принят в ОПК',
 `h_Квал. Принят в ОПК`),
 ('i_Встреча назначена',
 `i_Встреча назначена`),
 ('j_Не приехал назначить повторно',
 `j_Не приехал назначить повторно`),
 ('k_Встреча состоялась',
 `k_Встреча состоялась`),
 ('l_Предоплата получена',
 `l_Предоплата получена`),
 ('x_Успешно реализовано',
 `x_Успешно реализовано`),
 ('z_Закрыто и не реализовано',
 `z_Закрыто и не реализовано`)]) AS event
    FROM db1.deals_cityarbitr_sales_daily
) AS a
LEFT JOIN db1.calendar AS c ON c.date = toDate(a.event.2)
LEFT JOIN tempo_tag ON (tempo_tag.tag = a.`Теги источника сделки`) AND (tempo_tag.date = toDate(created_at_deal))
LEFT JOIN grouped_yandex_direct AS fyd ON (a.`Теги источника сделки` = fyd.tag) AND (toDate(created_at_deal) = toDate(fyd.Date))
WHERE ((a.event.2) IS NOT NULL) AND (fyd.Cost IS NOT NULL)
ORDER BY a.event.2 DESC
SETTINGS join_use_nulls = 1;