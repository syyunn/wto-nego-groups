CREATE EXTENSION IF NOT EXISTS aws_commons CASCADE;

CREATE SCHEMA IF NOT EXISTS raw___wto;

DROP TABLE IF EXISTS raw___wto.nego_mmbrs;

CREATE TABLE IF NOT EXISTS raw___wto.nego_mmbrs (
    nego_group text
    , member text
);

WITH s3_info AS (
    SELECT
        aws_commons.create_s3_uri ('wt-gc-m'
            , 'wto_nego_membership.csv'
            , 'ap-northeast-2') AS uri
        , aws_commons.create_aws_credentials ('AKIATHBCLRR3ZK5O6TGW'
            , 'f3icJzObixzX+04VHpakrlG+DZ7f8yTC1e92Atag'
            , NULL) AS cred
)
SELECT
    aws_s3.table_import_from_s3 ('raw___wto.nego_mmbrs' , '' , '(format csv, header)' , uri , cred)
FROM
    s3_info;

SELECT
    *
FROM
    raw___wto.nego_mmbrs
WHERE
    member LIKE '%Afgh%';

DROP TABLE IF EXISTS raw___wto.nego_groups_url;

CREATE TABLE IF NOT EXISTS raw___wto.nego_groups_url (
    nego_group text
    , url text
);

WITH s3_info AS (
    SELECT
        aws_commons.create_s3_uri ('wt-gc-m'
            , 'wto_nego_group_urls.csv'
            , 'ap-northeast-2') AS uri
        , aws_commons.create_aws_credentials (''
            , ''
            , NULL) AS cred
)
SELECT
    aws_s3.table_import_from_s3 ('raw___wto.nego_groups_url' , '' , '(format csv, header)' , uri , cred)
FROM
    s3_info;

SELECT
    *
FROM
    raw___wto.nego_groups_url;

SELECT DISTINCT
    nego_group
    , count(*) OVER (PARTITION BY nego_group)
FROM
    raw___wto.nego_mmbrs
ORDER BY
    count DESC;

SELECT
    *
FROM
    raw___wto.nego_mmbrs
WHERE
    nego_group LIKE '%Cotton-4%';

SELECT DISTINCT
    member
    , iso3
    , code
FROM
    raw___wto.nego_mmbrs AS mmbrs
    INNER JOIN raw___wto.iso_3166 ON mmbrs.member = name;

DROP TABLE IF EXISTS raw___wto.nego_mmbrs_iso;

CREATE TABLE IF NOT EXISTS raw___wto.nego_mmbrs_iso AS (
    SELECT DISTINCT ON (member)
        member , name , sim -- by using "distinct on" with "order by", we can get the best matching .
        , iso3 , iso2 , code
    FROM
        raw___wto.nego_mmbrs mmbrs , raw___wto.iso_3166 iso , similarity (mmbrs.member , iso.name
) AS sim
    ORDER BY
        member , sim DESC
);

SELECT
    *
FROM
    raw___wto.nego_mmbrs_iso;

DELETE FROM raw___wto.nego_mmbrs_iso
where member = 'United States' --or member = 'Liechtenstein' or member = 'European Union (formerly EC)' or member = 'Chinese Taipei'

INSERT INTO raw___wto.nego_mmbrs_iso VALUES
    ('Chinese Taipei', 'Taiwan, Republic of China', NULL, 'TWN', 'TW', '158');

INSERT INTO raw___wto.nego_mmbrs_iso VALUES
    ('Liechtenstein', 'Liechtenstein', NULL, 'LIE', 'LI', '438');

INSERT INTO raw___wto.nego_mmbrs_iso VALUES
    ('United States', 'United States of America', NULL, 'USA', 'US', '840');