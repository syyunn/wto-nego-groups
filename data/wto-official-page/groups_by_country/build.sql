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

SELECT
    distinct nego_group, count(*) over (partition by nego_group)
FROM
    raw___wto.nego_mmbrs
order by count desc;