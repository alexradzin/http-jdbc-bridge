create table test_all_types (
i INTEGER,
b BOOLEAN,
si SMALLINT,
bi BIGINT,
dcml DECIMAL,
d DOUBLE,
r REAL,
t TIME,
dt DATE,
ts TIMESTAMP,
bin BINARY LARGE OBJECT,
vc VARCHAR(128),
c CHAR,
blb BLOB,
clb CLOB
-- a ARRAY, -- error: type not found or user lacks privilege: ARRAY (although document mentions this type)
)