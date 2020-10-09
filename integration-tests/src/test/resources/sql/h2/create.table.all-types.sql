create table test_all_types (
i INT,
b BOOLEAN,
ti TINYINT,
si SMALLINT,
bi BIGINT,
id IDENTITY,
dcml DECIMAL,
d DOUBLE,
r REAL,
t TIME,
-- ttz TIME WITH TIME ZONE, -- issue in H2. postponed so far.
dt DATE,
ts TIMESTAMP,
tswtz TIMESTAMP WITH TIME ZONE,
bin BINARY,
o OTHER,
vc VARCHAR,
vci VARCHAR_IGNORECASE,
c CHAR,
blb BLOB,
clb CLOB,
u UUID,
a ARRAY,
-- e ENUM,
g GEOMETRY,
j JSON
)