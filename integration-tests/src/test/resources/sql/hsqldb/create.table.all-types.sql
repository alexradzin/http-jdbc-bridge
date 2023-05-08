create table if not exists test_all_types (
i INTEGER,
b BOOLEAN,
ti TINYINT,
si SMALLINT,
bi BIGINT,
f float,
dcml DECIMAL,
d DOUBLE,
r REAL,
t TIME,
ttz TIME WITH TIME ZONE, -- issue in H2. postponed so far.
dt DATE,
ts TIMESTAMP,
tswtz TIMESTAMP WITH TIME ZONE,
bin BINARY,
o OTHER,
vc VARCHAR(128),
c CHAR,
blb BLOB,
clb CLOB,
u UUID
-- a ARRAY, -- error: type not found or user lacks privilege: ARRAY (although document mentions this type)
)