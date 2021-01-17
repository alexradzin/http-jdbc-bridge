insert into test_all_types
(i, b, ti, si, bi, id, f, dcml, d, r,
t,
-- ttz, -- issue with H2. Postponed so far.
dt, ts, tswtz,
bin, o,
vc, vci, c,
blb, clb, u, a,
-- e ENUM,
g, j)
values
(12345, true, 1, 123, 123456789, 1, 3.14, 3.14, 3.1415926, -2.7,
'19:18:17',
-- '19:18:17 GMT',
'2020-10-08', '2020-10-08 19:18:17', '2020-10-08 19:18:17 GMT',
null, null,
'hello', 'WORLD', 'X',
null, null, null, (1,2,3),
null, null)