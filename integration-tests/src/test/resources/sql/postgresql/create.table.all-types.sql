create table if not exists test_all_types
(
   i int,
   b bool,
   si smallint,
   bi bigint,
   dcml numeric,
   f float4,
   r real,
   t time,
   ttz time,
   dt date,
   ts timestamp,
   tswtz timestamp,
   vc text,
   c char(1),
   u uuid,
   j json
)
