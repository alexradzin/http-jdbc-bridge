package com.nosqldriver.jdbc.http.permissions;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

class StatementPermissionsValidatorTest {
    private final StatementPermissionsValidator factory = new StatementPermissionsValidator();

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @CsvSource(delimiter = ';', value = {
            // select
            "select * from *;select * from my_table",
            "select * from *;select x from your_table",
            "select * from *;select x,y,z from my_table",
            "select x from *;select x from my_table",
            "select x from my_table;select x from my_table",
            "select * from my_table;select x from my_table",
            "select x,y from *;select x from my_table",
            "select x,y from *;select x,y from my_table",
            "select x,y,z from *;select x,y from my_table",
            // limit
            "select * from * limit *;select * from my_table limit 10",
            "select * from * limit 10;select * from my_table limit 10",
            "select * from * limit 100;select * from my_table limit 10",
            "select * from my_table limit 10<br>select * from * limit 5;select * from my_table limit 10",

            // order by
            "select * from * order by *;select * from my_table order by x",
            "select * from * order by y;select * from my_table order by y",

            // order by, limit
            "select * from * order by something limit 15;select * from my_table order by something limit 15",
            "select * from * order by something limit 15;select * from my_table order by something limit 10",

            // group by
            "select * from * group by *;select * from my_table group by x",
            "select * from * group by y;select * from my_table group by y",
            "select * from * group by x,y;select * from my_table group by y",

            // join
            "select * from * join *;select * from people p join address a on p.id = a.owner_id",
            "select * from * join *;select * from people p left join address a on p.id = a.owner_id",
            "select * from * join address;select * from people p left join address a on p.id = a.owner_id",

            // where
            "select * from * where *;select * from people where id = 1",
            "select * from people where id(=,in);select * from people where id = 1",
            "select * from people where id(*);select * from people where id >= 1",
            "select * from people where *(=,in);select * from people where id = 1",

            // insert
            "insert into * (*) limit 100;insert into people (id, name) values (?,?), (?,?)",
            "insert into * (id, name) limit 100;insert into people (id, name) values (?,?), (?,?)",
            "insert into * (id, name, year_of_birth) limit 100;insert into people (id, name) values (?,?,?), (?,?,?)",

            // update
            "update *;update people set name='John' where id=1",
            "update * set (*) where *(*);update people set name='John' where id=1",
            "update people;update people set name='John' where id=1",
            "update people set (name);update people set name='John' where id=1",
            "update people where id(=,in);update people set name='John' where id in (1,2,3)",
            "update people set (name) where id(=,>,<);update people set name='John' where id=1",

            // delete
            "delete from *;delete from people",
            "delete from * where *(*);delete from people where name=1",
            "delete from people;delete from people",
            "delete from people where *(*);delete from people where name='John'",
            "delete from people where id(*);delete from people where id > 100",
            "delete from people where id(=,in,>);delete from people where id > 100",
            "delete from * where id(=,in);delete from people where id in (1,2,3)",

            // truncate
            "truncate *;truncate my_table",
            "truncate my_table;truncate my_table",

            // create
            "create * *;create table people (id int, first_name string, last_name varchar(64))",
            // create table
            "create table *;create table if not exists people (id int, first_name string, last_name varchar(64))",
            "create table *;create table people (id int, first_name string, last_name varchar(64))",
            "create table my_table123;create temporary table my_table123 (id int, first_name string, last_name varchar(64))",

            // create schema
            "create schema *;create schema my_space321",
            "create schema *;create schema my_space321",

            // create index
            "create index * on *;create index my_index on my_table (id)",
            "create index * on my_table;create index my_index on my_table (id)",
            "create index my_index on *;create index my_index on my_table (name)",
            "create index my_index on *;create unique index my_index on my_table (pk)",
            "create index my_index on *;create spatial index my_index on my_table (name(10))",
            "create index my_index on my_table;create spatial index my_index on my_table (id)",

            "create view * from *;create view my_view as select * from my_table",
            "create view * from people;create view men as select * from people where gender='male'",
            "create view the_view4321 from *;create view the_view4321 as select * from my_table",

            "drop * *;drop table my_table",
            "drop table *;drop table my_table",
            "drop table my_table;drop table my_table",
            "drop * my_table;drop table my_table",

            "drop * *;drop view my_view",
            "drop view *;drop view my_view",
            "drop view my_view;drop view my_view",
            "drop * my_view;drop view my_view",

            "alter * *;alter table my_table drop column x",
            "alter table *;alter table my_table drop column x",
            "alter table my_table;alter table my_table drop column x",
            "alter * my_table;alter table my_table drop column x",

            "alter * *;alter view my_view as select * from my_table",
            "alter view *;alter view my_view as select * from my_table",
            "alter view my_view;alter view my_view as select * from my_table",
            "alter * my_view;alter view my_view as select * from my_table",
    })
    void allow(String conf, String query) throws ParseException, SQLException, IOException {
        addConfiguration(conf);
        factory.validate(query);
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @CsvSource(delimiter = ';', value = {
            ";call x;Statement is not allowed", // unsupported but valid SQL statement

            ";select * from your_table;Statement is not allowed",
            "select * from my_table;select * from your_table;Statement is not allowed",
            "select x from my_table;select * from my_table;Fields [*] cannot be queried",
            "select x from my_table;select y from my_table;Fields [y] cannot be queried",

            // limit
                //"select * from * no limit;select * from my_table limit 10;Limit cannot be used",
            "select * from * limit 10;select * from my_table limit 11;Actual limit 11 exceeds required one 10",
            "select * from * limit 10;select * from my_table;Query must be limited but was not",
            "select * from my_table limit 5<br>select * from * limit 10;select * from my_table limit 10;Actual limit 10 exceeds required one 5",

            // order by
            "select * from * order by x;select * from my_table order by y;Cannot order by [y]",
            "select * from * order by x,y;select * from my_table order by z;Cannot order by [z]",
            "select * from * order by x;select * from my_table order by y,z;Cannot order by [y, z]",

            // order by, limit
            "select * from * order by something limit 5;select * from my_table order by nothing limit 5;Cannot order by [nothing]",
            "select * from * order by something limit 5;select * from my_table order by something limit 10;Actual limit 10 exceeds required one 5",
            "select * from * order by something limit 5;select * from my_table order by nothing limit 10;Actual limit 10 exceeds required one 5", // both limit and order by are violated but limit is validated first

            // group by
            "select * from * group by x;select * from my_table group by y;Cannot group by [y]",
            "select * from * group by x,y;select * from my_table group by z;Cannot group by [z]",
            "select * from * group by x;select * from my_table group by y,z;Cannot group by [y, z]",

            // join
            "select * from * join company;select * from people p join address a on p.id = a.owner_id;Cannot join with [address]",
            "select * from * inner join company outer join DOES_NOT_EXIST;select * from people p outer join address a on p.id = a.owner_id;Cannot join with [address]",
            "select * from * inner join company;select * from people p left join address a on p.id = a.owner_id;Cannot join with [address]",
            "select * from * left join company;select * from people p join address a on p.id = a.owner_id;Cannot join with [address]",

            // where
            "select * from * where id(=,in);select * from people where id > 1;Condition id > is forbidden",
            "select * from * where id(=,in);select * from people where name = 'John';Condition name = is forbidden",
            "select * from * where *(=,in);select * from people where name like'%John%';Condition name like is forbidden",
            "select * from people where name(=,in)<br>select * from * where id(=,in);select * from people where age >120;Condition age > is forbidden",

            // insert
            "insert into my_table;insert into other_table (id, name) values (?,?);Statement is not allowed",
            "insert into * (*) limit 2;insert into people (id, name) values (?,?), (?,?), (?,?);Actual limit 3 exceeds required one 2",
            "insert into * (id, name) limit 100;insert into people (id, name, age) values (?,?,?);Fields [age] cannot be inserted",
            "insert into * (id, name) limit *;insert into people (id, name, age) values (?,?,?);Fields [age] cannot be inserted",
            "insert into * (id, name);insert into people (id, name, age) values (?,?,?);Fields [age] cannot be inserted",

            // update
            "update my_table;update people set name='John' where id=1;Statement is not allowed",
            "update people set (age);update people set name='John' where id=1;Fields [name] cannot be inserted",
            "update people where id(=);update people set name='John' where id in (1,2,3);Condition id in is forbidden",
            "update people set (age) where id(=,in);update people set name='John' where id=1;Fields [name] cannot be inserted",

            // delete
            "delete from * where *(*);delete from people;where clause is required here",
            "delete from people where *(*);delete from persons;Statement is not allowed", // wrong table
            "delete from people where id(*);delete from people where name='John';Condition name = is forbidden",
            "delete from people where id(=,in,>);delete from people where id < 100;Condition id < is forbidden",
            "delete from * where id(=,in);delete from people where id >= 123;Condition id >= is forbidden",

            // truncate
            ";truncate table2;Statement is not allowed",
            "truncate table1;truncate table2;Statement is not allowed",

            // create
            ";create schema your_schema;Statement is not allowed",
            ";create table my_table;Statement is not allowed",
            ";create view my_view as select * from my_table;Statement is not allowed",

            "create schema my_schema;create schema your_schema;Statement is not allowed",
            "create table my_table;create table your_table (id int);Statement is not allowed",
            "create view my_view from *;create view your_view as select * from my_table;Statement is not allowed",
            "create view my_view from your_table;create view my_view as select * from my_table;Creating view from my_table is not allowed",
            "create view * from my_table;create view my_view as select * from your_table;Creating view from your_table is not allowed",

            // drop
            ";drop table my_table;Statement is not allowed",
            "drop table *;drop view my_view;Dropping view my_view is not allowed",
            "drop table my_table;drop table your_table;Statement is not allowed",
            "drop * your_table;drop table my_table;Statement is not allowed",

            ";drop view my_table;Statement is not allowed",
            "drop view *;drop table my_table;Dropping table my_table is not allowed",
            "drop view my_view;drop view your_view;Statement is not allowed",
            "drop * your_view;drop view my_view;Statement is not allowed",

            // alter
            ";alter table my_table drop column x;Statement is not allowed",
            "alter table *;alter view my_view as select * from my_table;Altering view my_view is not allowed",
            "alter table my_table;alter table your_table drop column x;Statement is not allowed",
            "alter * your_table;alter table my_table drop column x;Statement is not allowed",

            ";alter view my_table as select * from my_table;Statement is not allowed",
            "alter view *;alter table my_table as select * from my_table;Altering table my_table is not allowed",
            "alter view my_view;alter view your_view as select * from my_table;Statement is not allowed",
            "alter * your_view;alter view my_view as select * from my_table;Statement is not allowed",
    })
    void disallow(String conf, String query, String errorMessage) throws IOException {
        addConfiguration(conf);
        assertEquals(errorMessage, assertThrows(SQLException.class, () -> factory.validate(query)).getMessage());
    }

    private void addConfiguration(String conf) throws IOException {
        if (conf != null) {
            factory.addConfiguration(new ByteArrayInputStream(conf.replace("<br>", "\n").getBytes()));
        }
    }
}
