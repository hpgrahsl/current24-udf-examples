-- MySQL
CREATE DATABASE udf_demo;
USE udf_demo;

create table hello (
    id int not null auto_increment,
    who varchar(255),
    lang char(2),
    primary key (id)
) engine=InnoDB;

insert into hello (who,lang) VALUES 
    ('Pedro','ES'),
    ('Hans-Peter','DE'),
    ('w o r l d',null),
    (null,'IT');

create table calc (
    id int not null auto_increment,
    operation varchar(255),
    operand1 float,
    operand2 float,
    primary key (id)
) engine=InnoDB;

insert into calc (operation,operand1,operand2) VALUES
    ('ADD',12.0,34.0),
    ('SUBTRACT',10.0,10.0),
    ('MULTIPLY',23.0,42.0),
    ('DIVIDE',1024.0,256.0);

create table overloaded (
    id int not null auto_increment,
    data1 varchar(255),
    data2 int,
    data3 boolean,
    primary key (id)
) engine=InnoDB;

insert into overloaded (data1,data2,data3) VALUES
    ('hello',23,FALSE),
    ('current',0,TRUE),
    ('apache',99,TRUE),
    ('flink',0,FALSE);

create table repeater (
    id int not null auto_increment,
    data1 varchar(255),
    data2 float,
    data3 int,
    data4 boolean,
    primary key (id)
) engine=InnoDB;

insert into repeater (data1,data2,data3,data4) VALUES
    ('hello',23.0,42,FALSE),
    ('current',0.0,2024,TRUE),
    ('apache',99.99,100,TRUE),
    ('flink',0.0,2024,FALSE);
