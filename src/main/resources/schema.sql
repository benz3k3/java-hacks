create table student

(

   id integer not null,

   name varchar(255) not null,

   passport_number varchar(255) not null,

   primary key(id)

);

create table old_student

(

   id integer not null,

   name varchar(255) not null,

   passport_number varchar(255) not null,

   primary key(id)

);

insert into student

values(10001,'Ranga', 'E1234567');

insert into student

values(10002,'Ravi', 'A1234568');

insert into student

values(10003,'Panga', 'E1234563');

insert into student

values(10004,'Kavi', 'A123456D');

