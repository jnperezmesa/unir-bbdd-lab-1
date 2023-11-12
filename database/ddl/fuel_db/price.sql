create table fuel_db.price
(
    id              int auto_increment
        primary key,
    fuel_type_id    int   null,
    fuel_station_id int   null,
    price           float null,
    create_at       date  null,
    constraint price_ibfk_1
        foreign key (fuel_type_id) references fuel_db.fuel_type (id),
    constraint price_ibfk_2
        foreign key (fuel_station_id) references fuel_db.fuel_station (id)
);

create index fuel_station_id
    on fuel_db.price (fuel_station_id);

create index fuel_type_id
    on fuel_db.price (fuel_type_id);

