create table fuel_db.fuel_station
(
    id             int auto_increment
        primary key,
    company_id     int                  null,
    community_id   int                  null,
    province_id    int                  null,
    city_id        int                  null,
    postal_code_id int                  null,
    address        text                 null,
    margin         char                 null,
    latitude       float                null,
    longitude      float                null,
    opening_hours  text                 null,
    is_maritime    tinyint(1) default 0 null,
    constraint fuel_station_ibfk_1
        foreign key (company_id) references fuel_db.company (id),
    constraint fuel_station_ibfk_2
        foreign key (community_id) references fuel_db.community (id),
    constraint fuel_station_ibfk_3
        foreign key (province_id) references fuel_db.province (id),
    constraint fuel_station_ibfk_4
        foreign key (city_id) references fuel_db.city (id),
    constraint fuel_station_ibfk_5
        foreign key (postal_code_id) references fuel_db.postal_code (id)
);

create index city_id
    on fuel_db.fuel_station (city_id);

create index community_id
    on fuel_db.fuel_station (community_id);

create index company_id
    on fuel_db.fuel_station (company_id);

create index postal_code_id
    on fuel_db.fuel_station (postal_code_id);

create index province_id
    on fuel_db.fuel_station (province_id);

