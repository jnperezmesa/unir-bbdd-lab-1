-- create database if not exists
CREATE DATABASE IF NOT EXISTS fuel_db
CHARACTER SET utf8mb4
COLLATE utf8mb4_spanish_ci;

-- use database
USE fuel_db;


-- company Table
CREATE TABLE IF NOT EXISTS company (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(250)
);

-- province Table
CREATE TABLE IF NOT EXISTS province (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(250)
);

-- municipality Table
CREATE TABLE IF NOT EXISTS municipality (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(250)
);

-- town Table
CREATE TABLE IF NOT EXISTS town (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(250)
);

-- postal_code Table
CREATE TABLE IF NOT EXISTS postal_code (
    id INT AUTO_INCREMENT PRIMARY KEY,
    code INT
);

-- fuel_station Table
CREATE TABLE IF NOT EXISTS fuel_station (
    id INT AUTO_INCREMENT PRIMARY KEY,
    company_id INT,
    province_id INT,
    municipality_id INT,
    town_id INT,
    postal_code_id INT,
    address TEXT,
    margin CHAR(1) NULL,
    latitude FLOAT,
    longitude FLOAT,
    opening_hours TEXT,
    is_maritime BOOL DEFAULT FALSE,
    FOREIGN KEY (company_id) REFERENCES company(id),
    FOREIGN KEY (province_id) REFERENCES province(id),
    FOREIGN KEY (municipality_id) REFERENCES municipality(id),
    FOREIGN KEY (town_id) REFERENCES town(id),
    FOREIGN KEY (postal_code_id) REFERENCES postal_code(id)
);

-- fuel_type Table
CREATE TABLE IF NOT EXISTS fuel_type (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(250)
);

-- price Table
CREATE TABLE IF NOT EXISTS price (
    id INT AUTO_INCREMENT PRIMARY KEY,
    fuel_type_id INT,
    fuel_station_id INT,
    price FLOAT,
    create_at DATE,
    FOREIGN KEY (fuel_type_id) REFERENCES fuel_type(id),
    FOREIGN KEY (fuel_station_id) REFERENCES fuel_station(id)
);
