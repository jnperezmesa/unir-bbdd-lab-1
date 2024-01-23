-- Nombre de la empresa con más estaciones de servicio terrestres.
EXPLAIN ANALYZE SELECT c.name, COUNT(*) AS number_of_stations
FROM company AS c
JOIN fuel_station AS fs ON c.id = fs.company_id
WHERE fs.is_maritime = FALSE
GROUP BY c.id
ORDER BY number_of_stations DESC
LIMIT 1;

-- Nombre de la empresa con más estaciones de servicio marítimas.
EXPLAIN ANALYZE SELECT c.name, COUNT(*) AS number_of_stations
FROM company AS c
JOIN fuel_station AS fs ON c.id = fs.company_id
WHERE fs.is_maritime = TRUE
GROUP BY c.id
ORDER BY number_of_stations DESC
LIMIT 1;

-- Localización, nombre de empresa, y margen de la estación con el precio más bajo para el combustible “Gasolina 95 E5” en la Comunidad de Madrid.
EXPLAIN ANALYZE SELECT fs.address, c.name, fs.margin
FROM price AS p
JOIN fuel_type AS ft ON p.fuel_type_id = ft.id
JOIN fuel_station AS fs ON p.fuel_station_id = fs.id
JOIN company AS c ON fs.company_id = c.id
JOIN province AS prov ON fs.province_id = prov.id
WHERE ft.name = 'Gasolina 95 E5' AND prov.name = 'Madrid'
ORDER BY p.price ASC
LIMIT 1;

-- Localización, nombre de empresa, y margen de la estación con el precio más bajo para el combustible “Gasóleo A” si resido en el centro de Albacete y no quiero desplazarme más de 10 KM.
EXPLAIN ANALYZE SELECT fs.address, c.name, fs.margin
FROM price AS p
JOIN fuel_type AS ft ON p.fuel_type_id = ft.id
JOIN fuel_station AS fs ON p.fuel_station_id = fs.id
JOIN company AS c ON fs.company_id = c.id
WHERE ft.name = 'Gasóleo A'
AND ST_Distance_Sphere(POINT(fs.longitude, fs.latitude), POINT(-1.8550, 38.9943)) <= 10000
ORDER BY p.price ASC
LIMIT 1;

-- Provincia en la que se encuentre la estación de servicio marítima con el combustible “Gasolina 95 E5” más caro.
EXPLAIN ANALYZE SELECT prov.name
FROM price AS p
JOIN fuel_type AS ft ON p.fuel_type_id = ft.id
JOIN fuel_station AS fs ON p.fuel_station_id = fs.id
JOIN province AS prov ON fs.province_id = prov.id
WHERE ft.name = 'Gasolina 95 E5' AND fs.is_maritime = TRUE
ORDER BY p.price DESC
LIMIT 1;

-- Listar todas las estaciones de servicio existentes con los precios de los combustibles
SELECT
    fs.is_maritime AS 'Maritima',
    fs.margin AS 'Margen',
    prov.name AS 'Provincia',
    muni.name AS 'Municipio',
    town.name AS 'Localidad',
    pc.code AS 'Codigo Postal',
    fs.address AS 'Direccion',
    fs.latitude AS 'Latitud',
    fs.longitude AS 'Longitud',
    (SELECT MIN(p.create_at)
     FROM price p
     WHERE p.fuel_station_id = fs.id) AS 'Fecha Primer Combustible',
    (
        SELECT GROUP_CONCAT(CONCAT(fuel_type.name, ': ', price.price) ORDER BY price.create_at SEPARATOR ', ')
        FROM price
        JOIN fuel_type ON price.fuel_type_id = fuel_type.id
        WHERE price.fuel_station_id = fs.id
    ) AS 'Combustibles'
FROM
    fuel_station fs
JOIN
    province prov ON fs.province_id = prov.id
JOIN
    municipality muni ON fs.municipality_id = muni.id
JOIN
    town ON fs.town_id = town.id
JOIN
    postal_code pc ON fs.postal_code_id = pc.id;