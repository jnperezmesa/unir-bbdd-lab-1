# bbdda-jdbc
Proyecto java utilizando JDBC para acceder a bases de datos relacionales

Instalar maven en windows: https://maven.apache.org/install.html

Instalar maven en mac: https://formulae.brew.sh/formula/maven

## Instalar dependencias con maven
```
mvn install
```

## Compilar
```
mvn compile
```

## Ejecutar
```
mvn exec:java -Dexec.mainClass="com.unir.app.LoadFuelStationDataApplication"
```

## Crear jar
```
mvn package
```

## Ejecutar jar
```
java -jar target/bbdda-jdbc-1.0-SNAPSHOT.jar
```

## Recargar dependencias
```
mvn clean
```
Después de ejecutar este comando, ejecutar el comando para instalar dependencias con maven

## Variables de entorno
```
MYSQL_USER=root;MYSQL_PASSWORD=mysql
```