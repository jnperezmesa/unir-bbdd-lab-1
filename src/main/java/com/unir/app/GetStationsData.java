package com.unir.app;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import com.unir.config.MySqlConnector;
import com.unir.model.FuelStation;
import com.unir.model.FuelType;
import com.unir.model.Province;
import com.unir.model.Municipality;
import com.unir.model.Town;
import com.unir.model.PostalCode;
import com.unir.model.Company;
import com.unir.model.Price;
import lombok.extern.slf4j.Slf4j;

import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Slf4j
public class GetStationsData {
    public static void main(String[] args) {
        System.out.println("Hello World!");
    }
}
