package com.unir.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import java.sql.Date;

@AllArgsConstructor
@Getter
public class Price {
    private int id;
    private int fuelTypeId;
    private int fuelStationId;
    private float price;
    private Date createAt;
}
