package com.unir.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class FuelStation {
    private int id;
    private int companyId;
    private int provinceId;
    private int municipalityId;
    private int townId;
    private int postalCodeId;
    private String address;
    private char margin;
    private float latitude;
    private float longitude;
    private String openingHours;
    private boolean isMaritime;

    public boolean getIsMaritime() {
        return isMaritime;
    }
}

