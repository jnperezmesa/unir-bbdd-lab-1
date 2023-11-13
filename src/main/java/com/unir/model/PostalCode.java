package com.unir.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Objects;

@AllArgsConstructor
@Getter
public class PostalCode {
    private int id;
    private int code;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PostalCode postalCode = (PostalCode) o;
        return Objects.equals(code, postalCode.code);
    }

    @Override
    public int hashCode() {
        return Objects.hash(code);
    }
}
