package com.onevizion.checksql.vo;

import java.util.Arrays;

public enum SqlStatementType {

    EMPTY(0L),
    SELECT(1L),
    PL_SQL(2L);

    private Long id;

    SqlStatementType(Long id) {
        this.id = id;
    }

    public Long getId() {
        return id;
    }

    public static SqlStatementType getById(Long id) {
        return Arrays.stream(values())
                     .filter(sst -> id.equals(sst.getId()))
                     .findAny()
                     .orElseThrow(() -> new IllegalArgumentException(
                             "Not supported Sql Statement Type id: [" + id + "]"));
    }

}
