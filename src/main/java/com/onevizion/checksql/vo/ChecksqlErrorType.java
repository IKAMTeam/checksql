package com.onevizion.checksql.vo;

import java.util.Arrays;

public enum ChecksqlErrorType {
    DEFAULT(0L),
    CONFIG(1L);

    private Long id;

    ChecksqlErrorType(Long id) {
        this.id = id;
    }

    public Long getId() {
        return id;
    }

    public static ChecksqlErrorType getById(Long id) {
        return Arrays.stream(values())
                     .filter(sst -> id.equals(sst.getId()))
                     .findAny()
                     .orElseThrow(() -> new IllegalArgumentException(
                             "Not supported Checksql Error Type id: [" + id + "]"));
    }
}
