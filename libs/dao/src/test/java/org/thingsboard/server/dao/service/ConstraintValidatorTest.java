package org.thingsboard.server.dao.service;

import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.thingsboard.server.common.data.kv.StringDataEntry;
import org.thingsboard.server.dao.exception.DataValidationException;

class ConstraintValidatorTest {

    private static final int MIN_IN_MS = 60000;
    private static final int _1M = 1_000_000;

    @Test
    void validateFields() {
        StringDataEntry stringDataEntryValid = new StringDataEntry("key", "value");
        StringDataEntry stringDataEntryInvalid1 = new StringDataEntry("<object type=\"text/html\"><script>alert(document)</script></object>", "value");

        Assert.assertThrows(DataValidationException.class, () -> ConstraintValidator.validateFields(stringDataEntryInvalid1));
        ConstraintValidator.validateFields(stringDataEntryValid);
    }

    @Test
    void validatePerMinute() {
        StringDataEntry stringDataEntryValid = new StringDataEntry("key", "value");

        long start = System.currentTimeMillis();
        for (int i = 0; i < _1M; i++) {
            ConstraintValidator.validateFields(stringDataEntryValid);
        }
        long end = System.currentTimeMillis();

        Assertions.assertTrue(MIN_IN_MS > end - start);
    }
}