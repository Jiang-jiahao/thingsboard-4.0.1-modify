package org.thingsboard.server.common.data;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EntityTypeTest {

    // backward-compatibility test
    @Test
    void getNormalNameTest() {
        assertThat(EntityType.ENTITY_VIEW.getNormalName()).isEqualTo("Entity View");
    }

}
