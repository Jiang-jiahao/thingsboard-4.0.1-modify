package com.jnks.iot.common.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 只测纯解析部分（cgroup 文件内容的读法）—— 真实的 cgroup 读数依赖运行环境，不在这里断言。
 */
class SystemUtilTest {

    private static final String MEMORY_STAT = """
            anon 1051938816
            file 19509248
            kernel_stack 16384
            inactive_file 12324864
            active_file 7184384
            """;

    private static final String CPU_STAT = """
            usage_usec 1803500552
            user_usec 1503774993
            system_usec 299665918
            nr_periods 0
            """;

    @Test
    void givenMemoryStat_whenParseInactiveFile_thenReturnValue() {
        assertThat(SystemUtil.parseKeyedValue(MEMORY_STAT, "inactive_file")).isEqualTo(12324864L);
    }

    @Test
    void givenCpuStat_whenParseUsageUsec_thenReturnValue() {
        assertThat(SystemUtil.parseKeyedValue(CPU_STAT, "usage_usec")).isEqualTo(1803500552L);
    }

    @Test
    void givenMissingKey_whenParseKeyedValue_thenReturnNull() {
        assertThat(SystemUtil.parseKeyedValue(MEMORY_STAT, "inactive")).isNull();
        assertThat(SystemUtil.parseKeyedValue(null, "inactive_file")).isNull();
    }

    /** 键名必须整段相等：anon 不能匹到 anon_thp，file 不能匹到 inactive_file。 */
    @Test
    void givenPrefixSharingKeys_whenParseKeyedValue_thenOnlyExactKeyMatches() {
        String content = "anon_thp 111\ninactive_file 222\n";
        assertThat(SystemUtil.parseKeyedValue(content, "anon")).isNull();
        assertThat(SystemUtil.parseKeyedValue(content, "inactive_file")).isEqualTo(222L);
    }

    @Test
    void givenQuotaAndPeriod_whenQuotaToCores_thenCeilToCores() {
        assertThat(SystemUtil.quotaToCores(200000, 100000)).hasValue(2);
        assertThat(SystemUtil.quotaToCores(250000, 100000)).hasValue(3);
        assertThat(SystemUtil.quotaToCores(50000, 100000)).hasValue(1);
    }

    /** v1 的 -1（不限额）与缺周期都要当成"没有配额"。 */
    @Test
    void givenNoQuota_whenQuotaToCores_thenEmpty() {
        assertThat(SystemUtil.quotaToCores(-1, 100000)).isEmpty();
        assertThat(SystemUtil.quotaToCores(200000, 0)).isEmpty();
        assertThat(SystemUtil.quotaToCores(0, 100000)).isEmpty();
    }
}
