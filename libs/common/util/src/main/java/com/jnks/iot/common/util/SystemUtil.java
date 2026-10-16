package com.jnks.iot.common.util;

import lombok.extern.slf4j.Slf4j;
import oshi.SystemInfo;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * 系统资源指标（供各节点上报自身系统信息、首页「集群信息」表展示）。
 * <p>
 * <b>容器优先</b>：CPU / 内存优先读 cgroup（v2 优先、v1 兜底）—— 那才是**本进程所属容器**的用量；
 * 读不到（非容器部署，或 cgroup 路径不认识）才回退 OSHI 的整机口径。
 * <p>
 * 为什么必须这样：OSHI 读 {@code /proc/meminfo}、{@code /proc/stat}，而这两者在 Linux 里
 * **不随容器命名空间隔离**（cgroup 只限用量、不改读数），所以容器里读到的是**宿主整机**。
 * 十几个服务挤在同一个 Docker VM 里时，集群信息表每行都会显示同一份整机数字
 * （实测 19 行 totalMemory 全等于 VM 的 11.7GB、cpuCount 全是 10），既看不出哪个服务在吃资源，
 * 内存列还会被 VM 的 page cache 顶成长期 95%+。
 * <p>
 * <b>口径</b>：
 * <ul>
 *   <li>内存 = {@code (memory.current - inactive_file) / 上限}，上限取 cgroup 限额；无限额时取宿主总量
 *       —— 即"本容器占整机多少"，与 {@code docker stats} 的 MemUsage/MemPerc 同口径
 *       （inactive_file 是可直接回收的 page cache，不算压力，docker 也扣掉它）。</li>
 *   <li>CPU = 采样窗口内本容器消耗 / 窗口时长 / 核数 —— 即"占整机百分之多少"（0-100）。
 *       注意这与 {@code docker stats} 的 CPUPerc **不同口径**：后者是"占单核百分之多少"，可以超过 100。</li>
 *   <li>核数取 cgroup 配额（{@code cpu.max}）折算，没有配额就是宿主机核数。</li>
 *   <li>磁盘仍走 {@code /}（cgroup 不统计磁盘用量），容器里读到的就是宿主盘 —— 这一项还是宿主口径。</li>
 * </ul>
 */
@Slf4j
public class SystemUtil {

    private static final HardwareAbstractionLayer HARDWARE;

    static {
        HARDWARE = new SystemInfo().getHardware();
    }

    /** cgroup v2（统一层级，新内核） */
    private static final Path CGROUP_V2_MEMORY_CURRENT = Paths.get("/sys/fs/cgroup/memory.current");
    private static final Path CGROUP_V2_MEMORY_MAX = Paths.get("/sys/fs/cgroup/memory.max");
    private static final Path CGROUP_V2_MEMORY_STAT = Paths.get("/sys/fs/cgroup/memory.stat");
    private static final Path CGROUP_V2_CPU_STAT = Paths.get("/sys/fs/cgroup/cpu.stat");
    private static final Path CGROUP_V2_CPU_MAX = Paths.get("/sys/fs/cgroup/cpu.max");
    /** cgroup v1（老内核的服务器） */
    private static final Path CGROUP_V1_MEMORY_USAGE = Paths.get("/sys/fs/cgroup/memory/memory.usage_in_bytes");
    private static final Path CGROUP_V1_MEMORY_LIMIT = Paths.get("/sys/fs/cgroup/memory/memory.limit_in_bytes");
    private static final Path CGROUP_V1_MEMORY_STAT = Paths.get("/sys/fs/cgroup/memory/memory.stat");
    private static final Path CGROUP_V1_CPU_ACCT_USAGE = Paths.get("/sys/fs/cgroup/cpuacct/cpuacct.usage");
    private static final Path CGROUP_V1_CPU_QUOTA = Paths.get("/sys/fs/cgroup/cpu/cpu.cfs_quota_us");
    private static final Path CGROUP_V1_CPU_PERIOD = Paths.get("/sys/fs/cgroup/cpu/cpu.cfs_period_us");

    private static final long CPU_SAMPLE_MILLIS = 1000L;

    /**
     * 内存占用百分比。
     */
    public static Optional<Integer> getMemoryUsage() {
        try {
            Optional<Long> containerUsed = readContainerMemoryUsed();
            if (containerUsed.isPresent()) {
                long limit = readContainerMemoryLimit().orElseGet(() -> HARDWARE.getMemory().getTotal());
                if (limit > 0) {
                    return Optional.of(toPercent(containerUsed.get(), limit));
                }
            }
            GlobalMemory memory = HARDWARE.getMemory();
            long total = memory.getTotal();
            long available = memory.getAvailable();
            return Optional.of(toPercent(total - available, total));
        } catch (Throwable e) {
            log.debug("Failed to get memory usage!!!", e);
        }
        return Optional.empty();
    }

    /**
     * 内存总量：容器有 cgroup 限额就是限额，否则是宿主总量。
     * <p>与 {@link #getMemoryUsage()} 的分母保持一致 —— 界面上「已用 / 总量」两个数才自洽。
     */
    public static Optional<Long> getTotalMemory() {
        try {
            Optional<Long> limit = readContainerMemoryLimit();
            if (limit.isPresent()) {
                return limit;
            }
            return Optional.of(HARDWARE.getMemory().getTotal());
        } catch (Throwable e) {
            log.debug("Failed to get total memory!!!", e);
        }
        return Optional.empty();
    }

    /**
     * CPU 占用百分比（占整机 0-100）：采样窗口内本容器消耗 / 窗口时长 / 核数。
     */
    public static Optional<Integer> getCpuUsage() {
        try {
            Optional<Long> first = readContainerCpuUsageNanos();
            if (first.isPresent()) {
                long startNanos = System.nanoTime();
                Thread.sleep(CPU_SAMPLE_MILLIS);
                Optional<Long> second = readContainerCpuUsageNanos();
                long wallNanos = System.nanoTime() - startNanos;
                if (second.isPresent() && wallNanos > 0) {
                    int cores = Math.max(1, getCpuCount().orElse(1));
                    int percent = toPercent(second.get() - first.get(), wallNanos * cores);
                    return Optional.of(Math.min(100, percent));
                }
            }
            return Optional.of((int) (HARDWARE.getProcessor().getSystemCpuLoad(1000) * 100.0));
        } catch (Throwable e) {
            log.debug("Failed to get cpu usage!!!", e);
        }
        return Optional.empty();
    }

    /**
     * 可用核数：容器有 CPU 配额就按配额折算（向上取整），否则是宿主逻辑核数。
     */
    public static Optional<Integer> getCpuCount() {
        try {
            Optional<Integer> quotaCores = readContainerCpuQuotaCores();
            if (quotaCores.isPresent()) {
                return quotaCores;
            }
            return Optional.of(HARDWARE.getProcessor().getLogicalProcessorCount());
        } catch (Throwable e) {
            log.debug("Failed to get total cpu count!!!", e);
        }
        return Optional.empty();
    }

    /** 磁盘：cgroup 不统计，容器里读到的是宿主盘，这一项始终是宿主口径。 */
    public static Optional<Integer> getDiscSpaceUsage() {
        try {
            FileStore store = Files.getFileStore(Paths.get("/"));
            long total = store.getTotalSpace();
            long available = store.getUsableSpace();
            return Optional.of(toPercent(total - available, total));
        } catch (Throwable e) {
            log.debug("Failed to get free disc space!!!", e);
        }
        return Optional.empty();
    }

    /** 磁盘总量（同上，宿主口径）。 */
    public static Optional<Long> getTotalDiscSpace() {
        try {
            FileStore store = Files.getFileStore(Paths.get("/"));
            return Optional.of(store.getTotalSpace());
        } catch (Throwable e) {
            log.debug("Failed to get total disc space!!!", e);
        }
        return Optional.empty();
    }

    // ── cgroup 读取 ───────────────────────────────────────────────

    /** 本容器已用内存（扣掉 inactive_file，与 docker stats 一致）；不在容器里返回 empty。 */
    private static Optional<Long> readContainerMemoryUsed() {
        Optional<Long> current = readLong(CGROUP_V2_MEMORY_CURRENT)
                .or(() -> readLong(CGROUP_V1_MEMORY_USAGE));
        if (current.isEmpty()) {
            return Optional.empty();
        }
        long inactiveFile = readKeyedLong(CGROUP_V2_MEMORY_STAT, "inactive_file")
                .or(() -> readKeyedLong(CGROUP_V1_MEMORY_STAT, "total_inactive_file"))
                .orElse(0L);
        return Optional.of(Math.max(0L, current.get() - inactiveFile));
    }

    /**
     * cgroup 内存限额。v2 无限制写 "max"；v1 无限制写一个接近 Long.MAX 的数。
     * 超过宿主总量的限额等同于"没限制"（Docker Desktop 默认就是这样），一律当成无限额。
     */
    private static Optional<Long> readContainerMemoryLimit() {
        Optional<Long> limit = readLong(CGROUP_V2_MEMORY_MAX)
                .or(() -> readLong(CGROUP_V1_MEMORY_LIMIT));
        if (limit.isEmpty()) {
            return Optional.empty();
        }
        long hostTotal = HARDWARE.getMemory().getTotal();
        return limit.get() > 0 && limit.get() < hostTotal ? limit : Optional.empty();
    }

    private static Optional<Long> readContainerCpuUsageNanos() {
        Optional<Long> v2 = readKeyedLong(CGROUP_V2_CPU_STAT, "usage_usec").map(usec -> usec * 1000L);
        return v2.isPresent() ? v2 : readLong(CGROUP_V1_CPU_ACCT_USAGE);
    }

    /** CPU 配额折算成核数；没有配额（v2 "max" / v1 -1）返回 empty。 */
    private static Optional<Integer> readContainerCpuQuotaCores() {
        Optional<String> cpuMax = readString(CGROUP_V2_CPU_MAX);
        if (cpuMax.isPresent()) {
            String[] parts = cpuMax.get().trim().split("\\s+");
            if (parts.length == 2 && !"max".equals(parts[0])) {
                long quota = parseLong(parts[0]).orElse(0L);
                long period = parseLong(parts[1]).orElse(0L);
                return quotaToCores(quota, period);
            }
            return Optional.empty();
        }
        long quota = readLong(CGROUP_V1_CPU_QUOTA).orElse(-1L);
        long period = readLong(CGROUP_V1_CPU_PERIOD).orElse(0L);
        return quotaToCores(quota, period);
    }

    static Optional<Integer> quotaToCores(long quota, long period) {
        if (quota <= 0 || period <= 0) {
            return Optional.empty();
        }
        return Optional.of((int) Math.ceil((double) quota / period));
    }

    private static Optional<Long> readKeyedLong(Path path, String key) {
        return readString(path).flatMap(content -> Optional.ofNullable(parseKeyedValue(content, key)));
    }

    /** 从 cgroup 的 "key value" 逐行文本里取某个键。测试用。 */
    static Long parseKeyedValue(String content, String key) {
        if (content == null) {
            return null;
        }
        for (String line : content.split("\n")) {
            String trimmed = line.trim();
            int idx = trimmed.indexOf(' ');
            if (idx <= 0 || !trimmed.substring(0, idx).trim().equals(key)) {
                continue;
            }
            return parseLong(trimmed.substring(idx + 1).trim()).orElse(null);
        }
        return null;
    }

    private static Optional<Long> parseLong(String value) {
        try {
            return Optional.of(Long.parseLong(value.trim()));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private static Optional<Long> readLong(Path path) {
        return readString(path).flatMap(SystemUtil::parseLong);
    }

    private static Optional<String> readString(Path path) {
        try {
            return Files.exists(path) ? Optional.of(new String(Files.readAllBytes(path))) : Optional.empty();
        } catch (Throwable e) {
            log.debug("Failed to read {}", path, e);
            return Optional.empty();
        }
    }

    private static int toPercent(long used, long total) {
        if (total <= 0) {
            return 0;
        }
        BigDecimal u = new BigDecimal(Math.max(0L, used));
        BigDecimal t = new BigDecimal(total);
        BigDecimal i = new BigDecimal(100);
        return u.multiply(i).divide(t, RoundingMode.HALF_UP).intValue();
    }
}
