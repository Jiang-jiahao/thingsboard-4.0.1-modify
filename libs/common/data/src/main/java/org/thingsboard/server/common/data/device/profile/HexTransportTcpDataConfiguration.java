package org.thingsboard.server.common.data.device.profile;

import lombok.Data;
import org.thingsboard.server.common.data.TransportTcpDataType;

import java.util.ArrayList;
import java.util.List;

/**
 * 原始字节（历史枚举名 {@link org.thingsboard.server.common.data.TransportTcpDataType#HEX}）的 TCP 传输数据配置。
 *
 * @author jiahaozz
 * <p>
 * <p>
 * {@link #hexCommandProfiles}：按命令字优先匹配，命中则使用对应规则的字段解析。
 * {@link #hexProtocolFields}：未命中任何命令规则时的回退解析；二者都为空则不做结构化解析。
 */
@Data
public class HexTransportTcpDataConfiguration implements TransportTcpDataTypeConfiguration {

    /**
     * 命令规则列表（按顺序，先匹配先生效）。
     */
    private List<TcpHexCommandProfile> hexCommandProfiles;

    /**
     * 未匹配任何命令规则时使用的字段列表；可为空。
     */
    private List<TcpHexFieldDefinition> hexProtocolFields;
    /**
     * 未匹配命令时可选的 LTV/TLV 重复段解析（在固定字段之后合并进同一遥测对象）。
     */
    private TcpHexLtvRepeatingConfig hexLtvRepeating;
    /**
     * 可选：整帧校验（NONE 或未设置则跳过）。
     */
    private TcpHexChecksumDefinition checksum;

    @Override
    public TransportTcpDataType getTransportTcpDataType() {
        return TransportTcpDataType.RAW_BYTES;
    }

    public void validateHexProtocolFields() {
        if (hexCommandProfiles != null) {
            for (TcpHexCommandProfile p : new ArrayList<>(hexCommandProfiles)) {
                if (p != null) {
                    p.validate();
                }
            }
        }
        if (hexLtvRepeating != null) {
            hexLtvRepeating.validate();
        }
        if (hexProtocolFields == null || hexProtocolFields.isEmpty()) {
            return;
        }
        List<TcpHexFieldDefinition> copy = new ArrayList<>(hexProtocolFields);
        for (TcpHexFieldDefinition f : copy) {
            if (f != null) {
                f.validate();
            }
        }
    }
}
