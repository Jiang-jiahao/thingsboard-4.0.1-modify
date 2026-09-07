/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package org.thingsboard.server.common.data.device.profile;

/**
 * 重复 LTV/TLV 段的字段顺序。长度字段均为<strong>仅 Value 负载</strong>的字节数。
 */
public enum TcpHexLtvChunkOrder {
    /** Length, Tag, Value */
    LTV,
    /** Tag, Length, Value */
    TLV
}
