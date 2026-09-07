/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.service.sync;

import org.thingsboard.server.service.sync.vc.GitRepository.FileType;
import org.thingsboard.server.service.sync.vc.GitRepository.RepoFile;

import java.util.List;

/**
 * Git 仓库定时同步服务。
 * <p>
 * 按业务 key 注册远程仓库，周期性 fetch；供 tb-core 读取仓库文件内容（与实体版本控制的 Git 队列不同，
 * 本服务直接在本进程 clone/fetch，不走 VC 队列）。
 */
public interface GitSyncService {

    /**
     * 注册一个按固定频率 fetch 的 Git 仓库。
     *
     * @param key              仓库业务标识
     * @param repoUri          远程仓库 URI
     * @param branch           跟踪的默认分支
     * @param fetchFrequencyMs fetch 间隔（毫秒）
     * @param onUpdate         仓库有更新时的回调，可为 {@code null}
     */
    void registerSync(String key, String repoUri, String branch, long fetchFrequencyMs, Runnable onUpdate);

    /**
     * 列出指定仓库路径下的文件。
     */
    List<RepoFile> listFiles(String key, String path, int depth, FileType type);

    /**
     * 读取指定仓库路径在跟踪分支上的文件内容。
     */
    byte[] getFileContent(String key, String path);

    /**
     * 构造 GitHub raw 内容 URL（基于仓库 URI 与默认分支）。
     */
    String getGithubRawContentUrl(String key, String path);

}
