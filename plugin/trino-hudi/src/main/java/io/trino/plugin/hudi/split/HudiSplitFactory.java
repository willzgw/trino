/*
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
package io.trino.plugin.hudi.split;

import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hudi.HudiFileStatus;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.query.HudiReadOptimizedDirectoryLister;
import io.trino.spi.TrinoException;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieLogFile;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_FILESYSTEM_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HudiSplitFactory
{
    private final HudiTableHandle hudiTableHandle;
    private final HudiSplitWeightProvider hudiSplitWeightProvider;

    public HudiSplitFactory(
            HudiTableHandle hudiTableHandle,
            HudiSplitWeightProvider hudiSplitWeightProvider)
    {
        this.hudiTableHandle = requireNonNull(hudiTableHandle, "hudiTableHandle is null");
        this.hudiSplitWeightProvider = requireNonNull(hudiSplitWeightProvider, "hudiSplitWeightProvider is null");
    }

    public Optional<HudiSplit> createSplit(List<HivePartitionKey> partitionKeys, FileSlice fileSlice)
    {
        Optional<HudiFileStatus> baseFile = Optional.ofNullable(fileSlice.getBaseFile()
                .map(HudiReadOptimizedDirectoryLister::getStoragePathInfo)
                .map(HudiReadOptimizedDirectoryLister::getHudiFileStatus)
                .orElse(null));

        List<HoodieLogFile> hoodieLogFiles = fileSlice.getLogFiles().collect(toImmutableList());

        if (baseFile.isEmpty() && hoodieLogFiles.isEmpty()) {
            return Optional.empty();
        }

        if (baseFile.isPresent() && baseFile.get().isDirectory()) {
            throw new TrinoException(HUDI_FILESYSTEM_ERROR, format("Not a valid location: %s", baseFile.get().location()));
        }

        List<HudiFileStatus> logFiles = hoodieLogFiles.stream()
                .map(HoodieLogFile::getPathInfo)
                .map(HudiReadOptimizedDirectoryLister::getHudiFileStatus)
                .collect(toImmutableList());

        long logFilesSize = !hoodieLogFiles.isEmpty() ? hoodieLogFiles.stream().map(HoodieLogFile::getFileSize).reduce(0L, Long::sum) : 0L;
        long fileSize = fileSlice.getBaseFile().map(hoodieBaseFile -> hoodieBaseFile.getFileLen() + logFilesSize).orElse(logFilesSize);

        return Optional.of(new HudiSplit(
                baseFile,
                logFiles,
                baseFile.map(HudiFileStatus::modificationTime).orElse(logFiles.getFirst().modificationTime()),
                hudiTableHandle.getRegularPredicates(),
                partitionKeys,
                hudiSplitWeightProvider.calculateSplitWeight(fileSize)));
    }
}
