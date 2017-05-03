/*
 * Copyright 2017 National Bank of Belgium
 * 
 * Licensed under the EUPL, Version 1.1 or - as soon they will be approved 
 * by the European Commission - subsequent versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 * 
 * http://ec.europa.eu/idabc/eupl
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the Licence is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Licence for the specific language governing permissions and 
 * limitations under the Licence.
 */
package internal.demetra.jackcess;

import com.google.common.base.Strings;
import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import ec.tss.tsproviders.HasFilePaths;
import ec.tstoolkit.utilities.GuavaCaches;
import ec.util.completion.AutoCompletionSource;
import static ec.util.completion.AutoCompletionSource.Behavior.ASYNC;
import static ec.util.completion.AutoCompletionSource.Behavior.NONE;
import static ec.util.completion.AutoCompletionSource.Behavior.SYNC;
import ec.util.completion.ExtAutoCompletionSource;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 *
 * @author Philippe Charles
 */
@lombok.experimental.UtilityClass
public class JackcessAutoCompletion {

    public AutoCompletionSource onTables(HasFilePaths paths, Supplier<File> file) {
        return ExtAutoCompletionSource
                .builder(o -> loadTables(paths, file))
                .behavior(o -> canLoadTables(file) ? ASYNC : NONE)
                .postProcessor(JackcessAutoCompletion::filterAndSortTables)
                .cache(GuavaCaches.ttlCacheAsMap(Duration.ofMinutes(1)), o -> getTableCacheKey(file), SYNC)
                .build();
    }

    public AutoCompletionSource onColumns(HasFilePaths paths, Supplier<File> file, Supplier<String> table) {
        return ExtAutoCompletionSource
                .builder(o -> loadColumns(paths, file, table))
                .behavior(o -> canLoadColumns(file, table) ? ASYNC : NONE)
                .postProcessor(JackcessAutoCompletion::filterAndSortColumns)
                .valueToString(Column::getName)
                .cache(GuavaCaches.ttlCacheAsMap(Duration.ofMinutes(1)), o -> getColumnCacheKey(file, table), SYNC)
                .build();
    }

    public String getDefaultColumnsAsString(HasFilePaths paths, Supplier<File> file, Supplier<String> table, CharSequence delimiter) throws Exception {
        return onColumns(paths, file, table).getValues("").stream().map(o -> ((Column) o).getName()).collect(Collectors.joining(delimiter));
    }

    private Database open(File db) throws IOException {
        return new DatabaseBuilder(db).setReadOnly(true).open();
    }

    private boolean canLoadTables(Supplier<File> file) {
        return file.get() != null && !file.get().getPath().isEmpty();
    }

    private List<String> loadTables(HasFilePaths paths, Supplier<File> file) throws IOException {
        try (Database o = open(paths.resolveFilePath(file.get()))) {
            return new ArrayList(o.getTableNames());
        }
    }

    private List<String> filterAndSortTables(List<String> values, String term) {
        return values.stream()
                .filter(ExtAutoCompletionSource.basicFilter(term)::test)
                .sorted()
                .collect(Collectors.toList());
    }

    private String getTableCacheKey(Supplier<File> file) {
        return file.get().getPath();
    }

    private boolean canLoadColumns(Supplier<File> file, Supplier<String> table) {
        return canLoadTables(file) && !Strings.isNullOrEmpty(table.get());
    }

    private List<Column> loadColumns(HasFilePaths paths, Supplier<File> file, Supplier<String> table) throws IOException {
        try (Database o = open(paths.resolveFilePath(file.get()))) {
            return new ArrayList<>(o.getTable(table.get()).getColumns());
        }
    }

    private List<Column> filterAndSortColumns(List<Column> values, String term) {
        Predicate<String> filter = ExtAutoCompletionSource.basicFilter(term);
        return values.stream()
                .filter(o -> filter.test(o.getName()) || filter.test(o.getType().name()) || filter.test(String.valueOf(o.getColumnIndex())))
                .sorted(Comparator.comparing(Column::getName))
                .collect(Collectors.toList());
    }

    private String getColumnCacheKey(Supplier<File> file, Supplier<String> table) {
        return getTableCacheKey(file) + "/ " + table.get();
    }
}
