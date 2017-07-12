/*
 * Copyright 2013 National Bank of Belgium
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
package internal.jackcess;

import internal.xdb.DbBasicSelect;
import internal.xdb.DbRawDataUtil;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Range;
import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.RowId;
import com.healthmarketscience.jackcess.Table;
import ec.tstoolkit.utilities.CheckedIterator;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static internal.jackcess.JackcessColumnComparator.BY_COLUMN_INDEX;
import java.util.Collections;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

/**
 *
 * @author Philippe Charles
 */
public final class JackcessStatement implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(JackcessStatement.class);

    private final Database database;
    private final Range<RowId> range;

    public JackcessStatement(@Nonnull Database database, @Nullable Range<RowId> range) {
        this.database = database;
        this.range = range != null ? range : Range.<RowId>all();
    }

    @Nonnull
    public JackcessResultSet executeQuery(@Nonnull DbBasicSelect query) throws IOException {
        Table table = database.getTable(query.getTableName());

        List<Column> selectColumns = getAllByName(table, query.getSelectColumns());
        List<Column> orderColumns = getAllByName(table, query.getOrderColumns());
        SortedSet<Column> dataColumns = mergeAndSortByInternalIndex(selectColumns, orderColumns);
        SortedMap<Column, String> filter = getFilter(table, query.getFilterItems());

        LOGGER.debug("Query : '{}'", query);

        Stopwatch sw = Stopwatch.createStarted();
        CheckedIterator<Object[], IOException> rows = new Adapter(CursorFacade.range(table, toColumnNames(query), range).withFilter(filter), dataColumns);
        LOGGER.debug("Iterator done in {}ms", sw.stop().elapsed(TimeUnit.MILLISECONDS));

        ToIndex toIndex = new ToIndex(dataColumns);

        if (query.isDistinct()) {
            sw.start();
            rows = DbRawDataUtil.distinct(rows, selectColumns, toIndex, ToDataType.INSTANCE, new Aggregator(dataColumns.size() + 1));
            LOGGER.debug("Distinct done in {}ms", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }

        if (DbRawDataUtil.isSortRequired(query.isDistinct(), selectColumns, orderColumns)) {
            sw.start();
            rows = DbRawDataUtil.sort(rows, orderColumns, toIndex, ToDataType.INSTANCE);
            LOGGER.debug("Sort done in {}ms", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }

        return new JackcessResultSet(selectColumns, DbRawDataUtil.createIndexes(selectColumns, toIndex), rows);
    }

    @Override
    public void close() throws IOException {
    }

    //<editor-fold defaultstate="collapsed" desc="Implementation details">
    private static List<Column> getAllByName(Table table, Collection<String> columnNames) {
        return columnNames.stream().map(table::getColumn).collect(Collectors.toList());
    }

    private static SortedSet<Column> mergeAndSortByInternalIndex(Iterable<Column>... list) {
        SortedSet<Column> result = new TreeSet<>(BY_COLUMN_INDEX);
        for (Iterable<Column> o : list) {
            o.forEach(result::add);
        }
        return result;
    }

    private static Collection<String> toColumnNames(DbBasicSelect query) {
        Collection<String> result = new HashSet<>();
        result.addAll(query.getSelectColumns());
        result.addAll(query.getOrderColumns());
        result.addAll(query.getFilterItems().keySet());
        return result;
    }

    private static SortedMap<Column, String> getFilter(Table table, Map<String, String> filterItems) {
        SortedMap<Column, String> result = new TreeMap<>(BY_COLUMN_INDEX);
        filterItems.forEach((k, v) -> result.put(table.getColumn(k), v));
        return result;
    }

    private static final class ToIndex implements ToIntFunction<Column> {

        private final int[] index;

        public ToIndex(SortedSet<Column> dataColumns) {
            Column max = dataColumns.comparator().equals(BY_COLUMN_INDEX) ? dataColumns.last() : Collections.max(dataColumns, BY_COLUMN_INDEX);
            this.index = new int[max.getColumnIndex() + 1];
            int i = 0;
            for (Column o : dataColumns) {
                index[o.getColumnIndex()] = i++;
            }
        }

        @Override
        public int applyAsInt(Column value) {
            return index[value.getColumnIndex()];
        }
    }

    private static final class ToDataType implements Function<Column, DbRawDataUtil.SuperDataType> {

        private static final ToDataType INSTANCE = new ToDataType();

        @Override
        public DbRawDataUtil.SuperDataType apply(Column column) {
            switch (column.getType()) {
                case BYTE:
                case INT:
                case LONG:
                case DOUBLE:
                case BOOLEAN:
                case FLOAT:
                case SHORT_DATE_TIME:
                case TEXT:
                case MONEY:
                case MEMO:
                case NUMERIC:
                case GUID:
                    return DbRawDataUtil.SuperDataType.COMPARABLE;
                case OLE:
                case BINARY:
                case UNKNOWN_0D:
                case UNKNOWN_11:
                    return DbRawDataUtil.SuperDataType.BYTE_ARRAY;
                default:
                    return DbRawDataUtil.SuperDataType.OTHER;
            }
        }
    }

    private static final class Aggregator implements BiConsumer<Object[], Object[]> {

        private final int lastPosIdx;

        public Aggregator(int lastPosIdx) {
            this.lastPosIdx = lastPosIdx;
        }

        @Override
        public void accept(Object[] t, Object[] u) {
            t[lastPosIdx] = u[lastPosIdx];
        }
    }

    private static final class Adapter extends CheckedIterator<Object[], IOException> {

        private final CursorFacade cursor;
        private final Column[] dataColumns;

        public Adapter(CursorFacade cursor, SortedSet<Column> dataColumns) {
            this.cursor = cursor;
            this.dataColumns = dataColumns.toArray(new Column[dataColumns.size()]);
        }

        @Override
        public boolean hasNext() throws IOException {
            return cursor.moveToNextRow();
        }

        @Override
        public Object[] next() throws IOException {
            Object[] result = new Object[dataColumns.length + 2];
            for (int i = 0; i < result.length - 2; i++) {
                result[i] = cursor.getCurrentRowValue(dataColumns[i]);
            }
            result[dataColumns.length] = result[dataColumns.length + 1] = cursor.getRowId();
            return result;
        }
    }
    //</editor-fold>
}
