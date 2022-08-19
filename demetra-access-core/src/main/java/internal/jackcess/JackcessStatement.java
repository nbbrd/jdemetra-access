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
import ec.tss.tsproviders.utils.IteratorWithIO;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;
import internal.xdb.DbRawDataUtil.SuperDataType;
import java.util.HashSet;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 *
 * @author Philippe Charles
 */
@lombok.extern.slf4j.Slf4j
public final class JackcessStatement implements Closeable {

    private final Database database;
    private final Range<RowId> range;

    public JackcessStatement(@NonNull Database database, @Nullable Range<RowId> range) {
        this.database = database;
        this.range = range != null ? range : Range.<RowId>all();
    }

    @NonNull
    public JackcessResultSet executeQuery(@NonNull DbBasicSelect query) throws IOException {
        Table input = database.getTable(query.getTableName());

        Function<String, Column> toColumn = input::getColumn;
        ToIntFunction<Column> toInternalIndex = Column::getColumnIndex;
        Function<Column, SuperDataType> toDataType = ToDataType.INSTANCE;

        List<Column> selectColumns = DbRawDataUtil.getColumns(toColumn, query.getSelectColumns());
        List<Column> orderColumns = DbRawDataUtil.getColumns(toColumn, query.getOrderColumns());
        SortedMap<Column, String> filter = DbRawDataUtil.getFilter(toInternalIndex, toColumn, query.getFilterItems());

        SortedSet<Column> dataColumns = DbRawDataUtil.mergeAndSort(toInternalIndex, selectColumns, orderColumns);
        ToIntFunction<Column> toIndex = DbRawDataUtil.ToIndex.of(toInternalIndex, dataColumns);

        log.debug("Query : '{}'", query);

        Stopwatch sw = Stopwatch.createStarted();
        IteratorWithIO<Object[]> rows = getRows(input, dataColumns, filter, toColumnNames(query));
        log.debug("Iterator done in {}ms", sw.stop().elapsed(TimeUnit.MILLISECONDS));

        if (query.isDistinct()) {
            sw.start();
            BiConsumer<Object[], Object[]> aggregator = new Aggregator(dataColumns.size() + 1);
            rows = DbRawDataUtil.distinct(rows, selectColumns, toIndex, toDataType, aggregator);
            log.debug("Distinct done in {}ms", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }

        if (DbRawDataUtil.isSortRequired(query.isDistinct(), selectColumns, orderColumns)) {
            sw.start();
            rows = DbRawDataUtil.sort(rows, orderColumns, toIndex, toDataType);
            log.debug("Sort done in {}ms", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }

        return new JackcessResultSet(selectColumns, DbRawDataUtil.createIndexes(selectColumns, toIndex), rows);
    }

    @Override
    public void close() throws IOException {
    }

    //<editor-fold defaultstate="collapsed" desc="Implementation details">
    private IteratorWithIO<Object[]> getRows(Table input, SortedSet<Column> dataColumns, SortedMap<Column, String> filter, Collection<String> columnNames) throws IOException {
        return new Adapter(
                CursorFacade.range(input, columnNames, range).withFilter(filter),
                dataColumns
        );
    }

    private static Collection<String> toColumnNames(DbBasicSelect query) {
        Collection<String> result = new HashSet<>();
        result.addAll(query.getSelectColumns());
        result.addAll(query.getOrderColumns());
        result.addAll(query.getFilterItems().keySet());
        return result;
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

    private static final class Adapter implements IteratorWithIO<Object[]> {

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

        @Override
        public void close() throws IOException {
        }
    }
    //</editor-fold>
}
