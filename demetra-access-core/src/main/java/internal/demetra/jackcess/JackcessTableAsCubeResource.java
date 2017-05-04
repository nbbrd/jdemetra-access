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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Range;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.RowId;
import ec.tss.tsproviders.cube.CubeId;
import ec.tss.tsproviders.cube.TableAsCubeAccessor;
import ec.tss.tsproviders.cube.TableAsCubeUtil;
import ec.tss.tsproviders.cube.TableDataParams;
import ec.tss.tsproviders.utils.ObsCharacteristics;
import ec.tss.tsproviders.utils.ObsGathering;
import ec.tss.tsproviders.utils.OptionalTsData;
import static ec.tss.tsproviders.utils.StrangeParsers.yearFreqPosParser;
import ec.tstoolkit.design.VisibleForTesting;
import ec.tstoolkit.utilities.LastModifiedFileCache;
import static internal.demetra.jackcess.JackcessFunc.onDate;
import static internal.demetra.jackcess.JackcessFunc.onGetObjectToString;
import static internal.demetra.jackcess.JackcessFunc.onGetStringArray;
import static internal.demetra.jackcess.JackcessFunc.onNull;
import static internal.demetra.jackcess.JackcessFunc.onNumber;
import internal.jackcess.JackcessResultSet;
import internal.jackcess.JackcessStatement;
import internal.xdb.DbBasicSelect;
import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 * @author Philippe Charles
 */
public final class JackcessTableAsCubeResource implements TableAsCubeAccessor.Resource<java.util.Date> {

    @Nonnull
    public static JackcessTableAsCubeResource create(
            @Nonnull File db,
            @Nonnull String table,
            @Nonnull List<String> dimColumns,
            @Nonnull TableDataParams tdp,
            @Nonnull ObsGathering gathering,
            @Nonnull String labelColumn) {
        return new JackcessTableAsCubeResource(db, table, CubeId.root(dimColumns), tdp, gathering, labelColumn);
    }

    private final File db;
    private final String table;
    private final CubeId root;
    private final TableDataParams tdp;
    private final ObsGathering gathering;
    private final String labelColumn;
    @VisibleForTesting
    final Cache<CubeId, Range<RowId>> rangeIndex;

    private JackcessTableAsCubeResource(File db, String table, CubeId root, TableDataParams tdp, ObsGathering gathering, String labelColumn) {
        this.db = db;
        this.table = table;
        this.root = root;
        this.tdp = tdp;
        this.gathering = gathering;
        this.labelColumn = labelColumn;
        this.rangeIndex = LastModifiedFileCache.from(db, CacheBuilder.newBuilder().<CubeId, Range<RowId>>build());
    }

    @Override
    public Exception testConnection() {
        return null;
    }

    @Override
    public CubeId getRoot() {
        return root;
    }

    @Override
    public TableAsCubeAccessor.AllSeriesCursor getAllSeriesCursor(CubeId id) throws Exception {
        return new AllSeriesQuery(id, table, labelColumn, rangeIndex).call(db, rangeIndex.getIfPresent(id));
    }

    @Override
    public TableAsCubeAccessor.AllSeriesWithDataCursor<Date> getAllSeriesWithDataCursor(CubeId id) throws Exception {
        return new AllSeriesWithDataQuery(id, table, labelColumn, tdp).call(db, rangeIndex.getIfPresent(id));
    }

    @Override
    public TableAsCubeAccessor.SeriesWithDataCursor<Date> getSeriesWithDataCursor(CubeId id) throws Exception {
        return new SeriesWithDataQuery(id, table, labelColumn, tdp).call(db, rangeIndex.getIfPresent(id));
    }

    @Override
    public TableAsCubeAccessor.ChildrenCursor getChildrenCursor(CubeId id) throws Exception {
        return new ChildrenQuery(id, table, rangeIndex).call(db, rangeIndex.getIfPresent(id));
    }

    @Override
    public String getDisplayName() throws Exception {
        return TableAsCubeUtil.getDisplayName(db.getPath(), table, tdp.getValueColumn(), gathering);
    }

    @Override
    public String getDisplayName(CubeId id) throws Exception {
        return TableAsCubeUtil.getDisplayName(id, JackcessTableAsCubeUtil.LABEL_COLLECTOR);
    }

    @Override
    public String getDisplayNodeName(CubeId id) throws Exception {
        return TableAsCubeUtil.getDisplayNodeName(id);
    }

    @Override
    public OptionalTsData.Builder2<Date> newBuilder() {
        return OptionalTsData.builderByDate(new GregorianCalendar(), gathering, ObsCharacteristics.ORDERED);
    }

    private static void closeAll(Exception root, AutoCloseable... items) {
        for (AutoCloseable o : items) {
            if (o != null) {
                try {
                    o.close();
                } catch (Exception ex) {
                    if (root == null) {
                        root = ex;
                    } else {
                        root.addSuppressed(ex);
                    }
                }
            }
        }
    }

    private static AutoCloseable asCloseable(JackcessResultSet rs, JackcessStatement stmt, Database conn) {
        return () -> closeAll(null, rs, stmt, conn);
    }

    private static String[] toSelect(CubeId ref) {
        String[] result = new String[ref.getDepth()];
        for (int i = 0; i < result.length; i++) {
            result[i] = ref.getDimensionId(ref.getLevel() + i);
        }
        return result;
    }

    private static Map<String, String> toFilter(CubeId id) {
        Map<String, String> result = new HashMap<>();
        for (int i = 0; i < id.getLevel(); i++) {
            result.put(id.getDimensionId(i), id.getDimensionValue(i));
        }
        return result;
    }

    @VisibleForTesting
    interface JackcessQuery<T> {

        @Nonnull
        DbBasicSelect getQuery();

        @Nullable
        T process(@Nonnull JackcessResultSet rs, @Nonnull AutoCloseable closeable) throws IOException;

        @Nullable
        default public T call(@Nonnull File db, @Nullable Range<RowId> range) throws IOException {
            Database conn = null;
            JackcessStatement stmt = null;
            JackcessResultSet rs = null;
            try {
                conn = new DatabaseBuilder(db).setReadOnly(true).open();
                stmt = new JackcessStatement(conn, range);
                DbBasicSelect query = getQuery();
                rs = stmt.executeQuery(query);
                return process(rs, asCloseable(rs, stmt, conn));
            } catch (IOException ex) {
                closeAll(ex, rs, stmt, conn);
                throw ex;
            }
        }
    }

    private static final class AllSeriesQuery implements JackcessQuery<TableAsCubeAccessor.AllSeriesCursor> {

        private final CubeId ref;
        private final String table;
        private final String label;
        private final Cache<CubeId, Range<RowId>> rangeIndex;

        AllSeriesQuery(CubeId id, String table, String label, Cache<CubeId, Range<RowId>> rangeIndex) {
            this.ref = id;
            this.table = table;
            this.label = label;
            this.rangeIndex = rangeIndex;
        }

        @Override
        public DbBasicSelect getQuery() {
            return DbBasicSelect.from(table)
                    .distinct(true)
                    .select(toSelect(ref)).select(label)
                    .filter(toFilter(ref))
                    .orderBy(toSelect(ref))
                    .build();
        }

        @Override
        public TableAsCubeAccessor.AllSeriesCursor process(JackcessResultSet rs, AutoCloseable closeable) throws IOException {
            JackcessFunc<String[]> toDimValues = onGetStringArray(0, ref.getDepth());
            JackcessFunc<String> toLabel = !label.isEmpty() ? onGetObjectToString(1) : onNull();

            return JackcessTableAsCubeUtil.allSeriesCursor(rs, closeable, toDimValues, toLabel, ref, rangeIndex);
        }
    }

    private static final class AllSeriesWithDataQuery implements JackcessQuery<TableAsCubeAccessor.AllSeriesWithDataCursor<Date>> {

        private final CubeId ref;
        private final String table;
        private final String label;
        private final TableDataParams tdp;

        AllSeriesWithDataQuery(CubeId id, String table, String label, TableDataParams tdp) {
            this.ref = id;
            this.table = table;
            this.label = label;
            this.tdp = tdp;
        }

        @Override
        public DbBasicSelect getQuery() {
            return DbBasicSelect.from(table)
                    .select(toSelect(ref)).select(tdp.getPeriodColumn(), tdp.getValueColumn()).select(label)
                    .filter(toFilter(ref))
                    .orderBy(toSelect(ref)).orderBy(tdp.getPeriodColumn(), tdp.getVersionColumn())
                    .build();
        }

        @Override
        public TableAsCubeAccessor.AllSeriesWithDataCursor<Date> process(JackcessResultSet rs, AutoCloseable closeable) throws IOException {
            JackcessFunc<String[]> toDimValues = onGetStringArray(0, ref.getDepth());
            JackcessFunc<java.util.Date> toPeriod = onDate(rs, ref.getDepth(), tdp.getObsFormat().dateParser().orElse(yearFreqPosParser()));
            JackcessFunc<Number> toValue = onNumber(rs, ref.getDepth() + 1, tdp.getObsFormat().numberParser());
            JackcessFunc<String> toLabel = !label.isEmpty() ? onGetObjectToString(ref.getDepth() + 2) : onNull();

            return JackcessTableAsCubeUtil.allSeriesWithDataCursor(rs, closeable, toDimValues, toPeriod, toValue, toLabel, ref);
        }
    }

    private static final class SeriesWithDataQuery implements JackcessQuery<TableAsCubeAccessor.SeriesWithDataCursor<Date>> {

        private final CubeId ref;
        private final String table;
        private final String label;
        private final TableDataParams tdp;

        SeriesWithDataQuery(CubeId id, String table, String label, TableDataParams tdp) {
            this.ref = id;
            this.table = table;
            this.label = label;
            this.tdp = tdp;
        }

        @Override
        public DbBasicSelect getQuery() {
            return DbBasicSelect.from(table)
                    .select(tdp.getPeriodColumn(), tdp.getValueColumn()).select(label)
                    .filter(toFilter(ref))
                    .orderBy(tdp.getPeriodColumn(), tdp.getVersionColumn())
                    .build();
        }

        @Override
        public TableAsCubeAccessor.SeriesWithDataCursor<Date> process(JackcessResultSet rs, AutoCloseable closeable) throws IOException {
            JackcessFunc<Date> toPeriod = onDate(rs, 0, tdp.getObsFormat().dateParser().orElse(yearFreqPosParser()));
            JackcessFunc<Number> toValue = onNumber(rs, 1, tdp.getObsFormat().numberParser());
            JackcessFunc<String> toLabel = !label.isEmpty() ? onGetObjectToString(2) : onNull();

            return JackcessTableAsCubeUtil.seriesWithDataCursor(rs, closeable, toPeriod, toValue, toLabel, ref);
        }
    }

    private static final class ChildrenQuery implements JackcessQuery<TableAsCubeAccessor.ChildrenCursor> {

        private final CubeId ref;
        private final String table;
        private final Cache<CubeId, Range<RowId>> rangeIndex;

        ChildrenQuery(CubeId id, String table, Cache<CubeId, Range<RowId>> rangeIndex) {
            this.ref = id;
            this.table = table;
            this.rangeIndex = rangeIndex;
        }

        @Override
        public DbBasicSelect getQuery() {
            String column = ref.getDimensionId(ref.getLevel());
            return DbBasicSelect.from(table)
                    .distinct(true)
                    .select(column)
                    .filter(toFilter(ref))
                    .orderBy(column)
                    .build();
        }

        @Override
        public TableAsCubeAccessor.ChildrenCursor process(JackcessResultSet rs, AutoCloseable closeable) throws IOException {
            JackcessFunc<String> toChild = onGetObjectToString(0);

            return JackcessTableAsCubeUtil.childrenCursor(rs, closeable, toChild, ref, rangeIndex);
        }
    }
}
