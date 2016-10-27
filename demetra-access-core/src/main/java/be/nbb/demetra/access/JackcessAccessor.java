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
package be.nbb.demetra.access;

import static be.nbb.demetra.access.JackcessFunc.*;
import be.nbb.jackcess.JackcessResultSet;
import be.nbb.jackcess.JackcessStatement;
import be.nbb.xdb.DbBasicSelect;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Range;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.RowId;
import ec.tss.tsproviders.db.DbAccessor;
import ec.tss.tsproviders.db.DbSeries;
import ec.tss.tsproviders.db.DbSetId;
import ec.tss.tsproviders.db.DbUtil;
import ec.tstoolkit.design.VisibleForTesting;
import ec.tstoolkit.utilities.LastModifiedFileCache;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Philippe Charles
 */
final class JackcessAccessor extends DbAccessor.Commander<JackcessBean> {

    private static final Logger LOGGER = LoggerFactory.getLogger(JackcessAccessor.class);

    @VisibleForTesting
    final Cache<DbSetId, Range<RowId>> rangeIndex;

    public JackcessAccessor(JackcessBean dbBean) {
        super(dbBean);
        this.rangeIndex = LastModifiedFileCache.from(dbBean.getFile(), CacheBuilder.newBuilder().<DbSetId, Range<RowId>>build());
    }

    @Override
    protected Callable<List<DbSetId>> getAllSeriesQuery(DbSetId ref) {
        return new JackcessQuery<List<DbSetId>>(ref) {
            @Override
            protected DbBasicSelect getQuery() {
                return DbBasicSelect
                        .from(dbBean.getTableName())
                        .distinct(true)
                        .select(ref.selectColumns())
                        .filter(toFilter(ref))
                        .orderBy(ref.selectColumns())
                        .build();
            }

            @Override
            protected List<DbSetId> process(final JackcessResultSet rs) throws IOException {
                final JackcessFunc<String[]> toDimValues = onGetStringArray(0, ref.getDepth());

                DbUtil.AllSeriesCursor<IOException> cursor = new DbUtil.AllSeriesCursor<IOException>() {
                    @Override
                    public boolean next() throws IOException {
                        boolean result = rs.next();
                        if (result) {
                            dimValues = toDimValues.apply(rs);
                            rangeIndex.put(ref.child(dimValues), rs.getRange());
                        }
                        return result;
                    }
                };

                return DbUtil.getAllSeries(cursor, ref);
            }
        };
    }

    @Override
    protected Callable<List<DbSeries>> getAllSeriesWithDataQuery(DbSetId ref) {
        return new JackcessQuery<List<DbSeries>>(ref) {
            @Override
            protected DbBasicSelect getQuery() {
                DbBasicSelect.Builder result = DbBasicSelect
                        .from(dbBean.getTableName())
                        .select(ref.selectColumns()).select(dbBean.getPeriodColumn(), dbBean.getValueColumn())
                        .filter(toFilter(ref))
                        .orderBy(ref.selectColumns());
                if (!dbBean.getVersionColumn().isEmpty()) {
                    result.orderBy(dbBean.getPeriodColumn(), dbBean.getVersionColumn());
                }
                return result.build();
            }

            @Override
            protected List<DbSeries> process(final JackcessResultSet rs) throws IOException {
                final JackcessFunc<String[]> toDimValues = onGetStringArray(0, ref.getDepth());
                final JackcessFunc<java.util.Date> toPeriod = onDate(rs, ref.getDepth(), dateParser);
                final JackcessFunc<Number> toValue = onNumber(rs, ref.getDepth() + 1, numberParser);

                DbUtil.AllSeriesWithDataCursor<IOException> cursor = new DbUtil.AllSeriesWithDataCursor<IOException>() {
                    @Override
                    public boolean next() throws IOException {
                        boolean result = rs.next();
                        if (result) {
                            dimValues = toDimValues.apply(rs);
                            period = toPeriod.apply(rs);
                            value = period != null ? toValue.apply(rs) : null;
                        }
                        return result;
                    }
                };

                return DbUtil.getAllSeriesWithData(cursor, ref, dbBean.getFrequency(), dbBean.getAggregationType());
            }
        };
    }

    @Override
    protected Callable<DbSeries> getSeriesWithDataQuery(DbSetId ref) {
        return new JackcessQuery<DbSeries>(ref) {
            @Override
            protected DbBasicSelect getQuery() {
                DbBasicSelect.Builder result = DbBasicSelect.from(dbBean.getTableName())
                        .select(dbBean.getPeriodColumn(), dbBean.getValueColumn())
                        .filter(toFilter(ref));
                if (!dbBean.getVersionColumn().isEmpty()) {
                    result.orderBy(dbBean.getPeriodColumn(), dbBean.getVersionColumn());
                }
                return result.build();
            }

            @Override
            protected DbSeries process(final JackcessResultSet rs) throws IOException {
                final JackcessFunc<Date> toPeriod = onDate(rs, 0, dateParser);
                final JackcessFunc<Number> toValue = onNumber(rs, 1, numberParser);

                DbUtil.SeriesWithDataCursor<IOException> cursor = new DbUtil.SeriesWithDataCursor<IOException>() {
                    int index = 0;

                    @Override
                    public boolean next() throws IOException {
                        boolean result = rs.next();
                        if (result) {
                            period = toPeriod.apply(rs);
                            value = period != null ? toValue.apply(rs) : null;
                            return true;
                        }
                        return false;
                    }
                };

                return DbUtil.getSeriesWithData(cursor, ref, dbBean.getFrequency(), dbBean.getAggregationType());
            }
        };
    }

    @Override
    protected Callable<List<String>> getChildrenQuery(DbSetId ref) {
        return new JackcessQuery<List<String>>(ref) {
            @Override
            protected DbBasicSelect getQuery() {
                String column = ref.getColumn(ref.getLevel());
                return DbBasicSelect
                        .from(dbBean.getTableName())
                        .distinct(true)
                        .select(column)
                        .filter(toFilter(ref))
                        .orderBy(column)
                        .build();
            }

            @Override
            protected List<String> process(final JackcessResultSet rs) throws IOException {
                final JackcessFunc<String> toChild = onGetObjectToString(0);

                DbUtil.ChildrenCursor<IOException> cursor = new DbUtil.ChildrenCursor<IOException>() {
                    int index = 0;

                    @Override
                    public boolean next() throws IOException {
                        boolean result = rs.next();
                        if (result) {
                            child = toChild.apply(rs);
                            rangeIndex.put(ref.child(child), rs.getRange());
                            return true;
                        }
                        return false;
                    }
                };

                return DbUtil.getChildren(cursor);
            }
        };
    }

    @Override
    public DbAccessor<JackcessBean> memoize() {
        Cache<DbSetId, List<DbSeries>> ttl = DbAccessor.BulkAccessor.newTtlCache(dbBean.getCacheTtl());
        Cache<DbSetId, List<DbSeries>> file = LastModifiedFileCache.from(dbBean.getFile(), ttl);
        return DbAccessor.BulkAccessor.from(this, dbBean.getCacheDepth(), file);
    }

    private abstract class JackcessQuery<T> implements Callable<T> {

        protected final DbSetId ref;

        protected JackcessQuery(DbSetId ref) {
            this.ref = ref;
        }

        abstract protected DbBasicSelect getQuery();

        abstract protected T process(JackcessResultSet rs) throws IOException;

        @Override
        public T call() throws IOException {
            DbBasicSelect query = getQuery();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(query.toSql());
            }
            try (Database conn = new DatabaseBuilder(dbBean.getFile()).setReadOnly(true).open()) {
                try (JackcessStatement stmt = new JackcessStatement(conn, rangeIndex.getIfPresent(ref))) {
                    try (JackcessResultSet rs = stmt.executeQuery(query)) {
                        return process(rs);
                    }
                }
            }
        }
    }

    private static Map<String, String> toFilter(DbSetId id) {
        Map<String, String> result = new HashMap<>();
        for (int i = 0; i < id.getLevel(); i++) {
            result.put(id.getColumn(i), id.getValue(i));
        }
        return result;
    }
}
