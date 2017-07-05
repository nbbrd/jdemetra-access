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

import com.google.common.collect.Range;
import com.healthmarketscience.jackcess.RowId;
import ec.tss.tsproviders.cube.CubeId;
import ec.tss.tsproviders.cube.TableAsCubeAccessor.AllSeriesCursor;
import ec.tss.tsproviders.cube.TableAsCubeAccessor.AllSeriesWithDataCursor;
import ec.tss.tsproviders.cube.TableAsCubeAccessor.ChildrenCursor;
import ec.tss.tsproviders.cube.TableAsCubeAccessor.SeriesCursor;
import ec.tss.tsproviders.cube.TableAsCubeAccessor.SeriesWithDataCursor;
import ec.tss.tsproviders.cube.TableAsCubeAccessor.TableCursor;
import internal.jackcess.JackcessResultSet;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * @author Philippe Charles
 */
@lombok.experimental.UtilityClass
class JackcessTableAsCubeUtil {

    final Collector<? super String, ?, String> LABEL_COLLECTOR = Collectors.joining(", ");

    AllSeriesCursor allSeriesCursor(JackcessResultSet rs, AutoCloseable closeable, JackcessFunc<String[]> toDimValues, JackcessFunc<String> toLabel, CubeId ref, Map<CubeId, Range<RowId>> rangeIndex) {
        return new ResultSetAllSeriesCursor(rs, closeable, toDimValues, toLabel, ref, rangeIndex);
    }

    AllSeriesWithDataCursor<Date> allSeriesWithDataCursor(JackcessResultSet rs, AutoCloseable closeable, JackcessFunc<String[]> toDimValues, JackcessFunc<Date> toPeriod, JackcessFunc<Number> toValue, JackcessFunc<String> toLabel, CubeId ref) {
        return new ResultSetAllSeriesWithDataCursor(rs, closeable, toDimValues, toPeriod, toValue, toLabel, ref);
    }

    SeriesWithDataCursor<Date> seriesWithDataCursor(JackcessResultSet rs, AutoCloseable closeable, JackcessFunc<Date> toPeriod, JackcessFunc<Number> toValue, JackcessFunc<String> toLabel, CubeId ref) {
        return new ResultSetSeriesWithDataCursor(rs, closeable, toPeriod, toValue, toLabel, ref);
    }

    ChildrenCursor childrenCursor(JackcessResultSet rs, AutoCloseable closeable, JackcessFunc<String> toChild, CubeId ref, Map<CubeId, Range<RowId>> rangeIndex) {
        return new ResultSetChildrenCursor(rs, closeable, toChild, ref, rangeIndex);
    }

    //<editor-fold defaultstate="collapsed" desc="Implementation details">
    private static abstract class ResultSetTableCursor implements TableCursor {

        private final JackcessResultSet rs;
        private final AutoCloseable closeable;
        private boolean closed;

        private ResultSetTableCursor(JackcessResultSet rs, AutoCloseable closeable) {
            this.rs = rs;
            this.closeable = closeable;
            this.closed = false;
        }

        protected abstract void processRow(JackcessResultSet rs) throws IOException;

        @Override
        public boolean isClosed() throws Exception {
            return closed;
        }

        @Override
        public boolean nextRow() throws Exception {
            boolean result = rs.next();
            if (result) {
                processRow(rs);
            }
            return result;
        }

        @Override
        public void close() throws Exception {
            closed = true;
            closeable.close();
        }
    }

    private static abstract class ResultSetSeriesCursor extends ResultSetTableCursor implements SeriesCursor {

        private ResultSetSeriesCursor(JackcessResultSet rs, AutoCloseable closeable) {
            super(rs, closeable);
        }

        @Override
        public Map<String, String> getMetaData() throws Exception {
            return Collections.emptyMap();
        }
    }

    private static final class ResultSetAllSeriesCursor extends ResultSetSeriesCursor implements AllSeriesCursor {

        private final JackcessFunc<String[]> toDimValues;
        private final JackcessFunc<String> toLabel;
        private final CubeId ref;
        private final Map<CubeId, Range<RowId>> rangeIndex;
        private String[] dimValues;
        private String label;

        private ResultSetAllSeriesCursor(JackcessResultSet rs, AutoCloseable closeable, JackcessFunc<String[]> toDimValues, JackcessFunc<String> toLabel, CubeId ref, Map<CubeId, Range<RowId>> rangeIndex) {
            super(rs, closeable);
            this.toDimValues = toDimValues;
            this.toLabel = toLabel;
            this.ref = ref;
            this.rangeIndex = rangeIndex;
            this.dimValues = null;
            this.label = null;
        }

        @Override
        public String getLabel() throws Exception {
            return label != null ? label : Stream.concat(ref.getDimensionValueStream(), Stream.of(dimValues)).collect(LABEL_COLLECTOR);
        }

        @Override
        public String[] getDimValues() throws Exception {
            return dimValues;
        }

        @Override
        protected void processRow(JackcessResultSet rs) throws IOException {
            dimValues = toDimValues.apply(rs);
            label = toLabel.apply(rs);
            rangeIndex.put(ref.child(dimValues), rs.getRange());
        }
    }

    private static final class ResultSetAllSeriesWithDataCursor extends ResultSetSeriesCursor implements AllSeriesWithDataCursor<Date> {

        private final JackcessFunc<String[]> toDimValues;
        private final JackcessFunc<Date> toPeriod;
        private final JackcessFunc<Number> toValue;
        private final JackcessFunc<String> toLabel;
        private final CubeId ref;
        private String[] dimValues;
        private java.util.Date period;
        private Number value;
        private String label;

        private ResultSetAllSeriesWithDataCursor(JackcessResultSet rs, AutoCloseable closeable, JackcessFunc<String[]> toDimValues, JackcessFunc<Date> toPeriod, JackcessFunc<Number> toValue, JackcessFunc<String> toLabel, CubeId ref) {
            super(rs, closeable);
            this.toDimValues = toDimValues;
            this.toPeriod = toPeriod;
            this.toValue = toValue;
            this.toLabel = toLabel;
            this.ref = ref;
            this.dimValues = null;
            this.period = null;
            this.value = null;
            this.label = null;
        }

        @Override
        public String getLabel() throws Exception {
            return label != null ? label : Stream.concat(ref.getDimensionValueStream(), Stream.of(dimValues)).collect(LABEL_COLLECTOR);
        }

        @Override
        public String[] getDimValues() throws Exception {
            return dimValues;
        }

        @Override
        public java.util.Date getPeriod() throws Exception {
            return period;
        }

        @Override
        public Number getValue() throws Exception {
            return value;
        }

        @Override
        protected void processRow(JackcessResultSet rs) throws IOException {
            dimValues = toDimValues.apply(rs);
            period = toPeriod.apply(rs);
            value = period != null ? toValue.apply(rs) : null;
            label = toLabel.apply(rs);
        }
    }

    private static final class ResultSetSeriesWithDataCursor extends ResultSetSeriesCursor implements SeriesWithDataCursor<Date> {

        private final JackcessFunc<Date> toPeriod;
        private final JackcessFunc<Number> toValue;
        private final JackcessFunc<String> toLabel;
        private final CubeId ref;
        private java.util.Date period;
        private Number value;
        private String label;

        private ResultSetSeriesWithDataCursor(JackcessResultSet rs, AutoCloseable closeable, JackcessFunc<Date> toPeriod, JackcessFunc<Number> toValue, JackcessFunc<String> toLabel, CubeId ref) {
            super(rs, closeable);
            this.toPeriod = toPeriod;
            this.toValue = toValue;
            this.toLabel = toLabel;
            this.ref = ref;
            this.period = null;
            this.value = null;
            this.label = null;
        }

        @Override
        public String getLabel() throws Exception {
            return label != null ? label : ref.getDimensionValueStream().collect(LABEL_COLLECTOR);
        }

        @Override
        public java.util.Date getPeriod() throws Exception {
            return period;
        }

        @Override
        public Number getValue() throws Exception {
            return value;
        }

        @Override
        protected void processRow(JackcessResultSet rs) throws IOException {
            period = toPeriod.apply(rs);
            value = period != null ? toValue.apply(rs) : null;
            label = toLabel.apply(rs);
        }
    }

    private static final class ResultSetChildrenCursor extends ResultSetTableCursor implements ChildrenCursor {

        private final JackcessFunc<String> toChild;
        private final CubeId ref;
        private final Map<CubeId, Range<RowId>> rangeIndex;
        private String child;

        private ResultSetChildrenCursor(JackcessResultSet rs, AutoCloseable closeable, JackcessFunc<String> toChild, CubeId ref, Map<CubeId, Range<RowId>> rangeIndex) {
            super(rs, closeable);
            this.toChild = toChild;
            this.ref = ref;
            this.rangeIndex = rangeIndex;
            this.child = null;
        }

        @Override
        public String getChild() throws Exception {
            return child;
        }

        @Override
        protected void processRow(JackcessResultSet rs) throws IOException {
            child = toChild.apply(rs);
            rangeIndex.put(ref.child(child), rs.getRange());
        }
    }
    //</editor-fold>
}
