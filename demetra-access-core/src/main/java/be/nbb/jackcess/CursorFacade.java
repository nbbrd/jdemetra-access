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
package be.nbb.jackcess;

import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.RowId;
import com.healthmarketscience.jackcess.Table;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.SortedMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 * @author Philippe Charles
 */
public abstract class CursorFacade {

    abstract public boolean moveToNextRow() throws IOException;

    @Nullable
    abstract public Object getCurrentRowValue(@Nonnull Column column) throws IOException;

    @Nonnull
    abstract public RowId getRowId() throws IOException;

    abstract public void moveBefore(@Nonnull RowId rowId) throws IOException;

    @Nonnull
    public CursorFacade withFilter(@Nonnull SortedMap<Column, String> filter) {
        return filter.isEmpty() ? this : new Filtering(this, filter);
    }

    @Nonnull
    public static CursorFacade basic(@Nonnull Table table, @Nonnull Collection<String> columnNames) throws IOException {
        return new BasicCursor(CursorBuilder.createCursor(table), columnNames);
    }

    @Nonnull
    public static CursorFacade range(@Nonnull Table table, @Nonnull Collection<String> columnNames, @Nonnull Range<RowId> range) throws IOException {
        CursorFacade basic = basic(table, columnNames);
        if (range.hasLowerBound()) {
            basic.moveBefore(range.lowerEndpoint());
        }
        return range.hasUpperBound() ? new UpperBounded(basic, range.upperEndpoint()) : basic;
    }

    //<editor-fold defaultstate="collapsed" desc="Implementation details">
    private static abstract class ForwardingCursor extends CursorFacade {

        @Nonnull
        protected abstract CursorFacade getDelegate();

        @Override
        public boolean moveToNextRow() throws IOException {
            return getDelegate().moveToNextRow();
        }

        @Override
        public Object getCurrentRowValue(Column column) throws IOException {
            return getDelegate().getCurrentRowValue(column);
        }

        @Override
        public RowId getRowId() throws IOException {
            return getDelegate().getRowId();
        }

        @Override
        public void moveBefore(RowId rowId) throws IOException {
            getDelegate().moveBefore(rowId);
        }
    }

    private static final class Filtering extends ForwardingCursor {

        private final CursorFacade delegate;
        private final Map.Entry<Column, String>[] filter;

        public Filtering(CursorFacade delegate, SortedMap<Column, String> filter) {
            this.filter = Iterables.toArray(filter.entrySet(), Map.Entry.class);
            this.delegate = delegate;
        }

        @Override
        protected CursorFacade getDelegate() {
            return delegate;
        }

        @Override
        public boolean moveToNextRow() throws IOException {
            while (super.moveToNextRow()) {
                if (currentRowMatches()) {
                    return true;
                }
            }
            return false;
        }

        private boolean currentRowMatches() throws IOException {
            for (Map.Entry<Column, String> o : filter) {
                if (!currentRowValueMatches(getCurrentRowValue(o.getKey()), o.getValue())) {
                    return false;
                }
            }
            return true;
        }

        private static boolean currentRowValueMatches(Object current, String expected) {
            return (current == expected) || (current != null && current.toString().equals(expected));
        }
    }

    private static final class BasicCursor extends CursorFacade {

        private final Cursor internalCursor;
        private final Collection<String> columnNames;
        private Row currentRow;

        private BasicCursor(Cursor cursor, Collection<String> columnNames) {
            this.internalCursor = cursor;
            this.columnNames = columnNames;
            this.currentRow = null;
        }

        @Override
        public boolean moveToNextRow() throws IOException {
            return (currentRow = internalCursor.getNextRow(columnNames)) != null;
        }

        @Override
        public Object getCurrentRowValue(Column column) throws IOException {
            return currentRow.get(column.getName());
        }

        @Override
        public RowId getRowId() throws IOException {
            return currentRow.getId();
        }

        @Override
        public void moveBefore(@Nonnull RowId rowId) throws IOException {
            internalCursor.findRow(rowId);
            internalCursor.moveToPreviousRow();
        }
    }

    private static final class UpperBounded extends ForwardingCursor {

        private final CursorFacade delegate;
        private final RowId upper;

        public UpperBounded(CursorFacade delegate, RowId upper) throws IOException {
            this.delegate = delegate;
            this.upper = upper;
        }

        @Override
        protected CursorFacade getDelegate() {
            return delegate;
        }

        @Override
        public boolean moveToNextRow() throws IOException {
            return delegate.moveToNextRow() && delegate.getRowId().compareTo(upper) <= 0;
        }
    }
    //</editor-fold>
}
