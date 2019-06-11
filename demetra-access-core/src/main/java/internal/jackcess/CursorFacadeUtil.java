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
package internal.jackcess;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.RowId;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.SortedMap;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 *
 * @author Philippe Charles
 */
@lombok.experimental.UtilityClass
class CursorFacadeUtil {

    private static abstract class ForwardingCursor implements CursorFacade {

        @NonNull
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

    static final class Filtering extends ForwardingCursor {

        private final CursorFacade delegate;
        private final Map.Entry<Column, String>[] filter;

        Filtering(CursorFacade delegate, SortedMap<Column, String> filter) {
            this.filter = filter.entrySet().toArray(new Map.Entry[filter.size()]);
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

    static final class BasicCursor implements CursorFacade {

        private final Cursor internalCursor;
        private final Collection<String> columnNames;
        private Row currentRow;

        BasicCursor(Cursor cursor, Collection<String> columnNames) {
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
        public void moveBefore(@NonNull RowId rowId) throws IOException {
            internalCursor.findRow(rowId);
            internalCursor.moveToPreviousRow();
        }
    }

    static final class UpperBounded extends ForwardingCursor {

        private final CursorFacade delegate;
        private final RowId upper;

        UpperBounded(CursorFacade delegate, RowId upper) throws IOException {
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
}
