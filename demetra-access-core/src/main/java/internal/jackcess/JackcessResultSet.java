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

import com.google.common.collect.Range;
import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.RowId;
import ec.tstoolkit.utilities.CheckedIterator;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 * @author Philippe Charles
 */
public final class JackcessResultSet implements Closeable {

    private final List<Column> columns;
    private final int[] indexes;
    private final CheckedIterator<Object[], IOException> rows;
    private Object[] currentRow = null;

    JackcessResultSet(@Nonnull List<Column> columns, @Nonnull int[] indexes, @Nonnull CheckedIterator<Object[], IOException> data) {
        this.columns = columns;
        this.indexes = indexes;
        this.rows = data;
    }

    public boolean next() throws IOException {
        boolean result = rows.hasNext();
        if (result) {
            currentRow = rows.next();
        }
        return result;
    }

    @Nullable
    public Object getValue(int index) throws IOException, IndexOutOfBoundsException {
        return currentRow[indexes[index]];
    }

    @Nonnull
    public Column getColumn(int index) throws IOException, IndexOutOfBoundsException {
        return columns.get(index);
    }

    @Nonnull
    public Range<RowId> getRange() throws IOException {
        RowId lower = (RowId) currentRow[currentRow.length - 2];
        RowId upper = (RowId) currentRow[currentRow.length - 1];
        return Range.closed(lower, upper);
    }

    @Override
    public void close() throws IOException {
    }
}
