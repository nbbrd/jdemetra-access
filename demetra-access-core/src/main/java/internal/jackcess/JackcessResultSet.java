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
import ec.tss.tsproviders.utils.IteratorWithIO;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 *
 * @author Philippe Charles
 */
public final class JackcessResultSet implements Closeable {

    private final List<Column> columns;
    private final int[] indexes;
    private final IteratorWithIO<Object[]> rows;
    private Object[] currentRow = null;

    JackcessResultSet(@NonNull List<Column> columns, @NonNull int[] indexes, @NonNull IteratorWithIO<Object[]> data) {
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

    @NonNull
    public Column getColumn(int index) throws IOException, IndexOutOfBoundsException {
        return columns.get(index);
    }

    @NonNull
    public Range<RowId> getRange() throws IOException {
        RowId lower = (RowId) currentRow[currentRow.length - 2];
        RowId upper = (RowId) currentRow[currentRow.length - 1];
        return Range.closed(lower, upper);
    }

    @Override
    public void close() throws IOException {
        rows.close();
    }
}
