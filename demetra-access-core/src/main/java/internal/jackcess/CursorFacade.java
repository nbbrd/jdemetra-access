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
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.RowId;
import com.healthmarketscience.jackcess.Table;
import java.io.IOException;
import java.util.Collection;
import java.util.SortedMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 * @author Philippe Charles
 */
public interface CursorFacade {

    boolean moveToNextRow() throws IOException;

    @Nullable
    Object getCurrentRowValue(@Nonnull Column column) throws IOException;

    @Nonnull
    RowId getRowId() throws IOException;

    void moveBefore(@Nonnull RowId rowId) throws IOException;

    @Nonnull
    default CursorFacade withFilter(@Nonnull SortedMap<Column, String> filter) {
        return filter.isEmpty() ? this : new CursorFacadeUtil.Filtering(this, filter);
    }

    @Nonnull
    static CursorFacade basic(@Nonnull Table table, @Nonnull Collection<String> columnNames) throws IOException {
        return new CursorFacadeUtil.BasicCursor(CursorBuilder.createCursor(table), columnNames);
    }

    @Nonnull
    static CursorFacade range(@Nonnull Table table, @Nonnull Collection<String> columnNames, @Nonnull Range<RowId> range) throws IOException {
        CursorFacade basic = basic(table, columnNames);
        if (range.hasLowerBound()) {
            basic.moveBefore(range.lowerEndpoint());
        }
        return range.hasUpperBound() ? new CursorFacadeUtil.UpperBounded(basic, range.upperEndpoint()) : basic;
    }
}
