/*
 * Copyright 2016 National Bank of Belgium
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

import com.google.common.collect.ImmutableSortedMap;
import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.RowId;
import com.healthmarketscience.jackcess.Table;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static be.nbb.jackcess.JackcessColumnComparator.BY_COLUMN_INDEX;
import com.google.common.collect.Range;
import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.TableBuilder;
import java.util.ArrayList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

/**
 *
 * @author Philippe Charles
 */
public class CursorFacadeTest {

    private static File file;

    @BeforeClass
    public static void beforeClass() throws IOException {
        file = createResource();
    }

    @AfterClass
    public static void afterClass() {
        file.delete();
    }

    @Test
    public void testMoveToNextRow() throws IOException {
        try (Database db = open(file)) {
            Table table = db.getTable("MyTable");

            List<RowId> all = toRowIds(CursorFacade.basic(table));

            assertThat(all).isSorted().hasSize(10);

            assertThat(toRowIds(CursorFacade.range(table, Range.<RowId>all())))
                    .containsExactlyElementsOf(all);

            assertThat(toRowIds(CursorFacade.range(table, Range.closed(all.get(0), all.get(0)))))
                    .containsExactly(all.get(0));

            assertThat(toRowIds(CursorFacade.range(table, Range.closed(all.get(9), all.get(9)))))
                    .containsExactly(all.get(9));

            assertThat(toRowIds(CursorFacade.range(table, Range.closed(all.get(4), all.get(6)))))
                    .containsExactly(all.get(4), all.get(5), all.get(6));
        }
    }

    @Test
    public void testGetCurrentRowValue() throws IOException {
        try (Database db = open(file)) {
            Table table = db.getTable("MyTable");
            CursorFacade cursor = CursorFacade.basic(table);
            int i = 0;
            while (cursor.moveToNextRow()) {
                assertEquals(i + "x0", cursor.getCurrentRowValue(table.getColumn("Col0")));
                assertEquals(i + "x1", cursor.getCurrentRowValue(table.getColumn("Col1")));
                i++;
            }
        }
    }

    @Test
    public void testGetRowId() throws IOException {
        try (Database db = open(file)) {
            Table table = db.getTable("MyTable");
            List<RowId> rowIds = toRowIds(CursorFacade.basic(table));
            assertThat(new TreeSet<>(rowIds)).containsExactlyElementsOf(rowIds);
        }
    }

    @Test
    public void testMoveBefore() throws IOException {
        try (Database db = open(file)) {
            Table table = db.getTable("MyTable");
            CursorFacade cursor = CursorFacade.basic(table);
            cursor.moveToNextRow();
            RowId first = cursor.getRowId();
            cursor.moveToNextRow();
            cursor.moveBefore(first);
            cursor.moveToNextRow();
            assertEquals(first, cursor.getRowId());
        }
    }

    @Test
    public void testWithFilter() throws IOException {
        try (Database db = open(file)) {
            Table table = db.getTable("MyTable");

            SortedMap<Column, String> f1 = ImmutableSortedMap.of();
            assertEquals(10, count(CursorFacade.basic(table).withFilter(f1)));

            SortedMap<Column, String> f2 = ImmutableSortedMap.<Column, String>orderedBy(BY_COLUMN_INDEX).put(table.getColumn("Col1"), "4x1").build();
            assertEquals(1, count(CursorFacade.basic(table).withFilter(f2)));

            SortedMap<Column, String> f3 = ImmutableSortedMap.<Column, String>orderedBy(BY_COLUMN_INDEX).put(table.getColumn("Col1"), "hello").build();
            assertEquals(0, count(CursorFacade.basic(table).withFilter(f3)));
        }
    }

    private static File createResource() throws IOException {
        File result = File.createTempFile("CursorFacadeTest", ".mdb");
        try (Database db = new DatabaseBuilder(result).setFileFormat(Database.FileFormat.V2007).create()) {

            Table table = new TableBuilder("MyTable")
                    .addColumn(new ColumnBuilder("Col0", DataType.TEXT))
                    .addColumn(new ColumnBuilder("Col1", DataType.TEXT))
                    .toTable(db);

            Object[] values = new Object[2];
            for (int i = 0; i < 10; i++) {
                for (int j = 0; j < values.length; j++) {
                    values[j] = i + "x" + j;
                }
                table.addRow(values);
            }
        }
        return result;
    }

    private static Database open(File file) throws IOException {
        return new DatabaseBuilder(file).setReadOnly(true).open();
    }

    private static int count(CursorFacade cursor) throws IOException {
        int result = 0;
        while (cursor.moveToNextRow()) {
            result++;
        }
        return result;
    }

    private static List<RowId> toRowIds(CursorFacade cursor) throws IOException {
        List<RowId> result = new ArrayList<>();
        while (cursor.moveToNextRow()) {
            result.add(cursor.getRowId());
        }
        return result;
    }
}
