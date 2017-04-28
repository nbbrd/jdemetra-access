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
package internal.jackcess;

import internal.xdb.DbBasicSelect;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.RowId;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author Philippe Charles
 */
public class JackcessStatementTest {

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
    public void testExecuteQuery() throws IOException {
        try (Database db = open(file)) {
            try (JackcessStatement stmt = new JackcessStatement(db, null)) {

                DbBasicSelect q1 = DbBasicSelect.from("T").select("C0").build();
                try (JackcessResultSet rs = stmt.executeQuery(q1)) {
                    assertThat(rs.getColumn(0).getName()).isEqualTo("C0");
                    assertThat(toValues(rs, 0)).containsExactly("B", "A", "A", "B");
                }

                DbBasicSelect q2 = DbBasicSelect.from("T").select("C1", "C0").build();
                try (JackcessResultSet rs = stmt.executeQuery(q2)) {
                    assertThat(rs.getColumn(0).getName()).isEqualTo("C1");
                    assertThat(rs.getColumn(1).getName()).isEqualTo("C0");
                    assertThat(toValues(rs, 0)).containsExactly(56.78, 12.34, 12.34, 56.78);
                }

                DbBasicSelect q3 = DbBasicSelect.from("T").select("C3").orderBy("C0").build();
                try (JackcessResultSet rs = stmt.executeQuery(q3)) {
                    assertThat(rs.getColumn(0).getName()).isEqualTo("C3");
                    assertThat(toValues(rs, 0)).containsExactly(1, 2, 0, 3);
                }

                DbBasicSelect q4 = DbBasicSelect.from("T").select("C0").distinct(true).orderBy("C0").build();
                try (JackcessResultSet rs = stmt.executeQuery(q4)) {
                    assertThat(rs.getColumn(0).getName()).isEqualTo("C0");
                    assertThat(toValues(rs, 0)).containsExactly("A", "B");
                }

                DbBasicSelect q5 = DbBasicSelect.from("T").select("C3").filter(ImmutableMap.of("C0", "A")).orderBy("C3").build();
                try (JackcessResultSet rs = stmt.executeQuery(q5)) {
                    assertThat(rs.getColumn(0).getName()).isEqualTo("C3");
                    assertThat(toValues(rs, 0)).containsExactly(1, 2);
                }

                DbBasicSelect q6 = DbBasicSelect.from("T").select("C0").distinct(true).orderBy("C0").build();
                try (JackcessResultSet rs = stmt.executeQuery(q6)) {
                    List<Range<RowId>> ranges = toRanges(rs);
                    assertThat(ranges).hasSize(2);
                    assertThat(ranges.get(0).lowerEndpoint()).isGreaterThan(ranges.get(1).lowerEndpoint());
                    assertThat(ranges.get(0).upperEndpoint()).isLessThan(ranges.get(1).upperEndpoint());
                    assertThat(ranges.get(0).lowerEndpoint()).isLessThan(ranges.get(0).upperEndpoint());
                }
            }
        }
    }

    private static File createResource() throws IOException {
        File result = File.createTempFile("JackcessStatementTest", ".mdb");
        try (Database db = new DatabaseBuilder(result).setFileFormat(Database.FileFormat.V2007).create()) {

            Table table = new TableBuilder("T")
                    .addColumn(new ColumnBuilder("C0", DataType.TEXT))
                    .addColumn(new ColumnBuilder("C1", DataType.DOUBLE))
                    .addColumn(new ColumnBuilder("C2", DataType.TEXT))
                    .addColumn(new ColumnBuilder("C3", DataType.LONG))
                    .toTable(db);

            Object[][] data = {
                {"B", 56.78, null, 0},
                {"A", 12.34, null, 1},
                {"A", 12.34, null, 2},
                {"B", 56.78, null, 3}
            };

            for (Object[] o : data) {
                table.addRow(o);
            }
        }
        return result;
    }

    private static Database open(File file) throws IOException {
        return new DatabaseBuilder(file).setReadOnly(true).open();
    }

    private static List<Object> toValues(JackcessResultSet rs, int index) throws IOException {
        List<Object> result = new ArrayList<>();
        while (rs.next()) {
            result.add(rs.getValue(index));
        }
        return result;
    }

    private static List<Range<RowId>> toRanges(JackcessResultSet rs) throws IOException {
        List<Range<RowId>> result = new ArrayList<>();
        while (rs.next()) {
            result.add(rs.getRange());
        }
        return result;
    }
}
