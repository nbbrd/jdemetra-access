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

import com.google.common.collect.Range;
import com.google.common.io.Resources;
import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.DateTimeType;
import com.healthmarketscience.jackcess.RowId;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import ec.tss.tsproviders.db.DbAccessor;
import ec.tss.tsproviders.utils.DataFormat;
import ec.tss.tsproviders.utils.Parsers;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 *
 * @author Philippe Charles
 */
public class JackcessAccessorTest extends DbAccessorTest<JackcessBean> {

    static File TOP5;

    @BeforeAll
    public static void beforeClass() throws IOException {
        TOP5 = createResource();
    }

    @AfterAll
    public static void afterClass() {
        TOP5.delete();
    }

    public static File createResource() throws IOException {
        File result = File.createTempFile("Top5Browsers", ".mdb");
        try (Database db = new DatabaseBuilder(result).setFileFormat(Database.FileFormat.V2007).create()) {
            db.setDateTimeType(DateTimeType.DATE);

            Parsers.Parser<String> toString = Parsers.stringParser();
            DataFormat dataFormat = DataFormat.create("fr_BE", "dd/MM/yyyy", null);
            Parsers.Parser<Date> toDate = dataFormat.dateParser();
            Parsers.Parser<Number> toNumber = dataFormat.numberParser();

            Table top5 = new TableBuilder("Top5")
                    .addColumn(new ColumnBuilder("Freq", DataType.TEXT))
                    .addColumn(new ColumnBuilder("Browser", DataType.TEXT))
                    .addColumn(new ColumnBuilder("Period", DataType.SHORT_DATE_TIME))
                    .addColumn(new ColumnBuilder("MarketShare", DataType.DOUBLE))
                    .toTable(db);
            fillTable(top5, "/Top5Browsers-Table-Top5.csv", toString, toString, toDate, toNumber);

            Table monthly = new TableBuilder("Monthly")
                    .addColumn(new ColumnBuilder("Browser", DataType.TEXT))
                    .addColumn(new ColumnBuilder("Period", DataType.SHORT_DATE_TIME))
                    .addColumn(new ColumnBuilder("MarketShare", DataType.DOUBLE))
                    .toTable(db);
            fillTable(monthly, "/Top5Browsers-Table-Monthly.csv", toString, toDate, toNumber);

            Table firefox = new TableBuilder("Firefox")
                    .addColumn(new ColumnBuilder("Period", DataType.SHORT_DATE_TIME))
                    .addColumn(new ColumnBuilder("MarketShare", DataType.DOUBLE))
                    .toTable(db);
            fillTable(firefox, "/Top5Browsers-Table-Firefox.csv", toDate, toNumber);
        }
        return result;
    }

    private static void fillTable(Table table, String resource, Parsers.Parser<?>... parsers) throws IOException {
        try (BufferedReader r = Resources.asCharSource(JackcessAccessorTest.class.getResource(resource), StandardCharsets.UTF_8).openBufferedStream()) {
            r.readLine(); // headers
            String line;
            Object[] values = new Object[parsers.length];
            while ((line = r.readLine()) != null) {
                String[] row = line.split(";", -1);
                for (int i = 0; i < row.length; i++) {
                    values[i] = parsers[i].parse(row[i]);
                }
                table.addRow(values);
            }
        }
    }

    @Override
    protected JackcessBean createFirefox() {
        JackcessBean result = new JackcessBean();
        result.setDbName(TOP5.getPath());
        result.setTableName("Firefox");
        result.setDimColumns("");
        result.setPeriodColumn("Period");
        result.setValueColumn("MarketShare");
        result.setCacheDepth(0);
        return result;
    }

    @Override
    protected JackcessBean createTop5() {
        JackcessBean result = new JackcessBean();
        result.setDbName(TOP5.getPath());
        result.setTableName("Top5");
        result.setDimColumns("Freq, Browser");
        result.setPeriodColumn("Period");
        result.setValueColumn("MarketShare");
        result.setCacheDepth(0);
        return result;
    }

    @Override
    protected DbAccessor createAccessor(JackcessBean bean) {
        return new JackcessAccessor(bean);
    }

    @Test
    public void testRangeIndex() throws Exception {
        JackcessAccessor accessor = new JackcessAccessor(createTop5());

        accessor.getChildren();
        assertEquals(Top5Table.DIM0.length, accessor.rangeIndex.size());
        Range<RowId> r1 = accessor.rangeIndex.getIfPresent(accessor.getRoot().child(Top5Table.DIM0[1]));
//        assertEquals(246, r1.lowerEndpoint());
//        assertEquals(329, r1.upperEndpoint());

        accessor.getChildren(Top5Table.DIM0[1]);
        assertEquals(Top5Table.DIM0.length + Top5Table.DIM1.length, accessor.rangeIndex.size());
        Range<RowId> r2 = accessor.rangeIndex.getIfPresent(accessor.getRoot().child(Top5Table.DIM0[1], Top5Table.DIM1[3]));
//        assertEquals(288, r2.lowerEndpoint());
//        assertEquals(301, r2.upperEndpoint());
//
        assertArrayEquals(Top5Table.QUARTERLY[3], accessor.getSeriesWithData(Top5Table.DIM0[1], Top5Table.DIM1[3]).getData().get().getValues().internalStorage(), 0);
        assertArrayEquals(Top5Table.QUARTERLY[5], accessor.getSeriesWithData(Top5Table.DIM0[1], Top5Table.DIM1[5]).getData().get().getValues().internalStorage(), 0);
    }
}
