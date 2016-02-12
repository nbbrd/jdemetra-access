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

import ec.tss.tsproviders.db.DbAccessor;
import ec.tss.tsproviders.db.DbBean;
import ec.tss.tsproviders.db.DbSeries;
import ec.tss.tsproviders.db.DbSetId;
import ec.tstoolkit.timeseries.simplets.TsData;
import ec.tstoolkit.timeseries.simplets.TsFrequency;
import ec.tstoolkit.timeseries.simplets.TsPeriod;
import java.util.List;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author Philippe Charles
 * @param <BEAN>
 */
public abstract class DbAccessorTest<BEAN extends DbBean> {

    BEAN firefox = createFirefox();
    BEAN top5 = createTop5();

    abstract protected BEAN createFirefox();

    abstract protected BEAN createTop5();

    abstract protected DbAccessor<BEAN> createAccessor(BEAN bean);

    DbAccessor<BEAN> A0 = createAccessor(firefox);
    DbAccessor<BEAN> A2 = createAccessor(top5);

    @Test(expected = IllegalArgumentException.class)
    public void testGetChildren_Val0Dim0() throws Exception {
        A0.getChildren();
    }

    @Test
    public void testGetChildren_Val0Dim2() throws Exception {
        assertArrayEquals(Top5Table.DIM0, A2.getChildren().toArray());
    }

    @Test
    public void testGetChildren_Val1Dim2() throws Exception {
        assertArrayEquals(Top5Table.DIM1, A2.getChildren(Top5Table.DIM0[0]).toArray());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetChildren_Val2Dim2() throws Exception {
        A2.getChildren(Top5Table.DIM0[0], Top5Table.DIM1[0]);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAllSeries_Val0Dim0() throws Exception {
        A0.getAllSeries();
    }

    @Test
    public void testGetAllSeries_Val0Dim2() throws Exception {
        List<DbSetId> data = A2.getAllSeries();
        for (int i = 0; i < Top5Table.DIM0.length; i++) {
            for (int j = 0; j < Top5Table.DIM1.length; j++) {
                assertEquals(A2.getRoot().child(Top5Table.DIM0[i], Top5Table.DIM1[j]), data.get((i * Top5Table.DIM1.length) + j));
            }
        }
    }

    @Test
    public void testGetAllSeries_Val1Dim2() throws Exception {
        for (String dim0 : Top5Table.DIM0) {
            List<DbSetId> data = A2.getAllSeries(dim0);
            for (int j = 0; j < Top5Table.DIM1.length; j++) {
                assertEquals(A2.getRoot().child(dim0, Top5Table.DIM1[j]), data.get(j));
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAllSeries_Val2Dim2() throws Exception {
        A2.getAllSeries(Top5Table.DIM0[0], Top5Table.DIM1[0]);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAllSeriesWithData_Val0Dim0() throws Exception {
        A0.getAllSeriesWithData();
    }

    @Test
    public void testGetAllSeriesWithData_Val0Dim2() throws Exception {
        List<DbSeries> data = A2.getAllSeriesWithData();
        for (int i = 0; i < Top5Table.DIM0.length; i++) {
            for (int j = 0; j < Top5Table.DIM1.length; j++) {
                DbSeries o = data.get(i * Top5Table.DIM1.length + j);
                assertEquals(A2.getRoot().child(Top5Table.DIM0[i], Top5Table.DIM1[j]), o.getId());
                assertArrayEquals(Top5Table.DATA[i][j], o.getData().get().getValues().internalStorage(), 0);
            }
        }
    }

    @Test
    public void testGetAllSeriesWithData_Val1Dim2() throws Exception {
        for (int i = 0; i < Top5Table.DIM0.length; i++) {
            List<DbSeries> data = A2.getAllSeriesWithData(Top5Table.DIM0[i]);
            for (int j = 0; j < Top5Table.DIM1.length; j++) {
                DbSeries o = data.get(j);
                assertEquals(A2.getRoot().child(Top5Table.DIM0[i], Top5Table.DIM1[j]), o.getId());
                assertArrayEquals(Top5Table.DATA[i][j], o.getData().get().getValues().internalStorage(), 0);
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAllSeriesWithData_Val2Dim2() throws Exception {
        A2.getAllSeriesWithData(Top5Table.DIM0[0], Top5Table.DIM0[1]);
    }

    @Test
    public void testSeriesWithData_Val0Dim0() throws Exception {
        DbSeries data = A0.getSeriesWithData();
        assertEquals(A0.getRoot(), data.getId());
        FirefoxTable.assertData(data.getData().get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSeriesWithData_Val0Dim2() throws Exception {
        A2.getSeriesWithData();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSeriesWithData_Val1Dim2() throws Exception {
        A2.getSeriesWithData(Top5Table.DIM0[0]);
    }

    @Test
    public void testSeriesWithData_Val2Dim2() throws Exception {
        for (int i = 0; i < Top5Table.DIM0.length; i++) {
            for (int j = 0; j < Top5Table.DIM1.length; j++) {
                DbSeries data = A2.getSeriesWithData(Top5Table.DIM0[i], Top5Table.DIM1[j]);
                assertEquals(A2.getRoot().child(Top5Table.DIM0[i], Top5Table.DIM1[j]), data.getId());
                assertArrayEquals(Top5Table.DATA[i][j], data.getData().get().getValues().internalStorage(), 0);
            }
        }
    }

    static class FirefoxTable {

        static double[] raw = {26.14, 26.08, 25.77, 25.54, 25.27, 25.23, 27.03, 27.85, 29.4, 29.67, 28.75, 30.33, 30.5, 31.28, 31.34, 31.82, 32.21, 31.97, 31.64, 31.82, 31.27, 31.74, 31.64, 31.15, 30.69, 31.09, 31.5, 31.24, 31.17, 30.76, 30.68, 30.37, 29.98, 29.67, 29.29, 28.34, 27.95, 27.49, 26.79, 26.39, 25.21};

        static void assertData(TsData data) {
            assertEquals(new TsPeriod(TsFrequency.Monthly, 2008, 6), data.getStart());
            assertArrayEquals(raw, data.getValues().internalStorage(), 0);
        }
    }

    static final class Top5Table {

        static final String[] DIM0 = {"Monthly", "Quarterly"};
        static final String[] DIM1 = {"Chrome", "Firefox", "IE", "Opera", "Other", "Safari"};

        static final double[][] MONTHLY = {
            {0, 0, 1.03, 1.02, 0.93, 1.21, 1.38, 1.52, 1.73, 2.07, 2.42, 2.82, 3.01, 3.38, 3.69, 4.17, 4.66, 5.45, 6.04, 6.72, 7.29, 8.06, 8.61, 9.24, 9.88, 10.76, 11.54, 12.39, 13.35, 14.85, 15.68, 16.54, 17.37, 18.29, 19.36, 20.65, 22.14, 23.16, 23.61, 25, 25.65},
            {26.14, 26.08, 25.77, 25.54, 25.27, 25.23, 27.03, 27.85, 29.4, 29.67, 28.75, 30.33, 30.5, 31.28, 31.34, 31.82, 32.21, 31.97, 31.64, 31.82, 31.27, 31.74, 31.64, 31.15, 30.69, 31.09, 31.5, 31.24, 31.17, 30.76, 30.68, 30.37, 29.98, 29.67, 29.29, 28.34, 27.95, 27.49, 26.79, 26.39, 25.21},
            {68.57, 68.91, 67.16, 67.68, 68.14, 67.84, 65.41, 64.43, 62.52, 61.88, 62.09, 59.49, 60.11, 58.69, 58.37, 57.96, 56.57, 55.72, 55.25, 54.5, 54.44, 53.26, 52.77, 52.86, 52.68, 51.34, 49.87, 49.21, 48.16, 46.94, 46, 45.44, 45.11, 44.52, 43.87, 43.58, 42.45, 41.89, 41.66, 40.18, 40.48},
            {1.78, 1.83, 2.86, 2.69, 3.01, 2.83, 2.92, 2.95, 2.94, 2.96, 3.23, 3.36, 2.64, 2.67, 2.62, 1.88, 2.02, 2.06, 2, 1.97, 1.97, 1.82, 1.96, 1.91, 1.91, 1.88, 2.03, 2, 2.01, 2.07, 2, 2, 1.97, 1.91, 1.84, 1.74, 1.66, 1.67, 1.72, 1.81, 1.83},
            {0.21, 0.2, 0.18, 0.17, 0.16, 0.48, 0.7, 0.67, 0.68, 0.68, 0.87, 1.07, 0.72, 0.73, 0.7, 0.7, 0.86, 1.31, 1.31, 0.91, 0.87, 0.89, 0.88, 0.77, 0.74, 0.7, 0.63, 0.6, 0.6, 0.58, 0.55, 0.55, 0.54, 0.57, 0.63, 0.61, 0.63, 0.61, 0.62, 0.69, 0.93},
            {3.3, 2.99, 3, 2.91, 2.49, 2.41, 2.57, 2.59, 2.73, 2.75, 2.65, 2.93, 3.02, 3.25, 3.28, 3.47, 3.67, 3.48, 3.76, 4.08, 4.16, 4.23, 4.14, 4.07, 4.09, 4.23, 4.42, 4.56, 4.7, 4.79, 5.09, 5.08, 5.02, 5.04, 5.01, 5.07, 5.17, 5.19, 5.6, 5.93, 5.9}
        };
        static final double[][] QUARTERLY = {
            {0.51, 1.07, 1.55, 2.45, 3.37, 4.78, 6.79, 8.64, 10.7, 13.54, 16.55, 19.42, 22.96, 25.3},
            {25.94, 25.32, 28.16, 29.57, 31.05, 32, 31.53, 31.51, 31.08, 31.06, 30.34, 29.11, 27.42, 25.84},
            {67.99, 67.9, 64.04, 61.12, 59.03, 56.73, 54.67, 52.96, 51.35, 48.1, 45.51, 44, 42, 40.32},
            {2.33, 2.85, 2.94, 3.19, 2.65, 1.99, 1.98, 1.89, 1.94, 2.03, 1.99, 1.83, 1.68, 1.82},
            {0.19, 0.3, 0.68, 0.88, 0.71, 0.97, 1, 0.85, 0.69, 0.59, 0.55, 0.6, 0.62, 0.8},
            {3.05, 2.56, 2.63, 2.78, 3.19, 3.54, 4.03, 4.15, 4.24, 4.69, 5.06, 5.04, 5.31, 5.92}
        };
        static final double[][][] DATA = {MONTHLY, QUARTERLY};
    }
}
