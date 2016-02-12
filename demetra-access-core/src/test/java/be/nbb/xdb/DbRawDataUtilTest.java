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
package be.nbb.xdb;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import static org.junit.Assert.assertArrayEquals;
import org.junit.Test;
import ec.tstoolkit.utilities.CheckedIterator;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author Philippe Charles
 */
public final class DbRawDataUtilTest {

    private static final DbRawDataUtil.ToIntFunction<Integer> TO_INDEX = new DbRawDataUtil.ToIntFunction<Integer>() {
        @Override
        public int applyAsInt(Integer value) {
            return value - 1;
        }
    };

    private static final Function<Integer, DbRawDataUtil.SuperDataType> TO_DATA_TYPE = new Function<Integer, DbRawDataUtil.SuperDataType>() {
        @Override
        public DbRawDataUtil.SuperDataType apply(Integer input) {
            return DbRawDataUtil.SuperDataType.OTHER;
        }
    };

    private static <T> CheckedIterator<T, RuntimeException> forArray(T... array) {
        return CheckedIterator.fromIterator(Iterators.forArray(array));
    }

    @Test
    public void testCreateIndexes() {
        assertArrayEquals(new int[]{2, 0, 1}, DbRawDataUtil.createIndexes(Arrays.asList(3, 1, 2), TO_INDEX));
        assertArrayEquals(new int[]{}, DbRawDataUtil.createIndexes(Collections.<Integer>emptyList(), TO_INDEX));
    }

    @Test
    public void testDistinct() {
        DbRawDataUtil.BiConsumer<Object[], Object[]> aggregator = new DbRawDataUtil.BiConsumer<Object[], Object[]>() {
            @Override
            public void accept(Object[] t, Object[] u) {
                t[3] = Math.max((int) t[3], (int) u[3]);
            }
        };
        {
            Object[][] data = {
                {"B", 56.78, null, 0},
                {"A", 12.34, null, 1},
                {"A", 12.34, null, 2},
                {"B", 56.78, null, 3}
            };
            List<Integer> selectColumns = Arrays.asList(3, 1, 2);
            Object[][] expected = {
                {"A", 12.34, null, 2},
                {"B", 56.78, null, 3}
            };
            assertArrayEquals(expected, DbRawDataUtil.distinct(forArray(data), selectColumns, TO_INDEX, TO_DATA_TYPE, aggregator).toArray(Object[].class));
        }
        {
            Object[][] data = {};
            List<Integer> selectColumns = Collections.emptyList();
            Object[][] expected = {};
            assertArrayEquals(expected, DbRawDataUtil.distinct(forArray(data), selectColumns, TO_INDEX, TO_DATA_TYPE, aggregator).toArray(Object[].class));
        }
    }

    @Test
    public void testSort() {
        {
            Object[][] data = {
                {"B", 56.78, null, 0},
                {"A", 12.34, null, 1},
                {"A", 12.34, null, 2},
                {"B", 56.78, null, 3}
            };
            List<Integer> orderColumns = Arrays.asList(3, 1);
            Object[][] expected = {
                {"A", 12.34, null, 1},
                {"A", 12.34, null, 2},
                {"B", 56.78, null, 0},
                {"B", 56.78, null, 3}
            };
            assertArrayEquals(expected, DbRawDataUtil.sort(forArray(data), orderColumns, TO_INDEX, TO_DATA_TYPE).toArray(Object[].class));
        }
        {
            Object[][] data = {};
            List<Integer> orderColumns = Collections.emptyList();
            Object[][] expected = {};
            assertArrayEquals(expected, DbRawDataUtil.sort(forArray(data), orderColumns, TO_INDEX, TO_DATA_TYPE).toArray(Object[].class));
        }
    }
}
