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
package internal.xdb;

import com.google.common.collect.Iterators;
import ec.tss.tsproviders.utils.IteratorWithIO;
import java.io.IOException;
import static org.junit.Assert.assertArrayEquals;
import org.junit.Test;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;

/**
 *
 * @author Philippe Charles
 */
public final class DbRawDataUtilTest {

    private static final ToIntFunction<Integer> TO_INDEX = o -> o - 1;

    private static final Function<Integer, DbRawDataUtil.SuperDataType> TO_DATA_TYPE = o -> DbRawDataUtil.SuperDataType.OTHER;

    private static <T> IteratorWithIO<T> forArray(T... array) {
        return IteratorWithIO.from(Iterators.forArray(array));
    }

    @Test
    public void testCreateIndexes() {
        assertArrayEquals(new int[]{2, 0, 1}, DbRawDataUtil.createIndexes(Arrays.asList(3, 1, 2), TO_INDEX));
        assertArrayEquals(new int[]{}, DbRawDataUtil.createIndexes(Collections.<Integer>emptyList(), TO_INDEX));
    }

    @Test
    public void testDistinct() throws IOException {
        BiConsumer<Object[], Object[]> aggregator = (t, u) -> t[3] = Math.max((int) t[3], (int) u[3]);
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
            assertIterEquals(expected, DbRawDataUtil.distinct(forArray(data), selectColumns, TO_INDEX, TO_DATA_TYPE, aggregator));
        }
        {
            Object[][] data = {};
            List<Integer> selectColumns = Collections.emptyList();
            Object[][] expected = {};
            assertIterEquals(expected, DbRawDataUtil.distinct(forArray(data), selectColumns, TO_INDEX, TO_DATA_TYPE, aggregator));
        }
    }

    @Test
    public void testSort() throws IOException {
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
            assertIterEquals(expected, DbRawDataUtil.sort(forArray(data), orderColumns, TO_INDEX, TO_DATA_TYPE));
        }
        {
            Object[][] data = {};
            List<Integer> orderColumns = Collections.emptyList();
            Object[][] expected = {};
            assertIterEquals(expected, DbRawDataUtil.sort(forArray(data), orderColumns, TO_INDEX, TO_DATA_TYPE));
        }
    }

    static void assertIterEquals(Object[][] expected, IteratorWithIO<Object[]> found) throws IOException {
        assertArrayEquals(expected, DbRawDataUtil.toList(found).toArray());
    }
}
