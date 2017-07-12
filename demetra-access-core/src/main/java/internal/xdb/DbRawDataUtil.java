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

import com.google.common.base.Equivalence;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import com.google.common.primitives.UnsignedBytes;
import ec.tstoolkit.utilities.CheckedIterator;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 *
 * @author Philippe Charles
 */
@lombok.experimental.UtilityClass
public class DbRawDataUtil {

    public enum SuperDataType implements Comparator<Object> {

        COMPARABLE {
            @Override
            public int compare(Object l, Object r) {
                return ((Comparable) l).compareTo(r);
            }
        },
        BYTE_ARRAY {
            @Override
            public int compare(Object l, Object r) {
                return UnsignedBytes.lexicographicalComparator().compare((byte[]) l, (byte[]) r);
            }
        },
        OTHER {
            @Override
            public int compare(Object l, Object r) {
                return l.toString().compareTo(r.toString());
            }
        };
    }

    @Nonnull
    public <C> int[] createIndexes(@Nonnull List<C> selectColumns, @Nonnull ToIntFunction<C> toIndex) {
        int[] result = new int[selectColumns.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = toIndex.applyAsInt(selectColumns.get(i));
        }
        return result;
    }

    @Nonnull
    @SuppressWarnings("null")
    public <C, T extends Throwable> CheckedIterator<Object[], T> distinct(
            @Nonnull CheckedIterator<Object[], T> rows,
            @Nonnull List<C> selectColumns,
            @Nonnull ToIntFunction<C> toIndex,
            @Nonnull Function<C, SuperDataType> toDataType,
            @Nonnull BiConsumer<Object[], Object[]> aggregator) throws T {

        TreeMap<Object[], Object[]> result = new TreeMap<>(newRowOrdering(selectColumns, toIndex, toDataType));
        Equivalence<Object[]> equivalence = newRowEquivalence(selectColumns, toIndex);
        Object[] first = null;
        while (rows.hasNext()) {
            Object[] current = rows.next();
            // pre-check to avoid using TreeMap#get(Object)
            if (!equivalence.equivalent(first, current)) {
                first = result.get(current);
                if (first == null) {
                    result.put(current, current);
                    first = current;
                } else {
                    aggregator.accept(first, current);
                }
            } else {
                aggregator.accept(first, current);
            }
        }
        return new SizedCheckedIterator(result.keySet());
    }

    @Nonnull
    public <C, T extends Throwable> CheckedIterator<Object[], T> sort(
            @Nonnull CheckedIterator<Object[], T> rows,
            @Nonnull List<C> orderColumns,
            @Nonnull ToIntFunction<C> toIndex,
            @Nonnull Function<C, SuperDataType> toDataType) throws T {

        Object[][] tmp = rows.toArray(Object[].class);
        Arrays.sort(tmp, newRowOrdering(orderColumns, toIndex, toDataType));
        return new SizedCheckedIterator<>(tmp);
    }

    @Nonnull
    public <C> boolean isSortRequired(boolean distinct, @Nonnull List<C> selectColumns, @Nonnull List<C> orderColumns) {
        return !orderColumns.isEmpty() && !(distinct && Iterables.elementsEqual(selectColumns, orderColumns));
    }

    //<editor-fold defaultstate="collapsed" desc="Implementation details">
    private static final class SizedCheckedIterator<T extends Throwable> extends CheckedIterator<Object[], T> {

        private final Iterator<Object[]> iterator;
        private int remaining;

        public SizedCheckedIterator(Object[][] set) {
            this.iterator = Iterators.forArray(set);
            this.remaining = set.length;
        }

        public SizedCheckedIterator(Collection<Object[]> set) {
            this.iterator = set.iterator();
            this.remaining = set.size();
        }

        @Override
        public boolean hasNext() throws T {
            return iterator.hasNext();
        }

        @Override
        public Object[] next() throws T, NoSuchElementException {
            remaining--;
            return iterator.next();
        }

        @Override
        public Object[][] toArray(Class<Object[]> type) throws T {
            Object[][] result = new Object[remaining][];
            int i = 0;
            while (hasNext()) {
                result[i++] = next();
            }
            return result;
        }
    }

    private <C> Equivalence<Object[]> newRowEquivalence(List<C> selectColumns, ToIntFunction<C> index) {
        return new ArrayEquivalence(createIndexes(selectColumns, index));
    }

    private <C> Comparator<Object[]> newRowOrdering(List<C> orderColumns, ToIntFunction<C> toIndex, Function<C, SuperDataType> toDataType) {
        List<Comparator<Object[]>> result = orderColumns.stream()
                .map(o -> new ArrayItemComparator(toIndex.applyAsInt(o), toDataType.apply(o)))
                .collect(Collectors.toList());
        return Ordering.compound(result);
    }

    private static final class ArrayItemComparator implements Comparator<Object[]> {

        private final int index;
        private final Comparator<Object> delegate;

        public ArrayItemComparator(int itemIndex, Comparator<Object> delegate) {
            this.index = itemIndex;
            this.delegate = delegate;
        }

        @Override
        public int compare(Object[] l, Object[] r) {
            Object lo = l[index];
            Object ro = r[index];
            return lo == ro ? 0 : lo == null ? -1 : ro == null ? 1 : delegate.compare(lo, ro);
        }
    }

    private static final class ArrayEquivalence extends Equivalence<Object[]> {

        private final int[] indexes;

        public ArrayEquivalence(int[] indexes) {
            this.indexes = indexes;
        }

        @Override
        protected boolean doEquivalent(Object[] a, Object[] b) {
            for (int o : indexes) {
                if (!Objects.equals(a[o], b[o])) {
                    return false;
                }
            }
            return true;
        }

        @Override
        protected int doHash(Object[] t) {
            int result = 1;
            for (int o : indexes) {
                result = 31 * result + Objects.hashCode(t[o]);
            }
            return result;
        }
    }
    //</editor-fold>
}
