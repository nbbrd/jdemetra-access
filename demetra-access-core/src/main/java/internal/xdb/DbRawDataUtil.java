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
import com.google.common.collect.Ordering;
import com.google.common.primitives.UnsignedBytes;
import ec.tss.tsproviders.utils.IteratorWithIO;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.AccessLevel;

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
    public <C> List<C> getColumns(@Nonnull Function<String, C> toColumn, @Nonnull Collection<String> columns) {
        return columns.stream().map(toColumn).collect(Collectors.toList());
    }

    @Nonnull
    public <C> SortedSet<C> mergeAndSort(@Nonnull ToIntFunction<C> toInternalIndex, @Nonnull Collection<C>... columns) {
        return Stream.of(columns)
                .flatMap(Collection::stream)
                .collect(Collectors.toCollection(() -> new TreeSet<>(Comparator.comparingInt(toInternalIndex))));
    }

    @Nonnull
    public <C> SortedMap<C, String> getFilter(@Nonnull ToIntFunction<C> toInternalIndex, @Nonnull Function<String, C> toColumn, @Nonnull Map<String, String> filterItems) {
        SortedMap<C, String> result = new TreeMap<>(Comparator.comparingInt(toInternalIndex));
        filterItems.forEach((k, v) -> result.put(toColumn.apply(k), v));
        return result;
    }

    @lombok.AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class ToIndex<C> implements ToIntFunction<C> {

        @Nonnull
        public static <C> ToIndex<C> of(@Nonnull ToIntFunction<C> toInternalIndex, @Nonnull SortedSet<C> dataColumns) {
            C max = Collections.max(dataColumns, Comparator.comparingInt(toInternalIndex));
            int[] index = new int[toInternalIndex.applyAsInt(max) + 1];
            int i = 0;
            for (C column : dataColumns) {
                index[toInternalIndex.applyAsInt(column)] = i++;
            }
            return new ToIndex<>(toInternalIndex, index);
        }

        private final ToIntFunction<C> toInternalIndex;
        private final int[] index;

        @Override
        public int applyAsInt(C value) {
            return index[toInternalIndex.applyAsInt(value)];
        }
    }

    public static final BiConsumer<Object[], Object[]> NO_AGGREGATION = (l, r) -> {
    };

    @Nonnull
    @SuppressWarnings("null")
    public <C> IteratorWithIO<Object[]> distinct(
            @Nonnull IteratorWithIO<Object[]> rows,
            @Nonnull List<C> selectColumns,
            @Nonnull ToIntFunction<C> toIndex,
            @Nonnull Function<C, SuperDataType> toDataType,
            @Nonnull BiConsumer<Object[], Object[]> aggregator) throws IOException {

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
        return IteratorWithIO.from(result.keySet().iterator());
    }

    @Nonnull
    public <C> IteratorWithIO<Object[]> sort(
            @Nonnull IteratorWithIO<Object[]> rows,
            @Nonnull List<C> orderColumns,
            @Nonnull ToIntFunction<C> toIndex,
            @Nonnull Function<C, SuperDataType> toDataType) throws IOException {

        List<Object[]> tmp = toList(rows);
        tmp.sort(newRowOrdering(orderColumns, toIndex, toDataType));
        return IteratorWithIO.from(tmp.iterator());
    }

    @Nonnull
    public <C> boolean isSortRequired(boolean distinct, @Nonnull List<C> selectColumns, @Nonnull List<C> orderColumns) {
        return !orderColumns.isEmpty() && !(distinct && Iterables.elementsEqual(selectColumns, orderColumns));
    }

    //<editor-fold defaultstate="collapsed" desc="Implementation details">
    static List<Object[]> toList(IteratorWithIO<Object[]> iterator) throws IOException {
        List<Object[]> result = new ArrayList<>();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }
        return result;
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
