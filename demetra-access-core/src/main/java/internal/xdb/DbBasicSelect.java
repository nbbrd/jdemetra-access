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

import ec.tstoolkit.design.IBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 *
 * @author Philippe Charles
 */
@Immutable
public final class DbBasicSelect {

    private final String tableName;
    private final boolean distinct;
    private final List<String> selectColumns;
    private final Map<String, String> filterItems;
    private final List<String> orderColumns;

    private DbBasicSelect(String tableName, boolean distinct, List<String> selectColumns, Map<String, String> filterItems, List<String> orderColumns) {
        this.tableName = tableName;
        this.distinct = distinct;
        this.selectColumns = selectColumns;
        this.filterItems = filterItems;
        this.orderColumns = orderColumns;
    }

    @Nonnull
    public String getTableName() {
        return tableName;
    }

    public boolean isDistinct() {
        return distinct;
    }

    @Nonnull
    public List<String> getSelectColumns() {
        return selectColumns;
    }

    @Nonnull
    public Map<String, String> getFilterItems() {
        return filterItems;
    }

    @Nonnull
    public List<String> getOrderColumns() {
        return orderColumns;
    }

    @Nonnull
    public String toSql() {
        StringBuilder result = new StringBuilder();
        result.append("SELECT ");
        if (distinct) {
            result.append("DISTINCT ");
        }
        result.append(selectColumns.stream().collect(COMMA_JOINER));
        result.append(" FROM ").append(tableName);
        if (!filterItems.isEmpty()) {
            result.append(" WHERE ");
            Iterator<Map.Entry<String, String>> iter = filterItems.entrySet().iterator();
            Map.Entry<String, String> current = iter.next();
            result.append(current.getKey()).append("='").append(current.getValue()).append("'");
            while (iter.hasNext()) {
                current = iter.next();
                result.append(" AND ").append(current.getKey()).append("='").append(current.getValue()).append("'");
            }
        }
        if (!orderColumns.isEmpty()) {
            result.append(" ORDER BY ");
            result.append(orderColumns.stream().collect(COMMA_JOINER));
        }
        return result.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Nonnull
    public static Builder from(@Nonnull String tableName) {
        return new Builder(tableName);
    }

    public static final class Builder implements IBuilder<DbBasicSelect> {

        private final String tableName;
        private boolean distinct = false;
        private final List<String> select = new ArrayList<>();
        private final Map<String, String> filterItems = new HashMap<>();
        private final List<String> order = new ArrayList<>();

        private Builder(String tableName) {
            this.tableName = Objects.requireNonNull(tableName);
        }

        private Builder addIfNotNullOrEmpty(List<String> list, String... values) {
            for (String o : values) {
                if (o != null && !o.isEmpty()) {
                    list.add(o);
                }
            }
            return this;
        }

        @Nonnull
        public Builder distinct(boolean distinct) {
            this.distinct = distinct;
            return this;
        }

        @Nonnull
        public Builder select(String... columns) {
            return addIfNotNullOrEmpty(select, columns);
        }

        @Nonnull
        public Builder filter(Map<String, String> filter) {
            this.filterItems.putAll(filter);
            return this;
        }

        @Nonnull
        public Builder orderBy(String... columns) {
            return addIfNotNullOrEmpty(this.order, columns);
        }

        @Override
        public DbBasicSelect build() {
            return new DbBasicSelect(tableName, distinct,
                    immmutableCopyOf(select),
                    immmutableCopyOf(filterItems),
                    immmutableCopyOf(order));
        }

        private static <X> List<X> immmutableCopyOf(List<X> input) {
            switch (input.size()) {
                case 0:
                    return Collections.emptyList();
                case 1:
                    return Collections.singletonList(input.get(0));
                default:
                    return Collections.unmodifiableList(new ArrayList(input));
            }
        }

        private static <K, V> Map<K, V> immmutableCopyOf(Map<K, V> input) {
            switch (input.size()) {
                case 0:
                    return Collections.emptyMap();
                case 1:
                    Map.Entry<K, V> single = input.entrySet().iterator().next();
                    return Collections.singletonMap(single.getKey(), single.getValue());
                default:
                    return Collections.unmodifiableMap(new HashMap(input));
            }
        }
    }

    private static final Collector<CharSequence, ?, String> COMMA_JOINER = Collectors.joining(",");
}
