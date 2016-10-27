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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import ec.tstoolkit.design.IBuilder;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
    private final ImmutableList<String> selectColumns;
    private final ImmutableMap<String, String> filterItems;
    private final ImmutableList<String> orderColumns;

    private DbBasicSelect(String tableName, boolean distinct, ImmutableList<String> selectColumns, ImmutableMap<String, String> filterItems, ImmutableList<String> orderColumns) {
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
        Joiner joiner = Joiner.on(", ");
        StringBuilder result = new StringBuilder();
        result.append("SELECT ");
        if (distinct) {
            result.append("DISTINCT ");
        }
        joiner.appendTo(result, selectColumns);
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
            joiner.appendTo(result, orderColumns);
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
        private final List<String> select = Lists.newArrayList();
        private final Map<String, String> filterItems = Maps.newHashMap();
        private final List<String> order = Lists.newArrayList();

        private Builder(String tableName) {
            this.tableName = Preconditions.checkNotNull(tableName);
        }

        private Builder addIfNotNullOrEmpty(List<String> list, String... values) {
            for (String o : values) {
                if (!Strings.isNullOrEmpty(o)) {
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
                    ImmutableList.copyOf(select),
                    ImmutableMap.copyOf(filterItems),
                    ImmutableList.copyOf(order));
        }
    }
}
