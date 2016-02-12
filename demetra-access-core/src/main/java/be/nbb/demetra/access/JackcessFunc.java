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

import be.nbb.jackcess.JackcessResultSet;
import com.healthmarketscience.jackcess.Column;
import ec.tss.tsproviders.db.DbUtil;
import ec.tss.tsproviders.utils.IParser;
import java.io.IOException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 * @author Philippe Charles
 */
abstract class JackcessFunc<T> implements DbUtil.Func<JackcessResultSet, T, IOException> {

    @Nonnull
    public static JackcessFunc<String[]> onGetStringArray(final int index, final int length) {
        return new JackcessFunc<String[]>() {
            @Override
            public String[] apply(JackcessResultSet rs) throws IOException {
                return getStringArray(rs, index, length);
            }
        };
    }

    @Nonnull
    public static JackcessFunc<String> onGetObjectToString(final int index) {
        return new JackcessFunc<String>() {
            @Override
            public String apply(JackcessResultSet rs) throws IOException {
                return getObjectToString(rs, index);
            }
        };
    }

    @Nonnull
    public static <X> JackcessFunc<X> compose(final int index, final IParser<X> parser) {
        return new JackcessFunc<X>() {
            @Override
            public X apply(JackcessResultSet rs) throws IOException {
                return getAndParse(rs, index, parser);
            }
        };
    }

    @Nonnull
    public static JackcessFunc<java.util.Date> onDate(JackcessResultSet rs, int index, IParser<java.util.Date> dateParser) throws IOException {
        JackcessFunc<java.util.Date> result = dateByDataType(rs.getColumn(index), index);
        return result != null ? result : compose(index, dateParser);
    }

    @Nonnull
    public static JackcessFunc<Number> onNumber(JackcessResultSet rs, int index, IParser<Number> numberParser) throws IOException {
        JackcessFunc<Number> result = numberByDataType(rs.getColumn(index), index);
        return result != null ? result : compose(index, numberParser);
    }

    //<editor-fold defaultstate="collapsed" desc="Implementation details">
    @Nullable
    private static String toString(@Nullable Object o) {
        return o != null ? o.toString() : null;
    }

    private static String[] getStringArray(JackcessResultSet rs, int index, int length) throws IOException {
        String[] result = new String[length];
        for (int i = 0; i < result.length; i++) {
            result[i] = toString(rs.getValue(index + i));
        }
        return result;
    }

    private static String getObjectToString(JackcessResultSet rs, int index) throws IOException {
        return toString(rs.getValue(index));
    }

    private static <X> X getAndParse(JackcessResultSet rs, int index, IParser<X> parser) throws IOException {
        return parser.parse(getObjectToString(rs, index));
    }

    private static <X> X getAndCast(JackcessResultSet rs, int index) throws IOException {
        return (X) rs.getValue(index);
    }

    @Nullable
    private static JackcessFunc<java.util.Date> dateByDataType(Column column, final int index) {
        switch (column.getType()) {
            case SHORT_DATE_TIME:
                return new JackcessFunc<java.util.Date>() {
                    @Override
                    public java.util.Date apply(JackcessResultSet o) throws IOException {
                        return getAndCast(o, index);
                    }
                };
        }
        return null;
    }

    @Nullable
    private static JackcessFunc<Number> numberByDataType(Column column, final int index) {
        switch (column.getType()) {
            case DOUBLE:
            case FLOAT:
            case INT:
            case LONG:
            case NUMERIC:
                return new JackcessFunc<Number>() {
                    @Override
                    public Number apply(JackcessResultSet o) throws IOException {
                        return getAndCast(o, index);
                    }
                };
        }
        return null;
    }
    //</editor-fold>
}
