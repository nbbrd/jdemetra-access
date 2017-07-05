/*
 * Copyright 2017 National Bank of Belgium
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
package internal.demetra.jackcess;

import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import ec.tss.TsCollectionInformation;
import ec.tss.TsInformation;
import ec.tstoolkit.timeseries.simplets.TsObservation;
import ec.tstoolkit.timeseries.simplets.TsPeriod;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 *
 * @author Philippe Charles
 */
@lombok.experimental.UtilityClass
public class JackcessTsExport {

    public enum WriteOption {
        APPEND, TRUNCATE_EXISTING;
    }

    public Database getDatabase(File file, Database.FileFormat fileFormat, WriteOption writeOption) throws IOException {
        return file.exists() && writeOption == WriteOption.APPEND
                ? DatabaseBuilder.open(file)
                : DatabaseBuilder.create(fileFormat, file);
    }

    public Table getTable(Database database, WriteOption writeOption, String tableName, String dimColumn, String periodColumn, String valueColumn, String versionColumn) throws IOException {
        if (database.getTableNames().contains(tableName) && writeOption == WriteOption.APPEND) {
            return database.getTable(tableName);
        }
        TableBuilder result = new TableBuilder(tableName);
        result.addColumn(new ColumnBuilder(dimColumn, DataType.TEXT));
        result.addColumn(new ColumnBuilder(periodColumn, DataType.SHORT_DATE_TIME));
        result.addColumn(new ColumnBuilder(valueColumn, DataType.DOUBLE));
        if (!versionColumn.isEmpty()) {
            result.addColumn(new ColumnBuilder(versionColumn, DataType.SHORT_DATE_TIME));
            result.setPrimaryKey(dimColumn, periodColumn, versionColumn);
        } else {
            result.setPrimaryKey(dimColumn, periodColumn);
        }
        return result.toTable(database);
    }

    public BiFunction<String, TsObservation, Object[]> getRowFunc(Table table, boolean beginPeriod, String dimColumn, String periodColumn, String valueColumn, String versionColumn) {
        final Function<TsPeriod, Date> toDate = beginPeriod ? TsPeriodDateFunc.FIRST : TsPeriodDateFunc.LAST;

        final int columnCount = table.getColumnCount();

        final int dimIndex = table.getColumn(dimColumn).getColumnIndex();
        final int periodIndex = table.getColumn(periodColumn).getColumnIndex();
        final int valueIndex = table.getColumn(valueColumn).getColumnIndex();

        if (versionColumn.isEmpty()) {
            return (name, obs) -> {
                Object[] result = new Object[columnCount];
                result[dimIndex] = name;
                result[periodIndex] = toDate.apply(obs.getPeriod());
                result[valueIndex] = obs.getValue();
                return result;
            };
        }

        final int versionIndex = table.getColumn(versionColumn).getColumnIndex();
        final Date version = new Date();

        return (name, obs) -> {
            Object[] result = new Object[columnCount];
            result[dimIndex] = name;
            result[periodIndex] = toDate.apply(obs.getPeriod());
            result[valueIndex] = obs.getValue();
            result[versionIndex] = version;
            return result;
        };
    }

    public void writeContent(Table table, BiFunction<String, TsObservation, Object[]> toRow, TsCollectionInformation col, int threshold) throws IOException {
        List<Object[]> bulk = new ArrayList<>();
        for (TsInformation ts : col.items) {
            if (ts.hasData() && ts.data != null) {
                ts.data.forEach(input -> bulk.add(toRow.apply(ts.name, input)));
                if (bulk.size() > threshold) {
                    table.addRows(bulk);
                    bulk.clear();
                }
            }
        }
        if (!bulk.isEmpty()) {
            table.addRows(bulk);
        }
    }

    private enum TsPeriodDateFunc implements Function<TsPeriod, Date> {
        FIRST {
            @Override
            public Date apply(TsPeriod input) {
                return input.firstday().getTime();
            }
        }, LAST {
            @Override
            public Date apply(TsPeriod input) {
                return input.lastday().getTime();
            }
        }
    }
}
