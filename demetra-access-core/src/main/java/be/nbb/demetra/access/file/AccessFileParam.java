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
package be.nbb.demetra.access.file;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import ec.tss.tsproviders.DataSet;
import ec.tss.tsproviders.DataSource;
import ec.tss.tsproviders.cube.CubeId;
import ec.tss.tsproviders.cube.CubeSupport;
import ec.tss.tsproviders.utils.DataFormat;
import ec.tss.tsproviders.utils.IConfig;
import ec.tss.tsproviders.utils.IParam;
import ec.tss.tsproviders.utils.ObsGathering;
import static ec.tss.tsproviders.utils.Params.onDataFormat;
import static ec.tss.tsproviders.utils.Params.onInteger;
import static ec.tss.tsproviders.utils.Params.onLong;
import static ec.tss.tsproviders.utils.Params.onObsGathering;
import static ec.tss.tsproviders.utils.Params.onString;
import static ec.tss.tsproviders.utils.Params.onStringList;
import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

/**
 *
 * @author Philippe Charles
 */
interface AccessFileParam extends IParam<DataSource, AccessFileBean> {

    @Nonnull
    String getVersion();

    @Nonnull
    IParam<DataSet, CubeId> getCubeIdParam(@Nonnull DataSource dataSource);

    static class V1 implements AccessFileParam {

        private final Splitter dimensionSplitter = Splitter.on(',').trimResults().omitEmptyStrings();
        private final Joiner dimensionJoiner = Joiner.on(',');

        private final IParam<DataSource, String> dbName = onString("", "dbName");
        private final IParam<DataSource, String> tableName = onString("", "tableName");
        private final IParam<DataSource, List<String>> dimColumns = onStringList(ImmutableList.of(), "dimColumns", dimensionSplitter, dimensionJoiner);
        private final IParam<DataSource, String> periodColumn = onString("", "periodColumn");
        private final IParam<DataSource, String> valueColumn = onString("", "valueColumn");
        private final IParam<DataSource, DataFormat> dataFormat = onDataFormat(DataFormat.DEFAULT, "locale", "datePattern", "numberPattern");
        private final IParam<DataSource, String> versionColumn = onString("", "versionColumn");
        private final IParam<DataSource, String> labelColumn = onString("", "labelColumn");
        private final IParam<DataSource, ObsGathering> obsGathering = onObsGathering(ObsGathering.DEFAULT, "frequency", "aggregationType", "cleanMissing");
        private final IParam<DataSource, Long> cacheTtl = onLong(TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES), "cacheTtl");
        private final IParam<DataSource, Integer> cacheDepth = onInteger(1, "cacheDepth");

        @Override
        public String getVersion() {
            return "20130226";
        }

        @Override
        public AccessFileBean defaultValue() {
            AccessFileBean result = new AccessFileBean();
            result.setFile(new File(dbName.defaultValue()));
            result.setTable(tableName.defaultValue());
            result.setDimColumns(dimColumns.defaultValue());
            result.setPeriodColumn(periodColumn.defaultValue());
            result.setValueColumn(valueColumn.defaultValue());
            result.setObsFormat(dataFormat.defaultValue());
            result.setVersionColumn(versionColumn.defaultValue());
            result.setLabelColumn(labelColumn.defaultValue());
            result.setObsGathering(obsGathering.defaultValue());
            result.setCacheTtl(Duration.ofMillis(cacheTtl.defaultValue()));
            result.setCacheDepth(cacheDepth.defaultValue());
            return result;
        }

        @Override
        public AccessFileBean get(DataSource dataSource) {
            AccessFileBean result = new AccessFileBean();
            result.setFile(new File(dbName.get(dataSource)));
            result.setTable(tableName.get(dataSource));
            result.setDimColumns(dimColumns.get(dataSource));
            result.setPeriodColumn(periodColumn.get(dataSource));
            result.setValueColumn(valueColumn.get(dataSource));
            result.setObsFormat(dataFormat.get(dataSource));
            result.setVersionColumn(versionColumn.get(dataSource));
            result.setLabelColumn(labelColumn.get(dataSource));
            result.setObsGathering(obsGathering.get(dataSource));
            result.setCacheTtl(Duration.ofMillis(cacheTtl.get(dataSource)));
            result.setCacheDepth(cacheDepth.get(dataSource));
            return result;
        }

        @Override
        public void set(IConfig.Builder<?, DataSource> builder, AccessFileBean value) {
            dbName.set(builder, value.getFile().getPath());
            tableName.set(builder, value.getTable());
            dimColumns.set(builder, value.getDimColumns());
            periodColumn.set(builder, value.getPeriodColumn());
            valueColumn.set(builder, value.getValueColumn());
            dataFormat.set(builder, value.getObsFormat());
            versionColumn.set(builder, value.getVersionColumn());
            labelColumn.set(builder, value.getLabelColumn());
            obsGathering.set(builder, value.getObsGathering());
            cacheTtl.set(builder, value.getCacheTtl().toMillis());
            cacheDepth.set(builder, value.getCacheDepth());
        }

        @Override
        public IParam<DataSet, CubeId> getCubeIdParam(DataSource dataSource) {
            return CubeSupport.idByName(CubeId.root(dimColumns.get(dataSource)));
        }
    }
}
