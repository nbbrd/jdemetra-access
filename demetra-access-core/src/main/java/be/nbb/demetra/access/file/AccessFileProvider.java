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

import be.nbb.demetra.access.JackcessFileFilter;
import ec.tss.ITsProvider;
import ec.tss.tsproviders.DataSet;
import ec.tss.tsproviders.DataSource;
import ec.tss.tsproviders.HasDataMoniker;
import ec.tss.tsproviders.HasDataSourceBean;
import ec.tss.tsproviders.HasDataSourceMutableList;
import ec.tss.tsproviders.HasFilePaths;
import ec.tss.tsproviders.IFileLoader;
import ec.tss.tsproviders.cube.CubeAccessor;
import ec.tss.tsproviders.cube.CubeId;
import ec.tss.tsproviders.cube.CubeSupport;
import ec.tss.tsproviders.cube.TableAsCubeAccessor;
import ec.tss.tsproviders.cube.TableDataParams;
import ec.tss.tsproviders.cursor.HasTsCursor;
import ec.tss.tsproviders.utils.DataSourcePreconditions;
import ec.tss.tsproviders.utils.IParam;
import ec.tstoolkit.utilities.GuavaCaches;
import internal.demetra.jackcess.JackcessTableAsCubeResource;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import org.openide.util.lookup.ServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Philippe Charles
 * @since 2.2.0
 */
@ServiceProvider(service = ITsProvider.class, supersedes = "be.nbb.demetra.access.JackcessProvider")
public final class AccessFileProvider implements IFileLoader {

    public static final String NAME = "ACCESS";

    @lombok.experimental.Delegate
    private final HasDataSourceMutableList mutableListSupport;

    @lombok.experimental.Delegate
    private final HasDataMoniker monikerSupport;

    @lombok.experimental.Delegate
    private final HasDataSourceBean<AccessFileBean> beanSupport;

    @lombok.experimental.Delegate
    private final HasFilePaths filePathSupport;

    @lombok.experimental.Delegate(excludes = HasTsCursor.class)
    private final CubeSupport cubeSupport;

    @lombok.experimental.Delegate
    private final ITsProvider tsSupport;

    private final JackcessFileFilter fileFilter;

    public AccessFileProvider() {
        Logger logger = LoggerFactory.getLogger(NAME);
        ConcurrentMap<DataSource, CubeAccessor> cache = GuavaCaches.softValuesCacheAsMap();
        AccessFileParam param = new AccessFileParam.V1();

        this.mutableListSupport = HasDataSourceMutableList.of(NAME, logger, cache::remove);
        this.monikerSupport = HasDataMoniker.usingUri(NAME);
        this.beanSupport = HasDataSourceBean.of(NAME, param, param.getVersion());
        this.filePathSupport = HasFilePaths.of(cache::clear);
        this.cubeSupport = CubeSupport.of(new AccessFileCubeResource(cache, param, filePathSupport));
        this.tsSupport = CubeSupport.asTsProvider(NAME, logger, cubeSupport, monikerSupport, cache::clear);

        this.fileFilter = new JackcessFileFilter();
    }

    @Override
    public String getDisplayName() {
        return "Access files";
    }

    @Override
    public String getFileDescription() {
        return fileFilter.getDescription();
    }

    @Override
    public boolean accept(File pathname) {
        return fileFilter.accept(pathname);
    }

    @lombok.AllArgsConstructor
    private static final class AccessFileCubeResource implements CubeSupport.Resource {

        private final ConcurrentMap<DataSource, CubeAccessor> cache;
        private final AccessFileParam param;
        private final HasFilePaths paths;

        @Override
        public CubeAccessor getAccessor(DataSource dataSource) throws IOException, IllegalArgumentException {
            DataSourcePreconditions.checkProvider(AccessFileProvider.NAME, dataSource);
            CubeAccessor result = cache.get(dataSource);
            if (result == null) {
                result = load(dataSource);
                cache.put(dataSource, result);
            }
            return result;
        }

        @Override
        public IParam<DataSet, CubeId> getIdParam(DataSource dataSource) throws IOException, IllegalArgumentException {
            DataSourcePreconditions.checkProvider(AccessFileProvider.NAME, dataSource);
            return param.getCubeIdParam(dataSource);
        }

        private CubeAccessor load(DataSource key) throws FileNotFoundException {
            AccessFileBean bean = param.get(key);
            JackcessTableAsCubeResource result = JackcessTableAsCubeResource.create(paths, bean.getFile(), bean.getTable(), bean.getDimColumns(), toDataParams(bean), bean.getObsGathering(), bean.getLabelColumn());
            return TableAsCubeAccessor.create(result).bulk(bean.getCacheDepth(), GuavaCaches.ttlCacheAsMap(bean.getCacheTtl()));
        }

        private static TableDataParams toDataParams(AccessFileBean bean) {
            return TableDataParams.builder()
                    .periodColumn(bean.getPeriodColumn())
                    .valueColumn(bean.getValueColumn())
                    .versionColumn(bean.getVersionColumn())
                    .obsFormat(bean.getObsFormat())
                    .build();
        }
    }
}
