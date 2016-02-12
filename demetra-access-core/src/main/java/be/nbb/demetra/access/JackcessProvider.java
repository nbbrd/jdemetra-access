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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import ec.tss.ITsProvider;
import ec.tss.TsAsyncMode;
import ec.tss.tsproviders.DataSource;
import ec.tss.tsproviders.IDataSourceBean;
import ec.tss.tsproviders.IFileLoader;
import ec.tss.tsproviders.db.DbAccessor;
import ec.tss.tsproviders.db.DbProvider;
import java.io.File;
import org.openide.util.lookup.ServiceProvider;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Philippe Charles
 */
@ServiceProvider(service = ITsProvider.class)
public final class JackcessProvider extends DbProvider<JackcessBean> implements IFileLoader {

    public static final String NAME = "ACCESS", VERSION = "20130226";
    private ImmutableList<File> paths;

    public JackcessProvider() {
        super(LoggerFactory.getLogger(JackcessProvider.class), NAME, TsAsyncMode.Once);
        this.paths = ImmutableList.of();
    }

    @Override
    protected DbAccessor<JackcessBean> loadFromBean(JackcessBean bean) throws Exception {
        return new JackcessAccessor(bean).memoize();
    }

    @Override
    public JackcessBean decodeBean(DataSource dataSource) throws IllegalArgumentException {
        return new JackcessBean(dataSource);
    }

    @Override
    public String getDisplayName() {
        return "Access files";
    }

    @Override
    public JackcessBean newBean() {
        return new JackcessBean();
    }

    @Override
    public DataSource encodeBean(Object bean) throws IllegalArgumentException {
        return IDataSourceBean.class.cast(bean).toDataSource(NAME, VERSION);
    }

    @Override
    public String getFileDescription() {
        return "Access file";
    }

    @Override
    public boolean accept(File pathname) {
        String tmp = pathname.getPath().toLowerCase();
        return tmp.endsWith(".mdb") || tmp.endsWith(".accdb");
    }

    @Override
    public File[] getPaths() {
        return Iterables.toArray(paths, File.class);
    }

    @Override
    public void setPaths(File[] paths) {
        this.paths = paths != null ? ImmutableList.copyOf(paths) : ImmutableList.<File>of();
    }
}
