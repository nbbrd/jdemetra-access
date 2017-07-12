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

import internal.demetra.jackcess.JackcessColumnRenderer;
import ec.nbdemetra.db.DbProviderBuddy;
import ec.nbdemetra.ui.tsproviders.IDataSourceProviderBuddy;
import ec.tss.tsproviders.TsProviders;
import ec.tstoolkit.utilities.GuavaCaches;
import ec.util.completion.AutoCompletionSource;
import ec.util.completion.AutoCompletionSources;
import internal.demetra.jackcess.JackcessAutoCompletion;
import java.awt.Image;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import javax.swing.ListCellRenderer;
import org.openide.util.ImageUtilities;
import org.openide.util.lookup.ServiceProvider;

/**
 *
 * @author Philippe Charles
 */
@Deprecated
@ServiceProvider(service = IDataSourceProviderBuddy.class)
public final class JackcessProviderBuddy extends DbProviderBuddy<JackcessBean> {

    private final ConcurrentMap autoCompletionCache;

    public JackcessProviderBuddy() {
        this.autoCompletionCache = GuavaCaches.ttlCacheAsMap(Duration.ofMinutes(1));
    }

    @Override
    protected boolean isFile() {
        return true;
    }

    @Override
    public String getProviderName() {
        return JackcessProvider.NAME;
    }

    @Override
    public Image getIcon(int type, boolean opened) {
        return ImageUtilities.loadImage("be/nbb/demetra/access/document-access.png", true);
    }

    @Override
    protected AutoCompletionSource getTableSource(JackcessBean bean) {
        return lookupProvider()
                .map(o -> JackcessAutoCompletion.onTables(o, bean::getFile, autoCompletionCache))
                .orElseGet(AutoCompletionSources::empty);
    }

    @Override
    protected AutoCompletionSource getColumnSource(JackcessBean bean) {
        return lookupProvider()
                .map(o -> JackcessAutoCompletion.onColumns(o, bean::getFile, bean::getTableName, autoCompletionCache))
                .orElseGet(AutoCompletionSources::empty);
    }

    @Override
    protected ListCellRenderer getColumnRenderer(JackcessBean bean) {
        return new JackcessColumnRenderer();
    }

    private static Optional<JackcessProvider> lookupProvider() {
        return TsProviders.lookup(JackcessProvider.class, JackcessProvider.NAME).toJavaUtil();
    }
}
