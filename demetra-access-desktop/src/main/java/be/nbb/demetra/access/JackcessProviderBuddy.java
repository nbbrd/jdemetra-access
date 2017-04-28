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

import com.google.common.base.Strings;
import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import ec.nbdemetra.db.DbColumnListCellRenderer;
import ec.nbdemetra.db.DbIcon;
import ec.nbdemetra.db.DbProviderBuddy;
import ec.nbdemetra.ui.tsproviders.IDataSourceProviderBuddy;
import ec.tstoolkit.utilities.GuavaCaches;
import ec.util.completion.AutoCompletionSource;
import static ec.util.completion.AutoCompletionSource.Behavior.ASYNC;
import static ec.util.completion.AutoCompletionSource.Behavior.NONE;
import static ec.util.completion.AutoCompletionSource.Behavior.SYNC;
import ec.util.completion.ExtAutoCompletionSource;
import java.awt.Image;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import javax.swing.Icon;
import javax.swing.ListCellRenderer;
import org.openide.util.ImageUtilities;
import org.openide.util.lookup.ServiceProvider;

/**
 *
 * @author Philippe Charles
 */
@ServiceProvider(service = IDataSourceProviderBuddy.class)
public final class JackcessProviderBuddy extends DbProviderBuddy<JackcessBean> {

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
        return ExtAutoCompletionSource
                .builder(o -> getTables(bean))
                .behavior(o -> !Strings.isNullOrEmpty(bean.getDbName()) ? ASYNC : NONE)
                .cache(GuavaCaches.ttlCacheAsMap(Duration.ofMinutes(1)), o -> bean.getDbName(), SYNC)
                .build();
    }

    @Override
    protected AutoCompletionSource getColumnSource(JackcessBean bean) {
        return ExtAutoCompletionSource
                .builder(o -> getColumns(bean))
                .behavior(o -> !Strings.isNullOrEmpty(bean.getDbName()) && !Strings.isNullOrEmpty(bean.getTableName()) ? ASYNC : NONE)
                .valueToString(Column::getName)
                .cache(GuavaCaches.ttlCacheAsMap(Duration.ofMinutes(1)), o -> bean.getDbName() + "/" + bean.getTableName(), SYNC)
                .build();
    }

    @Override
    protected ListCellRenderer getColumnRenderer(JackcessBean bean) {
        return new ColumnRenderer();
    }

    private static Database openDatabase(JackcessBean bean) throws IOException {
        return new DatabaseBuilder(bean.getFile()).setReadOnly(true).open();
    }

    private static List<String> getTables(JackcessBean bean) throws IOException {
        try (Database o = openDatabase(bean)) {
            return new ArrayList(o.getTableNames());
        }
    }

    private static List<Column> getColumns(JackcessBean bean) throws IOException {
        try (Database o = openDatabase(bean)) {
            return new ArrayList<>(o.getTable(bean.getTableName()).getColumns());
        }
    }

    private static final class ColumnRenderer extends DbColumnListCellRenderer<Column> {

        @Override
        protected String getName(Column value) {
            return value.getName();
        }

        @Override
        protected String getTypeName(Column value) {
            return value.getType().name();
        }

        @Override
        protected Icon getTypeIcon(Column value) {
            switch (value.getType()) {
                case BINARY:
                    return DbIcon.DATA_TYPE_BINARY;
                case BOOLEAN:
                    return DbIcon.DATA_TYPE_BOOLEAN;
                case BYTE:
                case INT:
                case LONG:
                case NUMERIC:
                case DOUBLE:
                case FLOAT:
                    return DbIcon.DATA_TYPE_DOUBLE;
                case SHORT_DATE_TIME:
                    return DbIcon.DATA_TYPE_DATETIME;
                case TEXT:
                    return DbIcon.DATA_TYPE_STRING;
                case MEMO:
                case COMPLEX_TYPE:
                case GUID:
                case MONEY:
                case OLE:
                case UNKNOWN_0D:
                case UNKNOWN_11:
                case UNSUPPORTED_FIXEDLEN:
                case UNSUPPORTED_VARLEN:
                    return DbIcon.DATA_TYPE_NULL;
            }
            return null;
        }
    }
}
