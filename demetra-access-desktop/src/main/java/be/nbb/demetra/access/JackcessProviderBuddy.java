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

import com.google.common.collect.Lists;
import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import ec.nbdemetra.db.DbColumnListCellRenderer;
import ec.nbdemetra.db.DbIcon;
import ec.nbdemetra.db.DbProviderBuddy;
import ec.nbdemetra.ui.tsproviders.IDataSourceProviderBuddy;
import ec.util.completion.AutoCompletionSource;
import ec.util.completion.AutoCompletionSource.Behavior;
import ec.util.completion.ext.QuickAutoCompletionSource;
import java.awt.Image;
import javax.swing.Icon;
import javax.swing.ListCellRenderer;
import org.openide.util.ImageUtilities;
import org.openide.util.lookup.ServiceProvider;

/**
 *
 * @author Philippe Charles
 */
@ServiceProvider(service = IDataSourceProviderBuddy.class)
public class JackcessProviderBuddy extends DbProviderBuddy<JackcessBean> {

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
        return new JackcessAutoCompletionSource<String>(bean) {
            @Override
            protected Iterable<String> getAllValues(Database database) throws Exception {
                return database.getTableNames();
            }
        };
    }

    @Override
    protected AutoCompletionSource getColumnSource(JackcessBean bean) {
        return new JackcessAutoCompletionSource<Column>(bean) {
            @Override
            protected Iterable<Column> getAllValues(Database database) throws Exception {
                return Lists.newArrayList(database.getTable(bean.getTableName()).getColumns());
            }

            @Override
            protected String getValueAsString(Column value) {
                return value.getName();
            }
        };
    }

    @Override
    protected ListCellRenderer getColumnRenderer(JackcessBean bean) {
        return new DbColumnListCellRenderer<Column>() {
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
        };
    }

    static abstract class JackcessAutoCompletionSource<T> extends QuickAutoCompletionSource<T> {

        final JackcessBean bean;

        JackcessAutoCompletionSource(JackcessBean bean) {
            this.bean = bean;
        }

        abstract protected Iterable<T> getAllValues(Database database) throws Exception;

        @Override
        protected Iterable<T> getAllValues() throws Exception {
            try (Database database = new DatabaseBuilder(bean.getFile()).setReadOnly(true).open()) {
                return getAllValues(database);
            }
        }

        @Override
        public Behavior getBehavior(String term) {
            return Behavior.ASYNC;
        }
    }
}
