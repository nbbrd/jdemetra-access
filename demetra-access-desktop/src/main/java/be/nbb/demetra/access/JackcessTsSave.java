/*
 * Copyright 2016 National Bank of Belgium
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

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import ec.nbdemetra.ui.notification.MessageType;
import ec.nbdemetra.ui.notification.NotifyUtil;
import ec.nbdemetra.ui.ns.AbstractNamedService;
import ec.nbdemetra.ui.properties.IBeanEditor;
import ec.nbdemetra.ui.properties.NodePropertySetBuilder;
import ec.nbdemetra.ui.properties.OpenIdePropertySheetBeanEditor;
import ec.nbdemetra.ui.tssave.ITsSave;
import ec.tss.Ts;
import ec.tss.TsCollectionInformation;
import ec.tss.TsInformation;
import ec.tss.TsInformationType;
import ec.tstoolkit.timeseries.simplets.TsObservation;
import ec.tstoolkit.timeseries.simplets.TsPeriod;
import ec.tstoolkit.utilities.NextJdk;
import ec.util.desktop.Desktop;
import ec.util.desktop.DesktopManager;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import javax.swing.SwingWorker;
import javax.swing.filechooser.FileFilter;
import org.netbeans.api.progress.ProgressHandle;
import org.netbeans.api.progress.ProgressHandleFactory;
import org.openide.filesystems.FileChooserBuilder;
import org.openide.nodes.Sheet;
import org.openide.util.Exceptions;
import org.openide.util.lookup.ServiceProvider;

/**
 *
 * @author Philippe Charles
 * @since 2.1.0
 */
@ServiceProvider(service = ITsSave.class)
public final class JackcessTsSave extends AbstractNamedService implements ITsSave {

    private final FileChooserBuilder fileChooserBuilder;
    private final OptionsEditor optionsEditor;
    private final OptionsBean optionsBean;

    public JackcessTsSave() {
        super(ITsSave.class, "JackcessTsSave");
        this.fileChooserBuilder = new FileChooserBuilder(JackcessTsSave.class)
                .setFileFilter(new SaveFileFilter())
                .setSelectionApprover(new SaveSelectionApprover());
        this.optionsEditor = new OptionsEditor();
        this.optionsBean = new OptionsBean();
    }

    @Override
    public void save(Ts[] ts) {
        File file = fileChooserBuilder.showSaveDialog();
        if (file != null) {
            if (optionsEditor.editBean(optionsBean)) {
                save(ts, file, optionsBean);
            }
        }
    }

    @Override
    public String getDisplayName() {
        return "Access file";
    }

    //<editor-fold defaultstate="collapsed" desc="Implementation details">
    private void save(final Ts[] data, final File file, final OptionsBean opts) {
        new SwingWorker<Void, String>() {
            final ProgressHandle progressHandle = ProgressHandleFactory.createHandle("Saving to access file");

            @Override
            protected Void doInBackground() throws Exception {
                progressHandle.start();
                progressHandle.progress("Initializing content");
                TsCollectionInformation col = getContent(data);

                progressHandle.progress("Creating content");
                try (Database database = getDatabase(file, opts.fileFormat, opts.writeOption)) {
                    Table table = getTable(database, opts.writeOption, opts.tableName, opts.dimColumn, opts.periodColumn, opts.valueColumn, opts.versionColumn);
                    BiFunction<String, TsObservation, Object[]> rowFunc = getRowFunc(table, opts.beginPeriod, opts.dimColumn, opts.periodColumn, opts.valueColumn, opts.versionColumn);

                    progressHandle.progress("Writing content");
                    writeContent(table, rowFunc, col, 10000);
                }

                return null;
            }

            @Override
            protected void done() {
                progressHandle.finish();
                try {
                    get();
                    NotifyUtil.show("Access file saved", "Show in folder", MessageType.SUCCESS, new ShowInFolderActionListener(file), null, null);
                } catch (InterruptedException | ExecutionException ex) {
                    NotifyUtil.error("Saving to access file failed", ex.getMessage(), ex);
                }
            }
        }.execute();
    }

    private static TsCollectionInformation getContent(Ts[] data) {
        TsCollectionInformation result = new TsCollectionInformation();
        for (Ts o : data) {
            result.items.add(new TsInformation(o, TsInformationType.All));
        }
        return result;
    }

    private static Database getDatabase(File file, Database.FileFormat fileFormat, WriteOption writeOption) throws IOException {
        return file.exists() && writeOption == WriteOption.APPEND
                ? DatabaseBuilder.open(file)
                : DatabaseBuilder.create(fileFormat, file);
    }

    private static Table getTable(Database database, WriteOption writeOption, String tableName, String dimColumn, String periodColumn, String valueColumn, String versionColumn) throws IOException {
        if (database.getTableNames().contains(tableName) && writeOption == WriteOption.APPEND) {
            return database.getTable(tableName);
        }
        TableBuilder builder = new TableBuilder(tableName);
        builder.addColumn(new ColumnBuilder(dimColumn, DataType.TEXT));
        builder.addColumn(new ColumnBuilder(periodColumn, DataType.SHORT_DATE_TIME));
        builder.addColumn(new ColumnBuilder(valueColumn, DataType.DOUBLE));
        if (!versionColumn.isEmpty()) {
            builder.addColumn(new ColumnBuilder(versionColumn, DataType.SHORT_DATE_TIME));
            builder.setPrimaryKey(dimColumn, periodColumn, versionColumn);
        } else {
            builder.setPrimaryKey(dimColumn, periodColumn);
        }
        return builder.toTable(database);
    }

    private static BiFunction<String, TsObservation, Object[]> getRowFunc(Table table, boolean beginPeriod, String dimColumn, String periodColumn, String valueColumn, String versionColumn) {
        final Function<TsPeriod, Date> toDate = beginPeriod ? TsPeriodDateFunc.FIRST : TsPeriodDateFunc.LAST;

        final int columnCount = table.getColumnCount();

        final int dimIndex = table.getColumn(dimColumn).getColumnIndex();
        final int periodIndex = table.getColumn(periodColumn).getColumnIndex();
        final int valueIndex = table.getColumn(valueColumn).getColumnIndex();

        if (versionColumn.isEmpty()) {
            return new BiFunction<String, TsObservation, Object[]>() {
                @Override
                public Object[] apply(String name, TsObservation o) {
                    Object[] result = new Object[columnCount];
                    result[dimIndex] = name;
                    result[periodIndex] = toDate.apply(o.getPeriod());
                    result[valueIndex] = o.getValue();
                    return result;
                }
            };
        }

        final int versionIndex = table.getColumn(versionColumn).getColumnIndex();
        final Date version = new Date();

        return new BiFunction<String, TsObservation, Object[]>() {
            @Override
            public Object[] apply(String name, TsObservation o) {
                Object[] result = new Object[columnCount];
                result[dimIndex] = name;
                result[periodIndex] = toDate.apply(o.getPeriod());
                result[valueIndex] = o.getValue();
                result[versionIndex] = version;
                return result;
            }
        };
    }

    private static void writeContent(Table table, BiFunction<String, TsObservation, Object[]> toRow, TsCollectionInformation col, int threshold) throws IOException {
        List<Object[]> bulk = new ArrayList<>();
        for (TsInformation ts : col.items) {
            if (ts.hasData() && ts.data != null) {
                FluentIterable.from(ts.data).transform(asFunction(toRow, ts.name)).copyInto(bulk);
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

    @NextJdk("")
    private interface BiFunction<T, U, R> {

        R apply(T t, U u);
    }

    private static <T, U, R> Function<U, R> asFunction(final BiFunction<T, U, R> biFunc, final T t) {
        return new Function<U, R>() {
            @Override
            public R apply(U input) {
                return biFunc.apply(t, input);
            }
        };
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

    @Deprecated
    private static final class ShowInFolderActionListener implements ActionListener {

        private final File file;

        public ShowInFolderActionListener(File file) {
            this.file = file;
        }

        @Override
        public void actionPerformed(ActionEvent e) {
            Desktop desktop = DesktopManager.get();
            if (desktop.isSupported(Desktop.Action.SHOW_IN_FOLDER)) {
                try {
                    desktop.showInFolder(file);
                } catch (IOException ex) {
                    Exceptions.printStackTrace(ex);
                }
            }
        }
    }

    private static final class SaveFileFilter extends FileFilter {

        private final JackcessFileFilter delegate = new JackcessFileFilter();

        @Override
        public boolean accept(File f) {
            return f.isDirectory() || delegate.accept(f);
        }

        @Override
        public String getDescription() {
            return delegate.getDescription();
        }
    }

    private static final class SaveSelectionApprover implements FileChooserBuilder.SelectionApprover {

        @Override
        public boolean approve(File[] selection) {
            return selection.length != 0;
        }
    }

    public enum WriteOption {
        APPEND, TRUNCATE_EXISTING;
    }

    public static final class OptionsBean {

        public String tableName = "TimeSeries";
        public String dimColumn = "Name";
        public String periodColumn = "Period";
        public String valueColumn = "Value";
        public String versionColumn = "";

        public WriteOption writeOption = WriteOption.TRUNCATE_EXISTING;
        public Database.FileFormat fileFormat = Database.FileFormat.V2010;
        public boolean beginPeriod = true;
    }

    private static final class OptionsEditor implements IBeanEditor {

        private Sheet getSheet(OptionsBean bean) {
            Sheet result = new Sheet();
            NodePropertySetBuilder b = new NodePropertySetBuilder();

            b.reset("Target");
            b.with(String.class).selectField(bean, "tableName").display("Table name").add();
            b.with(String.class).selectField(bean, "dimColumn").display("Dimension column").add();
            b.with(String.class).selectField(bean, "periodColumn").display("Period column").add();
            b.with(String.class).selectField(bean, "valueColumn").display("Value column").add();
            b.with(String.class).selectField(bean, "versionColumn").display("Version column").add();
            result.put(b.build());

            b.reset("Options");
            b.withEnum(WriteOption.class).selectField(bean, "writeOption").display("Write option").add();
            b.withEnum(Database.FileFormat.class).selectField(bean, "fileFormat").display("File format").add();
            b.withBoolean().selectField(bean, "beginPeriod").display("Begin period").add();
            result.put(b.build());

            return result;
        }

        @Override
        final public boolean editBean(Object bean) {
            OptionsBean config = (OptionsBean) bean;
            return OpenIdePropertySheetBeanEditor.editSheet(getSheet(config), "Options", null);
        }
    }
    //</editor-fold>
}
