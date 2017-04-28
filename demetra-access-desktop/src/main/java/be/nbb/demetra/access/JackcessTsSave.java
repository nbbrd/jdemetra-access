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

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Table;
import ec.nbdemetra.ui.DemetraUiIcon;
import ec.nbdemetra.ui.SingleFileExporter;
import ec.nbdemetra.ui.properties.NodePropertySetBuilder;
import ec.nbdemetra.ui.properties.PropertySheetDialogBuilder;
import ec.nbdemetra.ui.tssave.ITsSave;
import ec.tss.Ts;
import ec.tss.TsCollection;
import ec.tss.TsCollectionInformation;
import ec.tstoolkit.timeseries.simplets.TsObservation;
import ec.util.various.swing.OnAnyThread;
import ec.util.various.swing.OnEDT;
import internal.demetra.jackcess.JackcessTsExport;
import internal.desktop.TsSaveUtil;
import java.awt.Image;
import java.io.File;
import java.io.IOException;
import java.util.function.BiFunction;
import javax.swing.filechooser.FileFilter;
import org.netbeans.api.progress.ProgressHandle;
import org.openide.filesystems.FileChooserBuilder;
import org.openide.nodes.Sheet;
import org.openide.util.ImageUtilities;
import org.openide.util.lookup.ServiceProvider;

/**
 *
 * @author Philippe Charles
 * @since 2.1.0
 */
@ServiceProvider(service = ITsSave.class)
public final class JackcessTsSave implements ITsSave {

    private final FileChooserBuilder fileChooser;
    private final OptionsBean options;

    public JackcessTsSave() {
        this.fileChooser = TsSaveUtil.fileChooser(JackcessTsSave.class).setFileFilter(new SaveFileFilter());
        this.options = new OptionsBean();
    }

    @Override
    public String getName() {
        return "JackcessTsSave";
    }

    @Override
    public String getDisplayName() {
        return "Access file";
    }

    @Override
    public Image getIcon(int type, boolean opened) {
        return ImageUtilities.icon2Image(DemetraUiIcon.PUZZLE_16);
    }

    @Override
    public void save(Ts[] input) {
        save(TsSaveUtil.toCollections(input));
    }

    @Override
    public void save(TsCollection[] input) {
        TsSaveUtil.saveToFile(fileChooser, o -> editBean(options), o -> store(input, o, options));
    }

    //<editor-fold defaultstate="collapsed" desc="Implementation details">
    @OnEDT
    private static void store(TsCollection[] data, File file, OptionsBean opts) {
        new SingleFileExporter()
                .file(file)
                .progressLabel("Saving to access file")
                .onErrorNotify("Saving to access file failed")
                .onSussessNotify("Access file saved")
                .execAsync((f, ph) -> store(data, f, opts, ph));
    }

    @OnAnyThread
    private static File store(TsCollection[] data, File file, OptionsBean opts, ProgressHandle ph) throws IOException {
        ph.progress("Loading time series");
        TsCollectionInformation content = TsSaveUtil.loadContent(data);

        ph.progress("Creating content");
        try (Database database = JackcessTsExport.getDatabase(file, opts.fileFormat, opts.writeOption)) {
            Table table = JackcessTsExport.getTable(database, opts.writeOption, opts.tableName, opts.dimColumn, opts.periodColumn, opts.valueColumn, opts.versionColumn);
            BiFunction<String, TsObservation, Object[]> rowFunc = JackcessTsExport.getRowFunc(table, opts.beginPeriod, opts.dimColumn, opts.periodColumn, opts.valueColumn, opts.versionColumn);

            ph.progress("Writing file");
            JackcessTsExport.writeContent(table, rowFunc, content, 10000);
        }

        return file;
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

    public static final class OptionsBean {

        public String tableName = "TimeSeries";
        public String dimColumn = "Name";
        public String periodColumn = "Period";
        public String valueColumn = "Value";
        public String versionColumn = "";

        public JackcessTsExport.WriteOption writeOption = JackcessTsExport.WriteOption.TRUNCATE_EXISTING;
        public Database.FileFormat fileFormat = Database.FileFormat.V2010;
        public boolean beginPeriod = true;
    }

    private static boolean editBean(OptionsBean bean) {
        return new PropertySheetDialogBuilder().title("Options").editSheet(getSheet(bean));
    }

    private static Sheet getSheet(OptionsBean bean) {
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
        b.withEnum(JackcessTsExport.WriteOption.class).selectField(bean, "writeOption").display("Write option").add();
        b.withEnum(Database.FileFormat.class).selectField(bean, "fileFormat").display("File format").add();
        b.withBoolean().selectField(bean, "beginPeriod").display("Begin period").add();
        result.put(b.build());

        return result;
    }
    //</editor-fold>
}
