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
import ec.nbdemetra.ui.properties.DhmsPropertyEditor;
import ec.nbdemetra.ui.properties.FileLoaderFileFilter;
import ec.nbdemetra.ui.properties.NodePropertySetBuilder;
import ec.nbdemetra.ui.properties.PropertySheetDialogBuilder;
import ec.nbdemetra.ui.tsproviders.IDataSourceProviderBuddy;
import ec.tss.tsproviders.DataSet;
import ec.tss.tsproviders.IFileLoader;
import ec.tss.tsproviders.TsProviders;
import ec.tss.tsproviders.utils.DataFormat;
import ec.tss.tsproviders.utils.ObsGathering;
import ec.tstoolkit.timeseries.TsAggregationType;
import ec.tstoolkit.timeseries.simplets.TsFrequency;
import ec.util.completion.AutoCompletionSource;
import internal.demetra.jackcess.JackcessAutoCompletion;
import internal.demetra.jackcess.JackcessColumnRenderer;
import java.awt.Image;
import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import javax.swing.ListCellRenderer;
import org.openide.nodes.Sheet;
import org.openide.util.ImageUtilities;
import org.openide.util.NbBundle;
import org.openide.util.lookup.ServiceProvider;

/**
 *
 * @author Philippe Charles
 * @since 2.2.0
 */
@ServiceProvider(service = IDataSourceProviderBuddy.class, supersedes = "be.nbb.demetra.access.JackcessProviderBuddy")
public final class AccessFileProviderBuddy implements IDataSourceProviderBuddy {

    @Override
    public String getProviderName() {
        return AccessFileProvider.NAME;
    }

    @Override
    public Image getIcon(int type, boolean opened) {
        return ImageUtilities.loadImage("be/nbb/demetra/access/document-access.png", true);
    }

    @Override
    public Image getIcon(DataSet dataSet, int type, boolean opened) {
        switch (dataSet.getKind()) {
            case COLLECTION:
                return ImageUtilities.loadImage("ec/nbdemetra/ui/nodes/folder.png", true);
            case SERIES:
                return ImageUtilities.loadImage("ec/nbdemetra/ui/nodes/chart_line.png", true);
            case DUMMY:
                return null;
        }
        return IDataSourceProviderBuddy.super.getIcon(dataSet, type, opened);
    }

    @Override
    public Image getIcon(IOException ex, int type, boolean opened) {
        return ImageUtilities.loadImage("ec/nbdemetra/ui/nodes/exclamation-red.png", true);
    }

    @Override
    public boolean editBean(String title, Object bean) throws IntrospectionException {
        if (bean instanceof AccessFileBean) {
            Optional<AccessFileProvider> provider = lookupProvider();
            if (provider.isPresent()) {
                return new PropertySheetDialogBuilder()
                        .title(title)
                        .icon(getIcon(BeanInfo.ICON_COLOR_16x16, false))
                        .editSheet(createSheet((AccessFileBean) bean, provider.get()));
            }
        }
        return IDataSourceProviderBuddy.super.editBean(title, bean);
    }

    //<editor-fold defaultstate="collapsed" desc="Implementation details">
    private static Optional<AccessFileProvider> lookupProvider() {
        return TsProviders.lookup(AccessFileProvider.class, AccessFileProvider.NAME).toJavaUtil();
    }

    private static Sheet createSheet(AccessFileBean bean, IFileLoader loader) {
        Sheet result = new Sheet();
        NodePropertySetBuilder b = new NodePropertySetBuilder();
        result.put(withSource(b.reset("Source"), bean, loader).build());
        result.put(withStructure(b.reset("Structure"), bean, loader).build());
        result.put(withOptions(b.reset("Options"), bean).build());
        result.put(withCache(b.reset("Cache").description("Mechanism used to improve performance."), bean).build());
        return result;
    }

    @NbBundle.Messages({
        "bean.file.display=Database file",
        "bean.file.description=The path to the database file.",
        "bean.table.display=Table name",
        "bean.table.description=The name of the table (or view) that contains observations."})
    private static NodePropertySetBuilder withSource(NodePropertySetBuilder b, AccessFileBean bean, IFileLoader loader) {
        b.withFile()
                .select(bean, "file")
                .filterForSwing(new FileLoaderFileFilter(loader))
                .paths(loader.getPaths())
                .directories(false)
                .display(Bundle.bean_file_display())
                .description(Bundle.bean_file_description())
                .add();
        b.withAutoCompletion()
                .select(bean, "table")
                .source(JackcessAutoCompletion.onTables(loader, bean::getFile))
                .display(Bundle.bean_table_display())
                .description(Bundle.bean_table_description())
                .add();
        return b;
    }

    @NbBundle.Messages({
        "bean.dimColumns.display=Dimension columns",
        "bean.dimColumns.description=A comma-separated list of column names that defines the dimensions of the table.",
        "bean.periodColumn.display=Period column",
        "bean.periodColumn.description=A column name that defines the period of an observation.",
        "bean.valueColumn.display=Value column",
        "bean.valueColumn.description=A column name that defines the value of an observation.",
        "bean.versionColumn.display=Version column",
        "bean.versionColumn.description=An optional column name that defines the version of an observation.",
        "bean.labelColumn.display=Label column",
        "bean.labelColumn.description=An optional column name that defines the label of a series."})
    private static NodePropertySetBuilder withStructure(NodePropertySetBuilder b, AccessFileBean bean, IFileLoader loader) {
        AutoCompletionSource columnCompletion = JackcessAutoCompletion.onColumns(loader, bean::getFile, bean::getTable);
        ListCellRenderer columnRenderer = new JackcessColumnRenderer();
        b.withAutoCompletion()
                .select(bean, "dimColumns", List.class, Joiner.on(',')::join, Splitter.on(',').trimResults().omitEmptyStrings()::splitToList)
                .source(columnCompletion)
                .separator(",")
                .defaultValueSupplier(() -> JackcessAutoCompletion.getDefaultColumnsAsString(loader, bean::getFile, bean::getTable, ","))
                .cellRenderer(columnRenderer)
                .display(Bundle.bean_dimColumns_display())
                .description(Bundle.bean_dimColumns_description())
                .add();
        b.withAutoCompletion()
                .select(bean, "periodColumn")
                .source(columnCompletion)
                .cellRenderer(columnRenderer)
                .display(Bundle.bean_periodColumn_display())
                .description(Bundle.bean_periodColumn_description())
                .add();
        b.withAutoCompletion()
                .select(bean, "valueColumn")
                .source(columnCompletion)
                .cellRenderer(columnRenderer)
                .display(Bundle.bean_valueColumn_display())
                .description(Bundle.bean_valueColumn_description())
                .add();
        b.withAutoCompletion()
                .select(bean, "versionColumn")
                .source(columnCompletion)
                .cellRenderer(columnRenderer)
                .display(Bundle.bean_versionColumn_display())
                .description(Bundle.bean_versionColumn_description())
                .add();
        b.withAutoCompletion()
                .select(bean, "labelColumn")
                .source(columnCompletion)
                .cellRenderer(columnRenderer)
                .display(Bundle.bean_labelColumn_display())
                .display(Bundle.bean_labelColumn_display())
                .description(Bundle.bean_labelColumn_description())
                .add();
        return b;
    }

    @NbBundle.Messages({
        "bean.dataFormat.display=Data format",
        "bean.dataFormat.description=The format used to parse dates and numbers from character strings.",
        "bean.frequency.display=Frequency",
        "bean.frequency.description=The frequency of the observations in the table. An undefined frequency allows the provider to guess it.",
        "bean.aggregationType.display=Aggregation type",
        "bean.aggregationType.description=The aggregation method to use when a frequency is defined."})
    private static NodePropertySetBuilder withOptions(NodePropertySetBuilder b, AccessFileBean bean) {
        b.with(DataFormat.class)
                .select(bean, "obsFormat")
                .display(Bundle.bean_dataFormat_display())
                .description(Bundle.bean_dataFormat_description())
                .add();
        b.withEnum(TsFrequency.class)
                .select(bean, "obsGathering", ObsGathering.class, ObsGathering::getFrequency, o -> bean.getObsGathering().withFrequency(o))
                .name("frequency")
                .display(Bundle.bean_frequency_display())
                .description(Bundle.bean_frequency_description())
                .add();
        b.withEnum(TsAggregationType.class)
                .select(bean, "obsGathering", ObsGathering.class, ObsGathering::getAggregationType, o -> bean.getObsGathering().withAggregationType(o))
                .name("aggregationType")
                .display(Bundle.bean_aggregationType_display())
                .description(Bundle.bean_aggregationType_description())
                .add();
        return b;
    }

    @NbBundle.Messages({
        "bean.cacheDepth.display=Depth",
        "bean.cacheDepth.description=The data retrieval depth. It is always more performant to get one big chunk of data instead of several smaller parts. The downside of it is the increase of memory usage. Setting this value to zero disables the cache.",
        "bean.cacheTtl.display=Time to live",
        "bean.cacheTtl.description=The lifetime of the data stored in the cache. Setting this value to zero disables the cache."})
    private static NodePropertySetBuilder withCache(NodePropertySetBuilder b, AccessFileBean bean) {
        b.withInt()
                .select(bean, "cacheDepth")
                .display(Bundle.bean_cacheDepth_display())
                .description(Bundle.bean_cacheDepth_description())
                .min(0)
                .add();
        b.with(long.class)
                .select(bean, "cacheTtl", Duration.class, Duration::toMillis, Duration::ofMillis)
                .editor(DhmsPropertyEditor.class)
                .display(Bundle.bean_cacheTtl_display())
                .description(Bundle.bean_cacheTtl_description())
                .add();
        return b;
    }
    //</editor-fold>
}
