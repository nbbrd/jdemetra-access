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
package internal.desktop;

import ec.nbdemetra.ui.SingleFileExporter;
import ec.tss.Ts;
import ec.tss.TsCollection;
import ec.tss.TsCollectionInformation;
import ec.tss.TsFactory;
import ec.tss.TsInformation;
import ec.tss.TsInformationType;
import java.io.File;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.openide.filesystems.FileChooserBuilder;

/**
 *
 * @author Philippe Charles
 */
@lombok.experimental.UtilityClass
public class TsSaveUtil {

    public TsCollection[] toCollections(Ts[] input) {
        TsCollection col = TsFactory.instance.createTsCollection();
        col.quietAppend(Arrays.asList(input));
        return new TsCollection[]{col};
    }

    public TsCollectionInformation loadContent(TsCollection[] data) {
        TsCollectionInformation result = new TsCollectionInformation();
        for (TsCollection col : data) {
            col.load(TsInformationType.All);
            col.stream().map(o -> new TsInformation(o, TsInformationType.All)).forEach(result.items::add);
        }
        return result;
    }

    public void saveToFile(FileChooserBuilder fileChooser, Predicate<File> predicate, Consumer<File> action) {
        File target = fileChooser.showSaveDialog();
        if (target != null && predicate.test(target)) {
            action.accept(target);
        }
    }

    public FileChooserBuilder fileChooser(Class type) {
        return new FileChooserBuilder(type).setSelectionApprover(SingleFileExporter.overwriteApprover());
    }
}
