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
import com.healthmarketscience.jackcess.Database.FileFormat;
import java.io.File;
import java.io.FileFilter;
import java.util.EnumSet;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 *
 * @author Philippe Charles
 * @since 2.1.0
 */
public final class JackcessFileFilter implements FileFilter {

    private final EnumSet<Database.FileFormat> formats;
    private final String description;

    public JackcessFileFilter() {
        this.formats = EnumSet.allOf(Database.FileFormat.class);
        this.description = new StringBuilder()
                .append("Access file (")
                .append(formats.stream().map(FileFormat::getFileExtension).distinct().collect(Collectors.joining(",")))
                .append(")")
                .toString();
    }

    @Override
    public boolean accept(File f) {
        String path = f.getPath().toLowerCase(Locale.ROOT);
        return formats.stream().anyMatch(o -> path.endsWith(o.getFileExtension()));
    }

    public String getDescription() {
        return description;
    }
}
