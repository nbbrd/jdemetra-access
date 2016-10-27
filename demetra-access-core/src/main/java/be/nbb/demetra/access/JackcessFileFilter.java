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
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.healthmarketscience.jackcess.Database;
import java.io.File;
import java.io.FileFilter;

/**
 *
 * @author Philippe Charles
 * @since 2.1.0
 */
public final class JackcessFileFilter implements FileFilter {

    private final String description;

    public JackcessFileFilter() {
        this.description = new StringBuilder()
                .append("Access file (")
                .append(Joiner.on(", ").join(FluentIterable.of(Database.FileFormat.values()).transform(toFileExt()).toSet()))
                .append(")")
                .toString();
    }

    @Override
    public boolean accept(File f) {
        String tmp = f.getPath().toLowerCase();
        for (Database.FileFormat o : Database.FileFormat.values()) {
            if (tmp.endsWith(o.getFileExtension())) {
                return true;
            }
        }
        return false;
    }

    public String getDescription() {
        return description;
    }

    private Function<Database.FileFormat, String> toFileExt() {
        return new Function<Database.FileFormat, String>() {
            @Override
            public String apply(Database.FileFormat input) {
                return input.getFileExtension();
            }
        };
    }
}
