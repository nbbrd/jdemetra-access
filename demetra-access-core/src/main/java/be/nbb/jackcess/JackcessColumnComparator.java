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
package be.nbb.jackcess;

import com.google.common.primitives.Ints;
import com.healthmarketscience.jackcess.Column;
import java.util.Comparator;

/**
 *
 * @author Philippe Charles
 */
public enum JackcessColumnComparator implements Comparator<Column> {

    BY_COLUMN_INDEX {
        @Override
        public int compare(Column o1, Column o2) {
            return Ints.compare(o1.getColumnIndex(), o2.getColumnIndex());
        }
    };
}
