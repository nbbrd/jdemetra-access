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
package internal.demetra.jackcess;

import com.healthmarketscience.jackcess.Column;
import ec.nbdemetra.db.DbColumnListCellRenderer;
import ec.nbdemetra.db.DbIcon;
import javax.swing.Icon;

/**
 *
 * @author Philippe Charles
 */
public final class JackcessColumnRenderer extends DbColumnListCellRenderer<Column> {

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
