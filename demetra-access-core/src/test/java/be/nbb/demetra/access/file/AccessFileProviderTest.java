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

import be.nbb.demetra.access.JackcessAccessorTest;
import be.nbb.demetra.access.JackcessBean;
import be.nbb.demetra.access.JackcessProvider;
import static ec.tss.tsproviders.Assertions.assertThat;
import ec.tss.tsproviders.DataSource;
import ec.tss.tsproviders.IDataSourceLoaderAssert;
import ec.tss.tsproviders.utils.DataFormat;
import java.io.File;
import java.io.IOException;
import static java.util.Arrays.asList;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 *
 * @author Philippe Charles
 */
public class AccessFileProviderTest {

    private static File FILE;

    @BeforeAll
    public static void beforeClass() throws IOException {
        FILE = JackcessAccessorTest.createResource();
    }

    @AfterAll
    public static void afterClass() {
        FILE.delete();
    }

    @Test
    public void testEquivalence() throws IOException {
        assertThat(getProvider())
                .isEquivalentTo(getPreviousProvider(), AccessFileProviderTest::getSampleDataSource);
    }

    @Test
    public void testTspCompliance() {
        IDataSourceLoaderAssert.assertCompliance(AccessFileProviderTest::getProvider, o -> {
            AccessFileBean result = o.newBean();
            result.setFile(FILE);
            result.setTable("Top5");
            result.setDimColumns(asList("Freq", "Browser"));
            result.setPeriodColumn("Period");
            result.setValueColumn("MarketShare");
            result.setObsFormat(DataFormat.create(null, "yyyy-MM-dd", null));
            return result;
        });
    }

    private static AccessFileProvider getProvider() {
        AccessFileProvider result = new AccessFileProvider();
        return result;
    }

    private static JackcessProvider getPreviousProvider() {
        JackcessProvider result = new JackcessProvider();
        return result;
    }

    private static DataSource getSampleDataSource(JackcessProvider loader) {
        JackcessBean bean = loader.newBean();
        bean.setDbName(FILE.getPath());
        bean.setTableName("Top5");
        bean.setDimColumns("Freq, Browser");
        bean.setPeriodColumn("Period");
        bean.setValueColumn("MarketShare");
        bean.setDataFormat(DataFormat.create(null, "yyyy-MM-dd", null));
        return loader.encodeBean(bean);
    }
}
