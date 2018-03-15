/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.brgm.processors.geoformatconvertor;

import com.vividsolutions.jts.io.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.builder.ToStringExclude;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.junit.Assert.assertTrue;


public class WKBWKTConvertProcessorTest {

    private TestRunner testRunner;

    @Before
    public void init() {


    }


    @Test
    public void testProcessor() throws ParseException, IOException {

        InputStream content = new ByteArrayInputStream("{\"location_wgs84\":\"AQEAACDmEAAAYR9DsmdYDEANZypkMTJJQA==\"}".getBytes());
        testRunner = TestRunners.newTestRunner(WKBWKTConvertProcessor.class);
        testRunner.setProperty(WKBWKTConvertProcessor.WKBIN_PROPERTY, "$.location_wgs84");
        testRunner.setProperty(WKBWKTConvertProcessor.OUT_NAME_PROPERTY, "location_wgs84_WKT");
        testRunner.enqueue(content);
        testRunner.run(1);
        testRunner.assertQueueEmpty();
        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(WKBWKTConvertProcessor.SUCCESS);
        assertTrue("1 converted", results.size() == 1);
        MockFlowFile result = results.get(0);
        String resultValue = new String(testRunner.getContentAsByteArray(result));

        System.out.println("convertion: " + IOUtils.toString(testRunner.getContentAsByteArray(result)));

        // Test attributes and content
        result.assertAttributeEquals("location_wgs84_WKT", "POINT (3.54316653506605 50.3921323020023)");

        System.out.println("\n Ending");

    }

    @Test
    public void testProcessorWithEposMSG() throws IOException {

        InputStream content = new ByteArrayInputStream(("{\"oldbssindice\":\"BSS002PUGA\",\"oldbssdesignation\":\"DC53\",\"oldbssid\":\"BSS002PUGA/X\",\"boreholeid\":\"BSS002PUGA\",\"gmlidentifier\":\"http://ressource.brgm-rec.fr/data/BoreholeView/BSS002PUGA\",\"name\":\"Forage BSS002PUGA\",\"gsmlpidentifier\":\"http://ressource.brgm-rec.fr/data/Borehole/BSS002PUGA\",\"description\":\"Borehole from BSS (french subsoils databank)\",\"purpose\":\"unknown\",\"purpose_href\":\"http://www.opengis.net/def/nil/OGC/0/unknown\",\"boreholeuse\":\"unknown\",\"boreholeuse_href\":\"http://www.opengis.net/def/nil/OGC/0/unknown\",\"status\":\"drillingCompleted\",\"status_href\":\"http://resource.europe-geology.eu/vocabs/BoreholeStatus/drillingCompleted\",\"drillingmethod\":\"unknown\",\"drillingmethod_href\":\"http://www.opengis.net/def/nil/OGC/0/unknown\",\"operator\":null,\"driller\":null,\"drillstartdate\":null,\"drillenddate\":null,\"startpoint\":\"naturalLandSurface\",\"startpoint_href\":\"http://resource.europe-geology.eu/vocabs/BoreholeStartPoint/naturalLandSurface\",\"inclinationtype\":\"vertical\",\"inclinationtype_href\":\"http://resource.europe-geology.eu/vocabs/BoreholeInclinationType/vertical\",\"boreholematerialcustodian\":\"unknown\",\"boreholelength_m\":null,\"elevation_m\":null,\"elevation_srs\":\"http://www.opengis.net/def/crs/EPSG/0/5720\",\"positionalaccuracy\":null,\"source\":\"http://ficheinfoterre.brgm.fr/InfoterreFiche/ficheBss.action?id=BSS002PUGA/X\",\"parentborehole_uri\":\"Not applicable\",\"metadata_uri\":\"http://www.geocatalogue.fr/Detail.do?fileIdentifier=BR_BSS_BAA\",\"genericsymbolizer\":\"Not provided\",\"cored\":false,\"accesstophysicaldrillcore\":false,\"detaileddescription\":\"template\",\"detaileddescription_href\":\"http://www.opengis.net/def/nil/OGC/0/template\",\"geophysicallogs\":\"unknown\",\"geophysicallogs_href\":\"http://www.opengis.net/def/nil/OGC/0/unknown\",\"geologicaldescription\":\"template\",\"geologicaldescription_href\":\"http://www.opengis.net/def/nil/OGC/0/template\",\"groundwaterlevel\":\"unknown\",\"groundwaterlevel_href\":\"http://www.opengis.net/def/nil/OGC/0/unknown\",\"groundwaterchemistry\":\"unknown\",\"groundwaterchemistry_href\":\"http://www.opengis.net/def/nil/OGC/0/unknown\",\"rockgeochemistry\":\"unknown\",\"rockgeochemistry_href\":\"http://www.opengis.net/def/nil/OGC/0/unknown\",\"poregaschemistry\":\"unknown\",\"poregaschemistry_href\":\"http://www.opengis.net/def/nil/OGC/0/unknown\",\"geotechnicalinfo\":\"unknown\",\"geotechnicalinfo_href\":\"http://www.opengis.net/def/nil/OGC/0/unknown\",\"location_wgs84\":{\"wkb\":\"AQEAACDmEAAAYR9DsmdYDEANZypkMTJJQA==\",\"srid\":4326},\"location_wgs84_wkb\":\"AQEAACDmEAAAYR9DsmdYDEANZypkMTJJQA==\"}").getBytes());

        testRunner = TestRunners.newTestRunner(WKBWKTConvertProcessor.class);
        testRunner.setProperty(WKBWKTConvertProcessor.WKBIN_PROPERTY, "$.location_wgs84_wkb");
        testRunner.setProperty(WKBWKTConvertProcessor.OUT_NAME_PROPERTY, "location_wgs84_WKT");
        testRunner.enqueue(content);
        testRunner.run(1);
        testRunner.assertQueueEmpty();
        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(WKBWKTConvertProcessor.SUCCESS);
        assertTrue("1 converted", results.size() == 1);
        MockFlowFile result = results.get(0);
        String resultValue = new String(testRunner.getContentAsByteArray(result));

        System.out.println("convertion: " + IOUtils.toString(testRunner.getContentAsByteArray(result)));

        // Test attributes and content
        result.assertAttributeEquals("location_wgs84_WKT", "POINT (3.54316653506605 50.3921323020023)");

        System.out.println("\n Ending");

    }


}
