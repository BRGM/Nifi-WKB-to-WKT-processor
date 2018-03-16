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

import com.jayway.jsonpath.JsonPath;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Tags({"WKB","WKT","JSON"})
@CapabilityDescription("Transform WKB format to WKT on a content. Results is set to output jsonpath provided")
@SideEffectFree
public class WKBWKTConvertProcessor extends AbstractProcessor {

    public static final PropertyDescriptor WKBIN_PROPERTY = new PropertyDescriptor
            .Builder().name("wkb_field_name")
            .displayName("wkb fieldname jsonpath")
            .description("the field name in the wkb format in json path")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor OUT_NAME_PROPERTY = new PropertyDescriptor
            .Builder().name("out_field_name")
            .displayName("out attribute name")
            .description("the field name to store wkt format in json path")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Success relationship")
            .build();


    public static final Relationship FAILED = new Relationship.Builder()
            .name("FAILED")
            .description("Failed relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(WKBIN_PROPERTY);
        descriptors.add(OUT_NAME_PROPERTY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILED);
        this.relationships = Collections.unmodifiableSet(relationships);

    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
         FlowFile flowFile = session.get();
        final AtomicReference<String> value = new AtomicReference<>();

        if ( flowFile == null ) {
            return;
        }
        try{
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream in) throws IOException {

                    getLogger().debug("Processing flow file content : converting to json");
                    String json = new BufferedReader(new InputStreamReader(in)).lines()
                            .parallel().collect(Collectors.joining("\n"));
                    getLogger().debug("Processing flow file content : convert to json done");
                    getLogger().debug("Processing flow file content : Reading value "+context.getProperty(WKBIN_PROPERTY).getValue());
                    String result = JsonPath.read(json,context.getProperty(WKBIN_PROPERTY).getValue() );
                    getLogger().debug("Processing flow file content : Reading done, result : "+result);
                    value.set(result);
                }
            });

            String results= value.get();
            if(results != null && !results.isEmpty()){
                try {
                    getLogger().debug("Processing flow file content : Converting value : "+results);
                    byte[] back1 = Base64.getDecoder().decode(results);
                    getLogger().debug("Processing flow file content : decoded base 64 : "+back1);
                    WKBReader reader = new WKBReader();
                    Geometry geom = reader.read(back1);
                    getLogger().debug("Processing flow file content : getting geometry  : "+geom);
                    String wktString = geom.toText();
                    getLogger().debug("Processing flow file content : getting WKT  : "+wktString);
                    flowFile = session.putAttribute(flowFile, context.getProperty(OUT_NAME_PROPERTY).getValue(), wktString);
                    getLogger().debug("Processing flow file content : Result put to attribute  : "+context.getProperty(OUT_NAME_PROPERTY).getValue());
                }catch ( ParseException e){
                    throw new ProcessException(e);
                }
            }
            getLogger().info("Processing flow file content : Transferring flow file to success");
            session.transfer(flowFile, SUCCESS);

        }catch(Exception ex){

            getLogger().error("Failed to process incoming Flowfile json. " + ex.getMessage(), ex);
            FlowFile failedFlowFile = session.putAttribute(flowFile,
                    WKBWKTConvertProcessor.class.getName() + ".error", ex.getMessage());
            session.transfer(failedFlowFile, FAILED);

        }

    }
}
