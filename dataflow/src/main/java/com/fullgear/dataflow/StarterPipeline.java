/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.fullgear.dataflow;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import java.util.ArrayList;
import java.util.List;


import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A starter example for writing Google Cloud Dataflow programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectPipelineRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=BlockingDataflowPipelineRunner
 */
public class StarterPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);
  
  public static TableReference getTableReference() {
	  TableReference tableRef = new TableReference();
	  tableRef.setProjectId("in-full-gear");
	  tableRef.setDatasetId("Dataset1");
	  tableRef.setTableId("newest_table");
	  return tableRef;
	}
  
  private static TableSchema getSchema() {
	  List<TableFieldSchema> fields = new ArrayList<>();
	  //fields.add(new TableFieldSchema().setName("id").setType("STRING"));
	  //fields.add(new TableFieldSchema().setName("user").setType("STRING"));
	  fields.add(new TableFieldSchema().setName("text").setType("STRING"));
	  fields.add(new TableFieldSchema().setName("created_at").setType("TIMESTAMP"));
	  fields.add(new TableFieldSchema().setName("latitude").setType("FLOAT"));
	  //fields.add(new TableFieldSchema().setName("longtitude").setType("STRING"));
	  TableSchema schema = new TableSchema().setFields(fields);
	  return schema;
	}
  
  public static void main(String[] args) {
	String topic = "projects/in-full-gear/topics/NY_tweets";
	//PipelineOptions options = PipelineOptionsFactory.create();
	DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
	options.setTempLocation("gs://in-full-gear-temp");
	options.setStreaming(true);
	options.setProject("in-full-gear");
	options.setStagingLocation("gs://in-full-gear-temp");
	options.setRunner(BlockingDataflowPipelineRunner.class);
	Pipeline pipeline = Pipeline.create(options);
	
	pipeline.apply(PubsubIO.Read.topic(topic))
	.apply(ParDo.of(new DoFn<String, TableRow>(){
	      @Override
	      public void processElement(ProcessContext c) {	    	
	    	
	    	TableRow row = new TableRow();
	    	try{
	    		JSONParser parser = new JSONParser();
	    		Object obj = parser.parse(c.element());
	    		JSONObject jsonObject = (JSONObject) obj;
	    		String text = (String) jsonObject.get("text");
	    		Double latitude = (Double) jsonObject.get("latitude");
	    		row.set("text", text)
	    		.set("created_at", c.timestamp().toString())
	    		.set("latitude", latitude);
	    	}
	      	catch (ParseException e) {
	      		row.set("text", e.toString()).set("created_at", c.timestamp().toString());
	      	}
		    c.output(row);	    	  
	      }			
	}))
	.apply(BigQueryIO.Write.to(getTableReference()).withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED).
			withWriteDisposition(WriteDisposition.WRITE_APPEND).withSchema(getSchema()));
	pipeline.run();
		
  }
}
