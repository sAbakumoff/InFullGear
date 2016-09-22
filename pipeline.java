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

package in.full.gear;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;

import org.joda.time.Duration;
import org.joda.time.Instant;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.DoFn.ProcessContext;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

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
public class RealTimePipeline {	
	public static void main(String[] args){
		String topic = "projects/in-full-gear/topics/NY_tweets";
		// Start by defining the options for the pipeline.
		PipelineOptions options = PipelineOptionsFactory.create();
		// Then create the pipeline.
		Pipeline pipeline = Pipeline.create(options);
		// streamData is Unbounded; apply windowing afterward.
		// to prevent the duplicate messages, supply the id attribute :
		// when Dataflow receives multiple messages with the same ID 
		// (which will be read from the attribute with the name of the string you passed to idLabel), 
		// Dataflow will discard all but one of the messages.
		pipeline.apply(PubsubIO.Read.topic(topic))
		.apply(ParDo.of(new Wwc.AddTimestampFn()))
		.apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(1))))
		.apply(ParDo.of(new Wwc.ComputeWordLengthFn()))
		.apply(ParDo.of(new Wwc.FormatFn()))
        .apply(BigQueryIO.Write.to(Wwc.getTableReference()).withSchema(Wwc.getSchema()));		
		pipeline.run();
	}	
}

class Wwc {
	
	
	static class ComputeWordLengthFn extends DoFn<String, Integer> {
	    @Override
	    public void processElement(ProcessContext c) {
	      String word = c.element();	      
	      c.output(word.length());
	    }
	  }	
  /**
   * Concept #2: A DoFn that sets the data element timestamp. This is a silly method, just for
   * this example, for the bounded data case.
   *
   * <p>Imagine that many ghosts of Shakespeare are all typing madly at the same time to recreate
   * his masterworks. Each line of the corpus will get a random associated timestamp somewhere in a
   * 2-hour period.
   */
  static class AddTimestampFn extends DoFn<String, String> {
    private static final long RAND_RANGE = 7200000; // 2 hours in ms

    @Override
    public void processElement(ProcessContext c) {
      // Generate a timestamp that falls somewhere in the past two hours.
      long randomTimestamp = System.currentTimeMillis()
        - (int) (Math.random() * RAND_RANGE);
      /**
       * Concept #2: Set the data element with that timestamp.
       */
      c.outputWithTimestamp(c.element(), new Instant(randomTimestamp));
    }
  }


/** A DoFn that converts a Word and Count into a BigQuery table row. */
static class FormatFn extends DoFn<Integer, TableRow> {
@Override
  public void processElement(ProcessContext c) {
    TableRow row = new TableRow()
        .set("word", "wtf")
        .set("count", c.element())
        // include a field for the window timestamp
       .set("window_timestamp", c.timestamp().toString());
    c.output(row);
  }
}

/**
 * Helper method that defines the BigQuery schema used for the output.
 */
public static TableSchema getSchema() {
  List<TableFieldSchema> fields = new ArrayList<>();
  fields.add(new TableFieldSchema().setName("word").setType("STRING"));
  fields.add(new TableFieldSchema().setName("count").setType("INTEGER"));
  fields.add(new TableFieldSchema().setName("window_timestamp").setType("TIMESTAMP"));
  TableSchema schema = new TableSchema().setFields(fields);
  return schema;
}

/**
 * Concept #6: We'll stream the results to a BigQuery table. The BigQuery output source is one
 * that supports both bounded and unbounded data. This is a helper method that creates a
 * TableReference from input options, to tell the pipeline where to write its BigQuery results.
 */
public static TableReference getTableReference() {
  TableReference tableRef = new TableReference();
  tableRef.setProjectId("in-full-gear");
  tableRef.setDatasetId("Dataset1");
  tableRef.setTableId("table1");
  return tableRef;
}

}




