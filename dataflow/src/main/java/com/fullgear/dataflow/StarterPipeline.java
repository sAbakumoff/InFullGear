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
import com.google.api.services.language.v1beta1.model.Entity;
import com.google.api.services.language.v1beta1.model.Sentiment;
import com.google.api.services.language.v1beta1.model.Token;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StarterPipeline {
  public static TableReference getTableReference() {
	  TableReference tableRef = new TableReference();
	  tableRef.setProjectId("in-full-gear");
	  tableRef.setDatasetId("Dataset1");
	  tableRef.setTableId("debates_tweets");
	  return tableRef;
	}

  private static TableSchema getSchema() {
	  List<TableFieldSchema> fields = new ArrayList<>();
	  fields.add(new TableFieldSchema().setName("tweet_object").setType("STRING"));
	  fields.add(new TableFieldSchema().setName("polarity").setType("FLOAT"));
	  fields.add(new TableFieldSchema().setName("magnitude").setType("FLOAT"));
	  fields.add(new TableFieldSchema().setName("syntax").setType("STRING"));
	  TableSchema schema = new TableSchema().setFields(fields);
	  return schema;
	}

  public static void main(String[] args) {
	String topic = "projects/in-full-gear/topics/debates_tweets";
	DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
	options.setTempLocation("gs://in-full-gear-temp");
	options.setStreaming(true);
	options.setProject("in-full-gear");
	options.setStagingLocation("gs://in-full-gear-temp");
	options.setRunner(BlockingDataflowPipelineRunner.class);
	Pipeline pipeline = Pipeline.create(options);

	pipeline.apply(PubsubIO.Read.timestampLabel("created_at").topic(topic))
	.apply(ParDo.of(new DoFn<String, TableRow>(){
	      @Override
	      public void processElement(ProcessContext c) {
	    	TableRow row = new TableRow();
	    	try{
	    		JSONParser parser = new JSONParser();
	    		
	    		Object obj = parser.parse(c.element());
	    		JSONObject jsonObject = (JSONObject) obj;
	    		String text = (String) jsonObject.get("text");
	    		
	    		Analyze analyze = new Analyze(Analyze.getLanguageService());
	    		Sentiment sentiment = analyze.analyzeSentiment(text);
	    		
	    		List<Token> tokens = analyze.analyzeSyntax(text);
	    		JSONArray jsonTokens = new JSONArray();
	    		for (Token token : tokens){
	    			JSONObject jsonToken = new JSONObject();
	    			jsonToken.put("partOfSpeech", token.getPartOfSpeech().getTag());
	    			jsonToken.put("content", token.getText().getContent());
	    			jsonTokens.add(jsonToken);
	    		}
	    		row.set("tweet_object", c.element())
	    		.set("syntax", jsonTokens.toJSONString())
	    		.set("polarity", sentiment.getPolarity())
	    		.set("magnitude", sentiment.getMagnitude());
	    	}
	      	catch (ParseException | IOException | GeneralSecurityException e) {
	      		row.set("tweet_object", e.toString());
	      	}
	    	c.output(row);
	      }
	}))
	.apply(BigQueryIO.Write.to(getTableReference()).withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED).
			withWriteDisposition(WriteDisposition.WRITE_APPEND).withSchema(getSchema()));
	pipeline.run();
  }
}