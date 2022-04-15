/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.nwmissouri.bigDataPy.lkrohn;

// beam-playground:
//   name: MinimalWordCount
//   description: An example that counts words in Shakespeare's works.
//   multifile: false
//   pipeline_options:
//   categories:
//     - Combiners
//     - Filtering
//     - IO
//     - Core Transforms

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;


public class krohnJob1 {

 

  public static void main(String[] args) {
    String dataFolder = "./web04";

    // Create a PipelineOptions object. This object lets us set various execution
    // options for our pipeline, such as the runner you wish to use. This example
    // will run with the DirectRunner by default, based on the class path configured
    // in its dependencies.
    PipelineOptions options = PipelineOptionsFactory.create();


    Pipeline p = Pipeline.create(options);

    PCollection<KV<String, String>> pCollection1 = krohnMapper1("go.md", dataFolder, p);
    PCollection<KV<String, String>> pCollection2 = krohnMapper1("java.md", dataFolder, p);
    PCollection<KV<String, String>> pCollection3 = krohnMapper1("python.md", dataFolder, p);
    PCollection<KV<String, String>> pCollection4 = krohnMapper1("README.md", dataFolder, p);
    
    PCollectionList<KV<String, String>> pcList = PCollectionList.of(pCollection1).and(pCollection2).and(pCollection3).and(pCollection4);

    PCollection<KV<String, String>> mergedList = pcList.apply(Flatten.<KV<String, String>>pCollections());

    // Job 1 Reducer
    PCollection<KV<String, Iterable <String>>> reducedPairs = mergedList.apply(GroupByKey.<String,String>create());
    
    PCollection<String> kvOut = reducedPairs.apply(MapElements.into(TypeDescriptors.strings())
        .via((kvpairs) -> kvpairs.toString()));

        kvOut.apply(TextIO.write().to("krohnPR"));

    p.run().waitUntilFinish();
  }

  private static PCollection<KV<String, String>> krohnMapper1(String file, String path, Pipeline p) {
    PCollection<String> pcolInputLines = p.apply(TextIO.read().from(path + '/' + file));

    //.apply(Filter.by((String line) -> !line.isEmpty()))
    //.apply(Filter.by((String line) -> !line.equals(" ")))

    PCollection<String> pcolLinkLines = pcolInputLines.apply(Filter.by((String line) -> line.startsWith("[")));
    

    PCollection<String> pcolLinks = pcolLinkLines.apply(
          FlatMapElements.into(TypeDescriptors.strings())
              .via((String linkline) -> Arrays.asList(linkline.substring(linkline.indexOf("(")+1,linkline.length()-1))));


    PCollection<KV<String, String >> KVpairsJob1 = pcolLinks.apply(
          MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
              .via(
                  (String linkedPage) ->
                      KV.of(file, linkedPage)));
    return KVpairsJob1;
  }
}
