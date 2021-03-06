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

import java.util.ArrayList;
import java.util.Collection;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;


public class krohnPageRank {
  // DEFINE DOFNS
  // ==================================================================
  // You can make your pipeline assembly code less verbose by defining
  // your DoFns statically out-of-line.
  // Each DoFn<InputT, OutputT> takes previous output
  // as input of type InputT
  // and transforms it to OutputT.
  // We pass this DoFn to a ParDo in our pipeline.

  /**
   * DoFn Job1Finalizer takes KV(String, String List of outlinks) and transforms
   * the value into our custom RankedPage Value holding the page's rank and list
   * of voters.
   * 
   * The output of the Job1 Finalizer creates the initial input into our
   * iterative Job 2.
   */
  static class Job1Finalizer extends DoFn<KV<String, Iterable<String>>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<String>> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
      Integer contributorVotes = 0;
      if (element.getValue() instanceof Collection) {
        contributorVotes = ((Collection<String>) element.getValue()).size();
      }
      ArrayList<VotingPage> voters = new ArrayList<VotingPage>();
      for (String voterName : element.getValue()) {
        if (!voterName.isEmpty()) {
          voters.add(new VotingPage(voterName, contributorVotes));
        }
      }
      receiver.output(KV.of(element.getKey(), new RankedPage(element.getKey(), voters)));
    }
  }

  static class Job2Mapper extends DoFn<KV<String, RankedPage>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, RankedPage> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
      Integer votes = 0;
      ArrayList<VotingPage> voters = element.getValue().getVoters();
      
      if (voters instanceof Collection){
        votes = ((Collection<VotingPage>) voters).size();
      }
  
      for (VotingPage vp : voters) {
        String pageName = vp.getName();
        Double pageRank = vp.getRank();
        String contributingPageName = element.getKey();
        Double contributingPageRank = element.getValue().getRank();
        VotingPage contributor = new VotingPage(contributingPageName, contributingPageRank, votes);
        ArrayList<VotingPage> arr = new ArrayList<VotingPage>();
        arr.add(contributor);
        receiver.output(KV.of(vp.getName(), new RankedPage(pageName, pageRank, arr)));
      }
    }
  }

  static class Job2Updater extends DoFn<KV<String, Iterable<RankedPage>>, KV<String, RankedPage>>{
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<RankedPage>> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
          String thisPage = element.getKey();
          Iterable<RankedPage> rankedPages = element.getValue();
          Double dampingFactor = 0.85;
          Double updatedRank = (1 - dampingFactor);
          ArrayList<VotingPage> newVoters = new ArrayList<VotingPage>();
          for (RankedPage pg : rankedPages){
            if (pg != null){
              for (VotingPage vp : pg.getVoters()){
                newVoters.add(vp);
                updatedRank += (dampingFactor) * vp.getRank() / (double) vp.getVotes();
              }
            }
          }
          receiver.output(KV.of(thisPage, new RankedPage(thisPage, updatedRank, newVoters)));
    }
  }
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
    // Job 1 Mapper
    PCollection<KV<String, String>> mergedList = pcList.apply(Flatten.<KV<String, String>>pCollections());
    // Job 1 Reducer
    PCollection<KV<String, Iterable <String>>> reducedPairs = mergedList.apply(GroupByKey.<String,String>create());
    // Convert to a custom Value object (RankedPage) in preparation for Job 2
    PCollection<KV<String, RankedPage>> job2in = reducedPairs.apply(ParDo.of(new Job1Finalizer()));
    //   // END JOB 1
    // // ========================================
    // // KV{python.md, python.md, 1.00000, 0, [README.md, 1.00000,1]}
    // // KV{go.md, go.md, 1.00000, 0, [README.md, 1.00000,1]}
    // // KV{README.md, README.md, 1.00000, 0, [go.md, 1.00000,3, java.md, 1.00000,3,
    // // python.md, 1.00000,3]}
    // // ========================================
    // // BEGIN ITERATIVE JOB 2

    PCollection<KV<String, RankedPage>> job2out = null; 
    int iterations = 50;
    for (int i = 1; i <= iterations; i++) {
      // use job2in to calculate job2 out
      PCollection<KV<String,RankedPage>> krohnJob2Mapper = job2in.apply(ParDo.of(new Job2Mapper()));
      PCollection<KV<String,Iterable<RankedPage>>> krohnJob2GBK = krohnJob2Mapper.apply(GroupByKey.create());
      
      job2out = krohnJob2GBK.apply(ParDo.of(new Job2Updater()));
      job2in = job2out;
    }

    // END ITERATIVE JOB 2
    // ========================================
    // after 40 - output might look like this:
    // KV{java.md, java.md, 0.69415, 0, [README.md, 1.92054,3]}
    // KV{python.md, python.md, 0.69415, 0, [README.md, 1.92054,3]}
    // KV{README.md, README.md, 1.91754, 0, [go.md, 0.69315,1, java.md, 0.69315,1, python.md, 0.69315,1]}

    //Map KVs to strings before outputting
    PCollection<String> output = job2out.apply(MapElements.into(
        TypeDescriptors.strings())
        .via(kv -> kv.toString()));

    // Write from Beam back out into the real world
    
    // PCollection<String> kvOut = reducedPairs.apply(MapElements.into(TypeDescriptors.strings())
    //     .via((kvpairs) -> kvpairs.toString()));

        output.apply(TextIO.write().to("krohnPR"));

    p.run().waitUntilFinish();
  }

   /**
   * Run one iteration of the Job 2 Map-Reduce process
   * Notice how the Input Type to Job 2.
   * Matches the Output Type from Job 2.
   * How important is that for an iterative process?
   * 
   * @param kvReducedPairs - takes a PCollection<KV<String, RankedPage>> with
   *                       initial ranks.
   * @return - returns a PCollection<KV<String, RankedPage>> with updated ranks.
   */
  private static PCollection<KV<String, RankedPage>> runJob2Iteration(
      PCollection<KV<String, RankedPage>> kvReducedPairs) {
    PCollection<KV<String, RankedPage>> mappedKVs = kvReducedPairs.apply(ParDo.of(new Job2Mapper()));

    // KV{README.md, README.md, 1.00000, 0, [java.md, 1.00000,1]}
    // KV{README.md, README.md, 1.00000, 0, [go.md, 1.00000,1]}
    // KV{java.md, java.md, 1.00000, 0, [README.md, 1.00000,3]}

    PCollection<KV<String, Iterable<RankedPage>>> reducedKVs = mappedKVs
        .apply(GroupByKey.<String, RankedPage>create());

    // KV{java.md, [java.md, 1.00000, 0, [README.md, 1.00000,3]]}
    // KV{README.md, [README.md, 1.00000, 0, [python.md, 1.00000,1], README.md,
    // 1.00000, 0, [java.md, 1.00000,1], README.md, 1.00000, 0, [go.md, 1.00000,1]]}

    PCollection<KV<String, RankedPage>> updatedOutput = reducedKVs.apply(ParDo.of(new Job2Updater()));

    // KV{README.md, README.md, 2.70000, 0, [java.md, 1.00000,1, go.md, 1.00000,1,
    // python.md, 1.00000,1]}
    // KV{python.md, python.md, 0.43333, 0, [README.md, 1.00000,3]}
    return updatedOutput;
  }

  private static PCollection<KV<String, String>> krohnMapper1(String file, String path, Pipeline p) {
    PCollection<String> pcolInputLines = p.apply(TextIO.read().from(path + '/' + file));

    //.apply(Filter.by((String line) -> !line.isEmpty()))
    //.apply(Filter.by((String line) -> !line.equals(" ")))

    PCollection<String> pcolLinkLines = pcolInputLines.apply(Filter.by((String line) -> line.startsWith("[")));
    

    PCollection<String> pcolLinks = pcolLinkLines.apply(
          MapElements.into(TypeDescriptors.strings())
              .via((String linkline) -> linkline.substring(linkline.indexOf("(")+1,linkline.length()-1)));


    PCollection<KV<String, String >> KVpairsJob1 = pcolLinks.apply(
          MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
              .via(
                  (String linkedPage) ->
                      KV.of(file, linkedPage)));
    return KVpairsJob1;
  }
}
