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
package edu.nwmissouri.bigDataPy.naveen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
public class PageRankNaveenJob1 {

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
      int votes = 0;
      ArrayList<VotingPage> voters = element.getValue().getVoterList();
      if(voters instanceof Collection){
        votes = ((Collection<VotingPage>) voters).size();
      }
      for(VotingPage vp: voters){
        String pageName = vp.getVoterName();
        double pageRank = vp.getPageRank();
        String contributingPageName = element.getKey();
        double contributingPageRank = element.getValue().getRank();
        VotingPage contributor = new VotingPage(contributingPageName,votes,contributingPageRank);
        ArrayList<VotingPage> arr = new ArrayList<>();
        arr.add(contributor);
        receiver.output(KV.of(vp.getVoterName(), new RankedPage(pageName, pageRank, arr)));        
      }
    }

    static class Job2Updater extends DoFn<KV<String, Iterable<RankedPage>>, KV<String, RankedPage>> {
      @ProcessElement
      public void processElement(@Element KV<String, Iterable<RankedPage>> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
          Double dampingFactor = 0.85;
          Double updatedRank = (1 - dampingFactor);
          ArrayList<VotingPage> newVoters = new ArrayList<>();
          for(RankedPage rankPage:element.getValue()){
            if (rankPage != null) {
              for(VotingPage votingPage:rankPage.getVoterList()){
                newVoters.add(votingPage);
                updatedRank += (dampingFactor) * votingPage.getPageRank() / (double)votingPage.getContributorVotes();
                // newVoters.add(new VotingPageReddy(votingPage.getVoterName(),votingPage.getContributorVotes(),updatedRank));
              }
            }
          }
          receiver.output(KV.of(element.getKey(),new RankedPage(element.getKey(), updatedRank, newVoters)));
  
      }
  
    }

  public static void main(String[] args) {

    PipelineOptions options = PipelineOptionsFactory.create();

    Pipeline p = Pipeline.create(options);

    
    String dataFolder = "web04";
    //String dataFile = "go.md";
    //String dataPath = dataFolder + "/" + dataFile;
   // PCollection<String> pcolInput =
    //p.apply(TextIO.read().from(dataPath));
    //p.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/kinglear.txt"))

    // p.apply(TextIO.read().from(dataPath))

    PCollection<KV<String, String>> pcollectionkvpairs1 = naveenMapper(p,"go.md",dataFolder);
    PCollection<KV<String, String>> pcollectionkvpairs2 = naveenMapper(p,"java.md",dataFolder);
    PCollection<KV<String, String>> pcollectionkvpairs3 = naveenMapper(p,"python.md",dataFolder);
    PCollection<KV<String, String>> pcollectionkvpairs4 = naveenMapper(p,"README.md",dataFolder);
    
    PCollection<KV<String, String>> pcollectionkvpairs5 = naveenMapper(p,"compass.md",dataFolder);


        PCollectionList<KV<String, String>> pcCollectionKVpairs = PCollectionList.of(pcollectionkvpairs1).and(pcollectionkvpairs2).and(pcollectionkvpairs3).and(pcollectionkvpairs4).and(pcollectionkvpairs5);

        PCollection<KV<String, String>> myMergedList = pcCollectionKVpairs.apply(Flatten.<KV<String,String>>pCollections());
     
        PCollection<String> PCollectionLinksString =  myMergedList.apply(
          MapElements.into(  
            TypeDescriptors.strings())
              .via((myMergeLstout) -> myMergeLstout.toString()));
        
        PCollectionLinksString.apply(TextIO.write().to("naveenKV"));
        p.run().waitUntilFinish();
     }


     private static PCollection<KV<String, String>> naveenMapper(Pipeline p, String dataFile, String dataFolder) {
      String dataPath = dataFolder + "/" + dataFile;
   
      PCollection<String> pcolInputLines =  p.apply(TextIO.read().from(dataPath));
      PCollection<String> pcolLines  =pcolInputLines.apply(Filter.by((String line) -> !line.isEmpty()));
      PCollection<String> pcColInputEmptyLines=pcolLines.apply(Filter.by((String line) -> !line.equals(" ")));
      PCollection<String> pcolInputLinkLines=pcColInputEmptyLines.apply(Filter.by((String line) -> line.startsWith("[")));
   
      PCollection<String> pcolInputLinks=pcolInputLinkLines.apply(
              MapElements.into(TypeDescriptors.strings())
                  .via((String linkline) -> linkline.substring(linkline.indexOf("(")+1,linkline.indexOf(")")) ));
   
                  PCollection<KV<String, String>> pcollectionkvLinks=pcolInputLinks.apply(
                    MapElements.into(  
                      TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                        .via (linkline ->  KV.of(dataFile , linkline) ));
   
   
      return pcollectionkvLinks;
    }
}
