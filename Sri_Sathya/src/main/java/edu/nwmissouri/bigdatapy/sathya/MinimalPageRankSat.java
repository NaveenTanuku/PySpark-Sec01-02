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
package edu.nwmissouri.bigdatapy.sathya;

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


import java.util.Collection;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import org.apache.beam.sdk.values.TypeDescriptors;

public class MinimalPageRankSat {

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
      int vote = 0;
      ArrayList<VotingPage> voters = element.getValue().getVoterList();
      if(voters instanceof Collection){
        vote = ((Collection<VotingPage>) voters).size();
      }
      for(VotingPage vp: voters){
        String pageName = vp.getVoter();
        double pageRank = vp.getPageRank();
        String cPageName = element.getKey();
        double cPageRank = element.getValue().getRank();
        VotingPage contributor = new VotingPage(cPageName,vote,cPageRank);
        ArrayList<VotingPage> array = new ArrayList<>();
        array.add(contributor);
        receiver.output(KV.of(vp.getVoter(), new RankedPage(pageName, pageRank, array)));        
      }
    }
  }

  static class Job2Updater extends DoFn<KV<String, Iterable<RankedPage>>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<RankedPage>> value,
      OutputReceiver<KV<String, RankedPage>> receiver) {
        Double dampingFactor = 0.85;
        Double updatedRank = (1 - dampingFactor);
        ArrayList<VotingPage> newVoters = new ArrayList<>();
        for(RankedPage rankPage:value.getValue()){
          if (rankPage != null) {
            for(VotingPage votingPage:rankPage.getVoterList()){
              newVoters.add(votingPage);
              updatedRank += (dampingFactor) * votingPage.getPageRank() / (double)votingPage.getVotes();
              // newVoters.add(new VotingPageReddy(votingPage.getVoterName(),votingPage.getContributorVotes(),updatedRank));
            }
          }
        }
        receiver.output(KV.of(value.getKey(),new RankedPage(value.getKey(), updatedRank, newVoters)));

    }

  }

   public static void main(String[] args) {

   PipelineOptions options = PipelineOptionsFactory.create();

   
    // Create the Pipeline object with the options we defined above
    // p= pipeline
    Pipeline p = Pipeline.create(options);
    

    String dataFolder = "web04";
    // String dataFile = "go.md";
   //  String dataPath = dataFolder + "/" + dataFile;
    

    PCollection<KV<String, String>> pcollectionkvpairs1 = sathyaMapper1(p,"go.md",dataFolder);
    PCollection<KV<String, String>> pcollectionkvpairs2 = sathyaMapper1(p,"java.md",dataFolder);
    PCollection<KV<String, String>> pcollectionkvpairs3 = sathyaMapper1(p,"python.md",dataFolder);
    PCollection<KV<String, String>> pcollectionkvpairs4 = sathyaMapper1(p,"README.md",dataFolder);
 
   PCollectionList<KV<String, String>> pcCollectionKVpairs = 
       PCollectionList.of(pcollectionkvpairs1).and(pcollectionkvpairs2).and(pcollectionkvpairs3).and(pcollectionkvpairs4);

    PCollection<KV<String, String>> myMergedList = pcCollectionKVpairs.apply(Flatten.<KV<String,String>>pCollections());

   

    PCollection<KV<String, Iterable<String>>> pCollectionGroupByKey = myMergedList.apply(GroupByKey.create());
    // Convert to a custom Value object (RankedPage) in preparation for Job 2
    PCollection<KV<String, RankedPage>> job02Input = pCollectionGroupByKey.apply(ParDo.of(new Job1Finalizer()));
  
    PCollection<KV<String,RankedPage>> job2Mapper = job02Input.apply(ParDo.of(new Job2Mapper()));
  

  PCollection<KV<String, RankedPage>> job02Output = null; 
  PCollection<KV<String,Iterable<RankedPage>>> job02MapperGroupbkey = job2Mapper.apply(GroupByKey.create());
    
  job02Output = job02MapperGroupbkey.apply(ParDo.of(new Job2Updater()));

  
  job02MapperGroupbkey = job02Output.apply(GroupByKey.create());
    
  job02Output = job02MapperGroupbkey.apply(ParDo.of(new Job2Updater()));
  
  job02Output = job02MapperGroupbkey.apply(ParDo.of(new Job2Updater()));
  job02MapperGroupbkey = job02Output.apply(GroupByKey.create());    
  job02Output = job02MapperGroupbkey.apply(ParDo.of(new Job2Updater()));   
    
   
   

    // Change the KV pairs to String using toString of kv
    PCollection<String> pColStringLists = job02Output.apply(
        MapElements.into(
            TypeDescriptors.strings()).via(
                kvtoString -> kvtoString.toString()));
    // Write the output to the file


        
        pColStringLists.apply(TextIO.write().to("RankedPage-Sathya"));
       

        p.run().waitUntilFinish();
  }

  private static PCollection<KV<String, String>> sathyaMapper1(Pipeline p, String dataFile, String dataFolder) {
    String dataPath = dataFolder + "/" + dataFile;

    PCollection<String> pcolIpLines =  p.apply(TextIO.read().from(dataPath));
    PCollection<String> pcol  =pcolIpLines.apply(Filter.by((String line) -> !line.isEmpty()));
    PCollection<String> pcColInputEmptyLines=pcol.apply(Filter.by((String line) -> !line.equals(" ")));
    PCollection<String> pcolInputLinkLines=pcColInputEmptyLines.apply(Filter.by((String line) -> line.startsWith("[")));
   
    PCollection<String> pcolInputLinks=pcolInputLinkLines.apply(
            MapElements.into(TypeDescriptors.strings())
                .via((String linkline) -> linkline.substring(linkline.indexOf("(")+1,linkline.indexOf(")")) ));

                PCollection<KV<String, String>> pcollectionkvpairs=pcolInputLinks.apply(
                  MapElements.into(  
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                      .via (linkline ->  KV.of(dataFile , linkline) ));
     
                   
    return pcollectionkvpairs;
  }
}

    
  