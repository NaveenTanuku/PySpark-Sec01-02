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

import java.io.Serializable;
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
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TypeDescriptor;
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
      
      // set Integer votes to 0
      Integer votes = 0;
      // Create ArrayList<VotingPage>
      ArrayList<VotingPage> voters = element.getValue().getVoters();

      if(voters instanceof Collection){
        votes = ((Collection<VotingPage>)voters).size();
      }

      for(VotingPage vp:voters){
        String pageName = vp.getName();
        Double pageRank = vp.getRank();
        String contributingPageName = element.getKey();
        Double contributingPageRank = element.getValue().getRank();
        VotingPage contributor = new VotingPage(contributingPageName,contributingPageRank,votes);
        ArrayList<VotingPage> arr = new ArrayList<VotingPage>();
        arr.add(contributor);
        receiver.output(KV.of(vp.getName(),new RankedPage(pageName, pageRank,arr)));
      }
     
    }
  }


  static class Job2Updater extends DoFn<KV<String, Iterable<RankedPage>>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<RankedPage>> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
      
          String thisPage = element.getKey();
          Iterable<RankedPage> rankedPages = element.getValue();
          Double dampfactor = 0.85;
          Double updateRank = (1.0 -dampfactor);
          ArrayList<VotingPage> newVoters = new ArrayList<VotingPage>();

        for (RankedPage pg:rankedPages) {
        if (pg!=null) {
          for(VotingPage vp :pg.getVoters()){
            newVoters.add(vp);
            updateRank +=(dampfactor)*vp.getRank()/(double) vp.getVotes();
          }
        }
      }
      receiver.output(KV.of(thisPage, new RankedPage(thisPage,updateRank,newVoters)));
    }
  }


   public static void main(String[] args) {

   PipelineOptions options = PipelineOptionsFactory.create();

    // In order to run your pipeline, you need to make following runner specific changes:
    //
   
    // Create the Pipeline object with the options we defined above
    // p= pipeline
    Pipeline p = Pipeline.create(options);
    

    String dataFolder = "web04";
    // String dataFile = "go.md";
   //  String dataPath = dataFolder + "/" + dataFile;
    //p.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/kinglear.txt"))
    // add Pcollection pairs with mapper 1 

    PCollection<KV<String, String>> pcollectionkvpairs1 = sathyaMapper1(p,"go.md",dataFolder);
    PCollection<KV<String, String>> pcollectionkvpairs2 = sathyaMapper1(p,"java.md",dataFolder);
    PCollection<KV<String, String>> pcollectionkvpairs3 = sathyaMapper1(p,"python.md",dataFolder);
    PCollection<KV<String, String>> pcollectionkvpairs4 = sathyaMapper1(p,"README.md",dataFolder);
 
   PCollectionList<KV<String, String>> pcCollectionKVpairs = 
       PCollectionList.of(pcollectionkvpairs1).and(pcollectionkvpairs2).and(pcollectionkvpairs3).and(pcollectionkvpairs4);

    PCollection<KV<String, String>> myMergedList = pcCollectionKVpairs.apply(Flatten.<KV<String,String>>pCollections());

   

    PCollection<KV<String, Iterable<String>>> listReducedPairs =myMergedList.apply(GroupByKey.create());
    PCollection<KV<String, RankedPage>> job02 = listReducedPairs.apply(ParDo.of(new Job1Finalizer()));

    PCollection<KV<String,RankedPage>> job2Mapper = job02.apply(ParDo.of(new Job2Mapper()));

    // PCollection<KV<String, RankedPage>> job2out = null; 
    PCollection<KV<String,RankedPage>> job2Out = null;
    PCollection<KV<String,Iterable<RankedPage>>> job2MapperGrpByKey = job2Mapper.apply(GroupByKey.create());
    
    job2Out = job2MapperGrpByKey.apply(ParDo.of(new Job2Updater()));

    job2MapperGrpByKey = job2Out.apply(GroupByKey.create());
    
    job2Out = job2MapperGrpByKey.apply(ParDo.of(new Job2Updater()));

    job2Out = job2MapperGrpByKey.apply(ParDo.of(new Job2Updater()));
    job2MapperGrpByKey = job2Out.apply(GroupByKey.create());    
    job2Out = job2MapperGrpByKey.apply(ParDo.of(new Job2Updater()));   



       

      PCollection<String> linkString = job02.apply(
        MapElements.into(
          TypeDescriptors.strings()).via(
              kvtoString -> kvtoString.toString()));




       
        //
        // By default, it will write to a set of files with names like wordcounts-00001-of-00005
        
        linkString.apply(TextIO.write().to("RankedPage-Sathya"));
       

        p.run().waitUntilFinish();
  }

  private static PCollection<KV<String, String>> sathyaMapper1(Pipeline p, String dataFile, String dataFolder) {
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

    
  