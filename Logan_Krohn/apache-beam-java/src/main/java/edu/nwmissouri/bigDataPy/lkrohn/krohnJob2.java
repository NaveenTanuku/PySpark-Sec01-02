package edu.nwmissouri.bigDataPy.lkrohn;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.beam.runners.core.construction.resources.PipelineResourcesOptions;
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

public class krohnJob2 {
    String name = "unknown.md";
    Double rank = 1.000;
    ArrayList<VotingPage> voters = new ArrayList<VotingPage>();
    
    PipelineOptions options = PipelineOptionsFactory.create();

    Pipeline p = Pipeline.create(options);

    //PCollection<KV<String,String>> KVjob2
    
}
