package edu.nwmissouri.bigdatapy.sathya;

import java.io.Serializable;

public class VotingPage implements Serializable {

    public String name;
    public Double rank = 1.0;
    public Integer votes;
    



    public VotingPage(String nameIn, Integer votesIn){
        this.name = nameIn;

        this.votes = votesIn;                

    }


    public VotingPage(String nameIn, Double rankIn, Integer votesIn){
        this.name = nameIn;

        this.votes = votesIn;
        
        this.rank = rankIn ;

    }

    public Double getRank(){
        return this.rank;
    }

    public String getName(){
        return this.name;
    }

    public Integer getVotes(){
        return this.votes;
    }


    @Override
    public String toString(){
        return "voterName = "+ name +", Page rank = "+this.rank +" ContributorVotes = " + votes;
    }

}