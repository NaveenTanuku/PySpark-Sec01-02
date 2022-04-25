package edu.nwmissouri.bigdatapy.sathya;


import java.io.Serializable;
import java.util.ArrayList;

public class RankedPage implements Serializable {
    public String name;
    public Double rank;
    public ArrayList<VotingPage> voters;



    public RankedPage(String nameIn,ArrayList<VotingPage> votersIn) {
        this.name = nameIn;        
        this.voters = votersIn;
        this.rank = 1.0;
    }


    public RankedPage(String nameIn,Double rankIn, ArrayList<VotingPage> votersIn) {
        this.name = nameIn;
        this.rank=rankIn;
        this.voters = votersIn;
    }

    public Double getRank(){
        return rank;
    }

    public  ArrayList<VotingPage> getVoters(){
        return voters;
    }
    

    @Override
    public String toString() {
        return this.name +"<"+ this.rank +","+ voters +">";
    }


}