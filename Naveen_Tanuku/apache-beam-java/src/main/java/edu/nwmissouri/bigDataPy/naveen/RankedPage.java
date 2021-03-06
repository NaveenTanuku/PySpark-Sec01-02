package edu.nwmissouri.bigDataPy.naveen;

import java.io.Serializable;
import java.util.ArrayList;

public class RankedPage implements Serializable{
    String voter;
    double rank;
    ArrayList<VotingPage> voterList = new ArrayList<>();

    public RankedPage(){

    }

    public RankedPage(String voter,double rank, ArrayList<VotingPage> voterList){
        this.voter = voter;
        this.voterList = voterList;
        this.rank = rank;
    }    

    public RankedPage(String voter,  ArrayList<VotingPage> voterList){
        this.voter = voter;
        this.voterList = voterList;
        this.rank = 1.0;
    }

    public String getVoter() {
        return voter;
    }

    public void setVoter(String voter) {
        this.voter = voter;
    }

    public ArrayList<VotingPage> getVoterList() {
        return voterList;
    }

    public void setVoterList(ArrayList<VotingPage> voterList) {
        this.voterList = voterList;
    }

    @Override
    public String toString(){
        return this.voter +"<"+ this.rank +","+ voterList +">";
    }

    public double getRank() {
        return this.rank;
    }
}