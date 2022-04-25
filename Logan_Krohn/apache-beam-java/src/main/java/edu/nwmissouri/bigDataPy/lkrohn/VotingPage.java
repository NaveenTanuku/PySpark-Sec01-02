package edu.nwmissouri.bigDataPy.lkrohn;

import java.io.Serializable;

public class VotingPage implements Serializable{
    String voterName = "unknown.md";
    Double rank = 1.000;
    Integer votes = 0;
    
    /**
     * Constructor for page name and count of votes made
     *
     * @param nameIn    - name of contributing page
     * @param voters    - count of votes made by contributing page
     */
    public VotingPage(String nameIn, Integer votesIn) {
        this.voterName = nameIn;
        this.votes = votesIn;
    }
    
    /**
     * Constructor for page, rank, and count of votes made
     *
     * @param nameIn    - page name
     * @param rankIn    - page rank
     * @param votesIn   - count of votes made
     */
    public VotingPage(String nameIn, Double rankIn, Integer votesIn) {
        this.voterName = nameIn;
        this.rank = rankIn;
        this.votes = votesIn;
    }

    public Double getRank() {
        return this.rank;
    }

    public Integer getVotes() {
        return this.votes;
    }

    public String getName() {
        return this.voterName;
    }

    @Override
    public String toString() {
        return String.format("%s, %.5f, %d", this.voterName, this.rank, this.votes);
    }

}
