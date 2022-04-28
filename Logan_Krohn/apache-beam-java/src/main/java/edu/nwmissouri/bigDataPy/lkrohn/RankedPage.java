package edu.nwmissouri.bigDataPy.lkrohn;

import java.io.Serializable;
import java.util.ArrayList;

public class RankedPage implements Serializable{
    String name = "unknown.md";
    Double rank = 1.000;
    ArrayList<VotingPage> voters = new ArrayList<VotingPage>();

    /**
     * Constructor with name and list of pages pointing to (voting) for this page.
     *
     * @param nameIn    - this page name
     * @param voters    - array list of pages pointing to this page
     */
    public RankedPage(String nameIn, ArrayList<VotingPage> voters) {
        this.name = nameIn;
        this.voters = voters;
    }

    /**
     * Constructor with name and list of pages pointing to (voting) for this page.
     *
     * @param nameIn    - this page name
     * @param voters    - this page's rank
     * @param votersIn  - array list of pages pointing to this page
     */
    public RankedPage(String nameIn, Double rankIn, ArrayList<VotingPage> votersIn) {
        this.name = nameIn;
        this.rank = rankIn;
        this.voters = votersIn;
    }

    public Double getRank() {
        return this.rank;
    }

    public ArrayList<VotingPage> getVoters() {
        return this.voters;
    }

    @Override
    public String toString() {
        return String.format("%s, %.5f, %s", this.name, this.rank, this.voters.toString());
    }
}
