import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;

import static java.lang.Math.log;

public class Ranking {
    HashMap<Long,Float> docScores;
    static int totalNumberDocuments = 8841822;
    //Calculate the Inverted Document Frequency ,t_f is the term frequency for a term and n_docs the total n° of docs

    public Ranking(){
        this.docScores = new HashMap<>();
    }

    public float idf (long df){
        return (float) log(totalNumberDocuments/df);
    }

    public void updateScoreTFIDF(long docId,int tf,long df){
        float tfidfWeight = 0;
        //calcolare tf del term in quel docId
        tfidfWeight = (float) ((1 + log(tf))*idf(df));
        if (!this.docScores.containsKey(docId))
            this.docScores.put(docId,tfidfWeight);
        else
            this.docScores.replace(docId, this.docScores.get(docId), this.docScores.get(docId)+tfidfWeight);
    }

    public void calculateTFIDFScoreQueryTerm( ArrayList<Long> docIds, ArrayList<Integer> tfs,long df){
        for (int i = 0; i<docIds.size(); i++){
            updateScoreTFIDF(docIds.get(i),tfs.get(i),df);
        }
    }

    public void printRankingTerm(){
        for (Long key : docScores.keySet()){
            System.out.println("DOCID: "+key+" SCORE: "+docScores.get(key));
        }
    }

    public float computeTermUpperBound(){
        float max = 0;
        for (Long key : docScores.keySet()){
            if(max< docScores.get(key))
                max = docScores.get(key);
        }
        return max;
    }

    public void updateScoreRSVBM25(long docId,float bm25){
        if (!this.docScores.containsKey(docId))
            this.docScores.put(docId,bm25);
        else
            this.docScores.replace(docId, this.docScores.get(docId), this.docScores.get(docId)+bm25);
    }

    public void computeRSVbm25(ArrayList<Long> docIds, ArrayList<Integer> tfs,DocumentTable dt, int df){
        /*mi serve
        *
        * */
        for(int i=0; i<docIds.size(); i++){
            long id= docIds.get(i);
            float bm25= calculateRSVbm25(tfs.get(i),df,dt.getAverageLength(),dt.docTab.get(id) );
            updateScoreRSVBM25(id,bm25);
        }
    }

    public float calculateRSVbm25(int tf, float df,float averagedl, float dl){
        float b= 0.75F;
        float k= 1.2F;
        float bm25= (float) ( (tf/ ((k*( (1-b)+ (b*(dl/averagedl)) ))+tf) ) * log(totalNumberDocuments/df));
        return bm25;
    }


    public static void main(String[] args) throws FileNotFoundException {


        LexiconFinal lex = LexiconFinal.readFinalLexiconFromFile("LexiconFinal");
        lex.printLexiconFinal();

        DocumentTable dt = DocumentTable.readDocumentTable("document_table");

        dt.printDocumentTable();

        System.out.println("FINEE");



    }


    //bm25
    //termupperbound


}
