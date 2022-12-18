import org.apache.hadoop.io.Text;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;

import static java.lang.Math.log;

public class Ranking {
    HashMap<Long,Float> docScores;
    //Calculate the Inverted Document Frequency ,t_f is the term frequency for a term and n_docs the total n° of docs

    public Ranking(){
        this.docScores = new HashMap<>();
    }

    public float idf (long df, int nDocs ){
        return (float) log(nDocs/df);
    }
/*
    public float bm25_ranking(double k, double b, ){
        //come calcolo la TF di ogni parola della query nel document d?
        //che strutture dati ho per la query e il documento d per lavorarci?
        //implementa infine formula slide pack 4 pag 61
        return null;
    } */
    public void updateScoreTFIDF(long docId,int tf,long df, int nDocs){
        float tfidfWeight = 0;
        //calcolare tf del term in quel docId
        tfidfWeight = (float) ((1 + log(tf))*idf(df,nDocs));
        if (!this.docScores.containsKey(docId))
            this.docScores.put(docId,tfidfWeight);
        else
            this.docScores.replace(docId, this.docScores.get(docId), this.docScores.get(docId)+tfidfWeight);
    }

    public void calculateTFIDFScoreQueryTerm( ArrayList<Long> docIds, ArrayList<Integer> tfs,long df, int nDocs){
        for (int i = 0; i<docIds.size(); i++){
            updateScoreTFIDF(docIds.get(i),tfs.get(i),df, nDocs);
        }
    }


    public static void main(String[] args) throws FileNotFoundException {
        /*

        Lexicon lex0 = new Lexicon(0);
        InvertedIndex invInd0 = new InvertedIndex(0);

        lex0.addElement(new Text("ciao                "), 1, invInd0);
        lex0.addElement(new Text("miao                "), 1, invInd0);
        lex0.addElement(new Text("miao                "), 2, invInd0);
        lex0.addElement(new Text("a                   "), 2, invInd0);
        lex0.addElement(new Text("miao                "), 2, invInd0);
        lex0.addElement(new Text("miao                "), 2, invInd0);
        lex0.addElement(new Text("z                   "), 2, invInd0);

        lex0.updateAllOffsetsInList();
        lex0.updateAllOffsetsTF(invInd0);
        InvertedIndex.compressListOfDocIDsAndAssignOffsetsDocIDs(lex0);

        invInd0.saveTForDocIDsCompressedOnFile(invInd0.compressListOfDocIDsAndAssignOffsetsDocIDs(lex0), "DOCID"+0, 0 );
        invInd0.saveTForDocIDsCompressedOnFile(invInd0.compressListOfTFs(), "TF"+0, 0);
        lex0.sortLexicon();
        lex0.saveLexiconOnFile("LEX"+0, 0);

        Text query = new Text("miao a");


         */



    }



}
