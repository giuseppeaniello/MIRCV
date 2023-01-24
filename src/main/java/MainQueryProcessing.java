import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;


public class MainQueryProcessing {
    static int flagStopWordAndStemming;

    // first args (from args[0] to args[N-4] included are query terms
    // args[N-3] is the flag for stemming and stopwords removal, args[N-3]==1 the stemming and stopwords removal are applied
    // args[N-2] is the flag to choose between conjunctive and disjunctive queries. args[N-2]==0 means conjunctive and args[N-2]==1 means disjunctive
    // args[N-1] is the flag to choose the scoring function. args[N-1]==0 means TFID, args[N-1]==1 means BM25
    public static void main(String[] args){
        try {
            MainQueryProcessing.flagStopWordAndStemming = Integer.parseInt(args[args.length-3]);
            Preprocessing p = new Preprocessing();
            DocumentTable.readDocumentTable();
        } catch (IOException e) {
            e.printStackTrace();
        }
        while(true) {
            String query = "";
            for(int i=0; i<args.length-3; i++){
                query += args[i];
                query += " ";
            }
            try {
                ArrayList<Text> queryTerms = Preprocessing.preprocess(query);
                System.out.println(queryTerms);
                LexiconFinal lexQuery = Ranking.createLexiconWithQueryTerm(queryTerms);
                if(Integer.parseInt(args[args.length-2]) == 0){
                    ConjunctiveQuery cq = new ConjunctiveQuery(queryTerms.size(), Integer.parseInt(args[args.length-1]));
                    ResultQueue qq = cq.computeTopK(lexQuery);
                    for(QueueElement element : qq.queue)
                        System.out.println(element.getDocID());
                }
                else{
                    MaxScore dq = new MaxScore(queryTerms.size(), Integer.parseInt(args[args.length-1]));
                    ResultQueue qq = dq.maxScore(lexQuery);
                    for(QueueElement element : qq.queue)
                        System.out.println(element.getDocID());
                }
            break;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
