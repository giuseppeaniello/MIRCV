import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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
                long start = System.currentTimeMillis();
                ArrayList<Text> queryTerms = Preprocessing.preprocess(query);
                System.out.println(queryTerms);
                String lexPath;
                if(MainQueryProcessing.flagStopWordAndStemming == 1)
                    lexPath="LexiconFinalStemmedAndStopWordRemoved";
                else
                    lexPath = "LexiconFinalWithoutStemmingAndStopWordRemoval";
                RandomAccessFile lexFile = new RandomAccessFile(lexPath,"r");
                FileChannel lexChannel = lexFile.getChannel();
                LexiconFinal lexQuery = Ranking.createLexiconWithQueryTerm(queryTerms,lexChannel);
                if(Integer.parseInt(args[args.length-2]) == 0){
                    ConjunctiveQuery cq = new ConjunctiveQuery(lexQuery.lexicon.size(), Integer.parseInt(args[args.length-1]));
                    ResultQueue qq = cq.computeTopK(lexQuery);
                    for(QueueElement element : qq.queue)
                        System.out.println(element.getDocID());
                }
                else{
                    MaxScore dq = new MaxScore(lexQuery.lexicon.size(), Integer.parseInt(args[args.length-1]));
                    ResultQueue qq = dq.maxScore(lexQuery);
                    for(QueueElement element : qq.queue)
                        System.out.println(element.getDocID());
                }
                long end = System.currentTimeMillis();
                long time = end-start;
                System.out.println("tempo per la query: " + time + " millis");
                break;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
