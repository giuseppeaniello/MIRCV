package queryProcessing;

import indexing.DocumentTable;
import indexing.LexiconFinal;
import indexing.Ranking;
import org.apache.hadoop.io.Text;
import preprocessing.Preprocessing;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Scanner;

public class MainQueryProcessing {

    private static boolean flagStemmingAndStopWordRemoval; // 1 mean stemming and stopword removal
    private static boolean flagConjunctiveOrDisjunctive; // 1 means disjunctive
    private static boolean flagTfidfOrBM25; // 1 means BM25

    public MainQueryProcessing(){
        System.out.println("Do you want stemming and stopword removal? \n 0 = NO \n 1 = YES");
        Scanner input = new Scanner(System.in);
        if(input.nextLine().equals("1"))
            flagStemmingAndStopWordRemoval = true;
        else
            flagStemmingAndStopWordRemoval = false;
    }

    public static void setFlagStemmingAndStopWordRemoval(boolean flagStemmingAndStopWordRemoval) {
        MainQueryProcessing.flagStemmingAndStopWordRemoval = flagStemmingAndStopWordRemoval;
    }

    public static boolean getFlagStemmingAndStopWordRemoval() {
        return flagStemmingAndStopWordRemoval;
    }

    public static boolean getFlagConjunctiveOrDisjunctive() {
        return flagConjunctiveOrDisjunctive;
    }

    public static boolean getFlagTfidfOrBM25() {
        return flagTfidfOrBM25;
    }

    public static void main(String[] args){
        MainQueryProcessing queryProc = new MainQueryProcessing();
        try {
            Preprocessing p = new Preprocessing();
            String pathDocTable;
            if(MainQueryProcessing.flagStemmingAndStopWordRemoval)
                pathDocTable = "document_table_stemmed_and_stopword_removed";
            else
                pathDocTable = "document_table_without_stemming_and_stopword_removal";
            RandomAccessFile docTableFile = new RandomAccessFile(pathDocTable,"r");
            FileChannel docTableChannel = docTableFile.getChannel();
            DocumentTable.readDocumentTable(docTableChannel);
            Scanner input = new Scanner(System.in);
            while(true){
                System.out.println("Do you want to perform conjunctive or disjunctive query? \n 0 = CONJUNCTIVE \n 1 = DISJUNCTIVE");
                if(input.nextLine().equals("1"))
                    flagConjunctiveOrDisjunctive = true;
                else
                    flagConjunctiveOrDisjunctive = false;
                System.out.println("Do you want to use TFIDF or BM25 as scoring function? \n 0 = TFIDF \n 1 = BM25");
                if(input.nextLine().equals("1"))
                    flagTfidfOrBM25 = true;
                else
                    flagTfidfOrBM25 = false;
                System.out.println("Insert your query and press enter: ");
                String query = input.nextLine();
                long start = System.currentTimeMillis();
                ArrayList<Text> queryTerms = Preprocessing.preprocess(query, flagStemmingAndStopWordRemoval);
                String lexPath;
                if(flagStemmingAndStopWordRemoval)
                    lexPath="LexiconFinalStemmedAndStopWordRemoved";
                else
                    lexPath = "LexiconFinalWithoutStemmingAndStopWordRemoval";
                RandomAccessFile lexFile = new RandomAccessFile(lexPath,"r");
                FileChannel lexChannel = lexFile.getChannel();
                LexiconFinal lexQuery = Ranking.createLexiconWithQueryTerm(queryTerms,lexChannel);


                if(flagConjunctiveOrDisjunctive){
                    MaxScore dq = new MaxScore(lexQuery.lexicon.size(), flagTfidfOrBM25);
                    ResultQueue qq = dq.maxScore(lexQuery);
                    for(QueueElement element : qq.queue) {
                        if (element.getDocID() == -1)
                            System.out.println("No Result Found");
                        else
                            System.out.println(element.getDocID() - 1);
                    }
                }
                else{
                    ConjunctiveQuery cq = new ConjunctiveQuery(lexQuery.lexicon.size(), flagTfidfOrBM25);
                    ResultQueue qq = cq.computeTopK(lexQuery);
                    for(QueueElement element : qq.queue){
                        if (element.getDocID() == -1)
                            System.out.println("No Result Found");
                        else
                            System.out.println(element.getDocID()-1);

                    }
                }
                long end = System.currentTimeMillis();
                long time = end-start;
                System.out.println("Your query took: " + time + " milliseconds");
                System.out.println("Press 1 to perform another query, press 0 to stop the program");
                if (input.nextLine().equals("1"))
                    continue;
                else
                    System.out.println("Good bye!");
                    return;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }


}
