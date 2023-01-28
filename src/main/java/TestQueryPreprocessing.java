import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class TestQueryPreprocessing {
    private HashMap<Integer,String> query;
    private HashMap<Integer, ArrayList<Text>> queryPreproc;
    private HashMap<Integer,Long> result;

    public TestQueryPreprocessing(){
        this.query = new HashMap<>();
        this.queryPreproc = new HashMap<>();
        this.result = new HashMap<>();
    }
    public void readQueryFromDocument() throws IOException {
        File file = new File("queryTestRed.tsv");

        LineIterator it = FileUtils.lineIterator(file,"UTF-8");
        while(it.hasNext()){
            String docCurrent = it.nextLine();
            int queryId = Integer.parseInt(docCurrent.split("\\t")[0]);
            String queryText = docCurrent.split("\\t")[1];
            query.put(queryId,queryText);
        }
    }
    public void preprocessingQuery() throws IOException {
        for (Integer id : query.keySet()){
            ArrayList<Text> tmp = Preprocessing.preprocess(query.get(id));
            queryPreproc.put(id,tmp);
        }
    }
    public void saveResultOnFile(){
        File resultFile = new File("resultQueryRed.tsv");

    }
    public static void main (String []args) throws IOException {
        int flagStopWordAndStemming = 1;
        TestQueryPreprocessing tqp = new TestQueryPreprocessing();
        tqp.readQueryFromDocument();
        tqp.preprocessingQuery();
        String pathDocTable;
        if( flagStopWordAndStemming==1 )
            pathDocTable = "document_table_stemmed_and_stopword_removed";
        else
            pathDocTable = "document_table_without_stemming_and_stopword_removal";

        RandomAccessFile docTableFile = new RandomAccessFile(pathDocTable,"r");

        FileChannel docTableChannel = docTableFile.getChannel();
        DocumentTable.readDocumentTable(docTableChannel);
        String lexPath;
        if(MainQueryProcessing.flagStopWordAndStemming == 1)
            lexPath="LexiconFinalStemmedAndStopWordRemoved";
        else
            lexPath = "LexiconFinalWithoutStemmingAndStopWordRemoval";
        RandomAccessFile lexFile = new RandomAccessFile(lexPath,"r");
        FileChannel lexChannel = lexFile.getChannel();
        LexiconFinal lexQuery = new LexiconFinal();

        for(Integer id : tqp.queryPreproc.keySet() ) {
            lexQuery = Ranking.createLexiconWithQueryTerm(tqp.queryPreproc.get(id), lexChannel);
            if(!lexQuery.lexicon.isEmpty()) {
                MaxScore dq = new MaxScore(lexQuery.lexicon.size(), 1);
                ResultQueue qq = dq.maxScore(lexQuery);
                System.out.println(qq.queue.size());
            }
            else System.out.println("Vuoto");
            //System.out.println(qq.queue.size());
        }
    }
}
