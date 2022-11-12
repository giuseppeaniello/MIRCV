import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Watchable;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Scanner; // Import the Scanner class to read text files
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.analysis.*;
import org.tartarus.snowball.ext.PorterStemmer;


public class preprocessing {

    public static void preprocess (String doc_in) {  //applies the preprocessing

    String doc_out=doc_in;
    String nostopwords;
    int id=getDocID(doc_in);
    System.out.println("ID: "+ id);
    doc_out=textclean(doc_out);
    doc_out=doc_out.toLowerCase(); //To lower case
    System.out.println("DOC IN"+doc_in);
    System.out.println("DOC OUT"+doc_out);
    String stopwords= getStopwords();
    ArrayList<String> doc_no_sw=removeStopwords(doc_out,stopwords);
    ArrayList<String> doc_stemmed= stemming(doc_no_sw);
    for(String i:doc_stemmed)
        System.out.println(i);
    /*System.out.println("NO STOPWORDS");
    for(String i:doc_no_sw) {
        System.out.print(i+" ");

    }*/

    }

    public static String getStopwords(){ //ritorna il dizionario di stopwords
        String file=("C:\\Users\\Rauro\\OneDrive\\Desktop\\Uni\\Information Retrivial\\stopwords.txt");
        String stopwords="";
        String temp;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));) {
            temp=br.readLine();
            if(temp!=null) {
                stopwords=stopwords+(temp);
                temp=br.readLine();
                while (temp != null) {
                    stopwords=stopwords+(temp);
                    temp = br.readLine();
                }
                return stopwords;
            }
            else
                return null;
            // reads a byte at a time, if it reached end of the file, returns -1

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

    }
    //Remove the stopwords in the document that are present in the stopword dictionary
    public static ArrayList removeStopwords(String document_in,String stopwords){
        if(document_in==null){
            System.out.println("Document null");
            return null;
        }
        ArrayList<String> doc_final= new ArrayList<String>();
        String doc_w[]=document_in.split("\\s+"); //split the string by space separator
        String sw_w[]=stopwords.split("\\,+");
        int is_stopword=0;
        for(String i:doc_w){
            is_stopword=0;
            for(String y:sw_w){
                if (i.equals(y)) {
                    is_stopword = 1;
                    System.out.println("STOP WORD DETECTED");
                    break;
                }
            }
            if(is_stopword==0)
                doc_final.add(i);
        }
        return doc_final;
    }

    public static String textclean(String document_in) { //Convert the input document from ASCII to UNICODE

        document_in=getDocument(document_in);
        // strips off all non-ASCII characters
        document_in = document_in.replaceAll("[^\\x00-\\x7F]", "");

        // erases all the ASCII control characters
        document_in = document_in.replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", "");

        // removes non-printable characters from Unicode
        document_in = document_in.replaceAll("\\p{C}", "");
        document_in= document_in.replaceAll("'s","");
        document_in= document_in.replaceAll("[!-._'?\"]","");
        return document_in;
    }
    public static int getDocID(String row){ //return the document id part
        //String ids[]= row.split("\\s+");
        //System.out.println(ids[0]);
        //return Integer.valueOf(ids[0]);
        Pattern p = Pattern.compile("^[0-9].+\t");
        Matcher m = p.matcher(row);
        String res="";
        if(m.find())
        res= (m.group(0));
        if(res.equals("")){
            System.out.println("ID NOT FOUND");
            return -1;
        }
        res=res.replaceAll("\\s+","");
        return Integer.valueOf(res);
    }
    public static String getDocument(String row){ //return the document removing the id part
        return row=row.replaceAll("^[0-9].*\t"," ");
    }
    public static ArrayList<String> stemming(ArrayList<String> doc_in){
        ArrayList<String> doc_out= new ArrayList<String>();
        PorterStemmer stemmer= new PorterStemmer();
        if(doc_in.isEmpty()){
            System.out.println("Document not found. Can't stem");
            return null;
        }
        for(int i=0;i<doc_in.size();i++){
            stemmer.setCurrent(doc_in.get(i)); //set string you need to stem
            stemmer.stem();  //stem the word
            doc_out.add(stemmer.getCurrent());//get the stemmed word
        }
        return doc_out;
    }


        public static void main(String[] args){
        String text= "0	The presence of communication amid scientific minds was equally important to the success of the Manhattan Project as scientific intellect was. The only cloud hanging over the impressive achievement of the atomic researchers and engineers is what their success truly meant; hundreds of thousands of innocent lives obliterated.";
        String text2= "8841817\tThat's chemistry  123 too! Fireworks get their color from metal compounds (also known as metal salts) packed inside. You probably know that if you burn metals in a hot flame (such as a Bunsen burner in a school laboratory), they glow with very intense colorsâ\u0080\u0094 that's exactly what's happening in fireworks.";
        preprocessing.preprocess(text2);

    }


}
