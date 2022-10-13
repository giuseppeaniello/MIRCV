import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.text.Normalizer;
import java.util.Scanner; // Import the Scanner class to read text files

public class preprocessing {

    public static void preprocess (String doc_in) {  //applies the preprocessing

    String doc_out=doc_in;
    String nostopwords;
    doc_out=textclean(doc_out);
    System.out.println(doc_in);
    System.out.println(doc_out);
    String stopwords= getStopwords();
    String temp[]; //split del doc out in piu parole
    for(String word:stopwords){

    }

    }

    public static String getStopwords(){ //ritorna il dizionario di stopwords
        String file=("C:\\Users\\Rauro\\OneDrive\\Desktop\\Uni\\Information Retrivial\\stopwords.txt");
        String stopwords="";
        String temp;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(
                new FileInputStream(file), "UTF-8"));) {
            temp=br.readLine();
            System.out.println(temp);
            if(temp!=null) {
                stopwords=stopwords+(temp);
                System.out.println(stopwords);
                temp=br.readLine();
                while (temp != null) {
                    stopwords=stopwords+(temp);
                    temp = br.readLine();
                }
            }
            else
                return null;
            return stopwords;
            // reads a byte at a time, if it reached end of the file, returns -1

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

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
    public static String getDocument(String row){ //return the document removing the id part
        return row=row.replaceAll("^[0-9].*\t"," ");

    }
        public static void main(String[] args){
        String text= "0	The presence of communication amid scientific minds was equally important to the success of the Manhattan Project as scientific intellect was. The only cloud hanging over the impressive achievement of the atomic researchers and engineers is what their success truly meant; hundreds of thousands of innocent lives obliterated.";
        String text2= "8841817\tThat's chemistry  123 too! Fireworks get their color from metal compounds (also known as metal salts) packed inside. You probably know that if you burn metals in a hot flame (such as a Bunsen burner in a school laboratory), they glow with very intense colorsâ\u0080\u0094 that's exactly what's happening in fireworks.";
        preprocessing.preprocess(text2);

    }


}
