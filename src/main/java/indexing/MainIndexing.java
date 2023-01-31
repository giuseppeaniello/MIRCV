package indexing;

import java.io.IOException;

public class MainIndexing {

    public static void main(String[] args) {
        try { // flag==1 means stemming and stopword removal are applied
            Indexing.readDoc(1);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
