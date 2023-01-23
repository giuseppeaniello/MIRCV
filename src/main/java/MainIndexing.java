import java.io.IOException;

public class MainIndexing {

    public static void main(String[] args) {

        try {
            ReadingDocuments.readDoc();

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
