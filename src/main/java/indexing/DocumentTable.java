package indexing;
import org.jetbrains.annotations.NotNull;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

import static java.nio.file.StandardOpenOption.WRITE;
public class DocumentTable {
    //DocTab contains as key the DocId and as key the length of document
    private static HashMap<Long, Integer> docTab;
    // average length of all documents
    private static float averageLength;

    public DocumentTable(){
        docTab = new HashMap<>();
        averageLength = 0;
    }

    public static HashMap<Long, Integer> getDocTab() {
        return docTab;
    }

    public void setAverageLength(float averageLength) {
        this.averageLength = averageLength;
    }

    public static float getAverageLength() {
        return averageLength;
    }

    // method to save the document table on file filePath
    public void saveDocumentTable(FileChannel fc) throws IOException {
        ByteBuffer buffer = null;
        for(Long key : docTab.keySet()){
            buffer = ByteBuffer.wrap(convertLongToByteArr(key));
            while (buffer.hasRemaining()) {
                fc.write(buffer);
            }
            buffer.clear();
            byte[] valueByte = convertIntToByteArray(docTab.get(key));
            buffer = ByteBuffer.wrap(valueByte);
            while (buffer.hasRemaining()) {
                fc.write(buffer);
            }
            buffer.clear();
        }
        buffer = ByteBuffer.wrap(convertFloatToByteArray(averageLength));
        while (buffer.hasRemaining()) {
            fc.write(buffer);
        }
        buffer.clear();
    }

    // method to load the document table from disk to main memory
    public static DocumentTable readDocumentTable(FileChannel fc) throws IOException {
        System.out.println("Loading document table");
        DocumentTable result = new DocumentTable();
        ByteBuffer buffer = null;
        for(int i = 0; i<fc.size()-4; i+=12) {
            fc.position(i);
            buffer = ByteBuffer.allocate(8);
            do {
                fc.read(buffer);
            } while (buffer.hasRemaining());
            byte[] docIdByte = buffer.array();
            buffer.clear();
            fc.position( i+8 );
            buffer = ByteBuffer.allocate(4);
            do {
                fc.read(buffer);
            } while (buffer.hasRemaining());
            byte[] lenDocByte = buffer.array();
            buffer.clear();
            long docId = convertByteArrToLong(docIdByte); // method to convert byte[] to long
            int lenDoc = convertByteArrToInt(lenDocByte); // method to convert byte[] to int
            result.docTab.put(docId, lenDoc);
        }
        fc.position(fc.size()-4);
        buffer = ByteBuffer.allocate(4);
        do {
            fc.read(buffer);
        } while(buffer.hasRemaining());
        result.setAverageLength(ByteBuffer.wrap(buffer.array()).getFloat());
        System.out.println("Document table uploaded");
        return result;
    }

    // method to compute the average length
    public void calculateAverageLength(){
        int sum=0;
        for(Long docId:this.docTab.keySet()){
            sum += docTab.get(docId);
        }
        setAverageLength(sum/ docTab.size());
    }

    private static byte[] convertIntToByteArray(int length) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(length);
        return bb.array();
    }

    private static byte[] convertFloatToByteArray(float value) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putFloat(value);
        return bb.array();
    }

    private static long convertByteArrToLong(byte[] bytes) {
        long value = 0L;
        for (byte b : bytes) {
            value = (value << 8) + (b & 255);
        }
        return value;
    }

    private static int convertByteArrToInt(byte[] bytes){
        int value = 0;
        for (byte b : bytes) {
            value = (value << 8) + (b & 0xFF);
        }
        return value;
    }

    public static byte[] convertLongToByteArr(long number){
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(number);
        return bb.array();
    }

    //Method used to test the document table
    public void printDocumentTable(){
        for (Long key : docTab.keySet() ){
            System.out.println("Doc: "+ key+ " Length: "+ docTab.get(key));
        }
        System.out.println("Average length: " + averageLength);
    }
}
