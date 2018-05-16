import java.io.*;
import java.nio.ByteBuffer;
/**
 * Created by jarry on 14/05/2018.
 */

public class hashload implements dbimpl {

    int INTSIZE = (int) ((3.7*1000000) * 208);
    byte[] indexBucket = new byte[INTSIZE];
    int OFFSET = 8;

    public static void main(String args[]){
        // initalize new hashload
        hashload load = new hashload();

        // calculates the load time
        long startTime = System.currentTimeMillis();
        load.readArguments(args);
        long endTime = System.currentTimeMillis();

        // prints the load time to console
        System.out.println("Load time: " + (endTime - startTime) + "ms");
    }

    // read command line inputs
    public void readArguments(String args[]){
        // if there is only 1 arg input
        if (args.length == 1){
            if (isInteger(args[0])){
                readHeapFile(Integer.parseInt(args[0]));
            }
        }else{
            System.out.println("ERROR: Wrong numbr of command line arguments");
        }
    }

    // read the heapfile
    public void readHeapFile(int pagesize){
        // initalize variables
        // get the heapfile
        File heapfile = new File(HEAP_FNAME + pagesize);

        // create output file
        File outputfile = new File("hash." + pagesize);
        FileOutputStream outputtedFile = null;
        int size = 4;
        int pageCount = 0;
        int recordCount = 0;
        int recordLen = 0;
        int rid = 0;
        boolean isNextPage = true;
        boolean isNextRecord= true;
        try{
            // load input file
            FileInputStream inputFile = new FileInputStream(heapfile);
            outputtedFile = new FileOutputStream(outputfile);
            // while theres more pages keep running
            while (isNextPage){
                byte[] pageByteArray = new byte[pagesize];
                byte[] byteSize = new byte[size];
                inputFile.read(pageByteArray, 0, pagesize);

                // add page to pageByteArray array
                System.arraycopy(pageByteArray, pageByteArray.length-size, byteSize, 0, size);

                // if there more records
                isNextRecord= true;

                // while theres more records keep running
                while (isNextRecord){
                    byte[] byteRecordArray = new byte[RECORD_SIZE];
                    byte[] byteRid = new byte[size];
                    try{
                        // add array of record sizes from the page and add it to byteRecordArray
                        System.arraycopy(pageByteArray, recordLen, byteRecordArray, 0, RECORD_SIZE);

                        // check rid equals record number
                        System.arraycopy(byteRecordArray, 0, byteRid, 0, size);

                        // change the rid to int
                        rid = ByteBuffer.wrap(byteRid).getInt();

                        // Check if there are anymore records
                        if (rid != recordCount){
                            isNextRecord= false;
                        }else{
                            // get the offset
                            long offset = getOffSet(rid, pageCount, pagesize);

                            // hash the current record
                            hashRecord(byteRecordArray, offset);
                            recordLen += RECORD_SIZE;
                        }
                        // move the record counter
                        recordCount++;
                    }catch (ArrayIndexOutOfBoundsException e){
                        isNextRecord= false;
                        recordLen = 0;
                        recordCount = 0;
                        rid = 0;
                    }
                }
                // if there are anymore pages
                if (ByteBuffer.wrap(byteSize).getInt() != pageCount){
                    isNextPage = false;
                }
                // move the page counter
                pageCount++;
            }
            inputFile.close();
            // write to the outputfile
            outputtedFile.write(indexBucket);

            // inform user of that writing is done
            System.out.print("Finished");

            // Close the outputfile
            outputtedFile.close();

        }catch (FileNotFoundException e){
            // The input file is missing
            System.out.println("ERROR: " + HEAP_FNAME + pagesize + " not found");
        }catch (IOException e){
            // Error with trace
            e.printStackTrace();
        }
    }

    // use rid and pnumber and pagesize first then pass in
    // returns records containing the argument text from shell
    public void hashRecord(byte[] records, long offset)
    {
        //System.out.println(offset);
        // records business name as a string
        String record = new String(records);
        String BN_NAME = record.substring(RID_SIZE+REGISTER_NAME_SIZE,RID_SIZE+REGISTER_NAME_SIZE+BN_NAME_SIZE);

        // get record business name and it's position as bytes
        byte[] byteName = BN_NAME.getBytes();
        byte[] byteOffset = longToBytes(offset);
        byte[] size = new byte[BN_NAME_SIZE];
        System.arraycopy(byteName, 0, size, 0, byteName.length);

        // Add business name with its position in bRecord
        byte[] bRecord = new byte[208];
        System.arraycopy(size,0 , bRecord, 0,size.length);
        System.arraycopy(byteOffset,0 , bRecord, size.length ,byteOffset.length);

        // hash the busniess name
        int hash = hashfunction(BN_NAME.trim());

        //add the bRecord to the indexBucket array in the position of the hash
        System.arraycopy(bRecord,0, indexBucket, hash, bRecord.length);
    }


//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//                    TOOLS
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // int checker for string
    public boolean isInteger(String s){
      boolean isValidInt = false;
      try
      {
          Integer.parseInt(s);
          isValidInt = true;
      }
      catch (NumberFormatException e)
      {
          e.printStackTrace();
      }
      return isValidInt;
    }

    //  Convert Long to Byte
    public byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        // return Btyes
        return buffer.array();
    }

    //  Convert Byte to Long
    public long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        // flip its since it's backwards
        buffer.flip();
        // return Long
        return buffer.getLong();
    }

    // Get the offset
    public long getOffSet(int rid, int pnumber, int pagesize){
      long offset = (pnumber * pagesize)+(rid * RECORD_SIZE);
      return offset;
    }

    // Create hash
    public int hashfunction(String byteName){
        int hash = 0;
        int i = 0;
        int z =0;

        hash = Math.abs(((byteName.hashCode() + i) % INTSIZE));

        while(indexBucket[hash] != 0){
            z++;
            i = (z*208);
            hash = ((byteName.hashCode() + i) % INTSIZE) ;
        }
        return hash;
    }
}
