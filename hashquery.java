import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by jarry on 14/05/2018.
 */
public class hashquery  implements dbimpl {

    int INTSIZE = (int) ((3.7*1000000) * 208);
    int OFFSET = 8;

    public static void main(String args[])
    {
        hashquery load = new hashquery();
        // calculate query time
        long startTime = System.currentTimeMillis();
        load.readArguments(args);
        long endTime = System.currentTimeMillis();

        System.out.println("Query time: " + (endTime - startTime) + "ms");
    }

    @Override
    public void readArguments(String[] args) {
        if (args.length == 2){
            if (isInteger(args[1])){
                getRecord(args[0], Integer.parseInt(args[1]));
            }
        }else{
            System.out.println("Error: only pass in two arguments");
        }
    }

    public void getRecord(String name, int pagesize )
    {
        // get hashfile
        File hashfile = new File("hash."+ pagesize);

        //String heapname = "heap.pagesize";
        File heapfile = new File(HEAP_FNAME + pagesize);

        try
        {
            FileInputStream inputFile = new FileInputStream(hashfile);
            FileInputStream heapInputFile = new FileInputStream(heapfile);

            byte[] index = new byte[BN_NAME_SIZE + OFFSET];
            byte[] record = new byte[RECORD_SIZE];
            byte heapRecords[] = new byte[(int) heapfile.length()];

            // get the offset
            int hash = hashfunction(name);
            inputFile.skip(hash);
            inputFile.read(index, 0, index.length);
            byte[] businessName = new byte[BN_NAME_SIZE];
            byte[] offset = new byte[OFFSET];

            // Split the array into business name and offset
            System.arraycopy(index, businessName.length, offset,0,offset.length);
            Long offsetLong = new Long(bytesToLong(offset));
            System.arraycopy(index,0,businessName,0,businessName.length);

            // Read heapRecords
            heapInputFile.read(heapRecords);

            // copies bytes from heapfile to records bytes array
            System.arraycopy(heapRecords, Math.toIntExact(offsetLong), record,0,record.length);

            // Exiting process
            inputFile.close();
            heapInputFile.close();
          }catch (FileNotFoundException e){
              // The input file is missing
              System.out.println("ERROR: " + name + " not found");
          }catch (IOException e){
              // Error with trace
              e.printStackTrace();
          }
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

        // Create hash
        public int hashfunction(String busniessName){
            int hash = 0;
            int i = 0;
            hash = Math.abs(((busniessName.hashCode() + i) % INTSIZE));
            return hash;
        }
        // Get the offset
        public long getOffSet(int rid, int pnumber, int pagesize){
          long offset = (pnumber * pagesize)+(rid * RECORD_SIZE);
          return offset;
        }
        // Convert bytes to long
        public long bytesToLong(byte[] bytes) {
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.put(bytes);
            buffer.flip();
            return buffer.getLong();
        }

}
