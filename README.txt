structure:
----------
FIXED size fields:
   total RECORD_SIZE = 297

   RID_SIZE = 4
   REGISTER_NAME_SIZE = 14
   BN_NAME_SIZE = 200
   BN_STATUS_SIZE = 12
   BN_REG_DT_SIZE = 10
   BN_CANCEL_DT_SIZE = 10
   BN_RENEW_DT_SIZE = 10
   BN_STATE_NUM_SIZE = 10
   BN_STATE_OF_REG_SIZE = 3
   BN_ABN_SIZE = 20
   EOF_PAGENUM_SIZE = 4


steps to generate heapfile and run a query:
------

bash ./part3.sh BUSINESS_NAMES_201803.csv

This will:   
remove header (first line in csv file)

tail -n +2 BUSINESS_NAMES_201803.csv > BUSINESS_NAMES_201803.csv.nohead

then compile:

javac *.java

then create heap file (eg 4096 byte pages):

java dbload -p 4096 BUSINESS_NAMES_201803.csv.nohead

then query:

java dbquery "mf engineering" 4096



steps to generate a hashfile from the heapfile:
------

java hashload 4096

