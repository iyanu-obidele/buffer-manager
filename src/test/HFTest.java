package test;

import global.Convert;
import global.Minibase;
import global.RID;
import heap.HeapFile; 
import heap.HeapScan;

/**
 * Test suite for the heap layer.
 */
class HFTest extends TestDriver {

  /** The display name of the test suite. */
  private static final String TEST_NAME = "heap file tests";

  /**
   * Size of heap file to create in test cases (65 for multiple data pages; 6500
   * for multiple directory pages).
   */
  private static final int FILE_SIZE = 6500;
  
  /** used by all tests */
  RID rid = new RID();

  /**
   * Test application entry point; runs all tests.
   */
  public static void main(String argv[]) {

    // create a clean Minibase instance
    HFTest hft = new HFTest();
    hft.create_minibase();

    // run all the test cases
    System.out.println("\n" + "Running " + TEST_NAME + "...");
    boolean status = PASS;
    status &= hft.test1();
    status &= hft.test2();
    status &= hft.test3();
    status &= hft.test4();

    // display the final results
    System.out.println();
    if (status != PASS) {
      System.out.println("Error(s) encountered during " + TEST_NAME + ".");
    } else {
      System.out.println("All " + TEST_NAME + " completed successfully!");
    }

  } // public static void main (String argv[])

  /**
   * 
   */
  protected boolean test1() {
	  
	//Start saving count of I/Os
	initCounts();
	saveCounts(null);

    System.out.println("\n  Test 1: Insert and scan fixed-size records\n");
    boolean status = PASS;
    HeapFile f = null;

    System.out.println("  - Create a heap file\n");
    try {
      f = new HeapFile("file_1");
    } catch (Exception e) {
      System.err.println("*** Could not create heap file\n");
      e.printStackTrace();
      return false;
    }

    if (Minibase.BufferManager.getNumUnpinned() != 
    		Minibase.BufferManager.getNumFrames()) {
      System.err.println("*** The heap file has left pages pinned\n");
      status = FAIL;
    }

    System.out.println("  - Add " + FILE_SIZE + " records to the file\n");
    for (int i = 0; (i < FILE_SIZE) && (status == PASS); i++) {

      // fixed length record
      DummyRecord rec = new DummyRecord();
      rec.ival = i;
      rec.fval = (float) (i * 2.5);
      rec.name = "record" + i;

      try {
        rid = f.insertRecord(rec.toByteArray());
      } catch (Exception e) {
        status = FAIL;
        System.err.println("*** Error inserting record " + i + "\n");
        e.printStackTrace();
      }

      if (status == PASS
          && Minibase.BufferManager.getNumUnpinned() != Minibase.BufferManager
              .getNumFrames()) {

        System.err.println("*** Insertion left a page pinned\n");
        status = FAIL;
      }
    }

    //Check the size of the file
    try {
      if (f.getRecCnt() != FILE_SIZE) {
      status = FAIL;
        System.err.println("*** File reports " + f.getRecCnt()
            + " records, not " + FILE_SIZE + "\n");
      }
    } catch (Exception e) {
      status = FAIL;
      System.out.println("" + e);
      e.printStackTrace();
    }

    // In general, a sequential scan won't be in the same order as the
    // insertions. However, we're inserting fixed-length records here, and
    // in this case the scan must return the insertion order.

    System.out.println("  - Scan the records just inserted\n");
    
    //Open the scan
    HeapScan scan = null;
    try {
      scan = f.openScan();
    } catch (Exception e) {
      System.err.println("*** Error opening scan\n");
      e.printStackTrace();
      return false;
    }
    if (Minibase.BufferManager.getNumUnpinned() != Minibase.BufferManager
            .getNumFrames()-1) {
      System.err
          .println("*** The heap-file scan has not pinned the first page\n");
      status = FAIL;
    }

    //Initiate the scan
    DummyRecord rec = null;
    byte[] record = null;
    try {
  	  record = scan.getNext(rid);
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
    
    //while there is a record to process
    int len, i=0;
    while (record != null) {

      //if we did not make a mistake processing the previous record
      if (status == PASS ) {
    	 
    	//Assign attributes to the record
        try {
          rec = new DummyRecord(record);
        } catch (Exception e) {
          System.err.println("" + e);
          e.printStackTrace();
          status = FAIL;
        }

        //verify the length of the record
        len = record.length;
        if (len != rec.length()) {
          System.err.println("*** Record " + i + " had unexpected length "
              + len + "\n");
          status = FAIL;
        } 
        
        //verify the attributes of the record
        String name = ("record" + i);
        if ((rec.ival != i) || (rec.fval != (float) i * 2.5)
            || (!name.equals(rec.name))) {
          System.err.println("*** Record " + i
              + " differs from what we inserted\n");
          System.err.println("rec.ival: " + rec.ival + " should be " + i
              + "\n");
          System.err.println("rec.fval: " + rec.fval + " should be "
              + (i * 2.5) + "\n");
          System.err.println("rec.name: " + rec.name + " should be " + name
              + "\n");
          status = FAIL;
        }
      }
      
      //increment the counter, get the next record
      ++i;
      try {
      	record = scan.getNext(rid);
        } catch (Exception e) {
          status = FAIL;
          System.err.println("Could not retrieve record "+i+" .\n");
          e.printStackTrace();
          return false;
        }
    }

    // The scan is done, close it.
    scan.close();
    if (Minibase.BufferManager.getNumUnpinned() != Minibase.BufferManager
            .getNumFrames()) {
      System.err.println("*** The heap-file scan has not unpinned "
              + "its pages after finishing\n");
      status = FAIL;
    } else if (i != (FILE_SIZE)) {
      status = FAIL;
      System.err.println("*** Scanned " + i + " records instead of "
              + FILE_SIZE + "\n");
    }

    if (status == PASS)
      System.out.println("  Test 1 completed successfully.\n");
    
    //save I/O counts
    saveCounts("test1");
    saveCounts(null);
    
    return status;

  } // protected boolean test1()

  /**
   * 
   */
  protected boolean test2() {

    System.out.println("\n  Test 2: Delete fixed-size records, scan remaining records\n");
    boolean status = PASS;
    HeapScan scan = null;
    HeapFile f = null;

    System.out.println("  - Open the same heap file as test 1\n");
    try {
      f = new HeapFile("file_1");
    } catch (Exception e) {
      System.err.println(" Could not open heap file");
      e.printStackTrace();
      return false;
    }

    //Open the scan
    try {
      scan = f.openScan();
    } catch (Exception e) {
      System.err.println("*** Error opening scan\n");
      e.printStackTrace();
      return false;
    }

    //Initiate the scan
    byte[] record;
    try {
        record = scan.getNext(rid);
      } catch (Exception e) {
    	System.err.println("*** Error initiating scan\n");
        e.printStackTrace();
        return false;
      }

    System.out.println("  - Delete half the records\n");
    int i = 0;
    while (record != null) {

    	// Delete the odd-numbered ones.
    	if (i % 2 == 1) { 
        try {
          f.deleteRecord(rid);
        } catch (Exception e) {
          status = FAIL;
          System.err.println("*** Error deleting record " + i + "\n");
          e.printStackTrace();
          break;
        }
      }
      
      //increment the counter, get the next record
      ++i;
      try {
        	record = scan.getNext(rid);
      } catch (Exception e) {
        status = FAIL;
        System.err.println("*** Error retrieving record " + i + "\n");
        e.printStackTrace();
        break;
      }
    }

    //Close the scan
    try {
      scan.close();  
    } catch (Exception e) {
        status = FAIL;
        System.err.println("*** Error closing scan " + i + "\n");
        e.printStackTrace();
    }

    //Check that no pages are pinned
    if (status == PASS
        && Minibase.BufferManager.getNumUnpinned() != Minibase.BufferManager
            .getNumFrames()) {
      System.err.println("*** Deletion left a page pinned\n");
      status = FAIL;
    }

    //Open and initiate the scan
    try {
      scan = f.openScan();
    } catch (Exception e) {
      status = FAIL;
      System.err.println("*** Error opening scan\n");
      e.printStackTrace();
      return false;
    }
    try {
    	record = scan.getNext(rid);
    } catch (Exception e) {
    	System.err.println("*** Error initiating scan\n");
        e.printStackTrace();
        return false;
    }

    System.out.println("  - Scan the remaining records\n");
    i=0;
    DummyRecord rec = null;
    while (record != null) {

      try {
        rec = new DummyRecord(record);
      } catch (Exception e) {
        System.err.println("" + e);
        e.printStackTrace();
      }

      if ((rec.ival != i) || (rec.fval != (float) i * 2.5)) {
        System.err.println("*** Record " + i
          + " differs from what we inserted\n");
        System.err.println("rec.ival: " + rec.ival + " should be " + i
            + "\n");
        System.err.println("rec.fval: " + rec.fval + " should be "
            + (i * 2.5) + "\n");
        status = FAIL;
        break;
      }
        
      //increment the counter and get the next record
      i += 2; // Because we deleted the odd ones...
      try {
      	record = scan.getNext(rid);
      } catch (Exception e) {
      	System.err.println("Could not retrieve record "+i+" .\n");
          e.printStackTrace();
          break;
      }
    }
    
    scan.close();

    //save I/O counts
    saveCounts("test2");
    saveCounts(null);
    
    if (status == PASS)
      System.out.println("  Test 2 completed successfully.\n");
    return status;

  } // protected boolean test2()

  /**
   * 
   */
  protected boolean test3() {

    System.out.println("\n  Test 3-1: Update fixed-size records\n");
    boolean status = PASS;
    HeapScan scan = null;
    HeapFile f = null;

    System.out.println("  - Open the same heap file as tests 1 and 2\n");
    try {
      f = new HeapFile("file_1");
    } catch (Exception e) {
      System.err.println("*** Could not create heap file\n");
      e.printStackTrace();
      return false;
    }

    //Open and initiate the scan
    byte[] record;
    try {
      scan = f.openScan();
    } catch (Exception e) {
      System.err.println("*** Error opening scan\n");
      e.printStackTrace();
      return false;
    }
    try {
      record = scan.getNext(rid);
    } catch (Exception e) {
      System.err.println("*** Error initiating scan\n");
        e.printStackTrace();
        return false;
    }
    
    //Update every record
    System.out.println("  - Change the records\n");
    int i = 0;
    DummyRecord rec = null;
    while (record != null) {

      //assign attributes to the record
      try {
        rec = new DummyRecord(record);
      } catch (Exception e) {
        System.err.println("" + e);
        e.printStackTrace();
      }

      //change the record's float value and update the disk copy
      rec.fval = (float) 7 * i; 
      byte[] newrecord = null;
      newrecord = rec.toByteArray();
      try {
        f.updateRecord(rid, newrecord);
      } catch (Exception e) {
        status = FAIL;
        e.printStackTrace();
        System.err.println("*** Error updating record " + i + "\n");
        return false;
      }
        
      //increment counter and get next record
      i += 2; // Recall, we deleted every other record above.
      try {
       	record = scan.getNext(rid);
      } catch (Exception e) {
    	status = FAIL;
        System.err.println("Could not retrieve record "+i+" .\n");
          e.printStackTrace();
          return false;
      }
    }

    //close the scan and check that no pages are pinned.
    scan.close();
    scan = null;
    if (Minibase.BufferManager.getNumUnpinned() != Minibase.BufferManager
            .getNumFrames()) {
      System.out.println("t3, Number of unpinned buffers: "
          + Minibase.BufferManager.getNumUnpinned() + "\n");
      System.err.println("t3, getNumbfrs: "
          + Minibase.BufferManager.getNumFrames() + "\n");
      System.err.println("*** Updating left pages pinned\n");
      status = FAIL;
    }
      
    System.out.println("  - Check that the updates are really there\n");
    
    //Open and initiate a scan
    try {
        scan = f.openScan();
    } catch (Exception e) {
      System.err.println("*** Error opening scan\n");
      e.printStackTrace();
      return false;
    }
    try {
      record = scan.getNext(rid);
    } catch (Exception e) {
      System.err.println("*** Error initiating scan\n");
        e.printStackTrace();
      return false;
    }

    //Scan the records, verifying all values, including the update
    i = 0;
    DummyRecord rec2 = null;
    byte[] record2 = null;
    while (record != null) {

    	//Assign attributes
        rec = new DummyRecord(record);

        // While we're at it, test the selectRecord method too.
        try {
          record2 = f.selectRecord(rid);
        } catch (Exception e) {
          System.err.println("*** Error selecting record " + i + "\n");
          e.printStackTrace();
          return false;
        }

        rec2 = new DummyRecord(record2);

        if ((rec.ival != i) || (rec.fval != (float) i * 7)
            || (rec2.ival != i) || (rec2.fval != i * 7)) {
          System.err
              .println("*** Record " + i + " differs from our update\n");
          System.err.println("rec.ival: " + rec.ival + " should be " + i
              + "\n");
          System.err.println("rec.fval: " + rec.fval + " should be "
              + (i * 7.0) + "\n");
          return false;
        }

      
      //Increment the counter and get the next record
      i += 2; // Because we deleted the odd ones...
      try {
         	record = scan.getNext(rid);
        } catch (Exception e) {
          System.err.println("Could not retrieve record "+i+" .\n");
            e.printStackTrace();
            return false;
      }
    }    
    scan.close();

    //save I/O counts
    saveCounts("test3-1");
    saveCounts(null);
    
    System.out.println("  Test3-2: Test the efficiency of this implementation.\n");
	  
	//Try to insert a record that is so large it requires a new data page 
    //Hopefully it requires few if any I/Os.
	record = new byte[PAGE_SIZE - 24];
    try {
      rid = f.insertRecord(record);
    } catch (Exception e) {
      e.printStackTrace();
      status = FAIL;
      System.err.print("Long insert failed\n");
    }
    
	//save I/O counts
	saveCounts("test3-2");
	saveCounts(null);
	
    if (status == PASS)
      System.out.println("  Test 3 completed successfully.\n");
    return status;

  } // protected boolean test3()

  /**
   * 
   */
  protected boolean test4() {

    System.out.println("\n  Test 4: Test some error conditions\n");
    boolean status = PASS;
    HeapScan scan = null;
    RID rid = new RID();
    HeapFile f = null;

    //reopen the same file
    try {
      f = new HeapFile("file_1");
    } catch (Exception e) {
      System.err.println("*** Could not create heap file\n");
      e.printStackTrace();
      return false;
    }

    //Get the first record in the heapfile
    byte[] record;
    try {
      scan = f.openScan();
    } catch (Exception e) {
      System.err.println("*** Error opening scan\n");
      e.printStackTrace();
      return false;
    }
    try {
      record = scan.getNext(rid);
    } catch (Exception e) {
      System.err.println("*** Error getting the first record\n");
        e.printStackTrace();
        return false;
    }

    //Test whether tinkering with the size of
    // the records will cause any problem.
    System.out.println("  - Try to change the size of a record\n");

    //update the record with a shorter record - should fail
    DummyRecord rec = new DummyRecord(record);
    byte[] newrecord = null;
    rec.name = "short";
    newrecord = rec.toByteArray();
    try {
      f.updateRecord(rid, newrecord);
      status = FAIL;
      System.err.print("Short update: The expected exception was not thrown\n");
    } catch (IllegalArgumentException e) {
          System.out.println("  ** Shortening a record");
          System.out.println("  --> Failed as expected \n");
    } catch (Exception e) {
       e.printStackTrace();
       status = FAIL;
       System.err.print("Short update: The expected exception was not thrown\n");
    }

    scan.close();
    scan = null;

      System.out.println("  - Try to insert a record that's too long");
      record = new byte[PAGE_SIZE + 4];
      try {
        rid = f.insertRecord(record);
        status = FAIL;
        System.err.print("Long insert: The expected exception was not thrown\n");
      } catch (IllegalArgumentException e) {
        System.out.println("  --> Failed as expected \n");
      } catch (Exception e) {
        e.printStackTrace();
        status = FAIL;
        System.err.print("Long insert: The expected exception was not thrown\n");
      }

      //save and print I/O counts
      saveCounts("test4");
      printSummary(10);
      
    if (status == PASS)
      System.out.println("  Test 4 completed successfully.\n");
    return (status);

  } // protected boolean test4()

  /**
   * Used in fixed-length record test cases.
   */
  class DummyRecord {

	//The record will contain an integer, a float and a string
    public int ival;
    public float fval;
    public String name;

    /** Constructs with default values. */
    public DummyRecord() {
    }

    /** Constructs from a byte array. */
    public DummyRecord(byte[] data) {
      ival = Convert.getIntValue(0, data);
      fval = Convert.getFloatValue(4, data);
      name = Convert.getStringValue(8, data, NAME_MAXLEN);
    }

    /** Gets a byte array representation. */
    public byte[] toByteArray() {
      byte[] data = new byte[length()];
      Convert.setIntValue(ival, 0, data);
      Convert.setFloatValue(fval, 4, data);
      Convert.setStringValue(name, 8, data);
      return data;
    }

    /** Gets the length of the record. */
    public int length() {
      return 4 + 4 + name.length();
    }

  } // class DummyRecord

} // class HFTest extends TestDriver
