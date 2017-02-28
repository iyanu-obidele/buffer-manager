
package test;

import java.util.HashMap;

import global.Convert;
import global.Minibase;
import global.PageId;
import global.RID;
import heap.HeapFile; 
import heap.HeapScan;
import test.HFTest.DummyRecord;

/**
 * Test suite for the heap layer.
 */
class hfExtraTest extends TestDriver {

  /** The display name of the test suite. */
  private static final String TEST_NAME = "heap file extra tests";

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
	  hfExtraTest hft = new hfExtraTest();
    hft.create_minibase();

    // run all the test cases
    System.out.println("\n" + "Running " + TEST_NAME + "...");
    boolean status = PASS;
    status &= hft.test5();
    //status &= hft.test2();
  //  status &= hft.test3();
    status &= hft.test6();

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
  protected boolean test5() {
	  
	//Start saving count of I/Os
	initCounts();
	saveCounts(null);
	HashMap<Integer, RID> fhashMap = new HashMap<Integer, RID>();

    System.out.println("\n  Test 5: Insert and scan variable-size records\n");
    boolean status = PASS;
    HeapFile f = null;

    System.out.println("  - Create a heap file\n");
    try {
      f = new HeapFile("file_2");
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

      // variable length record
      DummyRecord rec = new DummyRecord();
      rec.ival = i;
      rec.fval = (float) (i * 2.5);
      if (i % 2 == 1){ // odd number records will have longer names
    	  rec.name = "recordRecordRecordRecordRecordRecordRecord" + i;
      }
      else{
    	  rec.name = "record" + i;  
      }
      

      try {
        rid = f.insertRecord(rec.toByteArray());
        fhashMap.put(i, rid);
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
    
    byte[] record2 = null;
    DummyRecord rec2=null;
    
    for (int i = 0; (i < FILE_SIZE) && (status == PASS); i++){
    	//System.out.println(fhashMap.get(i));
        try {
            record2 = f.selectRecord(fhashMap.get(i));
          } catch (Exception e) {
            System.err.println("*** Error selecting record " + i + "\n");
            e.printStackTrace();
            return false;
          }
        
        rec2 = new DummyRecord(record2);
        //System.out.println(rec2.name);
        
        if((rec2.ival != i) || (rec2.fval != i * 2.5)){
            System.err
                .println("*** Record " + i + " differs from our update\n");
            System.err.println("rec.ival: " + rec2.ival + " should be " + i
               + "\n");
            System.err.println("rec.fval: " + rec2.fval + " should be "
               + (i * 2.5) + "\n");
            
            return false;
        }

    }
    
    if (status == PASS)
        System.out.println("  Test 5 completed successfully.\n");
    
    return status;
  } // protected boolean test5()

  /**
   * 
   */
  protected boolean test6() {

    System.out.println("\n  Test 6: Test some error conditions\n");
    boolean status = PASS;
  //  HeapScan scan = null;
    //RID rid = new RID();
    HeapFile f = null;

    //reopen the same file
    try {
      f = new HeapFile("file_2");
    } catch (Exception e) {
      System.err.println("*** Could not open heap file\n");
      e.printStackTrace();
      return false;
    }

    //Get the first record in the heapfile
    byte[] record;

    //Test whether tinkering with the size of
    // the records will cause any problem.

    //update the record with a shorter record - should fail

      System.out.println("  - Try to insert a max size record (page size - header size - one slot size)");
      record = new byte[PAGE_SIZE - 20 - 4];
      try {
        rid = f.insertRecord(record);
        //status = PASS;
        System.out.print("Page size record insert: succeded\n");
      } catch (Exception e) {
        e.printStackTrace();
        status = FAIL;
        System.err.print("Page size record insert: failed\n");
      }

 
    if (status == PASS)
      System.out.println("  Test 6 completed successfully.\n");
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
