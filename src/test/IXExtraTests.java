package test;

import global.PageId;
import global.RID;
import global.SearchKey;
import index.HashIndex;
import index.HashScan;

/**
 * Test suite for the index layer.
 */
class IXExtraTests extends TestDriver {

  /** The display name of the test suite. */
  private static final String TEST_NAME = "hash index tests";

  /**
   * Size of index files to create in test cases.
   */
  private static final int FILE_SIZE = 3000;

  /**
   * Did I pass the current test?
   */
  private static boolean retval = true;

  /**
   * Did I find the current search key?
   */
  private static boolean found = true;
  // --------------------------------------------------------------------------

  /**
   * Test application entry point; runs all tests.
   */
  public static void main(String argv[]) {

    // create a clean Minibase instance
	  IXExtraTests hft = new IXExtraTests();
    hft.create_minibase();

    // run all the test cases
    System.out.println("\n" + "Running " + TEST_NAME + "...");
    boolean status = true;
   // status &= hft.test1();
    retval = true;
    status &= hft.test4();
    retval = true;
    status &= hft.test5();

    // display the final results
    System.out.println();
    if (!status) {
      System.out.println("Error(s) encountered during " + TEST_NAME + ".");
    } else {
      System.out.println("All " + TEST_NAME + " completed successfully!");
    }

  } // public static void main (String argv[])

  /**
   * Simple use of temp index.
   */
  
  /**
   * HashIndex under normal conditions.
   */
  protected boolean test4() {

    System.out.println();
    System.out.println("Test 4: Test the case of deleting all entries");

    for (int type = 1; type <= 1; type++) {

      System.out.println("\n(type == " + type + ")");
      initRandom();

      System.out.print("\n  ~> building an index of " + FILE_SIZE + " ");
      if (type == 1) {
        System.out.print("integer");
      } 
      System.out.println("s...");

      String fileName = "IX_Customers" + type;
      HashIndex index = new HashIndex(fileName);
      for (int i = 0; i < FILE_SIZE; i++) {

        // insert a random entry
        SearchKey key =new SearchKey(i); 
        //System.out.println(key);
        RID rid = new RID(new PageId(i), 0);
        index.insertEntry(key, rid);

      } // for

     // index.printSummary();
      
      System.out.println("\n  ~> scanning all entries...");
      RID rid2;
      for (int i = 0; i < FILE_SIZE; i += 1) {

        // search for the random entry
        SearchKey key = new SearchKey(i);
        RID rid = new RID(new PageId(i), 0);
        HashScan scan = index.openScan(key);
        rid2 = scan.getNext();
        while (rid2 != null) {
          if (rid2.equals(rid)) {
            found = true;
          }
          rid2 = scan.getNext();
        }
        scan.close();

        if (!found) {
          System.out.println("  ERROR: Search key not found in scan!");
          retval = false;
	  found = true;
        }

      } // for

      System.out.println("\n  ~> deleting all entries...");
      for (int i = 0; i < FILE_SIZE; i += 1) {

        // delete the random entry
        SearchKey key = new SearchKey(i);//randKey(type);
        //randKey(type); // to keep in sync with inserts
        RID rid = new RID(new PageId(i), 0);
        index.deleteEntry(key, rid);

      } // for

    // index.printSummary();

     
      // delete the file
      System.out.println("\n  ~> deleting the index file...");
      HashIndex byebye = new HashIndex(fileName);
      byebye.deleteFile();

    } // for type

    return retval;

  } // protected boolean test4()

  
  protected boolean test5() {

	    System.out.println();
	    System.out.println("Test 5: Building an index where all keys hash to the same bucket.");

	    for (int type = 1; type <= 1; type++) {

	      System.out.println("\n(type == " + type + ")");
	      initRandom();

	      System.out.print("\n  ~> building an index of " + FILE_SIZE + " ");
	      if (type == 1) {
	        System.out.print("integer");
	      } 
	      System.out.println("s...");

	      String fileName = "IX_Customers" + type;
	      HashIndex index = new HashIndex(fileName);
	      for (int i = 0; i < FILE_SIZE; i++) {

	        // insert a random entry
	        SearchKey key =new SearchKey(i * 128); 
	        //System.out.println(key);
	        RID rid = new RID(new PageId(i), 0);
	        index.insertEntry(key, rid);

	      } // for

	    //  index.printSummary();
	      
	      System.out.println("\n  ~> scanning all entries...");
	      RID rid2;
	      for (int i = 0; i < FILE_SIZE; i += 1) {

	        // search for the random entry
	        SearchKey key = new SearchKey(i * 128);
	        RID rid = new RID(new PageId(i), 0);
	        HashScan scan = index.openScan(key);
	        rid2 = scan.getNext();
	        while (rid2 != null) {
	          if (rid2.equals(rid)) {
	            found = true;
	          }
	          rid2 = scan.getNext();
	        }
	        scan.close();

	        if (!found) {
	          System.out.println("  ERROR: Search key not found in scan!");
	          retval = false;
		  found = true;
	        }

	      } // for


	 //    index.printSummary();

	     
	      // delete the file
	      System.out.println("\n  ~> deleting the index file...");
	      HashIndex byebye = new HashIndex(fileName);
	      byebye.deleteFile();

	    } // for type

	    return retval;

	  } // protected boolean test5()
  
} // class IXTest extends TestDriver
