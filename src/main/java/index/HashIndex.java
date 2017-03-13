package index;

import global.GlobalConst;
import global.Minibase;
import global.PageId;
import global.RID;
import global.SearchKey;

/**
 * <h3>Minibase Hash Index</h3>
 * This unclustered index implements static hashing as described on pages 371 to
 * 373 of the textbook (3rd edition).  The index file is a stored as a heapfile.  
 */
public class HashIndex implements GlobalConst {

  /** File name of the hash index. */
  protected String fileName;

  /** Page id of the directory. */
  protected PageId headId;
  
  //Log2 of the number of buckets - fixed for this simple index
  protected final int  DEPTH = 7;

  protected Boolean isTemp;

  // --------------------------------------------------------------------------

  /**
   * Opens an index file given its name, or creates a new index file if the name
   * doesn't exist; a null name produces a temporary index file which requires
   * no file library entry and whose pages are freed when there are no more
   * references to it.
   * The file's directory contains the locations of the 128 primary bucket pages.
   * You will need to decide on a structure for the directory.
   * The library entry contains the name of the index file and the pageId of the
   * file's directory.
   */
  public HashIndex(String name) {
      fileName = name;

      if (fileName == null){
          headId = null;    // temp file
          isTemp = true;
      } else {
          headId = Minibase.DiskManager.get_file_entry(fileName);
          isTemp = false;
      }
      if (headId == null) {
          HashDirPage hashDirPage = new HashDirPage();
          headId = Minibase.BufferManager.newPage(hashDirPage, 1);
          Minibase.BufferManager.unpinPage(headId, UNPIN_DIRTY);
          if (fileName != null)     // new non-temp index file
              Minibase.DiskManager.add_file_entry(fileName, headId);
      }

  } // public HashIndex(String fileName)

  /**
   * Called by the garbage collector when there are no more references to the
   * object; deletes the index file if it's temporary.
   */
  protected void finalize() throws Throwable {
      if (isTemp)
          deleteFile();
  } // protected void finalize() throws Throwable

   /**
   * Deletes the index file from the database, freeing all of its pages.
   */
  public void deleteFile() {
      PageId hashDirId = new PageId(headId.pid);
      HashDirPage hashDirPage = new HashDirPage();
      HashBucketPage bucketPage = new HashBucketPage();
      while (hashDirId.pid!=INVALID_PAGEID){
          Minibase.BufferManager.pinPage(hashDirId, hashDirPage, PIN_DISKIO);
          int count = hashDirPage.getEntryCount();
          for (int i=0; i<count; ++i){
              PageId bucketPageId = hashDirPage.getPageId(i);
              while(bucketPageId.pid != INVALID_PAGEID){
                  Minibase.BufferManager.pinPage(bucketPageId, bucketPage, PIN_DISKIO);
                  PageId nextBucketPageId = bucketPage.getNextPage();
                  Minibase.BufferManager.unpinPage(bucketPageId, UNPIN_CLEAN);
                  Minibase.BufferManager.freePage(bucketPageId);
                  bucketPageId = nextBucketPageId;
              }
          }
          PageId nextDirId = hashDirPage.getNextPage();
          Minibase.BufferManager.unpinPage(hashDirId, UNPIN_CLEAN);
          hashDirId = nextDirId;
      }
      if (!isTemp)
          Minibase.DiskManager.delete_file_entry(fileName);
  } // public void deleteFile()

  /**
   * Inserts a new data entry into the index file.
   * 
   * @throws IllegalArgumentException if the entry is too large
   */
  public void insertEntry(SearchKey key, RID rid) {
      DataEntry dataEntry = new DataEntry(key, rid);
      if (dataEntry.getLength() > SortedPage.MAX_ENTRY_SIZE)
          throw new IllegalArgumentException("Entry too large");

      int searchKeyHash = key.getHash(DEPTH);

      PageId hashDirId = new PageId(headId.pid);
      HashDirPage hashDirPage = new HashDirPage();
      HashBucketPage bucketPage = new HashBucketPage();
      while(searchKeyHash >= HashDirPage.MAX_ENTRIES){
          Minibase.BufferManager.pinPage(hashDirId, hashDirPage, PIN_DISKIO);
          PageId nextDirId = hashDirPage.getNextPage();
          Minibase.BufferManager.unpinPage(hashDirId, UNPIN_CLEAN);
          searchKeyHash -= HashDirPage.MAX_ENTRIES;
          hashDirId = nextDirId;
      }
      Minibase.BufferManager.pinPage(hashDirId, hashDirPage, PIN_DISKIO);

      PageId pageIdOfChosenPage = hashDirPage.getPageId(searchKeyHash);
      if (pageIdOfChosenPage.pid == INVALID_PAGEID){     // add new page before insertion
          pageIdOfChosenPage = Minibase.BufferManager.newPage(bucketPage, 1);
          hashDirPage.setPageId(searchKeyHash, pageIdOfChosenPage);
          Minibase.BufferManager.unpinPage(hashDirId, UNPIN_DIRTY);
      } else {
          Minibase.BufferManager.pinPage(pageIdOfChosenPage, bucketPage, PIN_DISKIO);
          Minibase.BufferManager.unpinPage(hashDirId, UNPIN_CLEAN);
      }
      Boolean dirtyOrNot = bucketPage.insertEntry(dataEntry);
      Minibase.BufferManager.unpinPage(pageIdOfChosenPage, dirtyOrNot);

  } // public void insertEntry(SearchKey key, RID rid)

  /**
   * Deletes the specified data entry from the index file.
   * 
   * @throws IllegalArgumentException if the entry doesn't exist
   */
  public void deleteEntry(SearchKey key, RID rid) {
      DataEntry dataEntry = new DataEntry(key, rid);
      int entryHash = key.getHash(DEPTH);

      PageId hashDirId = new PageId(headId.pid);
      HashDirPage hashDirPage = new HashDirPage();
      HashBucketPage bucketPage = new HashBucketPage();
      while(entryHash >= HashDirPage.MAX_ENTRIES){
          Minibase.BufferManager.pinPage(hashDirId, hashDirPage, PIN_DISKIO);
          PageId nextDirId = hashDirPage.getNextPage();
          Minibase.BufferManager.unpinPage(hashDirId, UNPIN_CLEAN);
          entryHash -= HashDirPage.MAX_ENTRIES;
          hashDirId = nextDirId;
      }
      Minibase.BufferManager.pinPage(hashDirId, hashDirPage, PIN_DISKIO);
      PageId pageIdOfChosenPage = hashDirPage.getPageId(entryHash);

      // sanity check
      if (pageIdOfChosenPage.pid == INVALID_PAGEID)
          throw new IllegalArgumentException("Data entry doesn't exist");

      Minibase.BufferManager.pinPage(pageIdOfChosenPage, bucketPage, PIN_DISKIO);
      try {
          boolean dirtyOrNot = bucketPage.deleteEntry(dataEntry);
          Minibase.BufferManager.unpinPage(pageIdOfChosenPage, dirtyOrNot);
      } catch (IllegalArgumentException ex) {
          Minibase.BufferManager.unpinPage(pageIdOfChosenPage, UNPIN_CLEAN);
          throw ex;
      }

  } // public void deleteEntry(SearchKey key, RID rid)

  /**
   * Initiates an equality scan of the index file.
   */
  public HashScan openScan(SearchKey key) {
    return new HashScan(this, key);
  }

  /**
   * Returns the name of the index file.
   */
  public String toString() {
    return fileName;
  }

  /**
   * Prints a high-level view of the directory, namely which buckets are
   * allocated and how many entries are stored in each one. Sample output:
   * 
   * <pre>
   * IX_Customers
   * ------------
   * 0000000 : 35
   * 0000001 : null
   * 0000010 : 27
   * ...
   * 1111111 : 42
   * ------------
   * Total : 1500
   * </pre>
   */
  public void printSummary() {
      int total = 0;    // accumulator

      System.out.printf("\n<%s>\n", isTemp ? "Temp" : fileName);
      int length = isTemp ? 4 : fileName.length();
      for (int a = 0; a < length; ++a){
          System.out.print("-");
      }
      System.out.println();

      PageId hashDirId = new PageId(headId.pid);
      HashDirPage hashDirPage = new HashDirPage();
      HashBucketPage bucketPage = new HashBucketPage();
      while(hashDirId.pid != INVALID_PAGEID){
          Minibase.BufferManager.pinPage(hashDirId, hashDirPage, PIN_DISKIO);
          int count = hashDirPage.getEntryCount();
          for (int i = 0; i < count; ++i){
              String binaryValue = Integer.toString(i, 2);
              for (int x = 0; x < DEPTH - binaryValue.length(); x++)
                  System.out.print('0');
              System.out.printf(new StringBuilder(String.valueOf(binaryValue)).append(" : ").toString());
              PageId pageId = hashDirPage.getPageId(i);
              if (pageId.pid == INVALID_PAGEID)
                  System.out.println("null");
              else{
                  Minibase.BufferManager.pinPage(pageId, bucketPage, PIN_DISKIO);
                  int bucketCount = bucketPage.countEntries();
                  System.out.println(bucketCount);
                  Minibase.BufferManager.unpinPage(pageId, UNPIN_CLEAN);
                  total += bucketCount;
              }
          }
          PageId nextDirPageId = hashDirPage.getNextPage();
          Minibase.BufferManager.unpinPage(hashDirId, UNPIN_CLEAN);
          hashDirId = nextDirPageId;
      }

      for (int a = 0; a < length; ++a){
          System.out.print("-");
      }
      System.out.println();
      System.out.printf("Total : %d\n", total);

  } // public void printSummary()

} // public class HashIndex implements GlobalConst
