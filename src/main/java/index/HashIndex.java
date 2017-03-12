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
  public HashIndex(String fileName) {

	  throw new UnsupportedOperationException("Not implemented");

  } // public HashIndex(String fileName)

  /**
   * Called by the garbage collector when there are no more references to the
   * object; deletes the index file if it's temporary.
   */
  protected void finalize() throws Throwable {

	  throw new UnsupportedOperationException("Not implemented");

  } // protected void finalize() throws Throwable

   /**
   * Deletes the index file from the database, freeing all of its pages.
   */
  public void deleteFile() {

	  throw new UnsupportedOperationException("Not implemented");

  } // public void deleteFile()

  /**
   * Inserts a new data entry into the index file.
   * 
   * @throws IllegalArgumentException if the entry is too large
   */
  public void insertEntry(SearchKey key, RID rid) {

	  throw new UnsupportedOperationException("Not implemented");

  } // public void insertEntry(SearchKey key, RID rid)

  /**
   * Deletes the specified data entry from the index file.
   * 
   * @throws IllegalArgumentException if the entry doesn't exist
   */
  public void deleteEntry(SearchKey key, RID rid) {

	  throw new UnsupportedOperationException("Not implemented");

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

	  throw new UnsupportedOperationException("Not implemented");

  } // public void printSummary()

} // public class HashIndex implements GlobalConst
