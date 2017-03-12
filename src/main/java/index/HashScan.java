package index;

import global.GlobalConst;
import global.Minibase;
import global.PageId;
import global.RID;
import global.SearchKey;

/**
 * A HashScan retrieves all records with a given key (via the RIDs of the records).  
 * It is created only through the function openScan() in the HashIndex class. 
 */
public class HashScan implements GlobalConst {

  /** The search key to scan for. */
  protected SearchKey key;

  /** Id of HashBucketPage being scanned. */
  protected PageId curPageId;

  /** HashBucketPage being scanned. */
  protected HashBucketPage curPage;

  /** Current slot to scan from. */
  protected int curSlot;

  // --------------------------------------------------------------------------

  /**
   * Constructs an equality scan by initializing the iterator state.
   */
  protected HashScan(HashIndex index, SearchKey key) {

	  throw new UnsupportedOperationException("Not implemented");

  } // protected HashScan(HashIndex index, SearchKey key)

  /**
   * Called by the garbage collector when there are no more references to the
   * object; closes the scan if it's still open.
   */
  protected void finalize() throws Throwable {

	  throw new UnsupportedOperationException("Not implemented");

  } // protected void finalize() throws Throwable

  /**
   * Closes the index scan, releasing any pinned pages.
   */
  public void close() {

	  throw new UnsupportedOperationException("Not implemented");

  } // public void close()

   /**
   * Gets the next entry's RID in the index scan.
   * 
   * @throws IllegalStateException if the scan has no more entries
   */
  public RID getNext() {

	  throw new UnsupportedOperationException("Not implemented");

  } // public RID getNext()

} // public class HashScan implements GlobalConst
