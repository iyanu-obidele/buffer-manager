package heap;

import global.GlobalConst;
import global.Minibase;
import global.PageId;
import global.RID;

/**
 * A HeapScan object is created only through the function openScan() in the
 * HeapFile class. It supports the getNext interface which will simply retrieve
 * the next record in the file.
 */
public class HeapScan implements GlobalConst {

  /** Currently pinned directory page (outer loop). */
  protected DirPage dirPage;

  /** Number of entries on the current directory page. */
  protected int count;

  /** Slot number of the current entry on the directory page. */
  protected int index;

  /** Currently pinned data page (inner loop). */
  protected DataPage dataPage;

  /** RID of the current record on the data page. */
  protected RID curRid;

  // --------------------------------------------------------------------------

  /**
   * Constructs a file scan by pinning the directory header page and initializing
   * iterator fields.
   */
  protected HeapScan(HeapFile hf) {

    // pin the head page and get the count
    dirPage = new DirPage();
    Minibase.BufferManager.pinPage(hf.headId, dirPage, PIN_DISKIO);
    count = dirPage.getEntryCnt();

    // initialize other data fields
    index = -1;
    dataPage = null;
    curRid = null;

  } // protected HeapScan(HeapFile hf)

  /**
   * Called by the garbage collector when there are no more references to the
   * object; closes the scan if it's still open.
   */
  protected void finalize() throws Throwable {

    // close the scan, if open
    if (dirPage != null) {
      close();
    }

  } // protected void finalize() throws Throwable

  /**
   * Closes the file scan, releasing any pinned pages.
   */
  public void close() {

    // unpin the pages where applicable
    if (dataPage != null) {
      Minibase.BufferManager.unpinPage(dataPage.getCurPage(), UNPIN_CLEAN);
      dataPage = null;
    }
    if (dirPage != null) {
      Minibase.BufferManager.unpinPage(dirPage.getCurPage(), UNPIN_CLEAN);
      dirPage = null;
    }

    // invalidate the other fields
    count = -1;
    index = -1;
    curRid = null;

  } // public void close()

   /**
   * Gets the next record in the file scan.
   * 
   * @param rid output parameter that identifies the returned record
   * @return the next record, or null if there is no next record
   * @throws IllegalStateException if it encounters an empty data page
   */
  public byte[] getNext(RID rid) {

	//If we are starting the scan, index = -1; dataPage = null; curRid = null;
	//If we are iterating within a data page, curRid != null
	//If we have just finished a data page, dataPage !=null and is pinned, curRid = null
	  
    // If we are iterating within the data page, increment curRid
	// If it is nonnull, return a record
    if (curRid != null) {
      curRid = dataPage.nextRecord(curRid);
      if (curRid != null) {
        rid.copyRID(curRid);
        return dataPage.selectRecord(rid);
      } 
    } 

    //Here curRid is null, either because we just began the scan, because we
    // just finished scanning a directory and data page, or because
    // we just finished a data page within a dir page.  In the first case dataPage == null.
    //If dataPage !=null, we must unpin it only if we move on to another data page.
    
    //Look for the next data page.
    // If there is another data entry in this dir page, process its data page
    if (index < count - 1) {

      // minor optimization
      if (dataPage == null) {//we just started the scan
        dataPage = new DataPage();
      } else {//we are moving on to a new data page, so unpin the old one
        Minibase.BufferManager.unpinPage(dataPage.getCurPage(), UNPIN_CLEAN);
      }
      
      // pin the next data page
      index++;
      Minibase.BufferManager.pinPage(dirPage.getPageId(index), dataPage,
          PIN_DISKIO);

      // reset the current record rid, get the first record and return it.
      //The scan is iterating within a data page.
      curRid = dataPage.firstRecord();
      if (curRid != null) {
        rid.copyRID(curRid);
        return dataPage.selectRecord(rid);
      } else{
    	int pageno = dataPage.getCurPage().pid;
    	throw new IllegalStateException("Data page "+pageno+" is empty.");
      }
      //control never gets here
      
    } // if more entries

    //Here curRid is null, either because we just began the scan or because
    // we just finished a data page and it was the last data page in a directory
    // page.  In in the former case dataPage == null.
    //If dataPage !=null, we must unpin it only if we move on to another data page.
    
    // move on to the next directory page
    PageId nextId = dirPage.getNextPage();
    if (nextId.pid != INVALID_PAGEID) {

      // unpin the current dir page, pin the next dir page
      Minibase.BufferManager.unpinPage(dirPage.getCurPage(), UNPIN_CLEAN);
      Minibase.BufferManager.pinPage(nextId, dirPage, PIN_DISKIO);

      // reset the counters and try again
      count = dirPage.getEntryCnt();
      index = -1;
      curRid = null;
      return getNext(rid);

    } // if more dir pages

    // otherwise, no more records
    return null;

  } // public byte[] getNext(RID rid)

} // public class HeapScan implements GlobalConst
