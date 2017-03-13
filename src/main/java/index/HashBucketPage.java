package index;

import global.Minibase;
import global.PageId;

/**
 * An object in this class is a page in a linked list.
 * The entire linked list is a hash table bucket.
 */
class HashBucketPage extends SortedPage {

  /**
   * Gets the number of entries in this page and later
   * (overflow) pages in the list.
   * <br><br>
   * To find the number of entries in a bucket, apply 
   * countEntries to the primary page of the bucket.
   */
  public int countEntries() {
      int accNumEntries = getEntryCount();
      PageId pageId = getNextPage();
      HashBucketPage bucketPage = new HashBucketPage();

      while(pageId.pid != INVALID_PAGEID){
          Minibase.BufferManager.pinPage(pageId, bucketPage, PIN_DISKIO);
          accNumEntries += bucketPage.getEntryCount();
          PageId nextPageId = bucketPage.getNextPage();
          Minibase.BufferManager.unpinPage(pageId, UNPIN_CLEAN);
          pageId = nextPageId;
      }

      return accNumEntries;
  } // public int countEntries()

  /**
   * Inserts a new data entry into this page. If there is no room
   * on this page, recursively inserts in later pages of the list.  
   * If necessary, creates a new page at the end of the list.
   * Does not worry about keeping order between entries in different pages.
   * <br><br>
   * To insert a data entry into a bucket, apply insertEntry to the
   * primary page of the bucket.
   * 
   * @return true if inserting made this page dirty, false otherwise
   */
  public boolean insertEntry(DataEntry entry) {
      try{
          super.insertEntry(entry);
          return true;
      } catch (IllegalStateException ex) {
          HashBucketPage nextBucket = new HashBucketPage();
          PageId nextPageId = getNextPage();
          if (nextPageId.pid != INVALID_PAGEID){
              Minibase.BufferManager.pinPage(nextPageId, nextBucket, PIN_DISKIO);
              boolean dirtyOrNot = nextBucket.insertEntry(entry);
              Minibase.BufferManager.unpinPage(nextPageId, dirtyOrNot);
              return false;
          }
          nextPageId = Minibase.BufferManager.newPage(nextBucket, 1);
          setNextPage(nextPageId);
          boolean dirtyOrNot = nextBucket.insertEntry(entry);
          Minibase.BufferManager.unpinPage(nextPageId, dirtyOrNot);
          return true;
      }

  } // public boolean insertEntry(DataEntry entry)

  /**
   * Deletes a data entry from this page.  If a page in the list 
   * (not the primary page) becomes empty, it is deleted from the list.
   * 
   * To delete a data entry from a bucket, apply deleteEntry to the
   * primary page of the bucket.
   * 
   * @return true if deleting made this page dirty, false otherwise
   * @throws IllegalArgumentException if the entry is not in the list.
   */
  public boolean deleteEntry(DataEntry entry) {
      try {
          super.deleteEntry(entry);
          return true;
      } catch (IllegalArgumentException ex) {
          HashBucketPage nextBucket = new HashBucketPage();
          PageId nextPageId = getNextPage();
          if (nextPageId.pid != INVALID_PAGEID) {
              Minibase.BufferManager.pinPage(nextPageId, nextBucket, PIN_DISKIO);
              boolean dirtyOrNot = nextBucket.deleteEntry(entry);
              if (nextBucket.getEntryCount() < 1) {  // bucket is empty and should be freed
                  setNextPage(nextBucket.getNextPage());
                  Minibase.BufferManager.unpinPage(nextPageId, dirtyOrNot);
                  Minibase.BufferManager.freePage(nextPageId);
                  return true;
              }
              Minibase.BufferManager.unpinPage(nextPageId, dirtyOrNot);
              return false;
          }
          throw ex;
      }
  } // public boolean deleteEntry(DataEntry entry)

} // class HashBucketPage extends SortedPage
