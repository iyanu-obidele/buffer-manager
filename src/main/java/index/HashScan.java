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
        int hash = key.getHash(index.DEPTH);
        this.key = new SearchKey(key);

        PageId hashDirId = new PageId(index.headId.pid);
        HashDirPage hashDirPage = new HashDirPage();

        while (hash >= HashDirPage.MAX_ENTRIES){
            Minibase.BufferManager.pinPage(hashDirId, hashDirPage, PIN_DISKIO);
            PageId nextDirId = hashDirPage.getNextPage();
            Minibase.BufferManager.unpinPage(hashDirId, UNPIN_CLEAN);
            hashDirId = nextDirId;
            hash -= HashDirPage.MAX_ENTRIES;
        }
        Minibase.BufferManager.pinPage(hashDirId, hashDirPage, PIN_DISKIO);
        curPageId = hashDirPage.getPageId(hash);
        Minibase.BufferManager.unpinPage(hashDirId, UNPIN_CLEAN);
        curPage = new HashBucketPage();
        if (curPageId.pid != INVALID_PAGEID){
            Minibase.BufferManager.pinPage(curPageId, curPage, PIN_DISKIO);
            curSlot = EMPTY_SLOT;
        }
    } // protected HashScan(HashIndex index, SearchKey key)

    /**
     * Called by the garbage collector when there are no more references to the
     * object; closes the scan if it's still open.
     */
    protected void finalize() throws Throwable {
        if (curPageId.pid != INVALID_PAGEID)
            close();
    } // protected void finalize() throws Throwable

    /**
     * Closes the index scan, releasing any pinned pages.
     */
    public void close() {
        if (curPageId.pid != INVALID_PAGEID){
            Minibase.BufferManager.unpinPage(curPageId, UNPIN_CLEAN);
            curPageId.pid = INVALID_PAGEID;
        }
    } // public void close()

    /**
     * Gets the next entry's RID in the index scan.
     *
     * @throws IllegalStateException if the scan has no more entries
     */
    public RID getNext() {
        if (!hasNext())
            return null;
        try{
            return curPage.getEntryAt(curSlot).rid;
        } catch (IllegalArgumentException ex) {
            throw new IllegalStateException("No more entries");
        }

    } // public RID getNext()

    private boolean hasNext() {
        while (curPageId.pid != INVALID_PAGEID){
            curSlot = curPage.nextEntry(key, curSlot);
            if (curSlot > 0)
                return true;
            PageId nextPageId = curPage.getNextPage();
            Minibase.BufferManager.unpinPage(curPageId, UNPIN_CLEAN);
            curPageId = nextPageId;
            if (curPageId.pid != INVALID_PAGEID)
                Minibase.BufferManager.pinPage(curPageId, curPage, PIN_DISKIO);
        }
        return false;
    }


} // public class HashScan implements GlobalConst
