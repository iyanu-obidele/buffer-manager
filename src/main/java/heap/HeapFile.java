package heap;

import global.GlobalConst;
import global.Minibase;
import global.PageId;
import global.RID;

/**
 * <h3>Minibase Heap Files</h3>
 * A heap file is the simplest database file structure.  It is an unordered 
 * set of records, stored on a set of data pages. <br>
 * This class supports inserting, selecting, updating, and deleting
 * records.<br>
 * Normally each heap file has an entry in the database's file library.
 * Temporary heap files are used for external sorting and in other
 * relational operators. A temporary heap file does not have an entry in the
 * file library and is deleted when there are no more references to it. <br>
 * A sequential scan of a heap file (via the HeapScan class)
 * is the most basic access method.
 */
public class HeapFile implements GlobalConst {

    /** HFPage type for directory pages. */
    protected static final short DIR_PAGE = 10;

    /** HFPage type for data pages. */
    protected static final short DATA_PAGE = 11;

    // --------------------------------------------------------------------------

    /** Is this a temporary heap file, meaning it has no entry in the library? */
    protected boolean isTemp;

    /** The heap file name.  Null if a temp file, otherwise
     * used for the file library entry.
     */
    protected String fileName;

    /** First page of the directory for this heap file. */
    protected PageId headId;

    // --------------------------------------------------------------------------

    /**
     * If the given name is in the library, this opens the corresponding
     * heapfile; otherwise, this creates a new empty heapfile.
     * A null name produces a temporary file which
     * requires no file library entry.
     */
    public HeapFile(String name) {
        fileName = name;
        if (fileName == null){
            headId = null;    // temp file
            isTemp = true;
        } else {
            headId = Minibase.DiskManager.get_file_entry(fileName);   // should go as an entry in the file library.
            isTemp = false;
        }
        if (headId == null) {
            DirPage dirPage = new DirPage();
            headId = Minibase.BufferManager.newPage(dirPage, 1);
            dirPage.setCurPage(headId);
            Minibase.BufferManager.unpinPage(headId, UNPIN_DIRTY);
            if (fileName != null)
                Minibase.DiskManager.add_file_entry(fileName, headId);
        }
    } // public HeapFile(String name)

    /**
     * Called by the garbage collector when there are no more references to the
     * object; deletes the heap file if it's temporary.
     */
    protected void finalize() throws Throwable {
        if (isTemp)
            deleteFile();
    } // protected void finalize() throws Throwable

    /**
     * Deletes the heap file from the database, freeing all of its pages
     * and its library entry if appropriate.
     */
    public void deleteFile() {
        PageId dirPageId = headId;
        PageId nextDirPageId;
        DirPage dirPage = new DirPage();
        RID rid;

        while (dirPageId.pid != INVALID_PAGEID){
            Minibase.BufferManager.pinPage(dirPageId, dirPage, PIN_DISKIO);
            for(rid = dirPage.firstRecord(); rid != null; dirPage.nextRecord(rid))
                Minibase.BufferManager.freePage(rid.pageno);
            nextDirPageId = dirPage.getNextPage();
            Minibase.BufferManager.freePage(dirPageId);
            dirPageId.pid = nextDirPageId.pid;
        }
        if (!isTemp)
            Minibase.DiskManager.delete_file_entry(fileName);
    } // public void deleteFile()

    /**
     * Inserts a new record into the file and returns its RID.
     * Should be efficient about finding space for the record.
     * However, fixed length records inserted into an empty file
     * should be inserted sequentially.
     * Should create a new directory and/or data page only if
     * necessary.
     *
     * @throws IllegalArgumentException if the record is too
     * large to fit on one data page
     */
    public RID insertRecord(byte[] record) {

        // sanity check on size
        if (record.length > 1004)
            throw new IllegalArgumentException("Record is too large");

        PageId spot = getAvailPage(record.length + 4);
        DataPage page = new DataPage();

        Minibase.BufferManager.pinPage(spot, page, PIN_NOOP);

        RID rid = page.insertRecord(record);
        short freeCount = page.getFreeSpace();

        Minibase.BufferManager.unpinPage(spot, UNPIN_DIRTY);
        updateDirEntry(spot, 1, freeCount);
        return rid;
    } // public RID insertRecord(byte[] record)

    /**
     * Reads a record from the file, given its rid.
     *
     * @throws IllegalArgumentException if the rid is invalid
     */
    public byte[] selectRecord(RID rid) {
        DataPage page = new DataPage();
        byte[] bytes;

        Minibase.BufferManager.pinPage(rid.pageno, page, PIN_DISKIO);
        // might throw an error, but that's the right behavior
        try {
            bytes = page.selectRecord(rid);
        } catch(IllegalArgumentException ex) {
            Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_CLEAN);
            throw ex;
        }
        Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_CLEAN);
        return bytes;
    } // public byte[] selectRecord(RID rid)

    /**
     * Updates the specified record in the heap file.
     *
     * @throws IllegalArgumentException if the rid or new record is invalid
     */
    public void updateRecord(RID rid, byte[] newRecord) {
        DataPage page = new DataPage();
        Minibase.BufferManager.pinPage(rid.pageno, page, PIN_DISKIO);
        try{
            page.updateRecord(rid, newRecord);
            Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_DIRTY);
        } catch (IllegalArgumentException ex) {
            Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_CLEAN);
            throw ex;
        }
    } // public void updateRecord(RID rid, byte[] newRecord)

    /**
     * Deletes the specified record from the heap file.
     * Removes empty data and/or directory pages.
     *
     * @throws IllegalArgumentException if the rid is invalid
     */
    public void deleteRecord(RID rid) {
        DataPage page = new DataPage();
        Minibase.BufferManager.pinPage(rid.pageno, page, PIN_DISKIO);
        try{
            page.deleteRecord(rid);
            short freeCount = page.getFreeSpace();
            Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_DIRTY);
            updateDirEntry(rid.pageno, -1, freeCount);
        } catch(IllegalArgumentException ex) {
            Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_CLEAN);
            throw ex;
        }
    } // public void deleteRecord(RID rid)

    /**
     * Gets the number of records in the file.
     */
    public int getRecCnt() {
        int recordCount = 0;
        PageId dirPageId = new PageId(headId.pid);
        DirPage dirPage = new DirPage();

        while (dirPageId.pid != INVALID_PAGEID) {
            Minibase.BufferManager.pinPage(dirPageId, dirPage, PIN_DISKIO);
            int count = dirPage.getEntryCnt();
            for (int i = 0; i < count; i++)
                recordCount += dirPage.getRecCnt(i);
            Minibase.BufferManager.unpinPage(dirPageId, UNPIN_CLEAN);
            dirPageId = dirPage.getNextPage();
        }
        return recordCount;
    } // public int getRecCnt()

    /**
     * Initiates a sequential scan of the heap file.
     */
    public HeapScan openScan() { return new HeapScan(this); }

    /**
     * Returns the name of the heap file.
     */
    public String toString() { return fileName; }

    /**
     * Searches the directory for the first data page with enough free space to store a
     * record of the given size. If no suitable page is found, this creates a new
     * data page.
     * A more efficient implementation would start with a directory page that is in the
     * buffer pool.
     */
    protected PageId getAvailPage(int reclen) {
        PageId victimPageID = null;
        PageId dirPageId = new PageId(headId.pid);
        DirPage dirPage = new DirPage();
        //TODO: Start search from pages in the bufferPool
        while (victimPageID == null && dirPageId.pid != INVALID_PAGEID){
            Minibase.BufferManager.pinPage(dirPageId, dirPage, PIN_DISKIO);
            int count = dirPage.getEntryCnt();
            for(int i = 0; i < count; i++){
                if (dirPage.getFreeCnt(i) >= reclen + 4) {
                    victimPageID = dirPage.getPageId(i);
                    break;
                }
            }
            Minibase.BufferManager.unpinPage(dirPageId, UNPIN_CLEAN);
            dirPageId = dirPage.getNextPage();
        }
        if (victimPageID == null)
            victimPageID = insertPage();
        return victimPageID;
    } // protected PageId getAvailPage(int reclen)

    /**
     * Helper method for finding directory entries of data pages.
     * A more efficient implementation would start with a directory
     * page that is in the buffer pool.
     *
     * @param pageno identifies the page for which to find an entry
     * @param dirId output param to hold the directory page's id (pinned)
     * @param dirPage output param to hold directory page contents
     * @return index of the data page's entry on the directory page
     */
    protected int findDirEntry(PageId pageno, PageId dirId, DirPage dirPage) {
        //TODO: Start search from pages in the bufferPool
        int pidOfEntry = -1;
        dirId.pid = headId.pid;
        outerLoop:
        while (dirId.pid != INVALID_PAGEID) {
            Minibase.BufferManager.pinPage(dirId, dirPage, PIN_DISKIO);
            int count = dirPage.getEntryCnt();
            for (int i = 0; i < count; i++) {
                if (pageno.pid == dirPage.getPageId(i).pid) {
                    pidOfEntry = i;
                    break outerLoop;
                }
            }
            Minibase.BufferManager.unpinPage(dirId, UNPIN_CLEAN);
            dirId.pid = dirPage.getNextPage().pid;
        }
        return pidOfEntry;
    } // protected int findDirEntry(PageId pageno, PageId dirId, DirPage dirPage)

    /**
     * Updates the directory entry for the given data page.
     * If the data page becomes empty, remove it.
     * If this causes a dir page to become empty, remove it
     * @param pageno identifies the data page whose directory entry will be updated
     * @param deltaRec input change in number of records on that data page
     * @param freecnt input new value of freecnt for the directory entry
     */
    protected void updateDirEntry(PageId pageno, int deltaRec, int freecnt) {
        PageId dirPageId = new PageId();
        DirPage dirPage = new DirPage();

        int entryIndex = findDirEntry(pageno, dirPageId, dirPage);
        int recordCount = dirPage.getRecCnt(entryIndex) + deltaRec;

        if (recordCount >= 1){
            dirPage.setRecCnt(entryIndex, (short) recordCount);
            dirPage.setFreeCnt(entryIndex, (short) freecnt);
            Minibase.BufferManager.unpinPage(dirPageId, UNPIN_DIRTY);
        } else {
            deletePage(pageno, dirPageId, dirPage, entryIndex);
        }
    } // protected void updateEntry(PageId pageno, int deltaRec, int deltaFree)

    /**
     * Inserts a new empty data page and its directory entry into the heap file.
     * If necessary, this also inserts a new directory page.
     * Leaves all data and directory pages unpinned
     *
     * @return id of the new data page
     */
    protected PageId insertPage() {
        int count;
        PageId dirPageID = new PageId(headId.pid);
        DirPage dirPage = new DirPage();
        PageId dataPageID;

        while(true) {   // loop to go through directory in search of first empty dir page or last entry.
            Minibase.BufferManager.pinPage(dirPageID, dirPage, PIN_DISKIO);
            count = dirPage.getEntryCnt();
            if (count < DirPage.MAX_ENTRIES) {      // add data_page, a directory page still has space
                break;
            }
            PageId nextPage = dirPage.getNextPage();
            if(nextPage.pid == INVALID_PAGEID){ // add directory page and then data page
                DirPage newDirPage = new DirPage();
                PageId newDirID = Minibase.BufferManager.newPage(newDirPage, 1);
                newDirPage.setCurPage(newDirID);
                dirPage.setNextPage(newDirID);
                newDirPage.setPrevPage(dirPageID);
                Minibase.BufferManager.unpinPage(dirPageID, UNPIN_DIRTY);
                dirPage = newDirPage;
                count = 0;
                dirPageID = newDirID;
                break;
            }
            Minibase.BufferManager.unpinPage(dirPageID, UNPIN_CLEAN);
            dirPageID = nextPage;
        }
        DataPage dataPage = new DataPage();
        dataPageID = Minibase.BufferManager.newPage(dataPage, 1);
        dataPage.setCurPage(dataPageID);
        dirPage.setRecCnt(count, (short) 0);
        dirPage.setFreeCnt(count, dataPage.getFreeSpace());
        dirPage.setPageId(count, dataPageID);
        dirPage.setEntryCnt((short) ++count);
        Minibase.BufferManager.unpinPage(dataPageID, UNPIN_DIRTY);
        Minibase.BufferManager.unpinPage(dirPageID, UNPIN_DIRTY);
        return dataPageID;
    } // protected PageId insertPage()

    /**
     * Deletes the given data page and its directory entry from the heap file. If
     * appropriate, this also deletes the directory page.
     *
     * @param pageno identifies the page to be deleted
     * @param dirId input param id of the directory page holding the data page's entry
     * @param dirPage input param to hold directory page contents
     * @param index input the data page's entry on the directory page
     */
    protected void deletePage(PageId pageno, PageId dirId, DirPage dirPage, int index) {
        Minibase.BufferManager.freePage(pageno);
        dirPage.compact(index);
        short count = dirPage.getEntryCnt();

        if (count == 1 && dirId.pid != headId.pid){
            DirPage page = new DirPage();
            PageId leftPageID = dirPage.getPrevPage();
            PageId rightPageID = dirPage.getNextPage();

            if (leftPageID.pid != INVALID_PAGEID){
                Minibase.BufferManager.pinPage(leftPageID, page, PIN_DISKIO);
                page.setNextPage(rightPageID);
                Minibase.BufferManager.unpinPage(leftPageID, UNPIN_DIRTY);
            }
            if (rightPageID.pid != INVALID_PAGEID){
                Minibase.BufferManager.pinPage(rightPageID, page, GlobalConst.PIN_DISKIO);
                page.setPrevPage(leftPageID);
                Minibase.BufferManager.unpinPage(rightPageID, UNPIN_DIRTY);
            }
            Minibase.BufferManager.unpinPage(dirId, UNPIN_CLEAN);
            Minibase.BufferManager.freePage(dirId);
        } else {
            dirPage.setEntryCnt(--count);
            Minibase.BufferManager.unpinPage(dirId, UNPIN_DIRTY);
        }
    } // protected void deletePage(PageId, PageId, DirPage, int)

} // public class HeapFile implements GlobalConst
