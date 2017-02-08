package main.bufmgr;

import main.global.GlobalConst;
import main.global.Minibase;
import main.global.Page;
import main.global.PageId;

import java.util.*;

/**
 * <h3>Minibase Buffer Manager</h3>
 * The buffer manager manages an array of main memory pages.  The array is
 * called the buffer pool, each page is called a frame.  
 * It provides the following services:
 * <ol>
 * <li>Pinning and unpinning disk pages to/from frames
 * <li>Allocating and deallocating runs of disk pages and coordinating this with
 * the buffer pool
 * <li>Flushing pages from the buffer pool
 * <li>Getting relevant data
 * </ol>
 * The buffer manager is used by access methods, heap files, and
 * relational operators.
 */
public class BufMgr implements GlobalConst {

    /* Data structures to keep state of buffer pool */
    private Frame[] bufferPool;
    private HashMap<Integer, Integer> bookKeeping;
    private Stack<Integer> freeIndexes = new Stack<>();

    /* Class variables/objects */
    private int maxNoOfFrames;
    private int current = 0;

    /**
     * Constructs a buffer manager by initializing member data.
     * bookKeeping keeps a map of pageId -> FrameIndex
     *
     * @param numOfFrames number of frames in the buffer pool
     */
    public BufMgr(int numOfFrames) {
        bufferPool = new Frame[numOfFrames];
        bookKeeping = new HashMap<>();
        maxNoOfFrames = numOfFrames;

        /* populate the stack with available index*/
        for (int i = 0; i < maxNoOfFrames; i++){
            freeIndexes.push(i);
        }
    } // public BufMgr(int numOfFrames)

    /**
     * The result of this call is that disk page number pageno should reside in
     * a frame in the buffer pool and have an additional pin assigned to it,
     * and mempage should refer to the contents of that frame. <br><br>
     *
     * If disk page pageno is already in the buffer pool, this simply increments
     * the pin count.  Otherwise, this<br>
     * <pre>
     * 	uses the replacement policy to select a frame to replace
     * 	writes the frame's contents to disk if valid and dirty
     * 	if (contents == PIN_DISKIO)
     * 		read disk page pageno into chosen frame
     * 	else (contents == PIN_MEMCPY)
     * 		copy mempage into chosen frame
     * 	[omitted from the above is maintenance of the frame table and hash map]
     * </pre>
     * @param pageno identifies the page to pin
     * @param mempage An output parameter referring to the chosen frame.  If
     * contents==PIN_MEMCPY it is also an input parameter which is copied into
     * the chosen frame, see the contents parameter.
     * @param contents Describes how the contents of the frame are determined.<br>
     * If PIN_DISKIO, read the page from disk into the frame.<br>
     * If PIN_MEMCPY, copy mempage into the frame.<br>
     * If PIN_NOOP, copy nothing into the frame - the frame contents are irrelevant.<br>
     * Note: In the cases of PIN_MEMCPY and PIN_NOOP, disk I/O is avoided.
     * @throws IllegalArgumentException if PIN_MEMCPY and the page is pinned.
     * @throws IllegalStateException if all pages are pinned (i.e. pool is full)
     */
    public void pinPage(PageId pageno, Page mempage, int contents) {
        if (bookKeeping.containsKey(pageno.pid)){
            pinPageAlreadyInBP(pageno, mempage, contents);
        } else {
            pinPageNotInPool(pageno, mempage, contents);
        }

    }// public void pinPage(PageId pageno, Page page, int contents)

    /* Helper method for pin Page, in the case where the page is in the bufferpool*/
    private void pinPageAlreadyInBP(PageId pageno, Page mempage, int contents){
        int frameIndex = bookKeeping.get(pageno.pid);
        Frame frame = bufferPool[frameIndex];
        if (contents == PIN_MEMCPY && frame.getPinCount() > 0) {
            throw new IllegalArgumentException("Page is already pinned and a copy was requested");
        }
        if (contents == PIN_DISKIO){
            Minibase.DiskManager.read_page(pageno, mempage); // additional IO
            frame.setPage(mempage);
            frame.setReferenced();
        }
        frame.incrementPinCount();
    }

    /**
     *  Helper method for pin Page, in the case where the page is not available in BP
     * */
    private void pinPageNotInPool(PageId pageno, Page mempage, int contents){
        /* Pick frame to use using a replacement policy */
        Frame frame = pinPageNotInPoolUtil(pageno, mempage, contents);
        /* When all pages pinned */
        if (frame == null){
            throw new IllegalStateException("Bufferpool is full and we couldn't find any frames to replace");
        }
        // Get available index. Note, this will never throw an exception, as long as
        // method "pinPageNotInPoolUtil" finds a frame to replace i.e. frame != null
        int frameIndex = freeIndexes.pop();

        /* Update books */
        bufferPool[frameIndex] = frame;
        bookKeeping.put(pageno.pid, frameIndex);
    }

    /**
     * PinPage helper function.
     */
    private Frame pinPageNotInPoolUtil(PageId pageno, Page mempage, int contents) {
        Frame frame;
        /* case a: If page is not in bufferpool and bufferpool has free frames */
        if (!freeIndexes.isEmpty()) {
            frame = new Frame(pageno);
            commonTasksFromPinPage(frame, pageno, mempage, contents);
        } else {
            /* case b: Bufferpool is filled, we need to pick a frame to replace if there's one */
            frame = replacementPolicy();
            if (frame != null) {
                commonTasksFromPinPage(frame, pageno, mempage, contents);
            }
        }
        return frame;
    } // private Frame pickVulnerableFrame(PageId pageno, Page mempage, int contents)

    /**
     * Common tasks in method: pinPageNotInPoolUtil
     * */
    private void commonTasksFromPinPage(Frame frame, PageId pageno, Page mempage, int contents){
        switch (contents) {
            case PIN_DISKIO:
                Minibase.DiskManager.read_page(pageno, mempage);
                frame.setPageId(pageno);
                frame.setPage(mempage);
                frame.incrementPinCount();
                frame.setReferenced();
                break;
            case PIN_NOOP:
            case PIN_MEMCPY:
                frame.setPageId(pageno);
                frame.setPage(mempage);
                frame.incrementPinCount();
                frame.setReferenced();
                break;
            default:
                break;
        }
    } // private void commonTasksFromPinPage(Frame frame, PageId pageno, Page mempage, int contents)

    /**
     * Clock Replacement policy
     * Returns null if all pages are pinned.
     * Another assumption is, this method is only called when all frames in the BP have been assigned.
     * Otherwise we could have just picked any one of the free frames to use.
     * Notes: Here i'm simulating a circular buffer by going through the bufferpool thrice
     * */
    private Frame replacementPolicy(){
        Frame currFrame = null;
        if (!isPoolExceeded()) {
            int rotation = 0;
            while (currFrame == null && rotation < 3) {
                if (bufferPool[current].getPinCount() == 0) {
                    if (bufferPool[current].getReferenced() == 1) {
                        bufferPool[current].unsetReferenced();
                    } else {
                        currFrame = bufferPool[current];
                        currFrame.freeFrame();
                        freeIndexes.push(current);
                        bookKeeping.remove(currFrame.getPageId().pid);
                    }
                }
                current = (current + 1) % maxNoOfFrames;
                if (current == 0)
                    rotation++;
            }
        }
        return currFrame;
    } // private replacementPolicy(int current)

    /**
     * Check if all pages in the BP have pinCount greater than 0
     * */
    private Boolean isPoolExceeded(){
        Boolean exceeded = true;
        if (freeIndexes.isEmpty()) {
            for (int i = 0; i < getNumFrames(); i++) {
                if (bufferPool[i].getPinCount() == 0) {
                    exceeded = false;
                    break;
                }
            }
        } else {
            exceeded = false;
        }
        return exceeded;
    }

    /**
     * Unpins a disk page from the buffer pool, decreasing its pin count.
     *
     * @param pageno identifies the page to unpin
     * @param dirty UNPIN_DIRTY if the page was modified, UNPIN_CLEAN otherwise
     * @throws IllegalArgumentException if the page is not in the buffer pool
     *  or not pinned
     */
    public void unpinPage(PageId pageno, boolean dirty) {
        if (!bookKeeping.containsKey(pageno.pid)){
            throw new IllegalArgumentException("Page is not in the bufferpool");
        }
        Frame frame = bufferPool[bookKeeping.get(pageno.pid)];
        frame.decrementPinCount();
        if (frame.getPinCount() == 0) {
            frame.freeFrame();
            freeIndexes.push(Arrays.asList(bufferPool).indexOf(frame));
            bookKeeping.remove(pageno.pid);
        }
        /* Should we check for the pincount value before writing ? */
        if (dirty){
            frame.setDirty();
            flushPage(frame);
        }
    } // public void unpinPage(PageId pageno, boolean dirty)

    /**
     * Allocates a run of new disk pages and pins the first one in the buffer pool.
     * The pin will be made using PIN_MEMCPY.  Watch out for disk page leaks.
     *
     * @param firstpg input and output: holds the contents of the first allocated page
     * and refers to the frame where it resides
     * @param run_size input: number of pages to allocate
     * @return page id of the first allocated page
     * @throws IllegalArgumentException if firstpg is already pinned
     * @throws IllegalStateException if all pages are pinned (i.e. pool exceeded)
     */
    public PageId newPage(Page firstpg, int run_size) {
        if (isPoolExceeded()) {
           throw new IllegalStateException("Pool exceeded");
        }
        PageId pageId = Minibase.DiskManager.allocate_page(run_size);
        pinPage(pageId, firstpg, PIN_MEMCPY);   // this could throw an exception, but that's the right behavior
        return pageId;
    } // public PageId newPage(Page firstpg, int run_size)

    /**
     * Deallocates a single page from disk, freeing it from the pool if needed.
     *
     * @param pageno identifies the page to remove
     * @throws IllegalArgumentException if the page is pinned
     */
    public void freePage(PageId pageno) {
        if (bookKeeping.containsKey(pageno.pid)){
            Frame frame = bufferPool[bookKeeping.get(pageno.pid)];
            if (frame.getPinCount() > 0){
                throw new IllegalArgumentException("Attempt to deallocate a page in use");
            }
            frame.freeFrame();
            /* Update books */
            int frameIndex = bookKeeping.get(pageno.pid);
            freeIndexes.push(frameIndex);
            bookKeeping.remove(pageno.pid);
        }
        Minibase.DiskManager.deallocate_page(pageno);
    } // public void freePage(PageId firstid)

    /**
     * Write all valid and dirty frames to disk.
     * Note flushing involves only writing, not unpinning or freeing
     * or the like.
     *
     */
    public void flushAllFrames() {
        for (int frameIndex: bookKeeping.values()){
            flushPage(bufferPool[frameIndex]);
        }
    } // public void flushAllFrames()

    /**
     * Write a page in the buffer pool to disk, if dirty.
     * No need to check if page is in pool, there's protection from the callers.
     *
     * @throws IllegalArgumentException if the page is not in the buffer pool
     */
    private void flushPage(Frame frame) {
        if (frame.getDirtyBit()){
            Minibase.DiskManager.write_page(frame.getPageId(), frame.getPage());
            frame.setClean();
        }
    } // private void flushPage(Frame frame)

    /**
     * Gets the total number of buffer frames.
     */
    public int getNumFrames() {
        return this.maxNoOfFrames;
    } // public int getNumFrames()

    /**
     * Gets the total number of unpinned buffer frames.
     */
    public int getNumUnpinned() {
        return this.freeIndexes.size();
    } // public int getNumUnpinned()

} // public class BufMgr implements GlobalConst
