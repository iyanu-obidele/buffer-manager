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
        /* case 0: PIN_NOOP -
        copy nothing into the frame means what exactly ?
        Return immediately from pinPage or allocate the frame but leave its content empty ?*/
        /* case 1: If page is already in buffer pool increase pinCount */
        if (bookKeeping.containsKey(pageno.pid)){
            int frameIndex = bookKeeping.get(pageno.pid);
            Frame frame = bufferPool[frameIndex];
            if (contents == PIN_MEMCPY && frame.getPinCount() > 0) {
                throw new IllegalArgumentException("Page is already pinned and a copy was requested");
            }
            frame.incrementPinCount();
        } else {
            /* Pick frame to use using a replacement policy */
            Frame frame = pickAndAssignPageToFrame(pageno, mempage, contents);

            /* When all pages pinned */
            if (frame == null){
                throw new IllegalStateException("Bufferpool is full and we couldn't find any frames to replace");
            }

            /* Get available index. Note, this wont throw an exception, as long as
            * method "pickAndAssignPageToFrame" finds a frame to replace */
            int frameIndex = freeIndexes.pop();

            /* Update books */
            bufferPool[frameIndex] = frame;
            bookKeeping.put(pageno.pid, frameIndex);
        }

    }// public void pinPage(PageId pageno, Page page, int contents)

    /**
     * PinPage helper function. There's a call to replacementPolicy.
     * TODO: refactor this method
     */
    private Frame pickAndAssignPageToFrame(PageId pageno, Page mempage, int contents) {
        Frame frame = new Frame(pageno);
        /* case 2a: If page is not in bufferpool and bufferpool has free frames */
        if (!freeIndexes.isEmpty()) {
            switch (contents){
                case PIN_DISKIO:
                    Minibase.DiskManager.read_page(pageno, mempage); // additional IO
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
                    frame = null;  // this branch should never be taken
                    break;
            }
        } else {
            /* case 2b: Bufferpool is filled, we need to pick a frame to replace if there's one */
            frame = replacementPolicy();
            if (frame != null) {
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
                        frame = null;   // this branch should never be taken
                        break;
                }
            }
        }
        return frame;
    } // private Frame pickVulnerableFrame(PageId pageno, Page mempage, int contents)

    /**
     * Clock Replacement policy
     * Returns null if all pages are pinned.
     * Another assumption is, this method is only called when all frames in the BP have been assigned.
     * Otherwise we could have just picked any one of the free frames to use.
     * */
    private Frame replacementPolicy(){
        int rotation = 0;
        Frame currFrame = null;
        while (currFrame == null && rotation < 3){
            if (bufferPool[current].getPinCount() == 0) {
                if (bufferPool[current].getReferenced() == 1) {
                    bufferPool[current].unsetReferenced();
                    current = (current + 1) % maxNoOfFrames;
                    if (current == 0)
                        rotation++;
                } else {
                    currFrame = bufferPool[current];
                    currFrame.unsetPage();
                    freeIndexes.push(current);
                    bookKeeping.remove(currFrame.getPageId().pid);
                }
            }
        }
        return currFrame;
    } // private replacementPolicy(int current)

    /**
     * Unpins a disk page from the buffer pool, decreasing its pin count.
     *
     * @param pageno identifies the page to unpin
     * @param dirty UNPIN_DIRTY if the page was modified, UNPIN_CLEAN otherwise
     * @throws IllegalArgumentException if the page is not in the buffer pool
     *  or not pinned
     */
    public void unpinPage(PageId pageno, boolean dirty) {
        if (!bookKeeping.containsKey(pageno.pid)) {
            throw new IllegalArgumentException("Attempt to unpin a page not in the buffer pool");
        }
        Frame frame = bufferPool[bookKeeping.get(pageno.pid)];
        frame.decrementPinCount();
        if (frame.getPinCount() == 0)
            frame.unsetPage();
            freeIndexes.push(Arrays.asList(bufferPool).indexOf(frame));
            bookKeeping.remove(pageno.pid);
            /*ArrayUtils.re*/
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
        if (freeIndexes.isEmpty()) {
            Frame frame = replacementPolicy();
            if (frame == null) {
                throw new IllegalStateException("Pool exceeded");
            }
        }
        PageId pageId = Minibase.DiskManager.allocate_page(run_size);
        try {
            pinPage(pageId, firstpg, PIN_MEMCPY);
        } catch (Exception ex){
            throw ex;
        }
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
            frame.unsetPage();

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
     *
     * @throws IllegalArgumentException if the page is not in the buffer pool
     */
    private void flushPage(Frame frame) {
        Set<Frame> set = new HashSet<>(Arrays.asList(bufferPool));
        if (!set.contains(frame)) {
            throw new IllegalArgumentException("Attempt to flush a page not in the bufferpool");
        }

        if (frame.getDirtyBit()){
            Minibase.DiskManager.write_page(frame.getPageId(), frame.getPage());
            frame.setClean();
        }
    }

    /**
     * Gets the total number of buffer frames.
     */
    public int getNumFrames() {
        return this.maxNoOfFrames;
    }

    /**
     * Gets the total number of unpinned buffer frames.
     */
    public int getNumUnpinned() {
        return this.freeIndexes.size();
    }

} // public class BufMgr implements GlobalConst
