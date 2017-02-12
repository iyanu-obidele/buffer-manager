package bufmgr;

import global.Page;
import global.PageId;

/* class representing the abstraction of a frame. */
public class Frame {
    private boolean dirty;
    private int pinCount;
    private Page page;
    private PageId pageId;
    private int referenced;

    /** new frame constructor */
    public Frame(PageId pageId){
        this(false, 0, 0, null, pageId);
    }

    /** constructor for the frame */
    public Frame(boolean dirtyBit, int count, int ref, Page somePage, PageId id){
        dirty = dirtyBit;
        pinCount = count;
        referenced = ref;
        page = somePage;
        pageId = id;
    }

    public PageId getPageId(){
        return this.pageId;
    }

    public void setPageId(PageId id){
        this.pageId = id;
    }

    public void setReferenced(){
        this.referenced = 1;
    }

    public void unsetReferenced(){
        this.referenced = 0;
    }

    public int getReferenced(){
        return this.referenced;
    }

    /** returns the current page stored in this frame*/
    public synchronized Page getPage(){
        return this.page;
    }

    /** sets page on a Frame.*/
    public synchronized void setPage(Page newPage){
        //this.page.copyPage(newPage);
        this.page = newPage;
    }

    /** returns the current pinCount of the page */
    public synchronized int getPinCount(){
        return this.pinCount;
    }

    /** increase number of usage for this page*/
    public synchronized void incrementPinCount(){
        this.pinCount++;
    }

    /** decrement pinCount, it's a no op if pinCount is already zero*/
    public synchronized void decrementPinCount(){
        if (this.pinCount > 0){
            this.pinCount--;
        }
    }

    /** returns the current dirty bit state of the page in the frame*/
    public synchronized boolean getDirtyBit(){
        return this.dirty;
    }

    /** set the page in the frame to dirty */
    public synchronized void setDirty(){
        this.dirty = true;
    }

    public void setClean() { this.dirty = false; }

    /** once a page is detached from the frame, it's safe to wipe it all clean */
    public synchronized void freeFrame(){
       this.dirty = false;
       this.pinCount = 0;
       this.referenced = 0;
    }
}
