package main.bufmgr;

import main.global.Page;
import main.global.PageId;

/* class representing the abstraction of a frame. */
public class Frame {
    private boolean dirty;
    private int pinCount;
    private Page page;
    private PageId pageId;

    /** new frame constructor */
    public Frame(PageId pageId){
        this(false, 0, null, pageId);
    }

    /** constructor for the frame */
    public Frame(boolean dirtyBit, int count, Page somePage, PageId id){
        dirty = dirtyBit;
        pinCount = count;
        page = somePage;
        pageId = id;
    }

    /** returns the current pinCount of the page */
    public synchronized int getPinCount(){
        return this.pinCount;
    }

    /** returns the current dirty bit state of the page in the frame*/
    public synchronized boolean getDirtyBit(){
        return this.dirty;
    }

    public PageId getPageId(){
        return this.pageId;
    }

    /** returns the current page stored in this frame*/
    public synchronized Page getPage(){
        return this.page;
    }

    /** sets page on a Frame.*/
    public synchronized void setPage(Page newPage){
        if (this.page != null || this.pinCount > 0){
            throw new IllegalStateException(
                    "You're trying to overwrite a page that might be in use");
        }
        this.page = newPage;
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

    /** set the page in the frame to dirty */
    public synchronized void setDirty(){
        this.dirty = true;
    }

    /** once a page is detached from the frame, it's safe to wipe it all clean */
    public synchronized void unsetPage(){
       this.dirty = false;
       this.pinCount = 0;
       this.page = null;
       this.pageId = null;
    }
}
