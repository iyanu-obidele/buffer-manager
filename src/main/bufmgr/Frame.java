package main.bufmgr;

import main.global.Page;

/* class representing the abstraction of a frame. */
public class Frame {
    private boolean dirty;
    private int pinCount;
    private Page page;

    /** new frame constructor */
    public Frame(){
        this(false, 0, null);
    }

    /** constructor for the frame adt */
    public Frame(boolean dirtyBit, int count, Page somePage){
        dirty = dirtyBit;
        pinCount = count;
        page = somePage;
    }

    /** returns the current pinCount of the page */
    public int getPinCount(){
        return this.pinCount;
    }

    /** returns the current dirty bit state of the frame*/
    public boolean getDirtyBit(){
        return this.dirty;
    }

    /** returns the current page stored in this frame*/
    public Page getPage(){
        return this.page;
    }

    /** sets page on a Frame.*/
    public void setPage(Page newPage){
        if (this.page != null || this.pinCount > 0){
            throw new IllegalStateException(
                    "You're trying to overwrite a page that might be in use");
        }
        this.page = newPage;
    }

    /** increase number of usage for this page*/
    public void incrementPinCount(){
        this.pinCount++;
    }

    /** decrement pinCount, it's a no op if pinCount is already zero*/
    public void decrementPinCount(){
        if (this.pinCount > 0){
            this.pinCount--;
        }
    }

    /** set the page in the frame to dirty */
    public void setDirty(){
        this.dirty = true;
    }

    /** once a page is detached from the frame, it's safe to wipe it all clean */
    public void unsetPage(){
       this.dirty = false;
       this.pinCount = 0;
       this.page = null;
    }
}
