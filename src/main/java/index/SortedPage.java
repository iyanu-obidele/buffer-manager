package index;

import global.Page;
import global.PageId;
import global.SearchKey;

/**
 * A base class for index pages that automatically stores records in ascending
 * order by key value. SortedPage supports variable-length records by using a
 * slot directory, with the slots at the front and the records in the back, both
 * growing and shrinking into and from the free space in the middle of the page.
 * This structure is similar to HFPage except that rids can change; slots are 
 * always in order of search key and all slots are always full.  It will be used 
 * for BTree internal index pages and leaf data entry pages.
 */
class SortedPage extends Page {

  /** Offset of the number of entries. */
  protected static final int ENTRY_CNT = 0;

  /** Offset of the used space pointer. */
  protected static final int USED_PTR = 2;

  /** Offset of the next page id. */
  protected static final int NEXT_PAGE = 4;

  // --------------------------------------------------------------------------

  /** Total size of the header fields. */
  protected static final int HEADER_SIZE = 8;

  /** Size of a slot (short length; short offset). */
  protected static final int SLOT_SIZE = 4;

  /** Maximum allowed size of an entry. */
  protected static final int MAX_ENTRY_SIZE = (PAGE_SIZE - HEADER_SIZE - SLOT_SIZE);

  // --------------------------------------------------------------------------

  /**
   * Default constructor; creates a sorted page with default values.
   */
  public SortedPage() {
    super();
    initDefaults();
  }

  /**
   * Constructor that wraps an existing sorted page.
   */
  public SortedPage(Page page) {
    super(page.getData());
  }

  /**
   * Initializes the sorted page with default values.
   */
  protected void initDefaults() {

    // initially no slots in use
    setShortValue((short) 0, ENTRY_CNT);

    // used pointer moves backwards
    setShortValue((short) PAGE_SIZE, USED_PTR);

    // set next page id to invalid
    setIntValue(INVALID_PAGEID, NEXT_PAGE);

  } // protected void initDefaults()

  // --------------------------------------------------------------------------

  /**
   * Gets the number of entries on the page.
   */
  public short getEntryCount() {
    return getShortValue(ENTRY_CNT);
  }

  /**
   * Gets the amount of free space (in bytes).
   */
  public short getFreeSpace() {
    return (short) (getShortValue(USED_PTR) - (HEADER_SIZE + getEntryCount()
        * SLOT_SIZE));
  }

  /**
   * Gets the next page's id.
   */
  public PageId getNextPage() {
    return new PageId(getIntValue(NEXT_PAGE));
  }

  /**
   * Sets the next page's id.
   */
  public void setNextPage(PageId pageno) {
    setIntValue(pageno.pid, NEXT_PAGE);
  }

  // --------------------------------------------------------------------------

  /**
   * Gets the length of the record referenced by the given slot.
   */
  protected short getSlotLength(int slotno) {
    return getShortValue(HEADER_SIZE + slotno * SLOT_SIZE);
  }

  /**
   * Gets the offset of the record referenced by the given slot.
   */
  protected short getSlotOffset(int slotno) {
    return getShortValue(HEADER_SIZE + slotno * SLOT_SIZE + 2);
  }

  /**
   * @throws IllegalArgumentException if the slot number is invalid
   */
  protected void checkSlotno(int slotno) {
    if ((slotno < 0) || (slotno > getEntryCount() - 1)) {
      throw new IllegalArgumentException("invalid slot number");
    }
  }

  /**
   * Gets the data entry at the given slot number.
   * 
   * @throws IllegalArgumentException if the slot number is invalid
   */
  public DataEntry getEntryAt(int slotno) {
    checkSlotno(slotno);
    return new DataEntry(data, getSlotOffset(slotno));
  }

  /**
   * Gets the search key from the data entry at the given slot number.
   * 
   * @throws IllegalArgumentException if the slot number is invalid
   */
  public SearchKey getKeyAt(int slotno) {
    checkSlotno(slotno);
    return new SearchKey(data, getSlotOffset(slotno));
  }

  // --------------------------------------------------------------------------

  /**
   * Inserts a new record into the page in sorted order.
   * 
   * @return true if inserting made this page dirty, false otherwise
   * @throws IllegalStateException if insufficient space
   */
  public boolean insertEntry(DataEntry entry) {

    // first check for sufficient space
    short reclen = entry.getLength();
    short spaceNeeded = (short) (reclen + SLOT_SIZE);
    if (spaceNeeded > getFreeSpace()) {
      throw new IllegalStateException("insufficient space");
    }

    // linear search for the appropriate slot
    short i, slotCnt = getEntryCount();
    for (i = 0; i < slotCnt; i++) {

      // if the slot's key comes after the new key
      if (getKeyAt(i).compareTo(entry.key) > 0) {
        break;
      }

    } // for

    // if inserting into the middle
    int slotpos = HEADER_SIZE + i * SLOT_SIZE;
    if (i < slotCnt) {

      // shift the subsequent slots down
      System.arraycopy(data, slotpos, data, slotpos + SLOT_SIZE, (slotCnt - i)
          * SLOT_SIZE);

    } // if

    // update the entry count and used space offset
    setShortValue(++slotCnt, ENTRY_CNT);
    short usedPtr = getShortValue(USED_PTR);
    usedPtr -= reclen;
    setShortValue(usedPtr, USED_PTR);

    // update the slot and insert the record
    setShortValue(reclen, slotpos);
    setShortValue(usedPtr, slotpos + 2);
    entry.writeData(data, usedPtr);
    return true;

  } // public boolean insertEntry(DataEntry entry)

  /**
   * Deletes a data entry from the page, compacting the free space 
   * (including the slot directory).
   * 
   * @return true if deleting made this page dirty, false otherwise
   * @throws IllegalArgumentException if the entry doesn't exist
   */
  public boolean deleteEntry(DataEntry entry) {

    // linear search for the entry's slot
    short i, slotCnt = getEntryCount();
    for (i = 0; i < slotCnt; i++) {

      // if the slot's entry matches the entry
      if (getEntryAt(i).equals(entry)) {
        break;
      }

    } // for

    // if the entry doesn't exist
    if (i == slotCnt) {
      throw new IllegalArgumentException("entry doesn't exist");
    }

    // calculate the compacting values
    short slotpos = (short) (HEADER_SIZE + i * SLOT_SIZE);
    short reclen = getSlotLength(i);
    short recoff = getSlotOffset(i);
    short usedPtr = getShortValue(USED_PTR);
    short newSpot = (short) (usedPtr + reclen);

    // compact the slot directory and free space, and advance the used pointer
    System.arraycopy(data, slotpos + SLOT_SIZE, data, slotpos,
        (slotCnt - i - 1) * SLOT_SIZE);
    System.arraycopy(data, usedPtr, data, newSpot, recoff - usedPtr);
    setShortValue(newSpot, USED_PTR);

    // adjust offsets of all valid slots that refer
    // to the left of the record being removed
    for (int j = 0, n = HEADER_SIZE; j < slotCnt; j++, n += SLOT_SIZE) {
      short chkoffset = getSlotOffset(j);
      if (chkoffset < recoff) {
        chkoffset += reclen;
        setShortValue(chkoffset, n + 2);
      }
    }

    // update the entry count
    setShortValue(--slotCnt, ENTRY_CNT);
    return true;

  } // public boolean deleteEntry(DataEntry entry)

  // --------------------------------------------------------------------------

   /**
   * Searches for the next entry that matches the given search key, and stored
   * after the given slot.
   * 
   * @return the slot number of the entry, or -1 if not found
   */
  public int nextEntry(SearchKey key, int slotno) {

    // linear search for the entry's slot
    int slotCnt = getEntryCount();
    for (int i = slotno + 1; i < slotCnt; i++) {

      // if the slot's entry matches the entry
      if (getKeyAt(i).compareTo(key) == 0) {
        return i;
      }

    } // for

    // otherwise not found
    return -1;

  } // public int nextEntry(SearchKey key, int slotno)

} // class SortedPage extends Page
