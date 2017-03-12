package index;

import global.Page;
import global.PageId;

/**
 * Hash directory pages simply contain page ids to data pages (i.e. buckets).
 */
class HashDirPage extends Page {

  /** Offset of the number of entries. */
  protected static final int ENTRY_CNT = 0;

  /** Offset of the next page id. */
  protected static final int NEXT_PAGE = 2;

  // --------------------------------------------------------------------------

  /** Relative offset of an entry's page id. */
  protected static final int IX_PAGEID = 0;

  /** Relative offset of an entry's local depth. */
  protected static final int IX_DEPTH = 4;

  // --------------------------------------------------------------------------

  /** Total size of the header fields. */
  protected static final int HEADER_SIZE = 6;

  /** The size of a directory entry. */
  protected static final int ENTRY_SIZE = 5;

  protected static final int MAX_ENTRIES = (PAGE_SIZE - HEADER_SIZE)
      / ENTRY_SIZE;

  /** Initial directory size (in buckets). */
  protected static final short INIT_SIZE = 128;

  // --------------------------------------------------------------------------

  /**
   * Default constructor; creates a directory page with default values.
   */
  public HashDirPage() {
    super();
    initDefaults();
  }

  /**
   * Constructor that wraps an existing directory page.
   */
  public HashDirPage(Page page) {
    super(page.getData());
  }

  /**
   * Initializes the directory page with default values.
   */
  protected void initDefaults() {

    // initialize the entry count
    setShortValue(INIT_SIZE, ENTRY_CNT);

    // set next page id to invalid
    setIntValue(INVALID_PAGEID, NEXT_PAGE);

    // set each entry's pageid to invalid
    for (int i = 0; i < MAX_ENTRIES; i++) {
      setIntValue(INVALID_PAGEID, HEADER_SIZE + i * ENTRY_SIZE + IX_PAGEID);
    }

  } // protected void initDefaults()

  // --------------------------------------------------------------------------

  /**
   * Gets the number of entries on the page.
   */
  public short getEntryCount() {
    return getShortValue(ENTRY_CNT);
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
   * Gets the first page id of the bucket for the given hash value.
   */
  public PageId getPageId(int hash) {
    return new PageId(getIntValue(HEADER_SIZE + hash * ENTRY_SIZE + IX_PAGEID));
  }

  /**
   * Sets the first page id of the bucket for the given hash value.
   */
  public void setPageId(int hash, PageId pageno) {
    setIntValue(pageno.pid, HEADER_SIZE + hash * ENTRY_SIZE + IX_PAGEID);
  }

  /**
   * Gets the local depth of the bucket for the given hash value.
   */
  public byte getDepth(int hash) {
    return data[HEADER_SIZE + hash * ENTRY_SIZE + IX_DEPTH];
  }

  /**
   * Sets the local depth of the bucket for the given hash value.
   */
  public void setDepth(int hash, byte depth) {
    data[HEADER_SIZE + hash * ENTRY_SIZE + IX_DEPTH] = depth;
  }

} // class HashDirPage extends Page
