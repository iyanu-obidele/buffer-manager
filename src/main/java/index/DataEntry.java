package index;

import global.RID;
import global.SearchKey;

/**
 * Records stored in an index file; using the textbook's "Alternative 2" (see
 * page 276) to allow for multiple indexes. Duplicate keys result in duplicate
 * DataEntry instances.
 */
class DataEntry {

  /** The search key (includes the type, i.e. integer, float, string, and the size). */
  public SearchKey key;

  /** The id of a data record (i.e. in some heap file). */
  public RID rid;

  // --------------------------------------------------------------------------

  /**
   * Constructs a DataEntry from the given values.
   */
  public DataEntry(SearchKey key, RID rid) {
    this.key = new SearchKey(key);
    this.rid = new RID(rid.pageno, rid.slotno);
  }

  /**
   * Constructs a DataEntry from the key and RID stored in the given data buffer.
   */
  public DataEntry(byte[] data, short offset) {

    // construct the search key
    key = new SearchKey(data, offset);

    // construct the RID
    rid = new RID(data, (short) (offset + key.getLength()));

  } // public DataEntry(byte[] data, short offset)

  /**
   * Writes the DataEntry into the given data buffer.
   */
  public void writeData(byte[] data, short offset) {

    // write the search key
    key.writeData(data, offset);

    // write the RID
    rid.writeData(data, (short) (offset + key.getLength()));

  } // public void writeData(byte[] data, short offset)

  /**
   * Gets the total length of the data entry (in bytes).
   */
  public short getLength() {
    return (short) (key.getLength() + rid.getLength());
  }

  /**
   * True if obj is a DataEntry with the same values; false otherwise.
   */
  public boolean equals(Object obj) {
    if (obj instanceof DataEntry) {
      DataEntry entry = (DataEntry) obj;
      return (key.compareTo(entry.key) == 0) && (rid.equals(entry.rid));
    }
    return false;
  }

} // class DataEntry
