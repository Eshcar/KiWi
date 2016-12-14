package kiwilocal;

import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.TreeSet;

/**
 * Created by dbasin on 11/30/15.
 */
public class ScanIndex<K extends Comparable<? super K>> {
    private long[] scanVersions;
    boolean isFirst;
    private int index;


    public ScanIndex(long[] scans, long currVersion)
    {
        scanVersions = scans;
        reset();
    }

    private void reset()
    {
        index = -1;
        isFirst = true;

    }
    public final void reset(K key) {
        reset();
    }


    /***
     *
     *
     * @param version -- we assume that version > 0
     * @return
     */
    public final boolean shouldKeep(long version) {

        //always save the first provided version.
        if(isFirst) return true;
        if(index >= scanVersions.length) return false;
        return scanVersions[index] >= version;

    }

    public final void savedVersion(long version)
    {
        isFirst = false;
        index++;
    }
}
