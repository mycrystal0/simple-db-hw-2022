package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private final File file;
    private final TupleDesc td;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
//        throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
//        throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        RandomAccessFile raf;
        HeapPage heapPage;
        byte data[] = new byte[BufferPool.getPageSize()];
            try {
                raf = new RandomAccessFile(this.file, "r");
                raf.seek((long)pid.getPageNumber() * BufferPool.getPageSize());
                int n = raf.read(data,0,data.length);
                if(n == -1){
                    throw new IOException("read page failed, reach the end of the file!");
                }
                heapPage = new HeapPage((HeapPageId) pid, data);
            }catch (IOException e){
                throw new RuntimeException(e);
            }
        return heapPage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // lab2
        RandomAccessFile rf = new RandomAccessFile(file, "rw");
        rf.seek(page.getId().getPageNumber() * BufferPool.getPageSize());
        rf.write(page.getPageData());
        rf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(this.file.length() * 1.0 / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        //lab2
        int pgNum = 0;
        HeapPage heapPage = null;

        while (pgNum < numPages()) {
            heapPage = (HeapPage) Database.getBufferPool().getPage(tid,new HeapPageId(getId(),pgNum), Permissions.READ_WRITE);
            if (heapPage.getNumEmptySlots() > 0) {
                break;
            }
            pgNum ++;
        }
        if(pgNum == numPages()) {
            heapPage = new HeapPage(new HeapPageId(getId(), pgNum), HeapPage.createEmptyPageData());
        }
        heapPage.insertTuple(t);
        writePage(heapPage);
        return Arrays.asList(heapPage);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // lab2
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        heapPage.deleteTuple(t);

        return new ArrayList<>(Arrays.asList(heapPage));
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this ,tid);
    }

}
class HeapFileIterator extends AbstractDbFileIterator {

    Iterator<Tuple> it = null;
    HeapPage heapPage = null;
    final HeapFile heapFile;
    final TransactionId tid;
    public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
        this.heapFile = heapFile;
        this.tid = tid;
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        // 当前为最后一个tuple
        if(it != null && !it.hasNext()){
            it = null;
        }
        // 寻找下一页
        while (it == null && heapPage != null){
            int pgNo = heapPage.pid.getPageNumber() + 1;
            // 判断下一页是否超出文件范围
            if(pgNo >= this.heapFile.numPages()){
                heapPage = null;
                break;
            }
            HeapPageId nextPageId = new HeapPageId(heapPage.pid.getTableId(),pgNo);
            this.heapPage = (HeapPage) Database.getBufferPool().getPage(tid, nextPageId, Permissions.READ_WRITE);
            it = this.heapPage.iterator();
            if(it != null && it.hasNext()){
                break;
            }
            it = null;
        }
        if (it == null)
            return null;
        return it.next();
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        PageId pid = new HeapPageId(this.heapFile.getId(), 0);
        this.heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
        it = this.heapPage.iterator();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    public void close() {
        super.close();
        it = null;
        heapPage = null;
    }
}

