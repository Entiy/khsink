package com.sina.tools;


import java.util.Arrays;

/**
 * Created by qiangshizhi on 2017/7/6.
 */
public class ByteBuffer {


    private byte[] buf;
    private int capacity;
    private int index;

    private ByteBuffer(int capacity) {
        if (capacity < 0)
            throw new IllegalArgumentException();
        this.capacity=(int)1.5*capacity;
        this.buf=new byte[this.capacity];
    }

    private ByteBuffer(){
        this(Integer.MAX_VALUE);
    }

    public static ByteBuffer allocate(int capacity){
        return  new ByteBuffer(capacity);
    }

    // 此方位为native方法。
    // public static native void arraycopy(
    // Object src, int srcPos, Object dest,
    // int destPos, int length);
    public void put(byte[] b){
        if (b.length<=capacity-index){
            try {
                System.arraycopy(b,0,buf,index,b.length);
                index+=b.length;
            }
            catch (ArrayIndexOutOfBoundsException ex){
                ex.printStackTrace();
            }
        }else{
            throw new IllegalArgumentException();
        }
    }

    public void clear(){
        buf=null;
        buf=new byte[capacity];
        index=0;
    }
    public int size(){
        return index;
    }

    public byte[] array(){
        byte[] tmp=new byte[size()];
        System.arraycopy(buf,0,tmp,0,size());
        return tmp;
    }


}
