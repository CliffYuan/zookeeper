package com.xiaoniudu.nio;

import java.nio.channels.SelectionKey;

/**
 * Created by xiaoniudu on 15-7-27.
 */
public class NIOKey {
    public static void main(String[] args) {
        System.out.println(SelectionKey.OP_READ);
        System.out.println(SelectionKey.OP_WRITE);
        System.out.println(SelectionKey.OP_ACCEPT);
        System.out.println(SelectionKey.OP_CONNECT);
    }
}
