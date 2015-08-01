package com.xiaoniudu.nio;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;

/**
 * 测试SelectionKey OP_WRITE 触发机制.
 *
 * Created by xiaoniudu on 15-7-22.
 */
public class NioServer implements Runnable {

    ServerSocketChannel ss;

    Selector selector = null;

    Thread thread = null;

    public static void main(String[] args) throws Exception{
        NioServer se=new NioServer();
        se.start(new InetSocketAddress(8987),12);
        se.start();
    }

    public void start() {
        // ensure thread is started once and only once
        if (thread.getState() == Thread.State.NEW) {
            thread.start();
            System.out.println("start......");
        }
    }

    public void start(InetSocketAddress addr, int maxcc) throws IOException {
        selector = Selector.open();
        thread = new Thread(this, "NIOServerCxn.Factory:" + addr);
        thread.setDaemon(false);
        ss = ServerSocketChannel.open();
        ss.socket().setReuseAddress(true);
        ss.socket().bind(addr);
        ss.configureBlocking(false);
        ss.register(selector, SelectionKey.OP_ACCEPT);
    }

    @Override
    public void run() {
        while (!ss.socket().isClosed()) {
            try {
                selector.select(10000);
                Set<SelectionKey> selected;
                System.out.println("look.................");
                synchronized (this) {
                    selected = selector.selectedKeys();
                }
                ArrayList<SelectionKey> selectedList = new ArrayList<SelectionKey>(
                        selected);
                Collections.shuffle(selectedList);
                for (SelectionKey k : selectedList) {
                    System.out.println(k.readyOps());
                    if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                        SocketChannel sc = ((ServerSocketChannel) k
                                .channel()).accept();
                        InetAddress ia = sc.socket().getInetAddress();

                        System.out.println("Accepted socket connection from "
                                + sc.socket().getRemoteSocketAddress());
                        sc.configureBlocking(false);
                        SelectionKey sk = sc.register(selector,
                                SelectionKey.OP_READ|SelectionKey.OP_WRITE);
                        sk.attach(sc);
                    } else if ((k.readyOps() & SelectionKey.OP_READ ) != 0) {//如果是read和write事件，则处理

                        System.out.println("read");
                    } else if ((k.readyOps() & SelectionKey.OP_WRITE ) != 0) {//如果是read和write事件，则处理
                        System.out.println("write");
                    } else {
                        System.out.println("no");
                    }
                }
                selected.clear();
            } catch (RuntimeException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
