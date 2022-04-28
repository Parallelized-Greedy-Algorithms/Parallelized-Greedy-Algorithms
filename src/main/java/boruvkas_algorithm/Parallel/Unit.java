package boruvkas_algorithm.Parallel;

import java.util.concurrent.atomic.AtomicInteger;

// occupies 1 cache line
// 1 cache line is 64 bytes
// Object header takes 12 bytes
// long == 8 bytes
// int == 4 bytes
// AtomicInteger == 20 bytes
public class Unit{
    public volatile int value = 0;
//    private long p1, p2, p3;
    private long p1, p2, p3, p4, p5;
    private int p7;
    public AtomicInteger atomic;

    Unit(){}

    Unit(int value){
        this.value = value;
    }
    Unit(AtomicInteger atomic){
        this.atomic = atomic;
    }

//    Unit(AtomicInteger atomic){
//        this.atomic = atomic;
//    }

    @Override
    public String toString(){
        return String.valueOf(value);
    }
}
