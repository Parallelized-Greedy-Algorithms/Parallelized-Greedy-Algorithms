package boruvkas_algorithm.Parallel;

// occupies 1 cache line
public class Unit{
    public volatile int value = 0;
    private long p1, p2, p3, p4, p5, p6;
    private int p7;

    Unit(){}

    Unit(int value){
        this.value = value;
    }

    @Override
    public String toString(){
        return String.valueOf(value);
    }
}
