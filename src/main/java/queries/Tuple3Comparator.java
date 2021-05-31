package queries;

import scala.Tuple3;

import java.io.Serializable;
import java.util.Comparator;

public class Tuple3Comparator<tuple1, tuple2, tuple3> implements Comparator<Tuple3<tuple1, tuple2, tuple3>>, Serializable {

    private static final long serialVersionUID = 1L;
    private final Comparator<tuple1> tuple1;
    private final Comparator<tuple2> tuple2;
    private final Comparator<tuple3> tuple3;

    public Tuple3Comparator(Comparator<tuple1> tuple1, Comparator<tuple2> tuple2, Comparator<tuple3> tuple3) {
        this.tuple2 = tuple2;
        this.tuple1 = tuple1;
        this.tuple3 = tuple3;
    }

    @Override
    public int compare(Tuple3<tuple1, tuple2, tuple3> o1, Tuple3<tuple1, tuple2, tuple3> o2) {
        int res = this.tuple1.compare(o1._1(), o2._1());
        int res2 = this.tuple2.compare(o1._2(), o2._2());
        int res3 = this.tuple3.compare(o1._3(), o2._3());
        if (res == 0) {
            if (res2 == 0) {
                return res3;
            } else {
                return res2;
            }
        }
        return res;
    }
}
