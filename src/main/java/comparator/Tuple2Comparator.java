package comparator;

import scala.Serializable;
import scala.Tuple2;
import java.util.Comparator;

public class Tuple2Comparator<tuple1,tuple2> implements Comparator<Tuple2<tuple1, tuple2>>, Serializable {

    private static final long serialVersionUID = 1L;
    private final Comparator<tuple1> tuple1;
    private final Comparator<tuple2> tuple2;

    public Tuple2Comparator(Comparator<tuple1> tuple1, Comparator<tuple2> tuple2){
        this.tuple2 = tuple2;
        this.tuple1 = tuple1;
    }

    @Override
    public int compare(Tuple2<tuple1, tuple2> o1, Tuple2<tuple1, tuple2> o2) {
        if (tuple1.compare(o1._1, o2._1) == 0){
            return this.tuple2.compare(o1._2, o2._2);
        } else{
            return this.tuple1.compare(o1._1, o2._1);
        }
    }
}
