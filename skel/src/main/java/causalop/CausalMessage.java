package causalop;

import java.util.Arrays;

public class CausalMessage<T> {
    int j, v[];
    public T payload;

    public CausalMessage(T payload, int j, int... v) {
        this.payload = payload;
        this.j = j;
        this.v = v;
    }

    public boolean equals(Object o){
        if (o instanceof CausalMessage){
            CausalMessage<T> m = (CausalMessage<T>) o;
            return this.j==m.j && Arrays.equals(this.v, m.v);
        }
        else{
            return false;
        }
    }
}
