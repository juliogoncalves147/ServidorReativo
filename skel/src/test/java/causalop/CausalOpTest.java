package causalop;

import io.reactivex.rxjava3.core.Observable;
import org.junit.Assert;
import org.junit.Test;

public class CausalOpTest {
    
    @Test
    public void testOk() {
        var l = Observable.just(
                        new CausalMessage<String>("a", 1, 0, 1),
                        new CausalMessage<String>("b", 0, 1, 0),
                        new CausalMessage<String>("c", 1, 1, 2)
                )
                .lift(new CausalOperator<String>(2))
                .toList().blockingGet();

        Assert.assertArrayEquals(l.toArray(), new String[]{"a","b","c"});
    }

    @Test
    public void test3Ok() {
        var l = Observable.just(
                        new CausalMessage<String>("a", 2, 0, 0, 1),
                        new CausalMessage<String>("b", 0, 1, 0, 0),
                        new CausalMessage<String>("c", 1, 1, 1, 1)
                )
                .lift(new CausalOperator<String>(3))
                .toList().blockingGet();

        Assert.assertArrayEquals(l.toArray(), new String[]{"a","b","c"});
    }
    
    @Test
    public void testReorder() {
        var l = Observable.just(
                        new CausalMessage<String>("c", 1, 1, 2),
                        new CausalMessage<String>("a", 1, 0, 1),
                        new CausalMessage<String>("b", 0, 1, 0)
                )
                .lift(new CausalOperator<String>(2))
                .toList().blockingGet();
                
        Assert.assertArrayEquals(l.toArray(), new String[]{"a","b","c"});
    }

    @Test
    public void test3Reorder() {
        var l = Observable.just(
                        new CausalMessage<String>("d", 2, 1, 0, 2),
                        new CausalMessage<String>("b", 2, 0, 1, 1),
                        new CausalMessage<String>("f", 2, 2, 1, 3),
                        new CausalMessage<String>("a", 1, 0, 1, 0),
                        new CausalMessage<String>("e", 0, 2, 0, 2),
                        new CausalMessage<String>("c", 0, 1, 1, 0)
                )
                .lift(new CausalOperator<String>(3))
                .toList().blockingGet();
                
        Assert.assertArrayEquals(l.toArray(), new String[]{"a","b","c","d","e","f"});
    }
    
    @Test
    public void testDupl() {
        var l = Observable.just(
                        new CausalMessage<String>("a", 1, 0, 1),
                        new CausalMessage<String>("b", 0, 1, 0),
                        new CausalMessage<String>("a", 1, 0, 1),
                        new CausalMessage<String>("c", 1, 1, 2)
                )
                .lift(new CausalOperator<String>(2))
                .toList().blockingGet();
        Assert.assertArrayEquals(l.toArray(), new String[]{"a","b","c"});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGap() {
        var l = Observable.just(
                        new CausalMessage<String>("c", 1, 1, 2),
                        new CausalMessage<String>("a", 1, 0, 1)
                )
                .lift(new CausalOperator<String>(2))
                .toList().blockingGet();
    }
    
    @Test
    public void testAllOK(){
        var l = Observable.just(
        // para testar "adicionar mensagem à fila"
        new CausalMessage<String>("c", 1, 0 , 2 , 0 ),
        // para testar "mensagem que ja existe na fila"
        new CausalMessage<String>("$", 1, 0 , 2 , 0 ),
        // para testar "mensagem é enviada logo apos ser recebida"
        new CausalMessage<String>("a", 0, 1 , 0 , 0 ),
        // para testar "mensagem é enviada logo apos ser recebida"
        new CausalMessage<String>("b", 1, 0 , 1 , 0 ),
        // para testar "mensagem é enviada apos enviar mensagem da fila"
        new CausalMessage<String>("g", 0, 2 , 2 , 3 ),
        // para testar "mensagem é enviada apos enviar mensagem da fila"
        new CausalMessage<String>("f", 2, 1 , 2 , 3 ),
        // para testar "mensagem da fila é enviada"
        new CausalMessage<String>("e", 2, 1 , 1 , 2 ),
        // para testar "mensagem da fila ja devia ter sido entregue"
        new CausalMessage<String>("£", 2, 0 , 2 , 3 ),
        new CausalMessage<String>("d", 2, 1 , 1 , 1 ),
        // para testar "mensagem ja devia ter sido entregue"
        new CausalMessage<String>("§", 2, 0 , 0 , 1 )
        )
        .lift(new CausalOperator<String>(3))
        .toList().blockingGet();

        Assert.assertArrayEquals(l.toArray(), new String[]{"a","b","c","d","e","f","g"});

    }

    @Test(expected = IllegalArgumentException.class)
    public void testPorEnviar() {
        var l = Observable.just(
            new CausalMessage<String>("c", 1, 0 , 2 , 0 ),
            new CausalMessage<String>("$", 1, 0 , 2 , 0 ),
            new CausalMessage<String>("a", 0, 1 , 0 , 0 ),
            new CausalMessage<String>("b", 1, 0 , 1 , 0 ),
            new CausalMessage<String>("%", 2, 2 , 2 , 5 ),
            new CausalMessage<String>("g", 0, 2 , 2 , 3 ),
            new CausalMessage<String>("f", 2, 1 , 2 , 3 ),
            new CausalMessage<String>("e", 2, 1 , 1 , 2 ),
            new CausalMessage<String>("£", 2, 0 , 2 , 3 ),
            new CausalMessage<String>("d", 2, 1 , 1 , 1 )
            )
            .lift(new CausalOperator<String>(3))
            .toList().blockingGet();
    }
    

}