package causalop;

import java.util.ArrayDeque;
import java.util.Queue;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.ObservableOperator;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.observers.DisposableObserver;

public class CausalOperator<T> implements ObservableOperator<T, CausalMessage<T>> {
    private final int n;
    private int[] maxSeqNumbers;
    private Queue<CausalMessage<T>> queue;

    public CausalOperator(int n) {
        this.n = n;
        this.maxSeqNumbers = new int[n];
        this.queue = new ArrayDeque<>();

        for (int i = 0; i < n; i++) {
            maxSeqNumbers[i] = 0;
        }
    }

    @Override
    public @NonNull Observer<? super CausalMessage<T>> apply(@NonNull Observer<? super T> down) throws Throwable {
        return new DisposableObserver<CausalMessage<T>>() {

            @Override
            public void onNext(@NonNull CausalMessage<T> m) {
                System.out.println("Recebi mensagem " + m.payload + " de " + m.j + " com v = " + m.v[0] + ", " + m.v[1]);
                // verifica se a mensagem pode ser entregue
                boolean canDeliver = true;
                for (int i = 0; i < n; i++) {
                    if (i != m.j && m.v[i] > maxSeqNumbers[i]) {
                        canDeliver = false;
                        break;
                    }
                }
            
                if (canDeliver) {
                    // mensagem pode ser entregue, passa para baixo e atualiza o vetor de números de sequência
                    down.onNext(m.payload);
                    System.out.println("Mandei logo a seguir a receber: " + m.payload);
                    for (int i = 0; i < n; i++) {
                    //    System.out.println("Antes - maxSeqNumbers[" + i + "] = " + maxSeqNumbers[i]);
                        maxSeqNumbers[i] = Math.max(maxSeqNumbers[i], m.v[i]);
                    //    System.out.println("Depois - maxSeqNumbers[" + i + "] = " + maxSeqNumbers[i]);
                    }
                    System.out.println("maxSeqNumbers = " + maxSeqNumbers[0] + ", " + maxSeqNumbers[1]);
                    
                    // verifica se há mensagens na fila de espera que podem ser entregues agora
                    while (!queue.isEmpty()) {
                        CausalMessage<T> queuedMsg = queue.peek();
                        boolean canDeliverQueuedMsg = true;
                        
                        for (int i = 0; i < n; i++) {
                            if (queuedMsg.v[i] > maxSeqNumbers[i]) {
                                canDeliverQueuedMsg = false;
                                System.out.println("Não mandei da Fila de Espera: " + queue.peek().payload);
                                System.out.println("queuedMsg.v[" + i + "] = " + queuedMsg.v[i] + " > maxSeqNumbers[" + i + "]= " + (maxSeqNumbers[i]));
                                break;
                            }
                        }
                    
                        if (canDeliverQueuedMsg) {
                            // mensagem na fila pode ser entregue, passa para baixo e atualiza o vetor de números de sequência
                            System.out.println("Mandei da Fila de Espera: " + queue.peek().payload);
                            down.onNext(queue.poll().payload);
                            for (int i = 0; i < n; i++) {
                                maxSeqNumbers[i] = Math.max(maxSeqNumbers[i], queuedMsg.v[i]);
                            }
                            System.out.println("maxSeqNumbers = " + maxSeqNumbers[0] + ", " + maxSeqNumbers[1]);
                        } else {
                            // mensagem na fila não pode ser entregue agora, pare de verificar a fila
                            break;
                        }
                    }
                } else {
                    // mensagem não pode ser entregue agora, coloca na fila de espera
                    queue.offer(m);
                }
            }

            @Override
            public void onError(@NonNull Throwable e) {
                down.onError(e); // FIXME
            }

            @Override
            public void onComplete() {
                down.onComplete(); // FIXME
            }
        };
    }
}
