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

    // -1 se a mensagem já devia ter sido entregue
    // 0 se a mensagem pode ser entregue
    // 1 se a mensagem ainda não pode ser entregue
    private int receiveVerify(int[] vm,int j){
        int canDeliver = 0;
        // se o número de sequência da mensagem recebida é o próximo a ser entregue
        if(maxSeqNumbers[j] + 1 > vm[j])
            return -1;
        // verifica se todas as mensagens que era esperado ter recebido de outros processos já foram recebidas
        for (int i = 0; i < n; i++) {
            if (i==j)
                continue;
            if (vm[i] > maxSeqNumbers[i]) {
                return 1;
            }
        }
        return canDeliver;
    }


    @Override
    public @NonNull Observer<? super CausalMessage<T>> apply(@NonNull Observer<? super T> down) throws Throwable {
        return new DisposableObserver<CausalMessage<T>>() {

            @Override
            public void onNext(@NonNull CausalMessage<T> m) {
                System.out.println("Recebi mensagem " + m.payload + " de " + m.j + " com v = " + m.v[0] + ", " + m.v[1]);
                // verifica se a mensagem pode ser entregue
                int canDeliver = receiveVerify(m.v, m.j);
            
                if (canDeliver==0) {
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
                    int qS = queue.size();
                    for (int i = 0; i < qS; i++) {
                        // pega na 1a mensagem na fila de espera
                        CausalMessage<T> queuedMsg = queue.poll();
                        int canDeliverQueuedMsg = receiveVerify(queuedMsg.v, queuedMsg.j);
                        
                        if (canDeliverQueuedMsg==1) {
                            System.out.println("Não mandei da Fila de Espera: " + queuedMsg.payload);
                        }
                    
                        if (canDeliverQueuedMsg==0) {
                            // mensagem na fila pode ser entregue, passa para baixo e atualiza o vetor de números de sequência
                            System.out.println("Mandei da Fila de Espera: " + queuedMsg.payload);
                            down.onNext(queuedMsg.payload);
                            for(int j = 0; j < n; j++) {
                                maxSeqNumbers[j] = Math.max(maxSeqNumbers[j], queuedMsg.v[j]);
                            }
                            System.out.println("maxSeqNumbers = " + maxSeqNumbers[0] + ", " + maxSeqNumbers[1]);
                        } else {
                            // mensagem na fila não pode ser entregue agora, pare de verificar a fila
                            // coloca no fim da fila
                            queue.offer(queuedMsg);
                            break;
                        }
                    }
                } else {
                    // mensagem já devia ter sido entregue
                    if (canDeliver==-1) {
                        System.out.println("Mensagem já devia ter sido entregue: " + m.payload);
                    }else{
                        // mensagem não pode ser entregue agora, coloca na fila de espera
                        queue.offer(m);
                    }
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
