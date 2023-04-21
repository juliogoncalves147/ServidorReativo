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
        // se o número de sequência da mensagem recebida ja foi entregue
        if(maxSeqNumbers[j] + 1 > vm[j])
            return -1;
        // se o número de sequência da mensagem recebida ainda não é o proximo a entregar
        if(maxSeqNumbers[j] + 1 < vm[j])
            return 1;
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

    private CausalMessage<T> checkQueue(){
        int qS = queue.size();
        Queue<CausalMessage<T>> fakeq;
        fakeq = new ArrayDeque<>(queue);
        for (int i = 0; i < qS; i++) {
            CausalMessage<T> m = fakeq.poll();
            int canDeliver = receiveVerify(m.v, m.j);
            if(canDeliver==0){
                queue.remove(m);
                return m;
            }else if (canDeliver==-1){
                System.out.println("Mensagem já devia ter sido entregue: " + m.payload);
                queue.remove(m);
            }
        }
        return null;
    }

    private String printV(int[] v){
        String sV = "";
        for (int i = 0; i < n; i++) {
            if(i!=0)
                sV += ",";
            sV += v[i];
        }
        sV += "";
        return sV;
    }

    @Override
    public @NonNull Observer<? super CausalMessage<T>> apply(@NonNull Observer<? super T> down) throws Throwable {
        return new DisposableObserver<CausalMessage<T>>() {

            @Override
            public void onNext(@NonNull CausalMessage<T> m) {
                String sV = printV(m.v);
                System.out.println("Recebi mensagem " + m.payload + " de " + m.j + " com v = " + sV);
                // verifica se a mensagem pode ser entregue
                int canDeliver = receiveVerify(m.v, m.j);
                
                // verifica se a mensagem já está na fila de espera
                if(queue.contains(m)){
                    System.out.println("Mensagem já está na fila de espera: " + m.payload);
                    return;
                }
                
                // mensagem pode ser entregue
                if(canDeliver==0){
                    // passa para baixo 
                    down.onNext(m.payload);
                    System.out.println("Mandei logo a seguir a receber: " + m.payload);
                    // atualiza o vetor de números de sequência
                    for (int i = 0; i < n; i++) {
                        maxSeqNumbers[i] = Math.max(maxSeqNumbers[i], m.v[i]);
                    }
                    sV = printV(maxSeqNumbers);
                    System.out.println("maxSeqNumbers = " + sV);
                    
                    // enquanto houver mensagens na fila de espera que possam ser entregues, entrega
                    while((m = checkQueue())!=null) {
                        System.out.println("Mandei da fila de espera: " + m.payload);
                        down.onNext(m.payload);
                        for (int i = 0; i < n; i++) {
                            maxSeqNumbers[i] = Math.max(maxSeqNumbers[i], m.v[i]);
                        }
                        sV = printV(maxSeqNumbers);
                        System.out.println("maxSeqNumbers = " + sV);
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
                down.onError(e);
            }

            @Override
            public void onComplete() {
                if(!queue.isEmpty())
                    onError(new IllegalArgumentException("Gap detected"));
                down.onComplete();
            }
        };
    }
}
