
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.*;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.io.Serializable;

public class FraudDetectionJob {

    public static class Transaction implements Serializable {
        public String customerId;
        public double amount;
        public long timestamp;

        public Transaction() {}

        public Transaction(String id, double amt, long time) {
            customerId = id;
            amount = amt;
            timestamp = time;
        }
    }

    public static class Detector
            extends KeyedProcessFunction<String, Transaction, String> {

        private transient ValueState<Transaction> previous;

        public void open(org.apache.flink.configuration.Configuration c) {
            previous = getRuntimeContext().getState(
                new ValueStateDescriptor<>("previous",
                    Types.POJO(Transaction.class)));
        }

        public void processElement(Transaction tx, Context ctx,
                                   Collector<String> out) throws Exception {

            if (tx.amount < 10000) return;

            Transaction p = previous.value();

            if (p != null) {
                long gap = tx.timestamp - p.timestamp;

                if (gap >= 0 && gap <= 60000)
                    out.collect("FRAUD ALERT: " + tx.customerId +
                        ", Previous=₹" + p.amount +
                        ", Current=₹" + tx.amount +
                        ", Gap=" + gap / 1000 + " seconds");
            }

            previous.update(tx);
        }
    }

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        long base = 1792404000000L;

        env.fromData(
            new Transaction("C101",15000,base+10000),
            new Transaction("C202",5000,base+20000),
            new Transaction("C101",20000,base+45000),
            new Transaction("C202",12000,base+50000),
            new Transaction("C101",3000,base+55000),
            new Transaction("C101",18000,base+90000),
            new Transaction("C202",14000,base+130000)
        )
        .keyBy(tx -> tx.customerId)
        .process(new Detector())
        .print();

        env.execute("Fraud Detection Job");
    }
}
