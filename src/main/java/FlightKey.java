import java.io.Serializable;

public class FlightKey implements Serializable {
    double delay;
    int counter, late, canceled;

    FlightKey(String del, String cancel){
        if (cancel.equals("0.00")) { //если по нулям, то рейс не отменен
            canceled = 0;
            if (del.equals("")) {//если задержки не было, то 0
                delay = 0;
            } else {
                delay = Double.parseDouble(del); //если не пусто, то есть значение, забираем его
                if (delay != 0) { //если есть значение не 0, то опаздывал
                    late = 1;
                }
            }
        } else { //если 1, то отменен, значит делея нет
            canceled = 1;
            delay = 0;
        }
        counter = 1; //общее кол-во
    }

    FlightKey(double delay, int counter, int late, int canceled) {
        this.delay = delay;
        this.counter = counter;
        this.late = late;
        this.canceled = canceled;

    }
}
