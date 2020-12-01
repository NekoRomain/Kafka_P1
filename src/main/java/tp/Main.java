package tp;


import tp.producter_client.ProducerUn;

import java.util.Timer;


public class Main {

    public Main() throws InterruptedException {

        ProducerUn p = new ProducerUn();

        Timer timer = new Timer();
        //TODO Mettre à 1 800 000 pour 30 min
        timer.schedule(p, 0, 1000*60*30);

        while (true) {

        }
    }

    public static void main(String[] args) throws InterruptedException {
        Main main = new Main();
    }
}