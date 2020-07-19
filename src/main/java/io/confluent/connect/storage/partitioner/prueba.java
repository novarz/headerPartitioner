package io.confluent.connect.storage.partitioner;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.Locale;

public class prueba {

    public static void main (String args[]){

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("'year='yyyy'/month='MM'/day='dd");
        System.out.println(LocalDate.now().format(formatter));




    }
}
