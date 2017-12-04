package fxo.utils

import java.time.{ZoneId,Instant,format}


object Timeutils {
  def datePath ( timeInstant:  Instant) : (String,String) = {
    val pathFormatter = format.DateTimeFormatter.ofPattern("yyyy/MM/dd/H/m/")
    val nameFormatter = format.DateTimeFormatter.ofPattern("s-A")  //seconds-millis
    val utcInstant = timeInstant.atZone(ZoneId.of("UTC"))
    (utcInstant.format(pathFormatter),utcInstant.format(nameFormatter))
  }

}
