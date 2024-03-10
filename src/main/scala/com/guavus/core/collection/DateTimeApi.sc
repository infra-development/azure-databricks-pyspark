import java.time._
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoUnit, TemporalAdjusters}

val ld : LocalDate = LocalDate.now()
ld.format(DateTimeFormatter.ofPattern("yyyy/MM/dd"))
ld.`with`(TemporalAdjusters.firstDayOfYear())

val ldt = LocalDateTime.now()
ldt.minus(1, ChronoUnit.DAYS)
ldt.plus(1, ChronoUnit.HOURS)

val christmas = MonthDay.of(Month.DECEMBER, 25)
christmas.format(DateTimeFormatter.ofPattern("MM/dd"))

