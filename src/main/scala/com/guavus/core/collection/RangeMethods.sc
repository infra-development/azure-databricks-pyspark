val rangeIncl = Range.inclusive(1,10)
rangeIncl.filter(_ % 2 == 0)
rangeIncl.map(_ * 2)
rangeIncl.take(2)
rangeIncl.drop(5)
rangeIncl.takeWhile(x => x < 5)
rangeIncl.dropWhile(_ < 6)

val rangeExcl = Range(1,10)

rangeExcl.toList
rangeIncl.toArray
rangeIncl.toVector

1 to 10

1 until 10

val oddRange = 1 to 100 by 2
oddRange.head
oddRange.last

val rangeStep = Range(1,10,2)
rangeStep.toList

val reverseRange = 20 to 1 by -2
reverseRange.head
reverseRange.last

val negativeRange = -1 to -10 by -2
negativeRange.toList

val decimalRange  = BigDecimal(1.0) to BigDecimal(2.0) by 0.2
decimalRange.toList