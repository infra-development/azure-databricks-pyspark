"hello scala".charAt(3)

"hello".compareTo("world")
"world".compareTo("world")

"Geeks".compareToIgnoreCase("geeks")

"Geeks".concat("forGeeks")

val a = new StringBuffer("Geeks")
"Geeks".contentEquals(a)

"Geeks".endsWith("s")

"Geeks".equals("Geeks")

"Geeks".equalsIgnoreCase("gEeks")

"ABCcba".getBytes()

"Geeks".indexOf('e')

"Geeksforgeeks".indexOf('g',5)

"Geeksforgeeeks".indexOf("ks")

"Geeks".lastIndexOf("ek")

"Geeks".length()

"potdotnothotokayslot".replaceAll(".ot","**")

"potdotnothotokayslot".replaceFirst(".ot","**")

"PfsoQmsoRcsoGfGkso".split(".so")

"Geeks".startsWith("ee", 1)

"Geeks".subSequence(1,4)

"Geeks".substring(3)

"Piyush".substring(1, 4)

"Ayushi".subSequence(1,4)

"GeeksforGeeks".toCharArray()

"GEekS".toLowerCase()

"Geeks".toUpperCase()

" \t  \nGeeks \n  ".trim() + "hello"

" \t  \nGeeks   \n ".strip() + "hello"

"Ayush".startsWith(" Ay")

"NidhifsoSinghmsoAcso".split(".so", 2)

"Ayushi".matches(".i.*")

"sanjay sharma".replace('s','$')

"Ayushi".hashCode()

"Ayushi".toCharArray

val S = "Hello World"

S.contains("e")

S.capitalize

S.reverse

S.isEmpty

S.nonEmpty

S.isBlank

val F = "%5s %3d %-3d %g"
F.format("cs206", 12, 3, math.Pi)

S.flatMap(x => Seq(x.toInt))

S.zipWithIndex

"""
  |SELECT *
  |FROM users
  |WHERE age > 18
  |""".stripMargin

"""
  $SELECT *
  $FROM users
  $WHERE age > 18
  $""".stripMargin('$')





