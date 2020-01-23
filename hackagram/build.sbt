avroScalaCustomTypes in Compile := {
  avrohugger.format.Standard.defaultTypes.copy(
    enum = avrohugger.types.ScalaCaseObjectEnum
  )
}

sourceGenerators in Compile += (avroScalaGenerate in Compile).taskValue
