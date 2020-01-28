avroScalaCustomTypes in Compile := {
  avrohugger.format.Standard.defaultTypes.copy(
    enum = avrohugger.types.ScalaCaseObjectEnum
  )
}

sourceGenerators in Compile += (avroScalaGenerate in Compile).taskValue

libraryDependencies ++= Seq(
  "com.github.kenbot" %%  "goggles-dsl"     % "1.0",
  "com.github.kenbot" %%  "goggles-macros"  % "1.0"
)
