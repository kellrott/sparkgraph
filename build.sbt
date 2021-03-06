
lazy val blueprints = project.in( file("blueprints") )

lazy val gremlin = project.in( file("gremlin") ).dependsOn(blueprints % "test->test;compile->compile")

lazy val tools = project.in( file("tools") ).dependsOn(blueprints, gremlin)
