
lazy val blueprints = project.in( file("blueprints") )

lazy val gremlin = project.in( file("gremlin") ).dependsOn(blueprints)
