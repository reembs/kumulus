.FORCE:

deploy: .FORCE
	@mvn -DskipTests clean package dokka:javadocJar deploy
