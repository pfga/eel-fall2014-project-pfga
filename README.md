eel-fall2014-project-pfga
=========================

To generate scoverage reports 

	sbt clean scoverage:test

Documentation: https://github.com/pfga/eel-fall2014-project-pfga-paper

How to run:
-----------

Upload the input files on S3

According to where the input files are uploaded and where the output should be produced make appropriate changes in the following files:
	src/main/resources/parse-config-100k.properties
	src/main/resources/parse-config-10k.properties
	src/main/resources/parse-config-1k.properties. 

To generate the jar
	sbt clean package

The jar will be generated at target/scala-2.10/eel-fall2014-project_2.10-1.0.jar, sbt will download the scala-library jar at ~/.ivy2/cache/org.scala-lang/scala-library/jars/scala-library-2.10.4.jar

Depending on where the file are to be uploaded, make appropriate changes in add_scala_jar.sh.
Upload generated jar, scala-library, and add_scala_jar.sh on S3

While creating the EMR cluster, choose Hadoop 1.0.3 version.
Add add_scala_jar.sh(using its S3 location, where it was uploaded) as a bootstrappig step, and create steps according to the respective config file; example, arguments for custom jar can be
	Main.RunAlgo parse-config-100k.properties
	Main.RunAlgo parse-config-10k.properties
	Main.RunAlgo parse-config-1k.properties

Detailed Output:
----------------

The following file is the input file, which is the precipitation data of Gainesville:
https://s3-us-west-2.amazonaws.com/eel-fall-2014/pfga/dataparseip/410119.csv

The following file is the aggregated values of events above over each day.
https://s3-us-west-2.amazonaws.com/eel-fall-2014/pfga/dataparseop-1k/ftsIpPath/TMP

100K indicates a population of 100K individuals, the first generation is the randomly generated individuals. While subsequent generation are propagated, mutated, underwent crossover and selected from the previous generation.
Each generation has the fittest individual which represents that generation, and the top 100 individuals from previous generations are used to crossover with the remaining population and always propagated into the next generation.

In the following files, the first line represents the MSEs(Mean Squared Errors) of the top 100 individuals, the remaining lines is the representation of the fittest individual(least MSE), by listing what was predicted by that individual.
The remaining lines are in the form "<time_slot>,<actual_events>,<forecasted_events>"

https://s3-us-west-2.amazonaws.com/eel-fall-2014/pfga/dataparseop-100k/ga_op1/BESTIND-r-00000
https://s3-us-west-2.amazonaws.com/eel-fall-2014/pfga/dataparseop-100k/ga_op66/BESTIND-r-00000

https://s3-us-west-2.amazonaws.com/eel-fall-2014/pfga/dataparseop-10k/ga_op1/BESTIND-r-00000
https://s3-us-west-2.amazonaws.com/eel-fall-2014/pfga/dataparseop-10k/ga_op74/BESTIND-r-00000

https://s3-us-west-2.amazonaws.com/eel-fall-2014/pfga/dataparseop-1k/ga_op1/BESTIND-r-00000
https://s3-us-west-2.amazonaws.com/eel-fall-2014/pfga/dataparseop-1k/ga_op100/BESTIND-r-00000 
