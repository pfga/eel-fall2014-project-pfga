sbt package
mkdir -p jar
cp target/scala-2.10/eel-fall2014-project_2.10-1.0.jar jar/
cp ~/.ivy2/cache/org.scala-lang/scala-library/jars/scala-library-2.10.4.jar jar

cd jar
jar xvf eel-fall2014-project_2.10-1.0.jar
rm eel-fall2014-project_2.10-1.0.jar
jar uvf scala-library-2.10.4.jar `ls -1 | grep -v scala-library-2.10.4.jar`
mv scala-library-2.10.4.jar ../eel-fall2014-project_2.10-1.0.jar
cd ..

rm -rf jar
