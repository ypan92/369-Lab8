program = SeedNormKNN
inputFilePath = /datasets/seeds_dataset.txt
outputFilePath = ./seedNormOutput
jarFilepath = ./../jars/*

default:
	javac -cp "$(jarFilepath)" $(program).java
jar:
	jar cvf $(program).jar *.class
job:
	hadoop jar $(program).jar $(program) $(inputFilePath) $(outputFilePath)
remove:
	hadoop fs -rm -r $(outputFilePath)