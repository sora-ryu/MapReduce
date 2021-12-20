#!/bin/sh

cd ../test_cases/mapreduce_multi
echo "--- Testing with the multiple processes ---"
echo "Experimenting the WordCount application."
javac WordCount.java
java WordCount

echo "Experimenting the CountSameLengthWords application."
javac CountSameLengthWords.java
java CountSameLengthWords

echo "Experimenting the Vowels application."
javac Vowels.java
java Vowels

echo "End of testing all applications. Please check application folders to check the output files.."
