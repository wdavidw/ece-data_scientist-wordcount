Grab twitter data from ECE

mkdir ece
wget --recursive --mirror -np -A.html  -Q100m -P ./ece http://www.ece.fr 

Upload them to HDFS

hadoop fs -put ./ece .

Install Eclipse for Java EE  and Maven
http://www.eclipse.org
http://maven.apache.org
 