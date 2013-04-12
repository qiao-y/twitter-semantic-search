all: Emailer.class Tweet.class TwitterCrawler.class
	rm -rf build
	mkdir build
	jar cf build/TwitterCrawler.jar src/

Emailer.class: src/edu/columbia/watson/twitter/Emailer.java
	javac src/edu/columbia/watson/twitter/Emailer.java -cp "lib/*" 

Tweet.class: src/edu/columbia/watson/twitter/Tweet.java
	javac src/edu/columbia/watson/twitter/Tweet.java -cp "lib/*"

TwitterCrawler.class: src/edu/columbia/watson/twitter/TwitterCrawler.java
	javac src/edu/columbia/watson/twitter/TwitterCrawler.java -cp "lib/*:src"

.PHONY:
clean:
	rm -f src/edu/columbia/watson/twitter/*.class build/TwitterCrawler.jar

