JFLAGS = -d ./bin -cp "./lib/*"
JC = javac
.SUFFIXES: .java .class
.java.class:
	$(JC) $(JFLAGS) $*.java

CLASSES = \
					src/edu/columbia/watson/twitter/lsa/util/SequenceFileMaker.java \
					src/edu/columbia/watson/twitter/lsa/util/MySQLVecPersister.java \
					src/edu/columbia/watson/twitter/lsa/util/MySQLVecReader.java \
					src/edu/columbia/watson/twitter/lsa/util/DiagonalMatrixInv.java

default: classes

classes: $(CLASSES:.java=.class)

clean:
	$(RM) -rf ./bin/*
