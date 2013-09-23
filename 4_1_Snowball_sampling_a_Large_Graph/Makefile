JFLAGS = -g
JC = javac
TESTFILE = abstract.tiny.test
.SUFFIXES: .java .class
.java.class:
	$(JC) $(JFLAGS) $*.java

CLASSES = \
        PangRankApprox.java \
        snowBall.java \
	snowBallNodes.java

default: classes

classes: $(CLASSES:.java=.class)

clean:
	$(RM) *.class

pagerank:
	java PangRankApprox A test.adj

snowball:
	java snowBallNodes DNA test.adj
