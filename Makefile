.PHONY: all test dependencies clean clean-all

ODDITY_URL = https://github.com/uwplse/oddity/releases/download/v0.38a/oddity.jar

FRAMEWORK_FILES = $(shell find framework -type f | sed 's/ /\\ /g')
LAB_FILES = $(shell find lab -type f | sed 's/ /\\ /g')
HANDOUT_FILES = $(shell find handout -type f | sed 's/ /\\ /g')

JAR_FILES = build/libs/framework.jar \
						build/libs/framework-compile.jar \
						build/libs/framework-sources.jar

OTHER_FILES = lombok.config deps/oddity.jar


ifeq ($(shell uname -s),Darwin)
	TAR = gtar
else
	TAR = tar
endif

ifeq ($(shell uname -s),Darwin)
	CP = gcp
else
	CP = cp
endif


all: build/handout/ dependencies

dependencies: deps/oddity.jar

$(JAR_FILES) build/doc/: $(FRAMEWORK_FILES)
	./gradlew assemble
	touch $@

deps/oddity.jar:
	mkdir -p deps
	wget -O $@ $(ODDITY_URL)

build/handout/: $(LAB_FILES) $(JAR_FILES) $(HANDOUT_FILES) $(OTHER_FILES)
	rm -rf $@
	mkdir $@
	$(CP) -r lab handout/. $(JAR_FILES) $(OTHER_FILES) $@

test:
	./gradlew test

clean:
	rm -rf build

clean-all: clean
	rm -rf deps .gradle
