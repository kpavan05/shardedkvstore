################################################################################
# Java compiler settings
################################################################################
JC = javac -source 1.8 -g


################################################################################
# Folders, files
################################################################################
OUT = out

JAVA_SRC = $(shell find lab/src -type f -name "*.java")
JAVA_TST = $(shell find lab/tst -type f -name "*.java")


################################################################################
# Targets
################################################################################
.PHONY: all

all: $(OUT)/src/ $(OUT)/tst/


$(OUT)/src/: $(JAVA_SRC)
	@ echo "[javac] $@"
	@ mkdir -p $@
	@ $(JC) -d $@ -cp framework-compile.jar $^

$(OUT)/tst/: $(JAVA_TST) $(OUT)/src
	@ echo "[javac] $@"
	@ mkdir -p $@
	@ $(JC) -d $@ -cp framework.jar:$(OUT)/src $(JAVA_TST)

