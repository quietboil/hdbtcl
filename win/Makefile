include local.mk
include $(TCL)/lib/tclConfig.sh

CC      = $(MINGW)/bin/gcc
CFLAGS  = -std=c99 -O2 -I $(DBCAPI_INCLUDE_DIR) -I $(TCL)/include -D USE_TCL_STUBS -Wl,--subsystem,windows -Wall
LDFLAGS = -L $(TCL)/lib $(TCL_STUB_LIB_FLAG:'%'=%)

all: hdbtcl.dll

hdbtcl.dll: ../hdbtcl.c
	$(CC) -o $@ -shared $(CFLAGS) $^ $(LDFLAGS)

pkgIndex.tcl: ../pkgIndex.tcl
	$(CP) $^ $@

clean:
	$(RM) hdbtcl.dll pkgIndex.tcl

test: hdbtcl.dll pkgIndex.tcl
	@$(TCL)/bin/tclsh ../test.tcl $(HDBTCLTESTNODE) $(HDBTCLTESTUSER) $(HDBTCLTESTPASS) -colorize
