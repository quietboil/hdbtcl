include local.mk

lib_search_path = $(patsubst SEARCH_DIR("=%");,%,$(shell ld --verbose|grep SEARCH_DIR))
tcl_config_list = $(foreach dir,$(lib_search_path),$(wildcard $(dir)/tclConfig.sh))
tcl_config := $(firstword $(tcl_config_list))
ifeq ($(tcl_config),)
$(error tclConfig.sh was not found on this system)
endif
include $(tcl_config)

CC      := $(TCL_CC:'%'=%)
CFLAGS  := $(TCL_SHLIB_CFLAGS:'%=%)
CFLAGS  := $(CFLAGS:%'=%)
LDFLAGS := $(TCL_STUB_LIB_SPEC:'%=%)
LDFLAGS := $(LDFLAGS:%'=%)
SO      := $(TCL_SHLIB_SUFFIX:'%'=%)

CFLAGS  := -std=c99 -O2 -I $(DBCAPI_INCLUDE_DIR) -D USE_TCL_STUBS $(CFLAGS) -Wall

all: hdbtcl$(SO)

hdbtcl$(SO): ../hdbtcl.c
	$(CC) -o $@ -shared $(CFLAGS) $^ $(LDFLAGS)

pkgIndex.tcl: ../pkgIndex.tcl
	cp $^ $@

clean:
	rm -f hdbtcl$(SO) pkgIndex.tcl

test: hdbtcl$(SO) pkgIndex.tcl
	@tclsh ../test.tcl $(HDBTCLTESTNODE) $(HDBTCLTESTUSER) $(HDBTCLTESTPASS) -colorize

