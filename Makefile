MODULES = record_inspect
DATA = record_inspect.sql
DOCS = README.record_inspect
REGRESS = record_inspect
REGRESS_OPTS = --load-lang=plpgsql

OBJS = record_inspect.o

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
