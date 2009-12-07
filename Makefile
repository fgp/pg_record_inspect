MODULES = record_inspect
DATA = record_inspect.sql
DOCS = README.record_inspect

OBJS = record_inspect.o

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
