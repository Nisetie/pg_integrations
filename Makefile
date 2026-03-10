EXTENSION=pg_integrations
DATA=pg_integrations--1.0.sql pg_integrations--1.0--1.1.sql
PG_CONFIG=pg_config
PGXS:=$(shell $(PG_CONFIG) --pgxs)
include $(PGXS)