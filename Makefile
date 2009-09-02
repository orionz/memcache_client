all: ebin
	(cd src;$(MAKE) all)

ebin:
	mkdir -p ebin

test:
	erl -pa ebin -eval "memcache_test:test(),halt()."

clean:
	(cd src;$(MAKE) clean)
