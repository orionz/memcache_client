all: ebin
	(cd src;$(MAKE) all)

ebin:
	mkdir -p ebin

clean:
	(cd src;$(MAKE) clean)
