SPYMEMCACHED_VERSION := 2.12.3
                              SPYMEMCACHED := third_party/spymemcached/spymemcached-$(SPYMEMCACHED_VERSION).jar
                              SPYMEMCACHED_BASE_URL := http://central.maven.org/maven2/net/spy/spymemcached/$(SPYMEMCACHED_VERSION)

                              $(SPYMEMCACHED): $(SPYMEMCACHED).md5
                              	set dummy "$(SPYMEMCACHED_BASE_URL)" "$(SPYMEMCACHED)"; shift; $(FETCH_DEPENDENCY)

                              THIRD_PARTY += $(SPYMEMCACHED)