AMQP_CLIENT_VERSION := 4.1.0
AMQP_CLIENT := third_party/amqp-client/amqp-client-$(AMQP_CLIENT_VERSION).jar
AMQP_CLIENT_BASE_URL := http://central.maven.org/maven2/com/rabbitmq/amqp-client/4.1.0

$(AMQP_CLIENT): $(AMQP_CLIENT).md5
	set dummy "$(AMQP_CLIENT_BASE_URL)" "$(AMQP_CLIENT)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(AMQP_CLIENT)