package qslv.kstream.enhancement;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.springframework.context.annotation.Bean;

import com.fasterxml.jackson.databind.JavaType;

import qslv.common.kafka.JacksonAvroDeserializer;
import qslv.common.kafka.JacksonAvroSerializer;
import qslv.common.kafka.TraceableMessage;
import qslv.kstream.LoggedTransaction;

public class TestSetup {

	static public final String SCHEMA_REGISTRY = "http://localhost:8081";

	static public final String LOGGED_TRANSACTION_TOPIC = "test.logged.transaction";
	static public final String RESERVATION_BY_UUID_TOPIC = "test.transaction.by.uuid";

	private ConfigProperties configProperties = new ConfigProperties();

	private Serde<TraceableMessage<LoggedTransaction>> transactionSerde;
	private Serde<LoggedTransaction> reservationByUuidSerde;

	private ReservationFilterTopology reservationFilterTopology = new ReservationFilterTopology();

	private TopologyTestDriver testDriver;

	private TestInputTopic<String, TraceableMessage<LoggedTransaction>> transactionTopic;
	private TestOutputTopic<UUID, LoggedTransaction> reservationByUuidTopic;

	public TestSetup() throws Exception {
		configProperties.setAitid("12345");
		configProperties.setTransactionLogTopic(LOGGED_TRANSACTION_TOPIC);
		configProperties.setReservationByUuidTopic(RESERVATION_BY_UUID_TOPIC);
		reservationFilterTopology.setConfigProperties(configProperties);

		Map<String, String> config = new HashMap<>();
		config.put("schema.registry.url", SCHEMA_REGISTRY);
		transactionSerde = transactionSerde(config);
		reservationByUuidSerde = reservationByUuidSerde(config);

		reservationFilterTopology.setReservationByUuidSerde(reservationByUuidSerde);
		reservationFilterTopology.setTransactionSerde(transactionSerde);

		StreamsBuilder builder = new StreamsBuilder();
		reservationFilterTopology.kStream(builder);

		Topology topology = builder.build();

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "unit.reservation");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
		testDriver = new TopologyTestDriver(topology, props);

		transactionTopic = testDriver.createInputTopic(LOGGED_TRANSACTION_TOPIC, Serdes.String().serializer(),
				transactionSerde.serializer());
		reservationByUuidTopic = testDriver.createOutputTopic(RESERVATION_BY_UUID_TOPIC, Serdes.UUID().deserializer(),
				reservationByUuidSerde.deserializer());
	}
	
	@Bean
	Serde<LoggedTransaction> reservationByUuidSerde (Map<String, ?> config) throws Exception {
		JacksonAvroSerializer<LoggedTransaction> serializer = new JacksonAvroSerializer<>();
		JacksonAvroDeserializer<LoggedTransaction> deserializer = new JacksonAvroDeserializer<>();
		serializer.configure(config, false);
		deserializer.configure(config, false);
		return Serdes.serdeFrom(serializer, deserializer);
	}
	
	@Bean
	Serde<TraceableMessage<LoggedTransaction>> transactionSerde(Map<String, ?> config) throws Exception {
		JacksonAvroSerializer<TraceableMessage<LoggedTransaction>> serializer = new JacksonAvroSerializer<>();
		JacksonAvroDeserializer<TraceableMessage<LoggedTransaction>> deserializer = new JacksonAvroDeserializer<>();
		JavaType type = serializer.getTypeFactory().constructParametricType(TraceableMessage.class, LoggedTransaction.class);
		serializer.configure(config, false, type);
		deserializer.configure(config, false);
		return Serdes.serdeFrom(serializer, deserializer);
	}
	
	public TopologyTestDriver getTestDriver() {
		return testDriver;
	}

	public ReservationFilterTopology getEnhancementTopology() {
		return reservationFilterTopology;
	}

	public TestInputTopic<String, TraceableMessage<LoggedTransaction>> getTransactionTopic() {
		return transactionTopic;
	}

	public TestOutputTopic<UUID, LoggedTransaction> getReservationByUuidTopic() {
		return reservationByUuidTopic;
	}

}
