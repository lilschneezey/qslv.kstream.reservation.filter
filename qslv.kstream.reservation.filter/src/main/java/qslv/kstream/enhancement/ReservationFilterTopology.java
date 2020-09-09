package qslv.kstream.enhancement;

import java.util.UUID;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import qslv.common.kafka.TraceableMessage;
import qslv.kstream.LoggedTransaction;

@Component
public class ReservationFilterTopology {
	private static final Logger log = LoggerFactory.getLogger(ReservationFilterTopology.class);

	@Autowired
	ConfigProperties configProperties;
	@Autowired
	Serde<TraceableMessage<LoggedTransaction>> transactionSerde;
	@Autowired
	Serde<LoggedTransaction> reservationByUuidSerde;

	public void setConfigProperties(ConfigProperties configProperties) {
		this.configProperties = configProperties;
	}
	public void setTransactionSerde(Serde<TraceableMessage<LoggedTransaction>> transactionSerde) {
		this.transactionSerde = transactionSerde;
	}

	public void setReservationByUuidSerde(Serde<LoggedTransaction> reservationByUuidSerde) {
		this.reservationByUuidSerde = reservationByUuidSerde;
	}
	@Bean
	public KStream<?, ?> kStream(StreamsBuilder builder) {
		log.debug("kStream ENTRY");
		KStream<String, TraceableMessage<LoggedTransaction>> rawRequests = builder
				.stream(configProperties.getTransactionLogTopic(),Consumed.with(Serdes.String(), transactionSerde));
		
		rawRequests.filter((k, v) -> { 
			String type = v.getPayload().getTransactionTypeCode();
			return ( type.equals(LoggedTransaction.RESERVATION) 
				|| type.equals(LoggedTransaction.RESERVATION_CANCEL) 
				|| type.equals(LoggedTransaction.RESERVATION_COMMIT) ); 
		} )
			.map((k,v) -> rekey(k,v))
			.to(configProperties.getReservationByUuidTopic(), Produced.with(Serdes.UUID(), reservationByUuidSerde));
		log.debug("kStream EXIT");
		return rawRequests;
	}

	KeyValue<UUID, LoggedTransaction> rekey( String key, TraceableMessage<LoggedTransaction> traceable ) {
		String type = traceable.getPayload().getTransactionTypeCode();
		UUID newKey = null;
		if ( type.equals(LoggedTransaction.RESERVATION_CANCEL) ) {
			newKey = traceable.getPayload().getReservationUuid();
		} else if ( type.equals(LoggedTransaction.RESERVATION_COMMIT) ) {
			newKey = traceable.getPayload().getReservationUuid();
		} else {
			newKey = traceable.getPayload().getTransactionUuid();
		}
		return new KeyValue<>(newKey, traceable.getPayload());
	}
}
