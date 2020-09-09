package qslv.kstream.enhancement;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDateTime;
import java.util.UUID;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import qslv.common.kafka.TraceableMessage;
import qslv.kstream.LoggedTransaction;
import qslv.util.Random;

@ExtendWith(MockitoExtension.class)
class Unit_FilterTopology {

	public final static String AIT = "237482"; 
	public final static String TEST_TAXONOMY_ID = "9.9.9.9.9";
	public final static String CORRELATION_ID = UUID.randomUUID().toString();
	public final static String VALID_STATUS = "EF";
	public final static String INVALID_STATUS = "CL";
	public final static String JSON_DATA = "{\"value\": 234934}";

	static TestSetup context;
	
	@BeforeAll
	static void beforeAl() throws Exception {
		context = new TestSetup();
	}
	
	@AfterAll
	static void afterAll() {
		context.getTestDriver().close();
	}
	
	private void drain(TestOutputTopic<?, ?> topic) {
		while ( topic.getQueueSize() > 0) {
			topic.readKeyValue();
		}
	}

	@Test
	void test_normal_filter() {
		test_filter(UUID.randomUUID(), LoggedTransaction.NORMAL, true);
	}
	@Test
	void test_rejected_filter() {
		test_filter(UUID.randomUUID(), LoggedTransaction.REJECTED_TRANSACTION, true);
	}
	@Test
	void test_reservation_filter() {
		test_filter(UUID.randomUUID(), LoggedTransaction.RESERVATION, false);
	}
	@Test
	void test_reservation_cancel_filter() {
		test_filter(UUID.randomUUID(), LoggedTransaction.RESERVATION_CANCEL, false);
	}
	@Test
	void test_reservation_commit_filter() {
		test_filter(UUID.randomUUID(), LoggedTransaction.RESERVATION_COMMIT, false);
	}
	@Test
	void test_transfer_from_filter() {
		test_filter(UUID.randomUUID(), LoggedTransaction.TRANSFER_FROM, true);
	}
	@Test
	void test_transfer_to_filter() {
		test_filter(UUID.randomUUID(), LoggedTransaction.TRANSFER_TO, true);
	}
	
	void test_filter(UUID key, String type, boolean filteredOut) {
    	drain(context.getReservationByUuidTopic());

    	// Setup 
    	TraceableMessage<LoggedTransaction> traceable = setupTraceableMessage(setupLoggedTransaction(key, type));
    	
    	// Execute
    	context.getTransactionTopic().pipeInput(traceable.getPayload().getAccountNumber(), traceable);

    	if ( filteredOut ) {
    		assertTrue( context.getReservationByUuidTopic().isEmpty());
    	} else {
	    	KeyValue<UUID, LoggedTransaction> keyvalue = context.getReservationByUuidTopic().readKeyValue();
	    	
	    	// -- verify -----
	    	assertEquals(key, keyvalue.key);
	    	assertNotNull(keyvalue.value);
	    	verifyTransactions( traceable.getPayload(), keyvalue.value);
    	}
	}

	<T> TraceableMessage<T> setupTraceableMessage(T payload) {
		TraceableMessage<T> message = new TraceableMessage<>();
		message.setBusinessTaxonomyId(TEST_TAXONOMY_ID);
		message.setCorrelationId(CORRELATION_ID);
		message.setMessageCreationTime(LocalDateTime.now());
		message.setPayload(payload);
		message.setProducerAit(AIT);
		return message;
	}
	
	LoggedTransaction setupLoggedTransaction( UUID key, String type ) {
		LoggedTransaction transaction = new LoggedTransaction();
		if (type.equals(LoggedTransaction.RESERVATION_COMMIT) || type.equals(LoggedTransaction.RESERVATION_CANCEL)) {
			transaction.setReservationUuid(key);
			transaction.setTransactionUuid(UUID.randomUUID());			
		} else {
			transaction.setReservationUuid(null);
			transaction.setTransactionUuid(key);
		}
		transaction.setAccountNumber(Random.randomDigits(12));
		transaction.setDebitCardNumber(Random.randomDigits(16));
		transaction.setRequestUuid(UUID.randomUUID());
		transaction.setRunningBalanceAmount(Random.randomLong());
		transaction.setTransactionAmount(0L);
		transaction.setTransactionMetaDataJson(JSON_DATA);
		transaction.setTransactionTime(LocalDateTime.now());
		transaction.setTransactionTypeCode(type);
		return transaction;
	}

	private void verifyTransactions(LoggedTransaction expected, LoggedTransaction actual) {
		assertEquals( expected.getAccountNumber(), actual.getAccountNumber());
		assertEquals( expected.getDebitCardNumber(), actual.getDebitCardNumber());
		assertEquals( expected.getRequestUuid(), actual.getRequestUuid());
		assertEquals( expected.getReservationUuid(), actual.getReservationUuid());
		assertEquals( expected.getRunningBalanceAmount(), actual.getRunningBalanceAmount());
		assertEquals( expected.getTransactionAmount(), actual.getTransactionAmount());
		assertEquals( expected.getTransactionMetaDataJson(), actual.getTransactionMetaDataJson());
		assertEquals( expected.getTransactionTime(), actual.getTransactionTime());
		assertEquals( expected.getTransactionTypeCode(), actual.getTransactionTypeCode());
		assertEquals( expected.getTransactionUuid(), actual.getTransactionUuid());
	}

}
