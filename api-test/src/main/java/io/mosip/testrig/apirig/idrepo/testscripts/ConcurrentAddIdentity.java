package io.mosip.testrig.apirig.idrepo.testscripts;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.core.MediaType;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.testng.ITest;
import org.testng.ITestContext;
import org.testng.Reporter;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import io.mosip.testrig.apirig.dto.TestCaseDTO;
import io.mosip.testrig.apirig.idrepo.utils.IdRepoArrayHandle;
import io.mosip.testrig.apirig.idrepo.utils.IdRepoConfigManager;
import io.mosip.testrig.apirig.idrepo.utils.IdRepoUtil;
import io.mosip.testrig.apirig.testrunner.HealthChecker;
import io.mosip.testrig.apirig.testrunner.JsonPrecondtion;
import io.mosip.testrig.apirig.testrunner.BaseTestCase;
import io.mosip.testrig.apirig.utils.AdminTestException;
import io.mosip.testrig.apirig.utils.AdminTestUtil;
import io.mosip.testrig.apirig.utils.GlobalConstants;
import io.mosip.testrig.apirig.utils.KernelAuthentication;
import io.mosip.testrig.apirig.utils.RestClient;
import io.mosip.testrig.apirig.utils.SchemaBasedIdentityTemplateBuilder;
import io.mosip.testrig.apirig.utils.SkipTestCaseHandler;
import io.restassured.response.Response;

/**
 * Bug-repro script for: "anonymous_profile data is null in IDRepo DB when multiple packets get
 * processed at the same time".
 *
 * <p>{@code AnonymousProfileHelper} on the service side is a Spring singleton bean whose per-request
 * state (oldUinData/newUinData/regId/...) is held in plain instance fields, not request-scoped or
 * thread-local. When two AddIdentity requests are processed concurrently, one request's data can be
 * overwritten by another before the async {@code buildAndsaveProfile} call reads it, producing a
 * null/incomplete {@code idrepo.anonymous_profile} row.
 *
 * <p>This test fires bursts of independently-valid AddIdentity requests (unique UIN/RID/email each)
 * at the same instant, in increasing thread counts within a single run: 2, 4, 8, 16, 32, 64, 100.
 * Each request is logged with its UIN/RID/email so the resulting DB rows can be inspected afterwards.
 * All other IdRepo suite test cases are commented out in IdrepositorySuite.xml on this branch so the
 * run is dedicated to this load.
 */
public class ConcurrentAddIdentity extends IdRepoUtil implements ITest {

	private static final Logger logger = Logger.getLogger(ConcurrentAddIdentity.class);

	// Doubling thread counts up to the requested ceiling of 100 concurrent requests, all in one run.
	private static final int[] THREAD_COUNTS = { 2, 4, 8, 16, 32, 64, 100 };

	// Gives each round's async anonymous-profile writes on the server time to drain before the next
	// round starts, so a null/incomplete row can be attributed to a specific thread count.
	private static final long INTER_ROUND_PAUSE_MS = 5000;

	protected String testCaseName = "IdRepo_ConcurrentAddIdentity_LoadTest";

	@Override
	public String getTestName() {
		return testCaseName;
	}

	@BeforeClass
	public static void setLogLevel() {
		if (IdRepoConfigManager.IsDebugEnabled())
			logger.setLevel(Level.ALL);
		else
			logger.setLevel(Level.INFO);
	}

	@Test
	public void test(ITestContext context) throws Exception {
		String ymlFile = context.getCurrentXmlTest().getLocalParameters().get("ymlFile");
		Object[] testCaseArr = getYmlTestData(ymlFile);
		if (testCaseArr.length == 0) {
			throw new SkipException("No test case found in " + ymlFile);
		}
		TestCaseDTO baseTestCaseDTO = (TestCaseDTO) testCaseArr[0];
		testCaseName = baseTestCaseDTO.getTestCaseName();
		// Normally set per-row by IdRepoUtil.isTestCaseValidForExecution, which this test bypasses (its
		// schema-skip/dependency-tracking logic is for the row-per-testcase model). AdminTestUtil methods
		// like replaceKeywordWithValue -> RemoveFromTheConsumersMap dereference this static field
		// unconditionally, so it must be set. Since testCaseName never changes for the rest of this run,
		// setting it once here (single-threaded, before any round starts) is safe for all worker threads.
		AdminTestUtil.currentTestCaseName = testCaseName;

		if (HealthChecker.signalTerminateExecution) {
			throw new SkipException(
					GlobalConstants.TARGET_ENV_HEALTH_CHECK_FAILED + HealthChecker.healthCheckFailureMapS);
		}
		if (SkipTestCaseHandler.isTestCaseInSkippedList(testCaseName)) {
			throw new SkipException(GlobalConstants.KNOWN_ISSUES);
		}

		AdminTestUtil.modifySchemaGenerateHbs(baseTestCaseDTO.isRegenerateHbs());
		String inputTemplate = SchemaBasedIdentityTemplateBuilder.buildAddIdentityTemplate();

		// Prime the Keycloak token cache single-threaded first: KernelAuthentication caches tokens in a
		// plain (non-thread-safe) HashMap, so a concurrent cache-miss stampede across a round's threads
		// could corrupt it. Reusing sendSingleAddIdentity here also warms the connection/JIT path so
		// round 1 (2 threads) isn't skewed by one-time setup cost.
		RequestOutcome warmup = sendSingleAddIdentity(baseTestCaseDTO, inputTemplate, 0, 0, new CyclicBarrier(1));
		logger.info("Warm-up AddIdentity request: " + warmup);

		List<RoundResult> allRounds = new ArrayList<>();
		for (int threadCount : THREAD_COUNTS) {
			if (HealthChecker.signalTerminateExecution) {
				throw new SkipException(
						GlobalConstants.TARGET_ENV_HEALTH_CHECK_FAILED + HealthChecker.healthCheckFailureMapS);
			}
			logger.info("=== Starting AddIdentity concurrency round: " + threadCount + " parallel threads ===");
			RoundResult roundResult = runRound(baseTestCaseDTO, inputTemplate, threadCount);
			allRounds.add(roundResult);
			logger.info(roundResult.summarize());
			Reporter.log(roundResult.summarize().replace("\n", "<br/>"));
			Thread.sleep(INTER_ROUND_PAUSE_MS);
		}

		int totalRequests = 0;
		int totalFailures = 0;
		StringBuilder finalSummary = new StringBuilder("AddIdentity concurrency load run complete. ")
				.append("Cross-check idrepo.anonymous_profile (and uin_history) in the DB for each UIN/RID logged ")
				.append("below for null/incomplete profile data.\n");
		for (RoundResult r : allRounds) {
			totalRequests += r.outcomes.size();
			totalFailures += r.outcomes.size() - r.successCount();
			finalSummary.append(r.summarize()).append("\n");
		}
		logger.info(finalSummary.toString());
		Reporter.log(finalSummary.toString().replace("\n", "<br/>"));

		if (totalFailures > 0) {
			throw new AdminTestException(totalFailures + " of " + totalRequests
					+ " concurrent AddIdentity requests failed at the HTTP/response level across all rounds; see logs for details.");
		}
	}

	/**
	 * Runs one round of {@code threadCount} concurrent, independently-valid AddIdentity requests and
	 * waits for all of them to finish before returning.
	 */
	private RoundResult runRound(TestCaseDTO baseTestCaseDTO, String inputTemplate, int threadCount)
			throws InterruptedException {
		ExecutorService executor = Executors.newFixedThreadPool(threadCount);
		// Every thread blocks here until all threadCount threads have built their payload, so the
		// actual HTTP calls leave together instead of being smeared out by per-thread setup time.
		CyclicBarrier barrier = new CyclicBarrier(threadCount);
		List<Future<RequestOutcome>> futures = new ArrayList<>(threadCount);
		try {
			for (int i = 0; i < threadCount; i++) {
				final int threadIndex = i;
				Callable<RequestOutcome> task = () -> sendSingleAddIdentity(baseTestCaseDTO, inputTemplate,
						threadCount, threadIndex, barrier);
				futures.add(executor.submit(task));
			}

			RoundResult roundResult = new RoundResult(threadCount);
			for (Future<RequestOutcome> future : futures) {
				try {
					roundResult.outcomes.add(future.get(180, TimeUnit.SECONDS));
				} catch (Exception e) {
					logger.error("A concurrent AddIdentity task failed for round " + threadCount, e);
					roundResult.outcomes
							.add(new RequestOutcome(threadCount, -1, null, null, null, -1, false, e.getMessage()));
				}
			}
			return roundResult;
		} finally {
			executor.shutdownNow();
			executor.awaitTermination(30, TimeUnit.SECONDS);
		}
	}

	/**
	 * Builds and fires exactly one valid, independent AddIdentity request. Mirrors the single-request
	 * flow in {@link AddIdentity#test}, minus the parts of that flow (autogen-id caching, output
	 * comparison bookkeeping) that assume one testCaseName maps to one request.
	 */
	private RequestOutcome sendSingleAddIdentity(TestCaseDTO baseTestCaseDTO, String inputTemplate, int threadCount,
			int threadIndex, CyclicBarrier barrier) {
		String uin = null;
		String rid = null;
		String email = null;
		try {
			uin = JsonPrecondtion.getValueFromJson(
					RestClient.getRequestWithCookie(ApplnURI + "/v1/idgenerator/uin", MediaType.APPLICATION_JSON,
							MediaType.APPLICATION_JSON, COOKIENAME,
							new KernelAuthentication().getTokenByRole(baseTestCaseDTO.getRole())).asString(),
					"response.uin");

			DateFormat dateFormatter = new SimpleDateFormat("yyyyMMddHHmmss");
			String timestampValue = dateFormatter.format(Calendar.getInstance().getTime());
			rid = "27847" + generateRandomNumberString(10) + timestampValue;
			email = "IdRepoConcurrencyLoad_t" + threadCount + "_" + threadIndex + "_" + System.nanoTime() + "_"
					+ BaseTestCase.runContext + "@mosip.net";

			String inputJson = getJsonFromTemplate(baseTestCaseDTO.getInput(), inputTemplate, false);

			if (inputJson.contains("$FUNCTIONALID$")) {
				inputJson = replaceKeywordWithValue(inputJson, "$FUNCTIONALID$",
						generateRandomNumberString(2) + Calendar.getInstance().getTimeInMillis());
			}
			inputJson = inputJson.replace("$UIN$", uin);
			inputJson = inputJson.replace("$RID$", rid);

			String phoneNumber = "";
			if (inputJson.contains("$PHONENUMBERFORIDENTITY$") || inputJson.contains("$EMAILVALUE$")) {
				if (!phoneSchemaRegex.isEmpty()) {
					try {
						phoneNumber = genStringAsperRegex(phoneSchemaRegex);
					} catch (Exception e) {
						logger.error(e.getMessage());
					}
				}
				inputJson = replaceKeywordWithValue(inputJson, "$PHONENUMBERFORIDENTITY$", phoneNumber);
			}
			if (inputJson.contains("$EMAILVALUE$")) {
				inputJson = replaceKeywordWithValue(inputJson, "$EMAILVALUE$", email);
			}
			// Must run before IdRepoArrayHandle.replaceArrayHandleValues, same ordering rule as AddIdentity.java.
			inputJson = IdRepoUtil.resolveGenericHandleValueTokens(inputJson);
			JSONObject jsonPayload = new JSONObject(inputJson);
			if (jsonPayload.getJSONObject("request").getJSONObject("identity").has("selectedHandles")) {
				inputJson = IdRepoArrayHandle.replaceArrayHandleValues(inputJson, testCaseName);
			}

			try {
				barrier.await(60, TimeUnit.SECONDS);
			} catch (Exception e) {
				logger.warn("Barrier wait failed for thread " + threadIndex + " in round " + threadCount + ": "
						+ e.getMessage());
			}

			Response response = postWithBodyAndCookie(ApplnURI + baseTestCaseDTO.getEndPoint(), inputJson,
					COOKIENAME, baseTestCaseDTO.getRole(), testCaseName);

			int statusCode = response == null ? -1 : response.getStatusCode();
			boolean hasErrors = response != null && response.jsonPath().get("errors") != null;
			boolean success = response != null && statusCode == 200 && !hasErrors;
			String errorMessage = success ? null
					: (response == null ? "null response" : "status=" + statusCode + " body=" + response.asString());

			return new RequestOutcome(threadCount, threadIndex, uin, rid, email, statusCode, success, errorMessage);
		} catch (Exception e) {
			logger.error("Exception sending AddIdentity for round=" + threadCount + " thread=" + threadIndex, e);
			return new RequestOutcome(threadCount, threadIndex, uin, rid, email, -1, false, e.getMessage());
		}
	}

	private static class RequestOutcome {
		final int threadCount;
		final int threadIndex;
		final String uin;
		final String rid;
		final String email;
		final int statusCode;
		final boolean success;
		final String errorMessage;

		RequestOutcome(int threadCount, int threadIndex, String uin, String rid, String email, int statusCode,
				boolean success, String errorMessage) {
			this.threadCount = threadCount;
			this.threadIndex = threadIndex;
			this.uin = uin;
			this.rid = rid;
			this.email = email;
			this.statusCode = statusCode;
			this.success = success;
			this.errorMessage = errorMessage;
		}

		@Override
		public String toString() {
			return "UIN=" + uin + " RID=" + rid + " EMAIL=" + email + " status=" + statusCode + " success=" + success
					+ (success ? "" : " error=" + errorMessage);
		}
	}

	private static class RoundResult {
		final int threadCount;
		final List<RequestOutcome> outcomes = Collections.synchronizedList(new ArrayList<>());

		RoundResult(int threadCount) {
			this.threadCount = threadCount;
		}

		int successCount() {
			int count = 0;
			for (RequestOutcome o : outcomes) {
				if (o.success) {
					count++;
				}
			}
			return count;
		}

		String summarize() {
			int success = successCount();
			StringBuilder sb = new StringBuilder();
			sb.append("Round[").append(threadCount).append(" threads] => success=").append(success).append(" failed=")
					.append(outcomes.size() - success).append("\n");
			for (RequestOutcome o : outcomes) {
				sb.append("    ").append(o).append("\n");
			}
			return sb.toString();
		}
	}
}
