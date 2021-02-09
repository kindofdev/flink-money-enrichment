import org.apache.flink.api.common.JobID
import org.kindofdev.query.QueryClient

import java.util.UUID

val jobId = JobID.fromHexString("38efedd7d66d6985c6cb32d339f38647")
val userId = UUID.fromString("9f872873-eae5-466e-8e04-5552564481d1")
val userSessionId = UUID.fromString("326a14a3-2e2a-4719-9c71-b9a3952eb211")
val userSessionId2 = UUID.fromString("326a14a3-2e2a-4719-9c71-b9a3952eb561")

val queryClient = new QueryClient(jobId)
//queryClient.executeQuery(MoneyMeetsUserFunction.UserInfoQN, userId)
//queryClient.executeQuery(MoneyMeetsSessionFunction.SessionInfoQN, userSessionId)
//queryClient.executeQuery(MoneyMeetsSessionFunction.SessionInfoQN, userSessionId2)