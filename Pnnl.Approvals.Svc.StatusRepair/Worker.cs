using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace StatusRepairService
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IConfiguration _configuration;

        // TODO: Add Date logic

        public Worker(ILogger<Worker> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogDebug($"Pnnl.Approvals.Svc.StatusRepair is starting.");

            stoppingToken.Register(() =>
                    _logger.LogDebug($" Pnnl.Approvals.Svc.StatusRepair background task is stopping."));

            while (!stoppingToken.IsCancellationRequested)
            {
                await DoWork(stoppingToken);

                await Task.Delay(1000 * 50 * 60, stoppingToken);
            }
            _logger.LogDebug($"Pnnl.Approvals.Svc.StatusRepair is stopping.");
        }

        private async Task DoWork(CancellationToken stoppingToken)
        {
            int count = await RepairProcessStatusAsync(stoppingToken);
            _logger.LogInformation("Worker running at {time} processed: {rows} items", DateTimeOffset.Now, count);
        }

        protected async Task<int> RepairProcessStatusAsync(CancellationToken stoppingToken)
        {

            using (SqlConnection connection = new SqlConnection(GetConnectionString()))
            {
                connection.Open();

                using (SqlCommand sqlCommand = connection.CreateCommand())
                {
                    sqlCommand.CommandText = RepairSqlStatement();
                    sqlCommand.CommandType = System.Data.CommandType.Text;

                    SqlParameter sqlCount = new SqlParameter();
                    sqlCount.ParameterName = "@count";
                    sqlCount.Direction = System.Data.ParameterDirection.Output;
                    sqlCount.DbType = System.Data.DbType.Int32;
                    sqlCommand.Parameters.Add(sqlCount);

                    int result = await sqlCommand.ExecuteNonQueryAsync();

                    return (int)sqlCount.Value;

                }
            }
        }

        private string RepairSqlStatement()
        {
            return $@"begin tran TAS
BEGIN TRY
    IF OBJECT_ID('tempdb..#PROCESSES') IS NOT NULL DROP TABLE #PROCESSES
 
	CREATE TABLE #PROCESSES(
		PROCESS_ID int
	)

	INSERT INTO #PROCESSES (PROCESS_ID)
	SELECT PROCESS_ID
	FROM PROCESS P
	WHERE PROCESS_ID in 
		(Select PROCESS_ID
		from PROCESS
		where state = 'PENDING'
			--AND STATUS is not null
		EXCEPT
		SELECT DISTINCT P.PROCESS_ID
		from PROCESS P
		INNER JOIN ACTIVITY A on P.PROCESS_ID = A.PROCESS_ID
		where P.state = 'PENDING'
			AND (A.STATE in ('PENDING', 'PENDING ESCALATED')
				or (A.STATE is null AND A.IS_GHOST = 0)))
	order by 1 desc

	IF OBJECT_ID('tempdb..#ACTIVITIES') IS NOT NULL DROP TABLE #ACTIVITIES
 
	CREATE TABLE #ACTIVITIES(
		PROCESS_ID int
		, ACTIVITY_ID int
		, ACTIVITY_NAME varchar(100)
		, STATE varchar(20)
		, STATUS varchar(12)
		, LAST_CHANGE_DATETIME datetime
		, [ACTED_USERID] varchar(11)
		, PROCESS_STATE varchar(255)
	)

	
	INSERT INTO #ACTIVITIES (PROCESS_ID, ACTIVITY_ID, ACTIVITY_NAME, STATE, STATUS, LAST_CHANGE_DATETIME, [ACTED_USERID], PROCESS_STATE)
	SELECT A.PROCESS_ID, A.ACTIVITY_ID, A.ACTIVITY_NAME, A.STATE, A.STATUS
		, A.LAST_CHANGE_DATETIME, A.ACTED_USERID
		, A.ACTIVITY_NAME + ': ' + A.STATUS + ' by: ' + A.ACTED_USERID
	FROM ACTIVITY A
	INNER JOIN (
	SELECT A.PROCESS_ID, MAX(A.LAST_CHANGE_DATETIME) as LAST_CHANGE_DATETIME
		FROM ACTIVITY A
		INNER JOIN #PROCESSES P on P.PROCESS_ID = A.PROCESS_ID
		GROUP BY A.PROCESS_ID)  G on G.PROCESS_ID = A.PROCESS_ID and G.LAST_CHANGE_DATETIME = A.LAST_CHANGE_DATETIME

	UPDATE P
		SET P.STATE = CASE WHEN A.STATUS = 'ACCEPTED' THEN 'APPROVED'
				ELSE 'TERMINATED' END
			,P.STATUS = A.PROCESS_STATE
			,P.LAST_CHANGE_DATETIME = A.LAST_CHANGE_DATETIME
			,P.STATUS_FAILURE_SW = CASE WHEN ISNULL(P.APPL_STATUS_WEB_SERVICE_LEVEL, 'NONE' ) = 'NONE'  THEN 0
				ELSE 1 END
	FROM PROCESS P
	INNER JOIN #ACTIVITIES A on P.PROCESS_ID = A.PROCESS_ID

    SELECT @count = @@ROWCOUNT
 
END TRY
BEGIN CATCH
    SELECT
        ERROR_NUMBER() AS ErrorNumber
        ,ERROR_SEVERITY() AS ErrorSeverity
        ,ERROR_STATE() AS ErrorState
        ,ERROR_PROCEDURE() AS ErrorProcedure
        ,ERROR_LINE() AS ErrorLine
        ,ERROR_MESSAGE() AS ErrorMessage;
 
    ROLLBACK TRANSACTION TAS
END CATCH
 
IF @@TRANCOUNT > 0
    COMMIT TRAN TAS;
";
        }
        private string GetConnectionString()
        {
            return "Data Source=ABBPROD02.pnl.gov,915;Initial Catalog=RAA;Integrated Security=true;";
        }
    }
}
