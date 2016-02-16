
DECLARE @NumDays INT;
DECLARE @period AS DATE;

IF  (SELECT DATENAME(WEEKDAY, GETDATE())) = 'Monday'
	SET @Numdays = -4
ELSE
	SET @NumDays = -1;

SET @period = GETDATE();

INSERT INTO [M3].[dbo].[M3_Checks] 
SELECT Period, SA_ID_Number, Membership_num, CampaignCode 
FROM OPENROWSET(BULK 'E:\M3 Export\M3_Checks.txt'
, FORMATFILE='E:\M3_P3_Admin\M3_Checks_format_CG.fmt'     
      ) as t1
	  WHERE t1.Period > DATEADD(d, @NumDays, @Period);  

