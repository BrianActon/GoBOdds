--  [dbo].[M3_Checks]			--  Data supplied by RBE/ODS
--  [dbo].[M3_Daily]			--  Data supplied by M3
--  [dbo].[campaignList_tble]	--	collection of current runs campaign codes
--  [dbo].[M3DailyBalances]		--	Used to create overview of balances and to aid in generating detailed lists
--							  --    	

USE M3;
DECLARE @NumDays INT;
DECLARE @RBEperiod AS DATE;
DECLARE @M3period AS DATE;
DECLARE @campaignID VARCHAR(10);

-- *need to make work for public holidays*
IF (select DATEPART(DW, GETDATE())) = 2  -- Mondays
SET @RBEperiod = DATEADD (d, -2, GETDATE())
ELSE
SET @RBEperiod = GETDATE();
SET @M3period = GETDATE();


IF OBJECT_ID('M3.dbo.campaignList_tble') IS NOT NULL
	DROP TABLE M3.dbo.campaignList_tble;
-- make list of camapaigns to compare
WITH cte_campaignList ( campaignID)
AS
	(
		SELECT DISTINCT [campaign_ID]
			FROM [dbo].[M3_AllCampaigns]
		UNION
		SELECT DISTINCT [CampaignCode]
			FROM [dbo].[M3_Checks]
	)
SELECT DISTINCT campaignID
INTO campaignList_tble
FROM cte_campaignList;

IF OBJECT_ID('M3.dbo.campaignAudienceList_tble') IS NOT NULL
	DROP TABLE M3.dbo.campaignAudienceList_tble;

-- Populate table with list of expected campaigns for the day	
WITH cte_campaignAudienceList ( campaignID, SA_ID_Number,  inM3, inRBE) --Period,
AS
	(
		SELECT DISTINCT [campaign_ID],
						[SA_ID_Number],
			--			@M3period,
						0 AS inM3,
						0 AS inRBE
			FROM M3.[dbo].[M3_AllCampaigns]
		UNION
		SELECT DISTINCT m.[CampaignCode],
						m.[SA_ID_Number],
			--			m.Period,
						0 AS inM3,
						0 AS inRBE
			FROM M3.[dbo].[M3_Checks] m
			WHERE m.Period >= @RBEperiod
	)
SELECT DISTINCT campaignID,
				[SA_ID_Number],
		--		Period,
				inM3,
				inRBE
INTO campaignAudienceList_tble
FROM cte_campaignAudienceList;

-- 
UPDATE campaignAudienceList_tble
	SET inM3 = 1
FROM campaignAudienceList_tble calt
	INNER JOIN M3.[dbo].[M3_AllCampaigns] mac
		ON calt.campaignID = mac.campaign_ID
		AND calt.SA_ID_Number = mac.SA_ID_Number;

UPDATE campaignAudienceList_tble
	SET inRBE = 1
FROM campaignAudienceList_tble calt
	INNER JOIN M3.[dbo].[M3_Checks] m3c
		ON calt.campaignID = m3c.CampaignCode
		AND calt.SA_ID_Number = m3c.SA_ID_Number;

SELECT * FROM
campaignAudienceList_tble calt
	INNER JOIN M3.[dbo].[M3_Checks] m3c
		ON calt.campaignID = m3c.CampaignCode
		AND calt.SA_ID_Number = m3c.SA_ID_Number;
		
		
IF OBJECT_ID('M3.dbo.M3DailyBalances') IS NOT NULL
	DROP TABLE M3.dbo.M3DailyBalances;

-- Final selection
SELECT 
	 @M3period AS date,
	 clt.CampaignID,
        -- In RBE
	 (  CASE  clt.CampaignID 
		WHEN 'AR0003'
			THEN
				(SELECT COUNT(*) 
				FROM M3_Checks m
				WHERE m.Period = @M3period and m.CampaignCode = clt.CampaignID)
		WHEN 'AR0066'
			THEN
				(SELECT COUNT(*) 
				FROM M3_Checks m
				WHERE m.Period = @M3period and m.CampaignCode = clt.CampaignID)
		WHEN 'AR0067'
			THEN
				(SELECT COUNT(*) 
				FROM M3_Checks m
				WHERE m.Period = @M3period and m.CampaignCode = clt.CampaignID)
		WHEN 'AR0068'
			THEN
				(SELECT COUNT(*) 
				FROM M3_Checks m
				WHERE m.Period = @M3period and m.CampaignCode = clt.CampaignID)
		WHEN 'AR0069'
			THEN
				(SELECT COUNT(*) 
				FROM M3_Checks m
				WHERE m.Period = @M3period and m.CampaignCode = clt.CampaignID)
		WHEN 'AR0070'
			THEN
				(SELECT COUNT(*) 
				FROM M3_Checks m
				WHERE m.Period = @M3period and m.CampaignCode = clt.CampaignID)
		WHEN 'AR0071'
			THEN
				(SELECT COUNT(*) 
				FROM M3_Checks m
				WHERE m.Period = @M3period and m.CampaignCode = clt.CampaignID)
		WHEN 'AR0072'
			THEN
				(SELECT COUNT(*) 
				FROM M3_Checks m
				WHERE m.Period = @M3period and m.CampaignCode = clt.CampaignID)
		ELSE
				(SELECT COUNT(*) 
				FROM M3_Checks m
				WHERE m.Period >= @RBEperiod and m.CampaignCode = clt.CampaignID)
			END	
														)	 as TotalInRBE,
        -- In M3 
	(SELECT COUNT(*) 
	FROM [dbo].[M3_AllCampaigns] ma
	WHERE ma.campaign_ID = clt.CampaignID) as TotalInM3,
        -- In RBE not M3
	(SELECT COUNT(DISTINCT calt.SA_ID_Number) 
	FROM campaignAudienceList_tble calt
	     LEFT JOIN [dbo].[M3_AllCampaigns] ma 
			ON	calt.CampaignID = clt.CampaignID
			AND	calt.SA_ID_Number = ma.[SA_ID_Number]
	WHERE calt.CampaignID = clt.CampaignID
		AND inM3 = 0 AND inRBE =1) AS InRBENotM3,
        -- In M3 not RBE
	(SELECT COUNT(DISTINCT calt.SA_ID_Number) 
	FROM campaignAudienceList_tble calt
     LEFT JOIN [dbo].[M3_Checks] m3
			ON calt.CampaignID = clt.CampaignID
			AND calt.SA_ID_Number = m3.[SA_ID_Number]
		WHERE calt.CampaignID = clt.CampaignID
		AND inRBE = 0 AND inM3 = 1)  AS InM3NotRBE

INTO M3.[dbo].[M3DailyBalances]
FROM [dbo].[campaignList_tble] clt;

UPDATE M3.[dbo].[M3DailyBalances] 
SET InRBENotM3 = (TotalInRBE - TotalInM3)
WHERE CampaignID = 'AR0003';

--PRINT 'DONE!!!';

--SELECT * FROM [dbo].[M3DailyBalances]
--ORDER BY campaignID

RETURN;