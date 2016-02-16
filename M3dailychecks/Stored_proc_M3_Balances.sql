-- ================================================
-- Template generated from Template Explorer using:
-- Create Procedure (New Menu).SQL
--
-- Use the Specify Values for Template Parameters 
-- command (Ctrl-Shift-M) to fill in the parameter 
-- values below.
--
-- This block of comments will not be included in
-- the definition of the procedure.
-- ================================================
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE <sp_M3_Daily_Balances, sysname, ProcedureName> 
	-- Add the parameters for the stored procedure here
	<@period, sysname, @p1> <Datatype_For_Param1, , DATE> = <'20150721', , 0>, 
	<@campaignID, sysname, @p2> <varchar(10), , int> = <'none', , 0>


AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

--  [dbo].[M3_Checks]  --  Data supplied by RBE/ODS
--  [dbo].[M3_Daily]   --  Data supplied by M3
--  [dbo].[campaignList_tble]  -- collection of current runs campaign codes
--  [dbo].[M3DailyBalances]    -- Used to create overview of balances and to aid in generating detailed lists
--							  --    	


USE Test_Go;

-- *need to make work for weekends and public holidays*


IF EXISTS(SELECT 1 FROM dbo.campaignList_tble)
DROP TABLE dbo.campaignList_tble;

WITH cte_campaignList ( campaignID)
AS
	(
		SELECT DISTINCT [campaign_ID]
			FROM [dbo].[M3_AllCampaigns]
		UNION
		SELECT DISTINCT [Campaign]
			FROM [dbo].[M3_Checks]
	)
SELECT DISTINCT campaignID
INTO campaignList_tble
FROM cte_campaignList;


--  [DBO}.[M3DailyBalances] to be used for reporting
--  Date, Campaign code, RBE count, M3 count, 
--

IF EXISTS(SELECT 1 FROM dbo.M3DailyBalances)
DROP TABLE [dbo].[M3DailyBalances];

SELECT 
        --In RBE not M3
	 @period as date,
	 clt.CampaignID,
	(SELECT COUNT(*) 
	from M3_Checks m
	     LEFT JOIN [dbo].[M3_AllCampaigns] ma on m.SA_ID_Number = ma.[SA_ID_Number]
	where m.Period >= @period and m.Campaign = clt.CampaignID
	and ma.[SA_ID_Number] is null ) as InRBENotM3,
        -- In M3 not RBE
	(SELECT COUNT(*) 
	FROM [dbo].[M3_AllCampaigns] ma
      left join M3_Checks m on ma.[SA_ID_Number] = m.SA_ID_Number 
	WHERE m.SA_ID_Number is null
		and ma.campaign_ID = clt.CampaignID) as InM3NotRBE
INTO [dbo].[M3DailyBalances]
FROM [dbo].[campaignList_tble] clt;






SELECT 'DONE';
    -- Insert statements for procedure here
	SELECT <@Param1, sysname, @p1>, <@Param2, sysname, @p2>
END
GO
