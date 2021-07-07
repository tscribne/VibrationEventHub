SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TelemetryData](
	applicationId varchar(255) NULL,
	messageSource varchar(255) NULL,
	deviceId varchar(255) NULL,
	msgschema varchar(255) NULL,
	templateId varchar(255) NULL,
	enqueuedTime varchar(255) NULL,
	accelerationX float NULL,
	accelerationY float NULL,
	accelerationZ float NULL,
	temperature float NULL,
	humidity float NULL,
    avgcurrent float NULL,
    voltage float NULL,
	rssi int NULL,
    wififrequency_tel float NULL,
   	wificonnecttime float NULL,
    azureconnecttime float NULL,
	connecttime float NULL,    
	wifi_fails int NULL,
	wifi_resets int NULL,
	azure_fails int NULL,
	fatals int NULL,
    vibfails int NULL,
    tempfails int NULL,
    updatefails int NULL,
    appupdates int NULL,
    osupdates int NULL
) ON [PRIMARY]
GO
