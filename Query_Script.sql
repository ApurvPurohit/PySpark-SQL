DELETE FROM Provider.dbo.OxfordPreviousProviderInformation;
INSERT INTO Provider.dbo.OxfordPreviousProviderInformation
SELECT * From Provider.dbo.OxfordCurrentProviderInformation;
DELETE FROM Provider.dbo.OxfordCurrentProviderInformation;

Select distinct PLP.AcnProvID AS ACNPROVID, PLP.ProviderID, PLP.OfficeLocationID AS Acn_Location_ID, PCON.ClientID, 
PCON.ProviderSpecialty AS PracticeSpecialty, PCON.ProviderSpecialty AS Degree, PCON.ContEffDate AS ParticipationEffectiveDate
FROM  
  Provider.dbo.T_ProviderContracts AS PCON WITH (NOLOCK) INNER JOIN    Provider.dbo.T_ProviderLocationPointer AS PLP WITH (NOLOCK) 
  ON PCON.AcnProvID = PLP.AcnProvID
  WHERE     
      (PLP.TermDate IS NULL) AND (PCON.ProviderSpecialty IN ('DC')) AND (PCON.ClientID = '160') 
      AND (PLP.ProviderID NOT IN ('693301', '693302', '999999', '663910')) 
      AND (PCON.ContTermDate IS NULL OR   PCON.ContTermDate > GETDATE() + 40);


      UPDATE Provider.dbo.OxfordCurrentProviderInformation 
      SET   Provider.dbo.OxfordCurrentProviderInformation.Phone = PE.provphone, 
            Provider.dbo.OxfordCurrentProviderInformation.Fax = PE.provFax, 
            Provider.dbo.OxfordCurrentProviderInformation.Email = PE.INETADDRESS
      FROM Provider.dbo.OxfordCurrentProviderInformation
      INNER JOIN Provider.dbo.T_ProviderElectronics as PE ON PE.AcnProvID = Provider.dbo.OxfordCurrentProviderInformation.ACNPROVID ;

      
      UPDATE  Provider.dbo.OxfordCurrentProviderInformation 
      SET   Provider.dbo.OxfordCurrentProviderInformation.SSN = PI.ProvSSN,--'999999999',
            Provider.dbo.OxfordCurrentProviderInformation.First_Name = PI.ProvFirstName,
            Provider.dbo.OxfordCurrentProviderInformation.Last_Name = PI.ProvLastName,
            Provider.dbo.OxfordCurrentProviderInformation.MI = PI.ProvMI,
            Provider.dbo.OxfordCurrentProviderInformation.Gender = PI.ProvSex,
            Provider.dbo.OxfordCurrentProviderInformation.DOB = PI.ProvDOB,--'01/01/1980',
            Provider.dbo.OxfordCurrentProviderInformation.Medicare_Number = PI.MedicareNumber,
            Provider.dbo.OxfordCurrentProviderInformation.UPIN = PI.UniqueProviderID,
            Provider.dbo.OxfordCurrentProviderInformation.MedicalSchoolDegree = PI.ProvSpec 
      FROM OxfordCurrentProviderInformation
      INNER JOIN Provider.dbo.T_ProviderInformation as PI ON PI.ProviderID = Provider.dbo.OxfordCurrentProviderInformation.ProviderID ;

      
      UPDATE  Provider.dbo.OxfordCurrentProviderInformation 
      SET   Provider.dbo.OxfordCurrentProviderInformation.Clinic_Name = OI.ClinicName,
            Provider.dbo.OxfordCurrentProviderInformation.PracticeAddress_1 = OI.LocAddress1,
            Provider.dbo.OxfordCurrentProviderInformation.PracticeAddress_2 = OI.LocAddress2,
            Provider.dbo.OxfordCurrentProviderInformation.PracticeCity = OI.LocCity,
            Provider.dbo.OxfordCurrentProviderInformation.PracticeState = OI.LocState,
            Provider.dbo.OxfordCurrentProviderInformation.PracticeZip = OI.LocZip,
            Provider.dbo.OxfordCurrentProviderInformation.PracticeCounty = OI.County,
            Provider.dbo.OxfordCurrentProviderInformation.Payment_Address_1 = OI.MailAddress1,
            Provider.dbo.OxfordCurrentProviderInformation.Payment_Address_2 = OI.MailAddress2,
            Provider.dbo.OxfordCurrentProviderInformation.Payment_City = OI.MailCity,
            Provider.dbo.OxfordCurrentProviderInformation.Payment_State = OI.MailState,
            Provider.dbo.OxfordCurrentProviderInformation.Payment_Zip = OI.MailZip,
            Provider.dbo.OxfordCurrentProviderInformation.Correspondence_Address_1 = OI.AuthRespAddress1,
            Provider.dbo.OxfordCurrentProviderInformation.Correspondence_Address_2 = OI.AuthRespAddress2,
            Provider.dbo.OxfordCurrentProviderInformation.Correspondence_City = OI.AuthRespCity,
            Provider.dbo.OxfordCurrentProviderInformation.Correspondence_State = OI.AuthRespState,
            Provider.dbo.OxfordCurrentProviderInformation.Correspondence_Zip = OI.AuthRespZip,
            Provider.dbo.OxfordCurrentProviderInformation.Medicaid = PLP.MedicaidNumber,
            Provider.dbo.OxfordCurrentProviderInformation.PrimaryLocation = PLP.PrimaryLoc

      FROM Provider.dbo.OxfordCurrentProviderInformation
        INNER JOIN Provider.dbo.T_ProviderLocationPointer As PLP ON dbo.OxfordCurrentProviderInformation.ProviderID = PLP.ProviderID 
        AND Provider.dbo.OxfordCurrentProviderInformation.Acn_Location_ID = PLP.OfficeLocationID 
        INNER JOIN Provider.dbo.T_OfficeLocationInfo AS OI ON PLP.OfficeLocationID = OI.OfficeLocationID;
        
      
      UPDATE Provider.dbo.OxfordCurrentProviderInformation 
      SET   Provider.dbo.OxfordCurrentProviderInformation.MarketNumber = UMZ.MarketNumber
      FROM  OxfordCurrentProviderInformation
      INNER JOIN provider.dbo.tblUnetMarketsByZipCode AS UMZ ON Provider.dbo.OxfordCurrentProviderInformation.PracticeZip = UMZ.ZipCode;

      
      UPDATE Provider.dbo.OxfordCurrentProviderInformation
      SET   Provider.dbo.OxfordCurrentProviderInformation.TIN = PTP.TinNumber,
            Provider.dbo.OxfordCurrentProviderInformation.TIN_EffectiveDate = PTP.EffDate,
            Provider.dbo.OxfordCurrentProviderInformation.Payment_Name = TI.Owner,
            Provider.dbo.OxfordCurrentProviderInformation.TinOwner = TI.Owner
      FROM OxfordCurrentProviderInformation
        INNER JOIN Provider.dbo.T_ProvLocTinPointer AS PTP ON Provider.dbo.OxfordCurrentProviderInformation.ProviderID = PTP.ProviderID 
        INNER JOIN Provider.dbo.T_TinInfo AS TI ON PTP.TinNumber = TI.TinNumber
        AND Provider.dbo.OxfordCurrentProviderInformation.Acn_Location_ID = PTP.OfficeLocationID 
      WHERE PTP.TermDate IS NULL;

      
      UPDATE Provider.dbo.OxfordCurrentProviderInformation 
      SET   Provider.dbo.OxfordCurrentProviderInformation.State_License_Number = PLI.LicenseNumber,
            Provider.dbo.OxfordCurrentProviderInformation.State_License_Expiration_Date = PLI.LicenseExpireDate
      FROM OxfordCurrentProviderInformation
        INNER JOIN Provider.dbo.tblProviderLicenseInfo AS PLI ON Provider.dbo.OxfordCurrentProviderInformation.ProviderID = PLI.ProviderID  
        AND Provider.dbo.OxfordCurrentProviderInformation.PracticeSpecialty = PLI.SpecialtyCode  
        AND Provider.dbo.OxfordCurrentProviderInformation.PracticeState = PLI.State;                            
      
      UPDATE  Provider.dbo.OxfordCurrentProviderInformation 
      SET   Provider.dbo.OxfordCurrentProviderInformation.Malp_Specific_Limit = MPL.PerIncidentAmount,
            Provider.dbo.OxfordCurrentProviderInformation.Malp_Aggregate_Limit = MPL.AggregateAmount,
            Provider.dbo.OxfordCurrentProviderInformation.MedicalSchool = PSI.CollegeAttended,
            Provider.dbo.OxfordCurrentProviderInformation.CompletionDate = PSI.CollegeGradDate
      FROM OxfordCurrentProviderInformation
            INNER JOIN Provider.dbo.ProviderSpecialtyInfo AS PSI ON Provider.dbo.OxfordCurrentProviderInformation.ProviderID = PSI.ProviderID 
            AND Provider.dbo.OxfordCurrentProviderInformation.PracticeSpecialty = PSI.SpecialtyCode 
            LEFT OUTER JOIN Provider.dbo.MalpracticePolicyLimits AS MPL ON PSI.MalpracticePolicyLimitsCode = MPL.Code;


            DELETE FROM Provider.dbo.OxfordCurrentProviderInformation
            WHERE ProviderID = '999999';
            DELETE FROM Provider.dbo.OxfordProviderChangeAddTerm;
      SELECT     OPPI.* 
      INTO #tempProvTerminations
      FROM  Provider.dbo.OxfordPreviousProviderInformation AS OPPI 
            LEFT OUTER JOIN Provider.dbo.OxfordCurrentProviderInformation ON OPPI.PracticeSpecialty = Provider.dbo.OxfordCurrentProviderInformation.PracticeSpecialty  
            AND OPPI.ClientID = Provider.dbo.OxfordCurrentProviderInformation.ClientID
            AND OPPI.ProviderID = Provider.dbo.OxfordCurrentProviderInformation.ProviderID
      WHERE Provider.dbo.OxfordCurrentProviderInformation.ProviderID IS NULL ;
      
      INSERT INTO Provider.dbo.OxfordProviderChangeAddTerm (
        ProviderID,
        TransferCode)
      SELECT DISTINCT 
        #tempProvTerminations.providerID,
        'T'
      From #tempProvTerminations ;

      
      SELECT Provider.dbo.OxfordCurrentProviderInformation.*  
      INTO #tempProvAdditions
      FROM       Provider.dbo.OxfordCurrentProviderInformation 
           LEFT OUTER JOIN Provider.dbo.OxfordPreviousProviderInformation AS OPPI ON Provider.dbo.OxfordCurrentProviderInformation.PracticeSpecialty = OPPI.PracticeSpecialty 
           AND Provider.dbo.OxfordCurrentProviderInformation.ClientID = OPPI.ClientID 
           AND Provider.dbo.OxfordCurrentProviderInformation.ProviderID = OPPI.ProviderID
      WHERE      OPPI.ProviderID IS NULL ;

      INSERT INTO Provider.dbo.OxfordProviderChangeAddTerm (
        ProviderID,
        TransferCode)
      SELECT DISTINCT 
        #tempProvAdditions.ProviderID,
        'A'
      FROM #tempProvAdditions ;
      
      SELECT DISTINCT OPPI.* 
      INTO #tempTinTermination
      FROM Provider.dbo.OxfordPreviousProviderInformation AS OPPI
        LEFT JOIN Provider.dbo.OxfordCurrentProviderInformation ON OPPI.TIN = Provider.dbo.OxfordCurrentProviderInformation.TIN 
        AND OPPI.ProviderID = Provider.dbo.OxfordCurrentProviderInformation.ProviderID
        AND OPPI.PracticeSpecialty = Provider.dbo.OxfordCurrentProviderInformation.PracticeSpecialty 
        AND OPPI.ClientID = Provider.dbo.OxfordCurrentProviderInformation.ClientID
      WHERE Provider.dbo.OxfordCurrentProviderInformation.TIN Is NULL;

      
      DELETE TT
      From #tempTinTermination AS TT
      Inner Join #tempProvTerminations AS PT ON PT.ProviderID = TT.providerID ;

      
      SELECT OPPI.*
      INTO #tempLocTermination
      FROM Provider.dbo.OxfordPreviousProviderInformation AS OPPI
          LEFT JOIN Provider.dbo.OxfordCurrentProviderInformation ON OPPI.ACNPROVID = Provider.dbo.OxfordCurrentProviderInformation.ACNPROVID
          AND OPPI.PracticeSpecialty = Provider.dbo.OxfordCurrentProviderInformation.PracticeSpecialty
          AND OPPI.ClientID = Provider.dbo.OxfordCurrentProviderInformation.ClientID
      WHERE     Provider.dbo.OxfordCurrentProviderInformation.ACNPROVID IS NULL ;
      
      DELETE  LT 
      FROM #tempLocTermination LT
      INNER JOIN #tempProvTerminations as PT on LT.ProviderID = PT.ProviderID ;

      
      SELECT Provider.dbo.OxfordCurrentProviderInformation.*
      INTO #tempLocAddition
      FROM Provider.dbo.OxfordCurrentProviderInformation 
        LEFT JOIN Provider.dbo.OxfordPreviousProviderInformation AS OPPI ON Provider.dbo.OxfordCurrentProviderInformation.Acn_Location_ID = OPPI.Acn_Location_ID 
        AND Provider.dbo.OxfordCurrentProviderInformation.ProviderID = OPPI.ProviderID
        AND Provider.dbo.OxfordCurrentProviderInformation.PracticeSpecialty = OPPI.PracticeSpecialty 
        AND Provider.dbo.OxfordCurrentProviderInformation.ClientID = OPPI.ClientID
      WHERE OPPI.Acn_Location_ID Is Null;
      DELETE  LA
      From #tempLocAddition LA
      Inner JOIN #tempProvAdditions as PA on LA.ProviderID = PA.ProviderID ;

      Select DISTINCT Provider.dbo.OxfordCurrentProviderInformation.* 
      INTO #tempOxfordProviderChangeAddTerm
      FROM  Provider.dbo.OxfordCurrentProviderInformation  
      INNER JOIN Provider.dbo.OxfordPreviousProviderInformation AS OPPI ON Provider.dbo.OxfordCurrentProviderInformation.ACNPROVID = OPPI.ACNPROVID 
      AND Provider.dbo.OxfordCurrentProviderInformation.ClientID = OPPI.ClientID
      AND Provider.dbo.OxfordCurrentProviderInformation.PracticeSpecialty = OPPI.PracticeSpecialty
      WHERE 
            ISNULL(Provider.dbo.OxfordCurrentProviderInformation.Last_Name,'') <> ISNULL(OPPI.Last_Name,'') OR
            ISNULL(Provider.dbo.OxfordCurrentProviderInformation.First_Name,'') <> ISNULL(OPPI.First_Name,'') OR
            ISNULL(Provider.dbo.OxfordCurrentProviderInformation.MI,'') <> ISNULL(OPPI.MI,'') OR
            ISNULL(Provider.dbo.OxfordCurrentProviderInformation.Gender,'') <> ISNULL(OPPI.Gender,'') OR
            ISNULL(Provider.dbo.OxfordCurrentProviderInformation.DOB,'') <> ISNULL(OPPI.DOB,'') OR
            --( LEN(RTRIM(ISNULL(Provider.dbo.OxfordCurrentProviderInformation.SSN,'')))> 9 AND LEN(RTRIM(ISNULL(OPPI.SSN,'')))> 9 
            --AND ISNULL(GLOBAL.dbo.fn_decrypt(Provider.dbo.OxfordCurrentProviderInformation.SSN)+RIGHT(Provider.dbo.OxfordCurrentProviderInformation.SSN,4),'') <> ISNULL(GLOBAL.dbo.fn_decrypt(OPPI.SSN)+RIGHT(OPPI.SSN,4),'')) OR   
            ISNULL(Provider.dbo.OxfordCurrentProviderInformation.TIN_EffectiveDate,'') <> ISNULL(OPPI.TIN_EffectiveDate,'') OR
            ISNULL(Provider.dbo.OxfordCurrentProviderInformation.TinOwner,'') <> ISNULL(OPPI.TinOwner,'') OR
            ISNULL(Provider.dbo.OxfordCurrentProviderInformation.State_License_Number,'') <> ISNULL(OPPI.State_License_Number,'') OR
            ISNULL(Provider.dbo.OxfordCurrentProviderInformation.State_License_Expiration_Date,'') <> ISNULL(OPPI.State_License_Expiration_Date,'') OR
            ISNULL(Provider.dbo.OxfordCurrentProviderInformation.TIN_EffectiveDate,'') <> ISNULL(OPPI.TIN_EffectiveDate,'') OR
            ISNULL(Provider.dbo.OxfordCurrentProviderInformation.PracticeAddress_1,'') <> ISNULL(OPPI.PracticeAddress_1,'') OR
            ISNULL(Provider.dbo.OxfordCurrentProviderInformation.PracticeAddress_2,'') <> ISNULL(OPPI.PracticeAddress_2,'') OR
            ISNULL(Provider.dbo.OxfordCurrentProviderInformation.PracticeCity,'') <> ISNULL(OPPI.PracticeCity,'') OR
            ISNULL(Provider.dbo.OxfordCurrentProviderInformation.PracticeState,'') <> ISNULL(OPPI.PracticeState,'') OR
            ISNULL(Provider.dbo.OxfordCurrentProviderInformation.PracticeZip,'') <> ISNULL(OPPI.PracticeZip,'') OR
            ISNULL(Provider.dbo.OxfordCurrentProviderInformation.PracticeCounty,'') <> ISNULL(OPPI.PracticeCounty,'') OR
            ISNULL(Provider.dbo.OxfordCurrentProviderInformation.Phone,'') <> ISNULL(OPPI.Phone,'') OR
            ISNULL(Provider.dbo.OxfordCurrentProviderInformation.Fax,'') <> ISNULL(OPPI.Fax,'') OR
            ISNULL(Provider.dbo.OxfordCurrentProviderInformation.TIN,'') <> ISNULL(OPPI.TIN,'') OR
            --ISNULL(Provider.dbo.OxfordCurrentProviderInformation.Clinic_Name,'') <> ISNULL(OPPI.Clinic_Name,'') OR
            ISNULL(Provider.dbo.OxfordCurrentProviderInformation.Payment_Name,'') <> ISNULL(OPPI.Payment_Name,'') OR
            ISNULL(Provider.dbo.OxfordCurrentProviderInformation.Payment_Address_1,'') <> ISNULL(OPPI.Payment_Address_1,'') OR
            ISNULL(Provider.dbo.OxfordCurrentProviderInformation.Payment_Address_2,'') <> ISNULL(OPPI.Payment_Address_2,'') OR
            ISNULL(Provider.dbo.OxfordCurrentProviderInformation.Payment_City,'') <> ISNULL(OPPI.Payment_City,'') OR
            ISNULL(Provider.dbo.OxfordCurrentProviderInformation.Payment_State,'') <> ISNULL(OPPI.Payment_State,'') OR
            ISNULL(Provider.dbo.OxfordCurrentProviderInformation.Payment_Zip,'') <> ISNULL(OPPI.Payment_Zip,'') ;

    INSERT INTO Provider.dbo.OxfordProviderChangeAddTerm
       (ProviderID,
       TransferCode)
    SELECT DISTINCT 
      ProviderID,
      'C'
    FROM #tempTinTermination

    UNION

    SELECT DISTINCT 
      ProviderID,
      'C'
    FROM #tempLocTermination

    UNION

    SELECT DISTINCT 
      ProviderID,
      'C'
    FROM #tempLocAddition

    UNION

    SELECT DISTINCT 
      ProviderID,
      'C'
    FROM #tempOxfordProviderChangeAddTerm ;

    Drop Table  #tempOxfordProviderChangeAddTerm;
    Drop Table #tempProvAdditions;
    Drop Table #tempLocTermination;
    Drop Table #tempLocAddition;
    Drop Table #tempProvTerminations;

    DELETE FROM Provider.dbo.OxfordProviderTransfer;
    
    SELECT DISTINCT
      Provider.dbo.OxfordCurrentProviderInformation.ACNPROVID,                 
      Provider.dbo.OxfordCurrentProviderInformation.ClientID, 
      Provider.dbo.OxfordCurrentProviderInformation.PracticeSpecialty,             
      Provider.dbo.OxfordCurrentProviderInformation.ProviderID, 
      Provider.dbo.OxfordCurrentProviderInformation.Acn_Location_ID,               
      Provider.dbo.OxfordCurrentProviderInformation.Last_Name, 
      Provider.dbo.OxfordCurrentProviderInformation.First_Name,                
      Provider.dbo.OxfordCurrentProviderInformation.MI, 
      Provider.dbo.OxfordCurrentProviderInformation.Degree,                    
      Provider.dbo.OxfordCurrentProviderInformation.Gender, 
      Provider.dbo.OxfordCurrentProviderInformation.DOB,                   
      Provider.dbo.OxfordCurrentProviderInformation.SSN, 
      Provider.dbo.OxfordCurrentProviderInformation.UPIN,                  
      Provider.dbo.OxfordCurrentProviderInformation.Medicare_Number, 
      Provider.dbo.OxfordCurrentProviderInformation.Medicaid,                  
      Provider.dbo.OxfordCurrentProviderInformation.TIN, 
      Provider.dbo.OxfordCurrentProviderInformation.TinOwner,                  
      Provider.dbo.OxfordCurrentProviderInformation.TIN_EffectiveDate, 
      Provider.dbo.OxfordCurrentProviderInformation.State_License_Number,          
      Provider.dbo.OxfordCurrentProviderInformation.State_License_Expiration_Date, 
      Provider.dbo.OxfordCurrentProviderInformation.PracticeAddress_1,             
      Provider.dbo.OxfordCurrentProviderInformation.PracticeAddress_2, 
      Provider.dbo.OxfordCurrentProviderInformation.PracticeCity,              
      Provider.dbo.OxfordCurrentProviderInformation.PracticeState, 
      Provider.dbo.OxfordCurrentProviderInformation.PracticeZip,               
      Provider.dbo.OxfordCurrentProviderInformation.PracticeCounty, 
      Provider.dbo.OxfordCurrentProviderInformation.Phone,                     
      Provider.dbo.OxfordCurrentProviderInformation.Fax, 
      Provider.dbo.OxfordCurrentProviderInformation.Credential_Date,               
      Provider.dbo.OxfordCurrentProviderInformation.MedicalSchool, 
      Provider.dbo.OxfordCurrentProviderInformation.MedicalSchoolDegree,           
      Provider.dbo.OxfordCurrentProviderInformation.CompletionDate, 
      Provider.dbo.OxfordCurrentProviderInformation.Clinic_Name,               
      Provider.dbo.OxfordCurrentProviderInformation.Payment_Name, 
      Provider.dbo.OxfordCurrentProviderInformation.Payment_Address_1,             
      Provider.dbo.OxfordCurrentProviderInformation.Payment_Address_2, 
      Provider.dbo.OxfordCurrentProviderInformation.Payment_City,              
      Provider.dbo.OxfordCurrentProviderInformation.Payment_State, 
      Provider.dbo.OxfordCurrentProviderInformation.Payment_Zip,               
      Provider.dbo.OxfordCurrentProviderInformation.Correspondence_Address_1, 
      Provider.dbo.OxfordCurrentProviderInformation.Correspondence_Address_2,          
      Provider.dbo.OxfordCurrentProviderInformation.Correspondence_City, 
      Provider.dbo.OxfordCurrentProviderInformation.Correspondence_State,          
      Provider.dbo.OxfordCurrentProviderInformation.Correspondence_Zip, 
      Provider.dbo.OxfordCurrentProviderInformation.Email,                     
      Provider.dbo.OxfordCurrentProviderInformation.ParticipationEffectiveDate, 
      Provider.dbo.OxfordCurrentProviderInformation.ParticipationTerminationDate,      
      Provider.dbo.OxfordCurrentProviderInformation.Malp_Specific_Limit, 
      Provider.dbo.OxfordCurrentProviderInformation.Malp_Aggregate_Limit,          
      Provider.dbo.OxfordCurrentProviderInformation.NameChanged, 
      Provider.dbo.OxfordCurrentProviderInformation.LocationTerminationDate,           
      Provider.dbo.OxfordCurrentProviderInformation.LocationEffectiveDate, 
      Provider.dbo.OxfordCurrentProviderInformation.EffectiveDateOf_All_Non_Tin_Changes,   
      Provider.dbo.OxfordCurrentProviderInformation.TerminationReason, 
      Provider.dbo.OxfordCurrentProviderInformation.MarketNumber,              
      Provider.dbo.OxfordCurrentProviderInformation.TIN_LocationFlag, 
      Provider.dbo.OxfordCurrentProviderInformation.Location_Tin_Flag,             
      Provider.dbo.OxfordCurrentProviderInformation.PrimaryLocation, 
      Provider.dbo.OxfordCurrentProviderInformation.MinContractEffDate,            
      Provider.dbo.OxfordCurrentProviderInformation.BillingAddressID,
      PCAT.TransferCode
    FROM Provider.dbo.OxfordCurrentProviderInformation
      INNER JOIN Provider.dbo.OxfordProviderChangeAddTerm AS PCAT ON Provider.dbo.OxfordCurrentProviderInformation.ProviderID = PCAT.ProviderID
    WHERE PCAT.TransferCode IN ('C' , 'A') AND
    Provider.dbo.OxfordCurrentProviderInformation.TIN IS NOT NULL;

    
    UPDATE OPT
    SET OPT.ParticipationTerminationDate = PC.ContTermDate, 
      OPT.TerminationReason = '31'
    FROM Provider.dbo.OxfordProviderTransfer OPT
      INNER JOIN Provider.dbo.T_ProviderContracts as PC ON OPT.ACNPROVID = PC.ACNPROVID
      AND PC.ClientID = OPT.ClientID
      AND PC.ProviderSpecialty = OPT.PracticeSpecialty
    WHERE OPT.TransferCode = 'T';

    
    INSERT INTO Provider.dbo.OxfordProviderTransfer(
      ACNPROVID,              
      ClientID, 
      PracticeSpecialty,              
      ProviderID, 
      Acn_Location_ID,                
      Last_Name, 
      First_Name,                 
      MI, 
      Degree,                     
      Gender, 
      DOB,                    
      SSN, 
      UPIN,                   
      Medicare_Number, 
      Medicaid,               
      TIN, 
      TinOwner,               
      TIN_EffectiveDate, 
      State_License_Number,           
      State_License_Expiration_Date, 
      PracticeAddress_1,              
      PracticeAddress_2, 
      PracticeCity,               
      PracticeState, 
      PracticeZip,                
      PracticeCounty, 
      Phone,                  
      Fax, 
      Credential_Date,                
      MedicalSchool, 
      MedicalSchoolDegree,            
      CompletionDate, 
      Clinic_Name,                
      Payment_Name, 
      Payment_Address_1,          
      Payment_Address_2, 
      Payment_City,               
      Payment_State, 
      Payment_Zip,                
      Correspondence_Address_1, 
      Correspondence_Address_2,           
      Correspondence_City, 
      Correspondence_State,           
      Correspondence_Zip, 
      Email,                  
      ParticipationEffectiveDate, 
      ParticipationTerminationDate,               
      Malp_Specific_Limit, 
      Malp_Aggregate_Limit,           
      NameChanged, 
      LocationTerminationDate,            
      LocationEffectiveDate, 
      EffectiveDateOf_All_Non_Tin_Changes,    
      TerminationReason, 
      MarketNumber,               
      TIN_LocationFlag,
      Location_Tin_Flag,              
      PrimaryLocation, 
      MinContractEffDate,             
      BillingAddressID,
      TransferCode,               
      TermedRecord)

    SELECT DISTINCT
      TT.ACNPROVID,               
      TT.ClientID, 
      TT.PracticeSpecialty,       
      TT.ProviderID, 
      TT.Acn_Location_ID,             
      TT.Last_Name, 
      TT.First_Name,              
      TT.MI, 
      TT.Degree,              
      TT.Gender, 
      TT.DOB,                     
      TT.SSN, 
      TT.UPIN,                
      TT.Medicare_Number, 
      TT.Medicaid,                
      TT.TIN, 
      TT.TinOwner,                
      TT.TIN_EffectiveDate, 
      TT.State_License_Number,            
      TT.State_License_Expiration_Date, 
      TT.PracticeAddress_1,           
      TT.PracticeAddress_2, 
      TT.PracticeCity,                
      TT.PracticeState, 
      TT.PracticeZip,                 
      TT.PracticeCounty, 
      TT.Phone,               
      TT.Fax, 
      TT.Credential_Date,             
      TT.MedicalSchool, 
      TT.MedicalSchoolDegree,             
      TT.CompletionDate, 
      TT.Clinic_Name,                 
      TT.Payment_Name, 
      TT.Payment_Address_1,           
      TT.Payment_Address_2, 
      TT.Payment_City,                
      TT.Payment_State, 
      TT.Payment_Zip,                 
      TT.Correspondence_Address_1, 
      TT.Correspondence_Address_2,        
      TT.Correspondence_City, 
      TT.Correspondence_State,            
      TT.Correspondence_Zip, 
      TT.Email,               
      TT.ParticipationEffectiveDate, 
      Null,                   
      TT.Malp_Specific_Limit, 
      TT.Malp_Aggregate_Limit,            
      TT.NameChanged, 
      TT.LocationTerminationDate,             
      TT.LocationEffectiveDate, 
      TT.EffectiveDateOf_All_Non_Tin_Changes,     
      TT.TerminationReason, 
      TT.MarketNumber,                
      NULL,
      NULL,                   
      TT.PrimaryLocation, 
      TT.MinContractEffDate,          
      NULL,
      'C',                    
      'Y'
    FROM #tempTinTermination AS TT WHERE TT.TIN IS NOT NULL ;

    
    SELECT DISTINCT
      OPPI.ACNPROVID,                 
      OPPI.ClientID, 
      OPPI.PracticeSpecialty,             
      OPPI.ProviderID, 
      OPPI.Acn_Location_ID,               
      OPPI.Last_Name, 
      OPPI.First_Name,                
      OPPI.MI, 
      OPPI.Degree,                    
      OPPI.Gender, 
      OPPI.DOB,                   
      OPPI.SSN, 
      OPPI.UPIN,                  
      OPPI.Medicare_Number, 
      OPPI.Medicaid,                  
      OPPI.TIN, 
      OPPI.TinOwner,                  
      OPPI.TIN_EffectiveDate, 
      OPPI.State_License_Number,          
      OPPI.State_License_Expiration_Date, 
      OPPI.PracticeAddress_1,             
      OPPI.PracticeAddress_2, 
      OPPI.PracticeCity,              
      OPPI.PracticeState, 
      OPPI.PracticeZip,               
      OPPI.PracticeCounty, 
      OPPI.Phone,                     
      OPPI.Fax, 
      OPPI.Credential_Date,               
      OPPI.MedicalSchool, 
      OPPI.MedicalSchoolDegree,           
      OPPI.CompletionDate, 
      OPPI.Clinic_Name,               
      OPPI.Payment_Name, 
      OPPI.Payment_Address_1,             
      OPPI.Payment_Address_2, 
      OPPI.Payment_City,              
      OPPI.Payment_State, 
      OPPI.Payment_Zip,               
      OPPI.Correspondence_Address_1, 
      OPPI.Correspondence_Address_2,          
      OPPI.Correspondence_City, 
      OPPI.Correspondence_State,          
      OPPI.Correspondence_Zip, 
      OPPI.Email,                     
      OPPI.ParticipationEffectiveDate, 
      --OPPI.ParticipationTerminationDate,
      OPPI.Malp_Specific_Limit, 
      OPPI.Malp_Aggregate_Limit,          
      OPPI.NameChanged, 
      OPPI.LocationTerminationDate,           
      OPPI.LocationEffectiveDate, 
      OPPI.EffectiveDateOf_All_Non_Tin_Changes,   
      OPPI.TerminationReason, 
      OPPI.MarketNumber,              
      --OPPI.TIN_LocationFlag, 
      --OPPI.Location_Tin_Flag,           
      OPPI.PrimaryLocation, 
      OPPI.MinContractEffDate,            
      OPPI.BillingAddressID,
      PCAT.TransferCode
    FROM Provider.dbo.OxfordPreviousProviderInformation AS OPPI
      INNER JOIN Provider.dbo.OxfordProviderChangeAddTerm AS PCAT ON OPPI.ProviderID = PCAT.ProviderID
    WHERE PCAT.TransferCode = 'T' AND
    OPPI.TIN IS NOT NULL ;

    UPDATE OPT
    SET OPT.ParticipationTerminationDate = PTP.TermDate, -- '02/17/2006'
      OPT.TerminationReason = '82'
    FROM Provider.dbo.OxfordProviderTransfer OPT
      INNER JOIN Provider.dbo.T_ProvLocTinPointer as PTP ON OPT.ProviderID = PTP.ProviderID
      AND OPT.Acn_Location_ID = PTP.OfficeLocationID
      AND OPT.TIN_EffectiveDate = PTP.EffDate
      AND OPT.TIN = PTP.TinNumber
    WHERE OPT.TermedRecord = 'Y' ;

    CREATE TABLE #Temp_TermedTin (
      ProviderID          CHAR( 6 )  NOT NULL,
      TIN                 VarChar (9),
      ParticipationTerminationDate  DATETIME  ) ;


    Insert INTO #Temp_TermedTin (
      ProviderID,     
      TIN,            
      ParticipationTerminationDate)
    Select Distinct
      ProviderID, 
      TIN,
      MIN(ParticipationTerminationDate) as ParticipationTerminationDate
    FROM  Provider.dbo.OxfordProviderTransfer
    WHERE TermedRecord = 'Y'
    AND NOT ParticipationTerminationDate IS NULL
    GROUP BY ProviderID,TIN ;

    UPDATE  OPT 
    SET   OPT.ParticipationTerminationDate = TT.ParticipationTerminationDate
    FROM    Provider.dbo.OxfordProviderTransfer AS OPT
          INNER   JOIN #Temp_TermedTin AS TT ON OPT.ProviderID = TT.ProviderID
          AND     OPT.TIN = TT.TIN
    WHERE TermedRecord = 'Y' ;

    
    UPDATE OPT
    SET OPT.ParticipationTerminationDate = Convert(varchar(10),GetDate(),101),
      OPT.TerminationReason = '82'
    FROM Provider.dbo.OxfordProviderTransfer OPT
    WHERE OPT.TermedRecord = 'Y'
    AND OPT.ParticipationTerminationDate IS NULL ;
    Drop Table #Temp_TermedTin ;

    
    CREATE TABLE #Temp_OxfordUniqTinToPull (
           ProviderID         CHAR( 6 )  NOT NULL,
           Acn_Location_ID    CHAR( 6 )  NOT NULL,
           TIN                CHAR( 9 )  Null) ;

    INSERT INTO #Temp_OxfordUniqTinToPull( 
      ProviderID, 
      Acn_Location_ID,
      TIN )
    SELECT    
      ProviderID, 
      MAX(Acn_Location_ID) AS Acn_Location_ID, 
      TIN
    FROM  OxfordProviderTransfer AS OPT
    WHERE       OPT.TermedRecord IS NULL OR  OPT.TermedRecord = ''
    GROUP BY    ProviderID, TIN ;


    UPDATE  OPT 
    SET   OPT.TIN_LocationFlag = '1',
          OPT.Location_TIN_Flag = '1'
    FROM    OxfordProviderTransfer AS OPT
          INNER   JOIN #Temp_OxfordUniqTinToPull AS OTTP ON OPT.ProviderID = OTTP.ProviderID
          AND     OPT.Acn_Location_ID = OTTP.Acn_Location_ID
          AND     OPT.TIN = OTTP.TIN ;

    DROP TABLE #Temp_OxfordUniqTinToPull ;

    
    CREATE TABLE #Temp_Min_ContractDate (
      ProviderID          CHAR( 6 )  NOT NULL,
      MinContEffDate      DATETIME NOT NULL) ;

    
    INSERT INTO #Temp_Min_ContractDate (ProviderID,MinContEffDate)                      
    SELECT      OPT.ProviderID, Min(OPT.ParticipationEffectiveDate) AS MinContEffDate
    FROM        Provider.dbo.OxfordProviderTransfer AS OPT
    GROUP BY    OPT.ProviderID ;

    
    UPDATE  OPT --OxfordProviderTransfer
    SET   OPT.MinContractEffDate = TMCD.MinContEffDate
    FROM    Provider.dbo.OxfordProviderTransfer AS OPT
    INNER Join #Temp_Min_ContractDate AS TMCD ON OPT.ProviderID = TMCD.ProviderID ;

    DROP TABLE #Temp_Min_ContractDate ;
    
    CREATE TABLE #Temp_UniqBillingID (
      ProviderID          CHAR( 6 )  NOT NULL,
      ACN_Location_ID     CHAR( 6 )  NOT NULL,
      TIN                 CHAR( 9 )  NOT NULL) ;

    INSERT INTO #Temp_UniqBillingID ( 
      ProviderID,
      Acn_Location_ID,
      TIN)
    SELECT    
      ProviderID,
      Acn_Location_ID, 
      TIN
    FROM    Provider.dbo.OxfordProviderTransfer AS OPT
    WHERE Tin_LocationFlag = '1' ;

    UPDATE    OPT
    SET   OPT.BillingAddressID = TUBID.Acn_Location_ID
    FROM    Provider.dbo.OxfordProviderTransfer AS OPT
          INNER JOIN #Temp_UniqBillingID AS TUBID ON OPT.ProviderID = TUBID.ProviderID
          AND OPT.TIN  = TUBID.TIN ;

    DROP TABLE #Temp_UniqBillingID ;
    
    UPDATE    OPT
    SET   OPT.PrimaryLocation = '0'
    FROM Provider.dbo.OxfordProviderTransfer AS OPT ;

    Create TABLE #Temp_UniqPrimaryLoc (
      ProviderID          CHAR( 6 )  NOT NULL,
      Acn_Location_ID     CHAR( 6 )  NOT NULL) ;

    INSERT INTO #Temp_UniqPrimaryLoc( ProviderID, Acn_Location_ID)
    SELECT     Distinct OPT.ProviderID, OPT.Acn_Location_ID
    FROM         OxfordProviderTransfer OPT
    INNER JOIN Provider.dbo.T_ProviderLocationPointer AS LP ON OPT.ProviderID = LP.ProviderID
    AND OPT.ACN_Location_ID = LP.OfficeLocationID
    Where (OPT.TermedRecord IS NULL or OPT.TermedRecord = '')
    AND LP.PrimaryLoc = 1 ;

    UPDATE    OPT
    SET   OPT.PrimaryLocation = '1'
    FROM OxfordProviderTransfer AS OPT
    INNER JOIN #Temp_UniqPrimaryLoc AS TUPL ON OPT.ProviderID = TUPL.ProviderID
    AND OPT.Acn_Location_ID  = TUPL.Acn_Location_ID 
    Where (OPT.TermedRecord IS NULL or OPT.TermedRecord = '') ;
    
    Select Distinct providerID, Tin
    INTO #tmpNoPrimary
    From OxfordProviderTransfer OPT
    Where PrimaryLocation = '0';
    --And (OPT.TermedRecord IS NULL or OPT.TermedRecord = '')

    --If one of the location atleast have a primary loc delete them
    Delete t From #tmpNoPrimary t
    Inner JOIN OxfordProviderTransfer o on t.ProviderID = o.providerID
    AND t.tin = o.tin 
    Where o.PrimaryLocation = '1';

    SELECT     Distinct OPT.ProviderID, MIN(Acn_Location_ID) AS Acn_Location_ID,OPT.Tin
    INTO #tmpPrimaryUpdater 
    FROM   OxfordProviderTransfer OPT 
    INNER JOIN #tmpNoPrimary as t on opt.ProviderID = t.ProviderID
    AND opt.tin = t.tin
    WHERE PrimaryLocation = '0'
    --Where OPT.TermedRecord IS NULL or OPT.TermedRecord = ''
    GROUP BY OPT.ProviderID,OPT.Tin order by OPT.providerID ;

    UPDATE    OPT
    SET   OPT.PrimaryLocation = '1'
    FROM OxfordProviderTransfer AS OPT
    INNER JOIN #tmpPrimaryUpdater AS t ON OPT.ProviderID = t.ProviderID
    AND OPT.Acn_Location_ID  = t.Acn_Location_ID 
    AND OPT.Tin = t.Tin;

    DROP TABLE #Temp_UniqPrimaryLoc;
    DROP TABLE #tmpNoPrimary;
    DROP TABLE #tmpPrimaryUpdater;
    

    CREATE TABLE ##Temp_ProviderRecords (
      RecordType          CHAR( 3 ) NOT NULL,
      ProviderId          CHAR( 6 ) NOT NULL,
      OfficeLocationId    CHAR( 6 ) NULL,
      TIN                 CHAR( 9 ) NULL,
      LocationType        CHAR( 4 ) NULL,
      PhoneType           CHAR( 1 ) NULL,
      FaxNumber           CHAR( 15 ) Null,
      Record              CHAR( 250 ) );
      
    INSERT INTO ##Temp_ProviderRecords
    SELECT    DISTINCT
      -- RecordType
      'PRV',
      -- ProviderId
      OPT.ProviderID,
      -- OfficeLocationId
      NULL,
      -- TIN
      NULL,
      -- LocationType
      NULL,
      -- PhoneType
      NULL,
      -- FaxNumber
      NULL,
      -- Record
      'PRV' +
      CONVERT( CHAR( 1 ), ISNULL( OPT.TransferCode, '' )) + --'A' +
      CONVERT( CHAR( 9 ), 'AC'+ OPT.ProviderID ) +
      SPACE( 12 ) +
      'IND' +
      CONVERT( CHAR( 40 ), ISNULL( OPT.Last_Name, '' ) ) +
      CONVERT( CHAR( 25 ), ISNULL( OPT.First_Name, '' ) ) +
      CONVERT( CHAR( 1 ), ISNULL( OPT.MI, '' )) +
      SPACE( 6 ) +  --Suffix
      CONVERT( CHAR( 9 ), '' ) +   ---Removed SSN as part of 70888 SPRF
      CONVERT( CHAR( 10 ), ISNULL( OPT.DOB, '' ), 101 ) +
      CONVERT( CHAR( 1 ), ISNULL( OPT.Gender, '' )) +
      CONVERT( CHAR(3),'DC') +  --Primary Degree
      SPACE( 12 ) + --UPIN
      SPACE( 15 ) + --Medicare Number
      SPACE( 13 ) + --Medicaid Number
      CONVERT( CHAR(2),'00') + --ProviderMedicatid Loc Cd
      SPACE( 3 ) + --Cred Status
      SPACE( 10 ) + --Cred Effective Date
      SPACE( 10 ) + --National Provider ID
      SPACE( 1 ) + -- Change Flag
      SPACE( 3 ) + -- Org Type
      SPACE( 58 ) -- Filler Space 
    FROM  OxfordProviderTransfer AS OPT;

    INSERT INTO ##Temp_ProviderRecords
    SELECT    DISTINCT
      -- RecordType
      'PSP',
      -- ProviderId
      OPT.ProviderID,
      -- OfficeLocationId
      NULL,
      -- TIN
      NULL,
      -- LocationType
      NULL,
      -- PhoneType
      NULL,
      -- FaxNumber
      NULL,
      -- Record
      'PSP' + -- Line Type
      CONVERT( CHAR( 9 ), 'AC'+ OPT.ProviderID) + --MPIN
      CONVERT( CHAR( 6 ), '035' ) + -- Provider Specialty Code
      SPACE( 1 ) + --Board Eligibility Certification Indicator 1
      SPACE( 10 ) + -- Board Code1
      SPACE( 10 ) + -- Certification Year 1
      SPACE( 10 ) + -- Expiration Certification Year 1
      SPACE( 10 ) + -- Recertification Year 1
      'P' + -- Primary/Secondary
      'Y' + -- Directory
      SPACE( 1 ) + -- Change Flag
      SPACE( 188 )-- Filler Space
    FROM  OxfordProviderTransfer AS OPT;

    INSERT INTO ##Temp_ProviderRecords
    SELECT    DISTINCT
      -- RecordType
      'PLI',
      -- ProviderId
      OPT.ProviderID,
      -- OfficeLocationId
      NULL,
      -- TIN
      NULL,
      -- LocationType
      NULL,
      -- PhoneType
      NULL,
      -- FaxNumberlo;
      NULL,
      -- Record
      'PLI' + -- Line Type
      CONVERT( CHAR( 9 ), 'AC'+ ISNULL(OPT.ProviderID,'') ) + -- MPIN
      CONVERT( CHAR( 2 ), ISNULL(OPT.PracticeState,'') ) + -- License Code
      CONVERT( CHAR( 10 ), ISNULL(OPT.State_License_Number,'') ) + --License Code (number)
      ISNULL( CONVERT( CHAR( 10 ), OPT.State_License_Expiration_Date, 101 ), SPACE( 10 )) + -- License Expiration Date
      CONVERT (CHAR(6),'STATE') + -- State/DCS/DEA License Indicator
      SPACE( 1 ) + -- Change Flag
      SPACE( 209 ) -- Filler Space
    FROM  OxfordProviderTransfer AS OPT;

    INSERT INTO ##Temp_ProviderRecords
    SELECT    DISTINCT
      -- RecordType
      'TIN',
      -- ProviderId
      OPT.ProviderID,
      -- OfficeLocationId
      NULL,--pull.OfficeLocationId,
      -- TIN
      OPT.TIN,
      -- LocationType
      'AAA',--NULL,
      -- PhoneType
      NULL,
      -- FaxNumber
      NULL,
      -- Record
      'TIN' + -- Line Type
      CONVERT( CHAR( 9 ), 'AC'+ OPT.ProviderID ) + -- MPIN
      CONVERT( CHAR( 9 ), OPT.TIN ) + -- Provdider FTIN
      CONVERT( CHAR( 9 ), OPT.TIN ) + -- Corp MPIN
      ISNULL( CONVERT( CHAR( 10 ), OPT.TIN_EffectiveDate, 101 ), SPACE( 10 )) + --Effective From Date
      CONVERT( CHAR( 40 ), OPT.Payment_Name ) + -- Tin Owner Last Name
      SPACE( 25 ) + -- Tin Owner First Name
      SPACE( 1 ) + -- Tin Owner Middle Initial
      SPACE( 1 ) + -- Change Flag
      SPACE( 143 ) -- Filler Space
    FROM  OxfordProviderTransfer AS OPT
    WHERE OPT.Tin_LocationFlag = '1' ;

    INSERT INTO ##Temp_ProviderRecords
    SELECT    DISTINCT
      -- RecordType
      'ADD',
      -- ProviderId
      OPT.ProviderID,
      -- OfficeLocationId
      OPT.Acn_Location_ID,
      -- TIN
      OPT.TIN,
      -- LocationType
      'PR  ',
      -- PhoneType
      NULL,
      -- FaxNumber
      NULL,
      -- Record
      'ADD' +
      CONVERT( CHAR( 9 ), 'AC'+ OPT.ProviderID) +
      --CONVERT(CHAR (12),LTRIM(STR(Provider.dbo.OxfordCurrentProviderInformation.Acn_Location_ID) + '000000')) +
      CONVERT(CHAR (12), '000000' + LTRIM(STR(OPT.Acn_Location_ID))) +
      CONVERT( CHAR( 32 ), ISNULL( OPT.PracticeAddress_1, '' ) + ' ' + ISNULL(OPT.PracticeAddress_2, '' ) ) +
      CONVERT( CHAR( 28 ), OPT.PracticeCity ) +
      CONVERT( CHAR( 2 ), OPT.PracticeState ) +
      CONVERT( CHAR( 5 ), OPT.PracticeZip ) +
      SPACE( 4 ) +
      SPACE( 25 ) + --CONVERT( CHAR( 25 ), oli.County ) +
      SPACE( 1 ) +
      SPACE( 3 ) +
      SPACE( 3 ) +
      'PR  ' +
      CASE OPT.PrimaryLocation WHEN 1 THEN 'Y' ELSE 'N' END +
      CASE OPT.PrimaryLocation WHEN 1 THEN 'P' ELSE 'S' END +
      CONVERT(CHAR (12), '000000' + LTRIM(STR(OPT.BillingAddressID))) +
      SPACE( 1 ) +
      SPACE( 104 )
    FROM  OxfordProviderTransfer AS OPT;

    -- 4 records for each address ( single address record provides 2 records - 1 for Billing and one for Practice location
    --    Billing Voice
    --    Billing Fax
    --    Practice Voice
    --    Practice Fax
    INSERT INTO ##Temp_ProviderRecords  --Bill phone
    SELECT    DISTINCT
      -- RecordType
      'PHO',
      -- ProviderId
      OPT.ProviderID,
      -- OfficeLocationId
      OPT.Acn_Location_ID,
      -- TIN
      OPT.TIN,
      -- LocationType
      'BILL',
      -- PhoneType
      'V',
      -- FaxNumber
      NULL,
      -- Record
      'PHO' +
      CONVERT( CHAR( 9 ),'AC'+ LEFT( OPT.ProviderID, 6 ) ) +
      CONVERT( CHAR( 7 ), ISNULL( SUBSTRING( OPT.Phone, 4, 7 ), SPACE( 7 ) ) ) +
      CONVERT( CHAR( 3 ), ISNULL( LEFT( OPT.Phone, 3 ), SPACE( 3 ) ) ) +
      'V' +
      'Y' +
      SPACE( 1 ) +
      SPACE( 225 )
    FROM  OxfordProviderTransfer AS OPT
    WHERE OPT.Location_Tin_Flag = '1'  

    UNION

    --INSERT INTO ##Temp_ProviderRecords  -- Bill Fax
    SELECT    DISTINCT
      -- RecordType
      'PHO',
      -- ProviderId
      OPT.ProviderID,
      -- OfficeLocationId
      OPT.Acn_Location_ID,
      -- TIN
      OPT.TIN,
      -- LocationType
      'BILL',
      -- PhoneType
      'F',
      -- FaxNumber
      OPT.Fax,
      -- Record
      'PHO' + -- Line Type
      CONVERT( CHAR( 9 ),'AC'+ LEFT( OPT.ProviderID, 6 ) ) + -- MPIN
      CONVERT( CHAR( 7 ), ISNULL( SUBSTRING( OPT.Fax, 4, 7 ), SPACE( 7 ) ) ) + -- Fax Number
      CONVERT( CHAR( 3 ), ISNULL( LEFT( OPT.Fax, 3 ), SPACE( 3 ) ) ) + -- Area Code
      'F' + -- Type Voice/Fax
      'Y' + -- Provier Primary Place of Service Directory Indicator
      SPACE( 1 ) + -- Change Flag
      SPACE( 225 ) -- Filler Space
    FROM  OxfordProviderTransfer AS OPT
    WHERE OPT.Location_Tin_Flag = '1' 
    --AND (NOT(Provider.dbo.OxfordCurrentProviderInformation.Fax Is NULL)) OR Provider.dbo.OxfordCurrentProviderInformation.Fax <> ''

    UNION

    --INSERT INTO ##Temp_ProviderRecords  -- Practice PHONE
    SELECT    DISTINCT
      -- RecordType
      'PHO',
      -- ProviderId
      OPT.ProviderID,
      -- OfficeLocationId
      OPT.Acn_Location_ID,
      -- TIN
      OPT.TIN,
      -- LocationType
      'PR',
      -- PhoneType
      'V',
      -- FaxNumber
      NULL,
      -- Record
      'PHO' + --Line Type
      CONVERT( CHAR( 9 ),'AC'+ LEFT( OPT.ProviderID, 6 ) ) + -- MPIN
      CONVERT( CHAR( 7 ), ISNULL( SUBSTRING( OPT.Phone, 4, 7 ), SPACE( 7 ) ) ) + --Phone Number
      CONVERT( CHAR( 3 ), ISNULL( LEFT( OPT.Phone, 3 ), SPACE( 3 ) ) ) + -- Area Code
      'V' + -- Tyupe Voice/Fax
      'Y' + -- Provider Primary Place of Service Directory Indicator
      SPACE( 1 ) + -- Change Flag
      SPACE( 225 )-- Filler Space
    FROM  OxfordProviderTransfer AS OPT 

    UNION

    --INSERT INTO ##Temp_ProviderRecords  -- Practice Fax
    SELECT    DISTINCT
      -- RecordType
      'PHO',
      -- ProviderId
      OPT.ProviderID,
      -- OfficeLocationId
      OPT.Acn_Location_ID,
      -- TIN
      OPT.TIN,
      -- LocationType
      'PR',
      -- PhoneType
      'F',
      -- FaxNumber
      OPT.Fax,
      -- Record
      'PHO' +
      CONVERT( CHAR( 9 ),'AC'+ LEFT( OPT.ProviderID, 6 ) ) +
      CONVERT( CHAR( 7 ), ISNULL( SUBSTRING( OPT.Fax, 4, 7 ), SPACE( 7 ) ) ) +
      CONVERT( CHAR( 3 ), ISNULL( LEFT( OPT.Fax, 3 ), SPACE( 3 ) ) ) +
      'F' +
      'Y' +
      SPACE( 1 ) +
      SPACE( 225 )
    FROM  OxfordProviderTransfer AS OPT;

    -- Delete PHO lines without Fax number
    DELETE temp
    From ##Temp_ProviderRecords temp
    Where temp.RecordType = 'PHO'
    AND temp.PhoneType = 'F'
    AND temp.FaxNumber is null or temp.faxNumber ='';

    --UPDATE THE Market number for non primary locations with the same market number as the primary

    CREATE TABLE #Temp_Market (
      ProviderID          VARCHAR( 6 )  NOT NULL,
      MarketNumber        VARCHAR (12)  );

    -- UPDATE Temp Table Created above with Min Cont Date
    INSERT INTO #Temp_Market (ProviderID,MarketNumber)
    --SELECT Provider.dbo.OxfordCurrentProviderInformation.ProviderID, Min(Convert(varchar(10),Provider.dbo.OxfordCurrentProviderInformation.ParticipationEffectiveDate,101)) AS MinContEffDate
    SELECT      Distinct OPT.ProviderID, OPT.MarketNumber
    FROM        OxfordProviderTransfer AS OPT
    WHERE OPT.PrimaryLocation = '1';

    -- Update OxfordCurrentProviderInformation table with MinContEffDate
    UPDATE  OPT --OxfordProviderTransfer
    SET   OPT.MarketNumber = Right('0000000' + MRK.MarketNumber, 7)  --RIGHT( '0000000' + OPT.MarketNumber, 7 )
    FROM    OxfordProviderTransfer AS OPT
    INNER Join #Temp_Market AS MRK ON OPT.ProviderID = MRK.ProviderID;

    INSERT INTO ##Temp_ProviderRecords
    SELECT    DISTINCT
      -- RecordType
      'CON',
      -- ProviderId
      OPT.ProviderID,
      -- OfficeLocationId
      NULL,--pull.OfficeLocationId,
      -- TIN
      OPT.TIN,--'999999999',--tp.TinNumber,
      -- LocationType
      Null,
      -- PhoneType
      Null,
      -- FaxNumber
      NULL,
      -- Record
      'CON' + -- Line Type
      CONVERT( CHAR( 9 ),'AC'+ LEFT( OPT.ProviderID, 6 ) ) + -- MPIN
      'S' + -- Provider Contract PCP Indicator
      SPACE (5) + -- Provider Contract Schedule #
      ISNULL( CONVERT( CHAR( 10 ), OPT.MinContractEffDate, 101 ), SPACE( 10 )) + -- Contract Effective Date
      'AC' + -- Product code
      SPACE (1) + -- Panel Status
      '522' + -- IPA Number
      OPT.MarketNumber + -- Market Number
      --RIGHT( '0000000' + OPT.MarketNumber, 7 )+ -- Market Number
      'Y' + -- H/Y Line indicator
      CONVERT( CHAR( 6 ), ISNULL( OPT.TerminationReason, '' ) ) + -- TerminationReason
      --CONVERT(CHAR(10),'12/31/9999')+
      ISNULL( CONVERT(CHAR(10),OPT.ParticipationTerminationDate,101),CONVERT(CHAR(10),'12/31/9999'))+ -- Provider Term Reason Cancel Date
      SPACE( 1 ) + -- Change Flag
      'C' + -- Payment Method Code
      SPACE( 190 ) -- Filler Space
    FROM  OxfordProviderTransfer AS OPT;

    DECLARE @ProviderCount    INTEGER;
    DECLARE @RecordCount  INTEGER;

    -- Get count of PRV records - There will be one for each provider
    SET @ProviderCount = (SELECT COUNT(*)
              FROM ##Temp_ProviderRecords
              WHERE RecordType = 'PRV' );

    -- Get count of all records, then add 1 for the header we are going to add
    SET @RecordCount = (  SELECT COUNT(*)
              FROM ##Temp_ProviderRecords ) + 1 ;

    INSERT INTO ##Temp_ProviderRecords
    SELECT    -- RecordType
      'HDR',
      -- ProviderId
      REPLICATE( '0', 6 ),
      -- OfficeLocationId
      NULL,
      -- TIN
      NULL,
      -- LocationType
      NULL,
      -- PhoneType
      NULL,
      -- FaxNumber
      NULL,
      -- Record
      'HDR' +
      'ACN' +
      RIGHT( '000000' + CONVERT( VARCHAR, @ProviderCount ), 6 )  +
      CONVERT(CHAR( 10 ), GETDATE(), 101) + ' ' + CONVERT(CHAR( 8 ), GETDATE(), 108)+
      RIGHT( '0000000' + CONVERT( VARCHAR, @RecordCount ), 7 )  +
      SPACE( 212 );
      
      
    
    SELECT Record 
    FROM ##Temp_ProviderRecords 
    ORDER BY providerId, 
      TIN, 
      CASE LocationType
          WHEN 'AAA' THEN 1
          WHEN 'BILL' THEN 2
          WHEN 'PR' THEN 3
          ELSE 4
          END,
      OfficeLocationId,
      CASE RecordType
          WHEN 'PRV' THEN 1
          WHEN 'PSP' THEN 2
          WHEN 'PLI' THEN 3
          WHEN 'TIN' THEN 4
          WHEN 'ADD' THEN 5
          WHEN 'PHO' THEN 6
          WHEN 'CON' THEN 7
          END,
      CASE PhoneType
          WHEN 'V' THEN 1
          WHEN 'F' THEN 2
          END;
