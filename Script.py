info_list  = [("LoadNum", 0
),("RecordType", 1
),("TPACarrierID", 10),(
"PHSCompanyNum", 2
),("SubscriberNum", 7
),("SubscriberSuffix", 2
),("LastName", 20),(
"MiddleInitial", 1
),("FirstName", 15),(
"Street", 30),(
"City", 20),(
"State", 2
),("ZipCode", 9
),("PhoneNum", 10),(
"SSN", 9
),("Sex", 1
),("DateOfBirth", 0
),("CoverPlan", 3
),("BenefitCd", 3
),("BenefitStartDate", 0
),("BenefitEndDate", 0
),("CopayAmt", 0
),("CopayType", 1
),("EmpGroup", 6
),("ProvGroup", 6
),("ProvFacility", 4
),("SpecialPlanCd", 5
),("ErisaInd", 1
),("Filler", 19),(
"CoverPlan2", 3
),("BenefitCd2", 3
),("BenefitStartDate2", 0
),("BenefitEndDate2", 0
),("CopayAmt2", 0
),("CopayType2", 1
),("EmpGroup2", 6
),("ProvGroup2", 6
),("ProvFacility2", 4
),("SpecialPlanCd2", 5
),("ERISAInd2", 1
),("Filler2", 19),(
"CoverPlan3", 3
),("BenefitCd3", 3
),("BenefitStartDate3", 0
),("BenefitEndDate3", 0
),("CopayAmt3", 0
),("CopayType3", 1
),("EmpGroup3", 6
),("ProvGroup3", 6
),("ProvFacility3", 4
),("SpecialPlanCd3",5), ("ERISAInd3", 1),("Filler3", 19),(
"CoverPlan4", 3
),("BenefitCd4", 3
),("BenefitStartDate4",0),("BenefitEndDate4", 0),("CopayAmt4", 0),("CopayType4", 1
),("EmpGroup4", 6
),("ProvGroup4", 6
),("ProvFacility4", 4
),("SpecialPlanCd4", 0
),("ERISAInd4", 1
),("Filler4", 19),(
"CoverPlan5", 3
),("BenefitCd5", 3
),("BenefitStartDate5", 0
),("BenefitEndDate5", 0
),("CopayAmt5", 0
),("CopayType5", 1
),("EmpGroup5", 6
),("ProvGroup5", 6
),("ProvFacility5", 4
),("SpecialPlanCd5", 5
),("ERISAInd5", 1
),("Filler5", 19),(
"CoverPlan6", 3
),("BenefitCd6", 3
),("BenefitStartDate6", 0
),("BenefitEndDate6", 0
),("CopayAmt6", 0
),("CopayType6", 1
),("EmpGroup6", 6
),("ProvGroup6", 6
),("ProvFacility6", 4
),("SpecialPlanCd6", 5
),("ERISAInd6", 1
),("Filler6", 19),(
"PrimarySubscriberNum", 7
),("PrimarySubscriberSuffix", 2
),("DependentCd", 2
),("MemberStatus", 1
),("RelCd", 2
),("UserDefined", 4)]

final_list = []
start = 1
for entry in info_list:
    print("df.value.substr(",start,",",entry[1],").alias('",entry[0],"'),")
    start = start+entry[1]
