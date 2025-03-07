match_employee_id_query = """
SELECT "EmpIdMapping_PGEmpID" 
FROM public."biometric_employeeidmapping" 
WHERE "EmpIdMapping_MSSQLEmpID" = %s
"""
