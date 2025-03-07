get_employee_details_query = """
SELECT 
    json_build_object(
        'employee_id', ue.id,
        'employee_name', ue.employee_name,
        'EmpIdMapping_PGEmpID', bem."EmpIdMapping_PGEmpID",
        'EmpIdMapping_MSSQLEmpID', bem."EmpIdMapping_MSSQLEmpID"
    ) AS employee_data
FROM 
    public."User_employee" ue
INNER JOIN 
    public.biometric_employeeidmapping bem
ON 
    ue.id = bem."EmpIdMapping_PGEmpID"
WHERE 
    ue.is_active = TRUE
    AND bem."EmpIdMapping_MSSQLEmpID" IS NOT NULL;
"""
