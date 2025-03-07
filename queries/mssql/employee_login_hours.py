get_employee_login_hours = """
SELECT 
    EmpIdN, 
    AttdDateD, 
    NWHN 
FROM 
    EmpDailyAttd 
WHERE 
    EmpIdN IN ({})  -- Dynamically insert placeholders for multiple IDs
    AND AttdDateD = %s;  -- Filter by date
"""
