get_employee_login_hours = """
SELECT 
    EM.EmpIdN, 
    EM.EmpNameC, 
    EDA.AttdDateD, 
    EDA.NWHN
FROM EmpMaster EM
INNER JOIN EmpDailyAttd EDA 
    ON EM.EmpIdN = EDA.EmpIdN
WHERE EDA.AttdDateD = %s
ORDER BY EDA.AttdDateD DESC;
"""
