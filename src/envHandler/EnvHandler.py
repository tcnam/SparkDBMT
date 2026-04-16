from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import Any, Dict, List, Union
from pathlib import Path
# from abc import ABC, abstractmethod
from logging import Logger


from src.const import const
from src.configLoader.ConfigFactory import createConfigLoader
from src.configLoader.ConfigLoader import ConfigLoader

# from utils.utils import getHostIp
# from utils.CustomLogger import CustomLogger


# logger: Logger = CustomLogger(name=__name__).getLogger()


class EnvHandler():
    def __init__(self, **kwargs):
        self.batchDate: str = kwargs.get("batchDate", "")
        self.projectName: str = kwargs.get("projectName", "")
        self.envName: str = kwargs.get("envName", "")
        self.etcConfigLoader: ConfigLoader = kwargs.get("etcConfigLoader", None)
        self.profileConfigLoader: ConfigLoader = kwargs.get("profileConfigLoader", None)
        self.profileAuthConfigLoader: ConfigLoader = kwargs.get("profileAuthConfigLoader", None)
        self.projectConfigLoader: ConfigLoader = kwargs.get("projectConfigLoader", None)
        self.modelConfigLoader: ConfigLoader = kwargs.get("modelConfigLoader", None)
        self.paramConfigLoader: ConfigLoader = kwargs.get("paramConfigLoader", None)
    
    def createJdbcUrl(self, **kwargs) -> str:
        jdbcType: str = kwargs.get("jdbcType", "")
        host: str = kwargs.get("host", "")
        port: int = kwargs.get("port", None)
        database: str = kwargs.get("database", "")

        if str.lower(jdbcType) == "hive":
            return f"jdbc:hive2://{host}:{port}/{database}"
        elif str.lower(jdbcType) == "sqlserver":
            return f"jdbc:sqlserver://{host}:{port};databaseName={database};encrypt=true;trustServerCertificate=true"
        elif str.lower(jdbcType) == "postgresql":
            return f"jdbc:postgresql://{host}:{port}/{database}"
        else:
            return ""
    
    def getGeneralVars(self) -> Dict[str, str]:
        jarPath: List[Union[str, Path]] = [p.as_posix() for p in const.jarDir.iterdir() if p.is_file()]
        envVars: Dict[str, str] = {
            "jarPath": jarPath
        }   
        return envVars

    def getBatchDateVars(self) -> Dict[str, str]:
        # 1. INITIAL CONVERSION
        baseDt = datetime.strptime(self.batchDate, '%Y%m%d')

        # 2. CALCULATE RESULTS FIRST (as datetime objects)
        yestDt = baseDt - timedelta(days=1)
        nextDt = baseDt + timedelta(days=1)
        
        # Months
        prevMonthDt = baseDt - relativedelta(months=1)
        nextMonthDt = baseDt + relativedelta(months=1)
        
        # Years
        prevYearDt = baseDt - relativedelta(years=1)
        nextYearDt = baseDt + relativedelta(years=1)

        # Current Real-Time Timestamp (for etl_timestamp)
        now = datetime.now()

        # 3. CONSTRUCT DICTIONARY AFTER CALCULATIONS
        batch_vars = {
            # Dates (yyyyMMdd)
            "batchDate": self.batchDate,
            # Formats to yyyy-mm-dd hh:mm:ss.sss
            "batchTimestamp": now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            "yesterday": yestDt.strftime('%Y%m%d'),
            "nextday": nextDt.strftime('%Y%m%d'),
            
            # Month Components (MM)
            "month": baseDt.strftime('%m'),
            "prevMonth": prevMonthDt.strftime('%m'),
            "nextMonth": nextMonthDt.strftime('%m'),
            
            # Year Components (yyyy)
            "year": baseDt.strftime('%Y'),
            "prevYear": prevYearDt.strftime('%Y'),
            "nextYear": nextYearDt.strftime('%Y'),
            
            # YearMonth Components (yyyyMM)
            "yearmonth": baseDt.strftime('%Y%m'),
            "prevYearmonth": prevMonthDt.strftime('%Y%m'),
            "nextYearmonth": nextMonthDt.strftime('%Y%m')
        }

        return batch_vars

    def getProfileVars(self) -> Dict[str, str]:
        profileConfig: Dict[str, Dict[Union[str, Dict]]] = self.profileConfigLoader.config.get(self.projectName)
        target: str = profileConfig.get("target")
        envVars: Dict[str, str] = {
            "target": target
            ,"envName": self.envName
            ,**profileConfig.get("outputs").get(target).get(self.envName)
        }
        if target == "thrift":
            jdbcUrl: str = self.createJdbcUrl(
                jdbcType = envVars.get("jdbcType")
                ,host = envVars.get("host")
                ,port = envVars.get("port")
                ,database = envVars.get("database")
            )
            envVars["jdbcUrl"] = jdbcUrl
        
        return envVars
    
    def getProfileAuthVars(self) -> Dict[str, str]:
        profileAuthConfig: Dict[str, Dict[Union[str, Dict]]] = self.profileAuthConfigLoader.config.get(self.projectName)
        envVars: Dict[str, str] = {
            **profileAuthConfig.get(self.envName)
        }
        return envVars

    def getModelVars(self) -> Dict[str, str]:
        modelFilePath: Path = self.modelConfigLoader.modelFilePath
        schemaFilePath: Path = self.modelConfigLoader.filePath
        envVars: Dict[str, str] = {
            "modelFilePath": modelFilePath
            ,"schemaFilePath": schemaFilePath
        }
        return envVars

    def getEnvVars(self) -> Dict[str, Union[str, int]]:
        envVars: Dict[str, Union[str, int]] = {}
        generalVars: Dict[str, str] = self.getGeneralVars()
        batchDateVars: Dict[str, str] = self.getBatchDateVars()
        profileVars: Dict[str, str] = self.getProfileVars()
        profileAuthVars: Dict[str, str] = self.getProfileAuthVars()
        modelVars: Dict[str, str] = self.getModelVars()
        envVars = {
            **generalVars
            ,**batchDateVars
            ,**profileVars
            ,**profileAuthVars
            ,**modelVars
        }
        return envVars

    def getParamVal(self, envVars: Dict[str, Union[str, int]]) -> Dict[str, Union[str, int]]:
        paramDict: Dict[str, str] = self.paramConfigLoader.config
        result: Dict[str, str] = {}
        for key in paramDict:
            result[key] = envVars[paramDict.get(key)]
        return result
            
