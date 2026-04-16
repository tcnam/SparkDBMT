from pyspark.sql import SparkSession
from typing import (Dict, Any, Union, List)

from src.const import const
from src.utils.utils import parseInpput
from src.configLoader.ConfigFactory import createConfigLoader
from src.configLoader.ConfigLoader import ConfigLoader
from src.envHandler.EnvHandler import EnvHandler
from src.sparkHandler.SparkConn import SparkConn


def main():
    parsedInfo: Dict[str, Union[str, List[str]]] = parseInpput(batchDate="20251205", modelInfo="datalake_raw_regions_spark")
    etcConfigLoader: ConfigLoader = createConfigLoader(
        type = "etc"
        ,fileName = "profile_info.yaml"
    )
    projectConigLoader: ConfigLoader = createConfigLoader(
        type = "project"
        ,relFolderPath = etcConfigLoader.config.get(parsedInfo.get("projectName")).get("path")
        ,fileName = "project.yaml"
    )
    profileConfigLoader: ConfigLoader = createConfigLoader(
        type = "project"
        ,relFolderPath = etcConfigLoader.config.get(parsedInfo.get("projectName")).get("path")
        ,fileName = "profile.yaml"
    )
    profileAuthConfigLoader: ConfigLoader = createConfigLoader(
        type = "project"
        ,relFolderPath = etcConfigLoader.config.get(parsedInfo.get("projectName")).get("path")
        ,fileName = "profile_auth.yaml"
    )
    modelConfigLoader: ConfigLoader = createConfigLoader(
        type = "model"
        ,relFolderPath = f'{etcConfigLoader.config.get(parsedInfo.get("projectName")).get("path")}/model/{parsedInfo.get("layer")}'
        ,fileName = parsedInfo.get("fileName")
    )
    paramConfigLoader: ConfigLoader = createConfigLoader(
        type = "etc"
        ,fileName = "params.yaml"
    )
    
    envHandler: EnvHandler = EnvHandler(
        batchDate = parsedInfo.get("batchDate")
        ,projectName = parsedInfo.get("projectName")
        ,envName = "local"
        ,etcConfigLoader = etcConfigLoader
        ,profileConfigLoader = profileConfigLoader
        ,profileAuthConfigLoader = profileAuthConfigLoader
        ,projectConigLoader = projectConigLoader
        ,modelConfigLoader = modelConfigLoader
        ,paramConfigLoader = paramConfigLoader
    )
    envVars: Dict[str, Union[str, int]] = envHandler.getEnvVars()
    paramVars: Dict[str, Union[str, int]] = envHandler.getParamVal(envVars=envVars)
    print(envVars)
    print(paramVars)
    
    sparkConn: SparkConn = SparkConn(
        envVars = envVars
        ,paramVars = paramVars
    )
    sparkConn.createConn()
    sparkConn.execute(queryFilePath=envVars.get("modelFilePath"))
    sparkConn.testConn()
    sparkConn.closeConn()


if __name__ == "__main__":
    main()