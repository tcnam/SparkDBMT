from typing import (
    Dict, List, Union
)


def parseInpput(**kwargs) -> Dict[str, Union[str, List[str]]]:
    batchDate: str = kwargs.get("batchDate", None)
    modelInfo: str = kwargs.get("modelInfo", None)
    result: Dict[str, str] = {}
    
    if batchDate and modelInfo:
        elements: List[str] = modelInfo.split("_")
        projectName: str = elements[0]
        pathElement: List[str] = elements[1:]
        layer: str = elements[1]
        fileName: str = modelInfo[len(projectName)+1:]
        modelFileName: str = f"{fileName}.sql"
        schemaFileName: str = f"{fileName}.yaml"
        
        result["modelInfo"] = modelInfo
        result["batchDate"] = batchDate
        result["projectName"] = projectName
        result["pathElement"] = pathElement
        result["layer"] = layer
        result["fileName"] = fileName
        result["modelFileName"] = modelFileName
        result["schemaFileName"] = schemaFileName
    
    return result